//! Co-hosting utilities for [`grpc-gw`].
//!
//! When you run the dynamic gateway in the **same process** as your real gRPC
//! server, you don't want the gateway's transcoded backend call to leave the
//! process: a loopback TCP hop just to reach a service living in your own
//! address space is wasteful (kernel socket buffers, an extra TLS/h2 handshake,
//! a second trip through the accept loop).
//!
//! [`in_process_channel`] removes that hop. It returns a tonic [`Channel`]
//! wired to an in-memory [`tokio::io::duplex`] transport, paired with the
//! [`Incoming`] connection stream that **you** serve with your gRPC service.
//! Plug the channel into the gateway's backend with
//! `grpc_gw::GrpcClient::with_channel(channel)` (or use it directly with a
//! generated tonic client) and the gRPC call rides an in-process pipe — only
//! HTTP/2 framing and protobuf encode/decode, no OS socket.
//!
//! You decide how to run the service: `Incoming` implements [`Stream`] (so it
//! feeds `tonic::transport::Server::serve_with_incoming`), or you can drive it
//! manually with [`Incoming::accept`]. For the raw client connector instead of
//! a `Channel`, use [`in_process_transport`], which returns
//! `(Incoming, Connector)`.
//!
//! ```no_run
//! # async fn demo() {
//! let (channel, incoming) = grpc_gw_util::in_process_channel();
//!
//! // Caller owns serving. `incoming` is a `Stream` of connections, so serve
//! // your gRPC service over it with tonic's own server:
//! //
//! //   let server = tokio::spawn(async move {
//! //       tonic::transport::Server::builder()
//! //           .add_service(my_server)
//! //           .serve_with_incoming(incoming)
//! //           .await
//! //   });
//! //   // ... `server.abort()` when done.
//!
//! // let backend = grpc_gw::GrpcClient::with_channel(channel);
//! // ... build the gateway with `backend` ...
//! # let _ = (channel, incoming);
//! # }
//! ```

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use http::Uri;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::mpsc;
use tonic::transport::server::Connected;
use tonic::transport::{Channel, Endpoint};

/// Size of each in-memory duplex pipe's buffer, in bytes.
const DUPLEX_BUF: usize = 64 * 1024;

/// The server side of one in-memory connection produced by [`Incoming`].
///
/// Wraps a [`tokio::io::DuplexStream`] and implements [`AsyncRead`] +
/// [`AsyncWrite`] plus tonic's [`Connected`], so an [`Incoming`] stream can be
/// fed straight to `tonic::transport::Server::serve_with_incoming`, or served
/// manually over hyper by wrapping it in [`TokioIo`].
#[derive(Debug)]
pub struct ServerIo(DuplexStream);

impl AsyncRead for ServerIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl AsyncWrite for ServerIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

impl Connected for ServerIo {
    type ConnectInfo = ();
    fn connect_info(&self) -> Self::ConnectInfo {}
}

/// The server side of the in-memory transport: a stream of inbound connections.
///
/// Each item is the server end of a fresh in-memory pipe created when the
/// paired [`Connector`] dials. `Incoming` implements [`Stream`] of
/// `io::Result<`[`ServerIo`]`>`, so it feeds
/// `tonic::transport::Server::serve_with_incoming`; for manual serving use
/// [`accept`](Incoming::accept).
pub struct Incoming {
    rx: mpsc::UnboundedReceiver<ServerIo>,
}

impl Incoming {
    /// Await the next inbound connection, or `None` once every [`Connector`]
    /// clone has been dropped.
    pub async fn accept(&mut self) -> Option<ServerIo> {
        self.rx.recv().await
    }
}

impl Stream for Incoming {
    type Item = io::Result<ServerIo>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx).map(|opt| opt.map(Ok))
    }
}

/// The client side of the in-memory transport: a [`tower::Service`] over [`Uri`]
/// that dials the paired [`Incoming`] stream.
///
/// Each call creates a fresh in-memory pipe, hands the server half to the
/// paired [`Incoming`] stream, and yields the client half. It is `Clone` and
/// plugs directly into
/// [`Endpoint::connect_with_connector`](tonic::transport::Endpoint::connect_with_connector)
/// (and the `_lazy` variant). The requested `Uri` is ignored.
#[derive(Clone)]
pub struct Connector {
    tx: mpsc::UnboundedSender<ServerIo>,
}

impl tower::Service<Uri> for Connector {
    type Response = TokioIo<DuplexStream>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: Uri) -> Self::Future {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (client_io, server_io) = tokio::io::duplex(DUPLEX_BUF);
            tx.send(ServerIo(server_io))
                .map_err(|_| io::Error::other("in-process server stopped accepting"))?;
            Ok(TokioIo::new(client_io))
        })
    }
}

/// Create a paired in-memory transport: an [`Incoming`] connection stream
/// (server side) and a [`Connector`] (client side).
///
/// This is the low-level building block behind [`in_process_channel`], exposed
/// so you can wire the two halves into the familiar server/client APIs
/// yourself:
///
/// - feed `incoming` to `tonic::transport::Server::serve_with_incoming` (it
///   implements [`Stream`]), and
/// - feed `connector` to
///   [`Endpoint::connect_with_connector`](tonic::transport::Endpoint::connect_with_connector).
///
/// Connections only exist once `connector` dials; the server half of each new
/// pipe then arrives on `incoming`. Dropping every `Connector` clone ends the
/// `incoming` stream.
pub fn in_process_transport() -> (Incoming, Connector) {
    let (tx, rx) = mpsc::unbounded_channel();
    (Incoming { rx }, Connector { tx })
}

/// Return a tonic [`Channel`] that reaches an in-memory transport (no OS
/// socket), paired with the [`Incoming`] connection stream the caller serves
/// however it likes.
///
/// This is a convenience over [`in_process_transport`] for the common case
/// where you want a ready-to-use client `Channel` rather than the raw
/// [`Connector`]. The caller owns serving: feed `incoming` to
/// `tonic::transport::Server::serve_with_incoming` (it implements [`Stream`])
/// or a manual accept loop ([`Incoming::accept`]) — and decides how to mount
/// the service and when to stop (drop the serving task / its
/// [`JoinHandle`](tokio::task::JoinHandle)).
///
/// The channel is lazy and reconnectable: it dials on first use, and if the
/// underlying connection is lost it dials again — each dial delivers a fresh
/// connection on `incoming`. A single HTTP/2 connection multiplexes many
/// concurrent RPCs, so one channel can be shared by several clients (e.g. a
/// native tonic client and the gateway's backend hop) at once.
pub fn in_process_channel() -> (Channel, Incoming) {
    let (incoming, connector) = in_process_transport();

    // The authority is irrelevant (the custom connector ignores it); it only
    // populates the outgoing `:authority` header.
    let channel =
        Endpoint::from_static("http://grpc-gw.invalid").connect_with_connector_lazy(connector);

    (channel, incoming)
}
