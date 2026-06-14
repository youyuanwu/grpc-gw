//! Shared in-process gRPC backend harness for the integration tests.
//!
//! Provides the 5-byte gRPC frame helpers, a finite [`FrameList`] response
//! body, and a [`Backend`] guard that spawns a minimal HTTP/2 gRPC server (no
//! tonic *server*) on an ephemeral port and aborts it — accept loop plus every
//! live connection — on drop, so a fake backend's lifetime is tied
//! deterministically to the test scope rather than to runtime teardown.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use http::HeaderMap;
use hyper::body::{Body, Frame, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};

/// Prefix `payload` with the 5-byte gRPC frame header (uncompressed).
pub fn frame(payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(5 + payload.len());
    buf.put_u8(0);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    buf.freeze()
}

/// Strip the 5-byte gRPC frame header, returning the message payload.
pub fn deframe(buf: &[u8]) -> Bytes {
    assert!(buf.len() >= 5, "frame header present");
    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    Bytes::copy_from_slice(&buf[5..5 + len])
}

/// A finite response body yielding a fixed list of frames (data and/or
/// trailers) in order — enough to model a unary gRPC reply without pulling in a
/// streaming-body dependency.
pub struct FrameList {
    frames: VecDeque<Frame<Bytes>>,
}

impl FrameList {
    /// Body from an explicit, ordered list of frames.
    pub fn new(frames: Vec<Frame<Bytes>>) -> Self {
        FrameList {
            frames: frames.into(),
        }
    }

    /// A unary OK reply: one framed `message` data frame followed by a
    /// `grpc-status: 0` trailers frame.
    pub fn unary_ok(message: &[u8]) -> Self {
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().unwrap());
        FrameList::new(vec![Frame::data(frame(message)), Frame::trailers(trailers)])
    }

    /// An empty body (no frames) — for trailers-only responses where the status
    /// is carried in the leading header block.
    pub fn empty() -> Self {
        FrameList::new(Vec::new())
    }
}

impl Body for FrameList {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(self.frames.pop_front().map(Ok))
    }
}

/// Handle to a spawned fake backend. Dropping it aborts the accept loop and —
/// via the inner [`JoinSet`] — every live connection task, so the server's
/// lifetime is tied deterministically to the test scope rather than relying on
/// the runtime being torn down at test exit.
pub struct Backend {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl Backend {
    /// Bind an ephemeral port and serve `handler` for every inbound HTTP/2
    /// connection. `handler` is cloned per connection, so any captured state
    /// (channels, behavior flags) is shared across connections via the clone.
    pub async fn spawn<S, Fut>(handler: S) -> Backend
    where
        S: Fn(Request<Incoming>) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<Response<FrameList>, Infallible>> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut conns = JoinSet::new();
            while let Ok((stream, _)) = listener.accept().await {
                let handler = handler.clone();
                conns.spawn(async move {
                    let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), service_fn(handler))
                        .await;
                });
            }
        });
        Backend { addr, task }
    }

    /// The address the backend is listening on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.task.abort();
    }
}
