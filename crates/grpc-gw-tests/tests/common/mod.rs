//! Shared helpers for the co-hosting integration tests: a real tonic `Greeter`
//! service, a server-lifetime guard, and the typed gRPC / raw REST clients used
//! by both the socket-backed ([`cohosting_socket`]) and in-process
//! ([`cohosting_inprocess`]) variants.
//!
//! This is a test support module (`tests/common/mod.rs`), compiled into each
//! integration-test binary that declares `mod common;`. `dead_code` is allowed
//! because any given test uses only a subset of these helpers.

#![allow(dead_code)]

use std::net::SocketAddr;

use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http::Request;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use grpc_gw_tests::greeter::greeter_client::GreeterClient;
use grpc_gw_tests::greeter::greeter_server::GreeterServer;
use grpc_gw_util::Incoming;

// Re-export the shared service impl so tests can `use common::GreeterImpl`.
pub use grpc_gw_tests::GreeterImpl;

/// A guard for a spawned server task with **graceful shutdown**.
///
/// Call [`shutdown`](ServerHandle::shutdown) to signal the server (via the
/// `oneshot` wired into `serve_with_incoming_shutdown`) and await a clean drain.
/// If the guard is merely dropped, it still signals shutdown and then aborts the
/// task as a fallback (a `Drop` can't await).
pub struct ServerHandle {
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl ServerHandle {
    pub fn new(shutdown: oneshot::Sender<()>, handle: JoinHandle<()>) -> Self {
        ServerHandle {
            shutdown: Some(shutdown),
            handle: Some(handle),
        }
    }

    /// Signal graceful shutdown and await the server task draining.
    pub async fn shutdown(mut self) {
        self.signal();
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }

    fn signal(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.signal();
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

/// A running co-hosted server bound to `addr`. Call
/// [`shutdown`](Cohosted::shutdown) for a graceful drain, or drop it (which
/// signals shutdown then aborts as a fallback).
pub struct Cohosted {
    pub addr: SocketAddr,
    server: ServerHandle,
}

impl Cohosted {
    pub fn new(addr: SocketAddr, shutdown: oneshot::Sender<()>, handle: JoinHandle<()>) -> Self {
        Cohosted {
            addr,
            server: ServerHandle::new(shutdown, handle),
        }
    }

    /// Signal graceful shutdown and await the server task draining.
    pub async fn shutdown(self) {
        self.server.shutdown().await;
    }
}

/// Serve `greeter` over an in-memory [`Incoming`] with tonic's HTTP/2 server and
/// **graceful shutdown**, returning a [`ServerHandle`] guard. This is the
/// in-process backend behind the gateway in the duplex/TLS co-hosting tests.
pub fn spawn_inprocess_backend(
    greeter: GreeterServer<GreeterImpl>,
    incoming: Incoming,
) -> ServerHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(greeter)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    ServerHandle::new(shutdown_tx, handle)
}

/// Open a typed tonic client to the co-hosted port (native gRPC over h2c).
pub async fn grpc_client(addr: SocketAddr) -> GreeterClient<tonic::transport::Channel> {
    GreeterClient::connect(format!("http://{addr}"))
        .await
        .expect("tonic client connects")
}

/// Send one plaintext HTTP/2 (h2c) request to the co-hosted port and return
/// (status, body). The co-hosted front is tonic's HTTP/2-only server, so REST
/// JSON rides h2c on the same port as native gRPC.
pub async fn rest_call(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Bytes,
) -> (http::StatusCode, Bytes) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let req = Request::builder()
        .method(method)
        .uri(path)
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(body))
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, bytes)
}
