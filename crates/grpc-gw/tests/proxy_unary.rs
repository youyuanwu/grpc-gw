//! End-to-end proxy test: drive [`GrpcClient::unary`] against a minimal
//! in-process HTTP/2 gRPC backend (no tonic). The fake backend reads the
//! framed request, echoes a framed response, and sets `grpc-status` trailers —
//! enough to prove framing, the hyper-util h2 client, and trailer/status
//! parsing all line up.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use grpc_gw::proxy::{decode_single_frame, encode_frame, GrpcClient};
use grpc_gw::Code;
use http::{HeaderMap, Request, Response};
use http_body_util::BodyExt;
use hyper::body::{Body, Frame, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;

/// A finite response body that yields a pre-built list of frames (data and/or
/// trailers) in order — enough to model a unary gRPC reply without pulling in a
/// streaming-body dependency.
struct FrameList {
    frames: std::collections::VecDeque<Frame<Bytes>>,
}

impl FrameList {
    fn new(frames: Vec<Frame<Bytes>>) -> Self {
        FrameList {
            frames: frames.into(),
        }
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

/// How the fake backend should reply to a unary call.
#[derive(Clone)]
enum Behavior {
    /// Echo the request payload back with `grpc-status: 0` in trailers.
    EchoOk,
    /// Reply with a non-OK trailers-only status (no message frame).
    Status(i32, &'static str),
}

async fn handle(
    req: Request<Incoming>,
    behavior: Behavior,
) -> Result<Response<FrameList>, Infallible> {
    // Read and de-frame the inbound request message.
    let collected = req.into_body().collect().await.unwrap();
    let req_bytes = collected.to_bytes();
    let payload = decode_single_frame(&req_bytes).expect("request is framed");

    match behavior {
        Behavior::EchoOk => {
            // data frame (echoed message) then a trailers frame with status 0.
            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            let body = FrameList::new(vec![
                Frame::data(encode_frame(&payload)),
                Frame::trailers(trailers),
            ]);
            Ok(Response::builder()
                .header("content-type", "application/grpc+proto")
                .body(body)
                .unwrap())
        }
        Behavior::Status(code, message) => {
            // Trailers-only: status carried in the leading header block.
            Ok(Response::builder()
                .header("content-type", "application/grpc+proto")
                .header("grpc-status", code.to_string())
                .header("grpc-message", message)
                .body(FrameList::new(Vec::new()))
                .unwrap())
        }
    }
}

/// Start the fake backend on an ephemeral port; returns its address. The
/// server serves exactly one connection then keeps accepting in the background.
async fn spawn_backend(behavior: Behavior) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            let behavior = behavior.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(move |req| handle(req, behavior.clone()));
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(io, service)
                    .await;
            });
        }
    });

    addr
}

#[tokio::test]
async fn unary_echo_round_trip() {
    let addr = spawn_backend(Behavior::EchoOk).await;
    let backend = format!("http://{addr}").parse().unwrap();
    let client = GrpcClient::plaintext(backend);

    let reply = client
        .unary("/greeter.v1.Greeter/Ping", b"ping-bytes", HeaderMap::new())
        .await
        .expect("call succeeds");

    assert_eq!(reply.status, Code::Ok);
    assert_eq!(reply.message_bytes.as_deref(), Some(&b"ping-bytes"[..]));
}

#[tokio::test]
async fn unary_non_ok_status_maps_through() {
    let addr = spawn_backend(Behavior::Status(5, "greeter not found")).await;
    let backend = format!("http://{addr}").parse().unwrap();
    let client = GrpcClient::plaintext(backend);

    let reply = client
        .unary("/greeter.v1.Greeter/SayHello", b"", HeaderMap::new())
        .await
        .expect("call returns a reply");

    assert_eq!(reply.status, Code::NotFound);
    assert_eq!(reply.message, "greeter not found");
    assert!(reply.message_bytes.is_none());
}
