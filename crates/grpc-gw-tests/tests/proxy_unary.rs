//! End-to-end proxy test: drive [`GrpcClient::unary`] against a minimal
//! in-process HTTP/2 gRPC backend (no tonic *server*). The fake backend reads
//! the framed request, echoes a framed response, and sets `grpc-status` —
//! proving the tonic-backed client interoperates with a real gRPC wire peer.

use std::convert::Infallible;

use bytes::Bytes;
use grpc_gw::proxy::GrpcClient;
use grpc_gw::Code;
use grpc_gw_tests::{deframe, Backend, FrameList};
use http::{HeaderMap, Request, Response};
use http_body_util::BodyExt;
use hyper::body::Incoming;

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
    let payload = deframe(&collected.to_bytes());

    match behavior {
        Behavior::EchoOk => {
            // data frame (echoed message) then a trailers frame with status 0.
            Ok(Response::builder()
                .header("content-type", "application/grpc+proto")
                .body(FrameList::unary_ok(&payload))
                .unwrap())
        }
        Behavior::Status(code, message) => {
            // Trailers-only: status carried in the leading header block.
            Ok(Response::builder()
                .header("content-type", "application/grpc+proto")
                .header("grpc-status", code.to_string())
                .header("grpc-message", message)
                .body(FrameList::empty())
                .unwrap())
        }
    }
}

/// Start the fake backend on an ephemeral port. The returned [`Backend`] keeps
/// the server alive until it is dropped, at which point the accept loop and all
/// in-flight connections are aborted.
async fn spawn_backend(behavior: Behavior) -> Backend {
    Backend::spawn(move |req| handle(req, behavior.clone())).await
}

#[tokio::test]
async fn unary_echo_round_trip() {
    let server = spawn_backend(Behavior::EchoOk).await;
    let backend = format!("http://{}", server.addr()).parse().unwrap();
    let client = GrpcClient::plaintext(backend).expect("valid backend");

    let reply = client
        .unary(
            "/greeter.v1.Greeter/Ping",
            Bytes::from_static(b"ping-bytes"),
            HeaderMap::new(),
        )
        .await
        .expect("call succeeds");

    assert_eq!(reply.status, Code::Ok);
    assert_eq!(reply.message_bytes.as_deref(), Some(&b"ping-bytes"[..]));
}

#[tokio::test]
async fn unary_non_ok_status_maps_through() {
    let server = spawn_backend(Behavior::Status(5, "greeter not found")).await;
    let backend = format!("http://{}", server.addr()).parse().unwrap();
    let client = GrpcClient::plaintext(backend).expect("valid backend");

    let reply = client
        .unary(
            "/greeter.v1.Greeter/SayHello",
            Bytes::new(),
            HeaderMap::new(),
        )
        .await
        .expect("call returns a reply");

    assert_eq!(reply.status, Code::NotFound);
    assert_eq!(reply.message, "greeter not found");
    assert!(reply.message_bytes.is_none());
}
