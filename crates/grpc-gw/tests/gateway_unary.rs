//! End-to-end gateway test: build a [`Gateway`] over the committed fixture
//! descriptor set, point it at a minimal in-process HTTP/2 gRPC backend (no
//! tonic *server*), and drive real JSON requests through [`Gateway::handle`].
//!
//! The fake backend replies with a framed, protobuf-encoded message built via
//! `prost-reflect` — so this exercises the whole chain: route match → request
//! transcode → gRPC framing/call (tonic) → response transcode.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use grpc_gw::{Gateway, GatewayOptions, GrpcClient};
use http::HeaderMap;
use http_body_util::BodyExt;
use hyper::body::{Body, Frame, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, Value};
use tokio::net::TcpListener;

const GREETER_PB: &[u8] = include_bytes!("fixtures/greeter.pb");

/// Prefix `payload` with the 5-byte gRPC frame header (uncompressed).
fn frame(payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(5 + payload.len());
    buf.put_u8(0);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    buf.freeze()
}

/// A finite response body yielding a fixed list of frames in order.
struct FrameList {
    frames: std::collections::VecDeque<Frame<Bytes>>,
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

/// Encode a `greeter.v1.PingReply { pong }` to protobuf bytes via reflection.
fn ping_reply(pong: &str) -> Bytes {
    let pool = DescriptorPool::decode(GREETER_PB).unwrap();
    let desc = pool.get_message_by_name("greeter.v1.PingReply").unwrap();
    let mut msg = DynamicMessage::new(desc);
    msg.set_field_by_name("pong", Value::String(pong.to_owned()));
    Bytes::from(msg.encode_to_vec())
}

/// Backend that answers every unary call with one framed `PingReply` + an OK
/// trailer, regardless of the request.
async fn handle(_req: Request<Incoming>) -> Result<Response<FrameList>, Infallible> {
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", "0".parse().unwrap());
    let body = FrameList {
        frames: vec![
            Frame::data(frame(&ping_reply("pong!"))),
            Frame::trailers(trailers),
        ]
        .into(),
    };
    Ok(Response::builder()
        .header("content-type", "application/grpc+proto")
        .body(body)
        .unwrap())
}

async fn spawn_backend() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), service_fn(handle))
                    .await;
            });
        }
    });
    addr
}

/// Build a gateway wired to a live fake backend.
async fn gateway_to_backend() -> Gateway {
    let addr = spawn_backend().await;
    let backend = format!("http://{addr}").parse().unwrap();
    let client = GrpcClient::plaintext(backend).expect("valid backend");
    Gateway::builder(GREETER_PB.to_vec())
        .backend(client)
        .options(GatewayOptions::default())
        .build()
        .expect("gateway builds")
}

/// Build a gateway with a backend that is never actually reached (for the
/// pre-dispatch error paths: 404, 501).
fn gateway_offline() -> Gateway {
    let client = GrpcClient::plaintext("http://127.0.0.1:1".parse().unwrap()).unwrap();
    Gateway::builder(GREETER_PB.to_vec())
        .backend(client)
        .build()
        .expect("gateway builds")
}

#[tokio::test]
async fn unary_default_binding_round_trip() {
    let gateway = gateway_to_backend().await;

    // Ping is unannotated → reachable via the synthesized default binding.
    let resp = gateway
        .handle(
            "POST",
            "/greeter.v1.Greeter/Ping",
            &HeaderMap::new(),
            Bytes::from_static(b"{}"),
        )
        .await;

    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/json"
    );
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, &br#"{"pong":"pong!"}"#[..]);
}

#[tokio::test]
async fn empty_body_is_accepted_for_default_binding() {
    let gateway = gateway_to_backend().await;
    let resp = gateway
        .handle(
            "POST",
            "/greeter.v1.Greeter/Ping",
            &HeaderMap::new(),
            Bytes::new(),
        )
        .await;
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn unknown_route_returns_404() {
    let gateway = gateway_offline();
    let resp = gateway
        .handle("POST", "/no/such/path", &HeaderMap::new(), Bytes::new())
        .await;

    assert_eq!(resp.status(), 404);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["code"], 5); // NOT_FOUND
}

#[tokio::test]
async fn field_body_selector_returns_501() {
    let gateway = gateway_offline();
    // UpdateGreeting carries `body: "greeting"` (a field selector) on the
    // literal annotated path — deferred to M2, so the gateway answers 501.
    let resp = gateway
        .handle(
            "POST",
            "/v1/greeter/{name}/greeting",
            &HeaderMap::new(),
            Bytes::from_static(b"{}"),
        )
        .await;

    assert_eq!(resp.status(), 501);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["code"], 12); // UNIMPLEMENTED
}

#[tokio::test]
async fn invalid_json_returns_400() {
    let gateway = gateway_offline();
    let resp = gateway
        .handle(
            "POST",
            "/greeter.v1.Greeter/Ping",
            &HeaderMap::new(),
            Bytes::from_static(b"{not json"),
        )
        .await;

    assert_eq!(resp.status(), 400);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["code"], 3); // INVALID_ARGUMENT
}

#[tokio::test]
async fn serve_connection_serves_over_a_socket() {
    // Tier-1 embedding: accept a TCP connection and serve the gateway over it.
    let gateway = gateway_to_backend().await;
    let front = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front_addr = front.local_addr().unwrap();

    tokio::spawn(async move {
        let (stream, _) = front.accept().await.unwrap();
        grpc_gw::serve_connection(stream, gateway).await.unwrap();
    });

    // Drive a real h2c request through serve_connection using a tonic-less
    // hyper client over the raw socket.
    let stream = tokio::net::TcpStream::connect(front_addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let req = Request::builder()
        .method("POST")
        .uri("/greeter.v1.Greeter/Ping")
        .header("content-type", "application/json")
        .body(http_body_util::Full::new(Bytes::from_static(b"{}")))
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, &br#"{"pong":"pong!"}"#[..]);
}
