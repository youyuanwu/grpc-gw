//! End-to-end gateway test: build a [`Gateway`] over the committed fixture
//! descriptor set, point it at a minimal in-process HTTP/2 gRPC backend (no
//! tonic *server*), and drive real JSON requests through [`Gateway::handle`].
//!
//! The fake backend replies with a framed, protobuf-encoded message built via
//! `prost-reflect` — so this exercises the whole chain: route match → request
//! transcode → gRPC framing/call (tonic) → response transcode.

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use grpc_gw::{Gateway, GatewayOptions, GrpcClient};
use http::HeaderMap;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, Value};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use grpc_gw_tests::{deframe, Backend, FrameList, GREETER_PB};

/// Encode a `greeter.v1.PingReply { pong }` to protobuf bytes via reflection.
fn ping_reply(pong: &str) -> Bytes {
    let pool = DescriptorPool::decode(GREETER_PB).unwrap();
    let desc = pool.get_message_by_name("greeter.v1.PingReply").unwrap();
    let mut msg = DynamicMessage::new(desc);
    msg.set_field_by_name("pong", Value::String(pong.to_owned()));
    Bytes::from(msg.encode_to_vec())
}

/// Decode a protobuf message of the given fully-qualified type from bytes.
fn decode_msg(type_name: &str, bytes: &[u8]) -> DynamicMessage {
    let pool = DescriptorPool::decode(GREETER_PB).unwrap();
    let desc = pool.get_message_by_name(type_name).unwrap();
    DynamicMessage::decode(desc, bytes).unwrap()
}

/// Build a `HelloReply { message }` as protobuf bytes.
fn hello_reply(message: String) -> Bytes {
    let pool = DescriptorPool::decode(GREETER_PB).unwrap();
    let desc = pool.get_message_by_name("greeter.v1.HelloReply").unwrap();
    let mut msg = DynamicMessage::new(desc);
    msg.set_field_by_name("message", Value::String(message));
    Bytes::from(msg.encode_to_vec())
}

fn str_field(msg: &DynamicMessage, name: &str) -> String {
    msg.get_field_by_name(name)
        .map(|v| v.as_str().unwrap_or("").to_owned())
        .unwrap_or_default()
}

/// Backend dispatcher that echoes request fields into the reply so tests can
/// assert exactly what the gateway bound from path/body/query:
/// - `/SayHello` → `HelloReply { message: "hello {name}" }`
/// - `/UpdateGreeting` → `HelloReply { message: "{name}: {greeting}" }`
/// - `/Search` → `SearchResponse { result: HelloReply { message:
///   "{category}/{q}/{limit}" } }`
/// - `/Echo` → echoes the framed request message verbatim
/// - anything else → `PingReply { pong: "pong!" }`
async fn handle(req: Request<Incoming>) -> Result<Response<FrameList>, Infallible> {
    let path = req.uri().path().to_owned();
    let request_body = req.into_body().collect().await.unwrap().to_bytes();
    let msg = deframe(&request_body);

    let reply = if path.ends_with("/Echo") {
        msg // echo verbatim
    } else if path.ends_with("/SayHello") {
        let req = decode_msg("greeter.v1.HelloRequest", &msg);
        hello_reply(format!("hello {}", str_field(&req, "name")))
    } else if path.ends_with("/UpdateGreeting") {
        let req = decode_msg("greeter.v1.UpdateGreetingRequest", &msg);
        hello_reply(format!(
            "{}: {}",
            str_field(&req, "name"),
            str_field(&req, "greeting")
        ))
    } else if path.ends_with("/Search") {
        let req = decode_msg("greeter.v1.SearchRequest", &msg);
        let category = str_field(&req, "category");
        let q = str_field(&req, "q");
        let limit = req
            .get_field_by_name("limit")
            .and_then(|v| v.as_i32())
            .unwrap_or(0);
        // SearchResponse { result: HelloReply { message } }
        let pool = DescriptorPool::decode(GREETER_PB).unwrap();
        let resp_desc = pool
            .get_message_by_name("greeter.v1.SearchResponse")
            .unwrap();
        let inner_desc = pool.get_message_by_name("greeter.v1.HelloReply").unwrap();
        let mut inner = DynamicMessage::new(inner_desc);
        inner.set_field_by_name("message", Value::String(format!("{category}/{q}/{limit}")));
        let mut resp = DynamicMessage::new(resp_desc);
        resp.set_field_by_name("result", Value::Message(inner));
        Bytes::from(resp.encode_to_vec())
    } else {
        ping_reply("pong!")
    };

    Ok(Response::builder()
        .header("content-type", "application/grpc+proto")
        .body(FrameList::unary_ok(&reply))
        .unwrap())
}

/// Backend that always replies with a non-OK trailers-only gRPC status.
async fn handle_status(_req: Request<Incoming>) -> Result<Response<FrameList>, Infallible> {
    Ok(Response::builder()
        .header("content-type", "application/grpc+proto")
        .header("grpc-status", "7") // PERMISSION_DENIED
        .header("grpc-message", "nope")
        .body(FrameList::empty())
        .unwrap())
}

async fn spawn_backend() -> Backend {
    Backend::spawn(handle).await
}

/// Spawn a backend that always returns `PERMISSION_DENIED`.
async fn spawn_status_backend() -> Backend {
    Backend::spawn(handle_status).await
}

/// Build a gateway wired to a live fake backend. The returned [`Backend`] guard
/// must be kept alive for the duration of the test — dropping it tears the
/// backend down.
async fn gateway_to_backend() -> (Gateway, Backend) {
    let backend = spawn_backend().await;
    let uri = format!("http://{}", backend.addr()).parse().unwrap();
    let client = GrpcClient::plaintext(uri).expect("valid backend");
    let gateway = Gateway::builder(GREETER_PB.to_vec())
        .backend(client)
        .options(GatewayOptions::default())
        .build()
        .expect("gateway builds");
    (gateway, backend)
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
    let (gateway, _backend) = gateway_to_backend().await;

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
    let (gateway, _backend) = gateway_to_backend().await;
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
async fn kitchen_echo_round_trip_through_gateway() {
    // End-to-end proto3-JSON type coverage: a populated Kitchen message goes
    // JSON → request transcode → gRPC (echoed by the backend) → response
    // transcode → canonical JSON, with all the tricky kinds intact.
    let (gateway, _backend) = gateway_to_backend().await;

    let input = br#"{
        "i32": 7,
        "i64": "9007199254740993",
        "flag": true,
        "text": "hi",
        "blob": "aGVsbG8=",
        "flavor": "SOUR",
        "nested": { "label": "n", "count": 3 },
        "tags": ["a", "b"],
        "scores": { "x": 1 },
        "at": "2026-06-13T12:00:00Z"
    }"#;

    let resp = gateway
        .handle(
            "POST",
            "/greeter.v1.Greeter/Echo",
            &HeaderMap::new(),
            Bytes::from_static(input),
        )
        .await;

    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["i32"], 7);
    assert_eq!(json["i64"], "9007199254740993");
    assert_eq!(json["flag"], true);
    assert_eq!(json["text"], "hi");
    assert_eq!(json["blob"], "aGVsbG8=");
    assert_eq!(json["flavor"], "SOUR");
    assert_eq!(json["nested"]["label"], "n");
    assert_eq!(json["nested"]["count"], 3);
    assert_eq!(json["tags"], serde_json::json!(["a", "b"]));
    assert_eq!(json["scores"]["x"], 1);
    assert_eq!(json["at"], "2026-06-13T12:00:00Z");
}

#[tokio::test]
async fn non_ok_grpc_status_maps_to_http_error() {
    // A backend PERMISSION_DENIED (7) must render as HTTP 403 with the
    // Status-proto JSON envelope.
    let server = spawn_status_backend().await;
    let client =
        GrpcClient::plaintext(format!("http://{}", server.addr()).parse().unwrap()).unwrap();
    let gateway = Gateway::builder(GREETER_PB.to_vec())
        .backend(client)
        .build()
        .expect("gateway builds");

    let resp = gateway
        .handle(
            "POST",
            "/greeter.v1.Greeter/Ping",
            &HeaderMap::new(),
            Bytes::from_static(b"{}"),
        )
        .await;

    assert_eq!(resp.status(), 403);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["code"], 7); // PERMISSION_DENIED
    assert_eq!(json["message"], "nope");
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
async fn path_variable_binds_and_routes() {
    // GET /v1/greeter/{name} → HelloRequest.name = "alice", echoed back.
    let (gateway, _backend) = gateway_to_backend().await;
    let resp = gateway
        .handle("GET", "/v1/greeter/alice", &HeaderMap::new(), Bytes::new())
        .await;

    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["message"], "hello alice");
}

#[tokio::test]
async fn field_body_selector_with_path_var() {
    // POST /v1/greeter/{name}/greeting with body: "greeting": the JSON body is
    // the `greeting` field, while `name` comes from the path.
    let (gateway, _backend) = gateway_to_backend().await;
    let resp = gateway
        .handle(
            "POST",
            "/v1/greeter/ada/greeting",
            &HeaderMap::new(),
            Bytes::from_static(br#""hi there""#),
        )
        .await;

    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["message"], "ada: hi there");
}

#[tokio::test]
async fn additional_binding_patch_is_reachable() {
    // The PATCH additional_binding on UpdateGreeting reaches the same method.
    let (gateway, _backend) = gateway_to_backend().await;
    let resp = gateway
        .handle(
            "PATCH",
            "/v1/greeter/ada/greeting",
            &HeaderMap::new(),
            Bytes::from_static(br#""hello""#),
        )
        .await;

    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["message"], "ada: hello");
}

#[tokio::test]
async fn query_params_and_response_body() {
    // GET /v1/search/{category}?q=..&limit=.. binds the path var + query params,
    // and response_body: "result" narrows the reply to the inner HelloReply.
    let (gateway, _backend) = gateway_to_backend().await;
    let resp = gateway
        .handle(
            "GET",
            "/v1/search/books?q=rust&limit=5",
            &HeaderMap::new(),
            Bytes::new(),
        )
        .await;

    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    // response_body unwrapped the SearchResponse to just `result` (HelloReply).
    assert_eq!(json["message"], "books/rust/5");
    assert!(json.get("result").is_none(), "response_body should unwrap");
}

#[tokio::test]
async fn unmatched_template_path_returns_404() {
    // A path that matches no template (extra segment) is a route miss.
    let (gateway, _backend) = gateway_to_backend().await;
    let resp = gateway
        .handle("GET", "/v1/greeter/a/b/c", &HeaderMap::new(), Bytes::new())
        .await;
    assert_eq!(resp.status(), 404);
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
    let (gateway, _backend) = gateway_to_backend().await;
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

/// Fires `cancelled` from its `Drop` — i.e. when the backend handler future it
/// lives in is dropped because the upstream gRPC stream was reset.
struct CancelGuard(Option<oneshot::Sender<()>>);

impl Drop for CancelGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

/// Spawn a backend whose unary handler signals `started`, then hangs forever
/// holding a [`CancelGuard`]. If the gateway resets the upstream stream (because
/// its inbound connection was dropped), hyper drops the handler future and the
/// guard fires `cancelled` — proving the call was not leaked.
async fn spawn_hanging_backend(
    started: oneshot::Sender<()>,
    cancelled: oneshot::Sender<()>,
) -> Backend {
    let started = Arc::new(Mutex::new(Some(started)));
    let cancelled = Arc::new(Mutex::new(Some(cancelled)));

    Backend::spawn(move |_req: Request<Incoming>| {
        let started = started.lock().unwrap().take();
        let guard = cancelled
            .lock()
            .unwrap()
            .take()
            .map(|tx| CancelGuard(Some(tx)));
        async move {
            if let Some(tx) = started {
                let _ = tx.send(());
            }
            // Hold the guard across an await that never resolves; dropping this
            // future (stream reset) drops the guard.
            let _guard = guard;
            std::future::pending::<Result<Response<FrameList>, Infallible>>().await
        }
    })
    .await
}

#[tokio::test]
async fn client_disconnect_cancels_upstream_call() {
    let (started_tx, started_rx) = oneshot::channel();
    let (cancelled_tx, cancelled_rx) = oneshot::channel();
    let server = spawn_hanging_backend(started_tx, cancelled_tx).await;

    let client = GrpcClient::plaintext(format!("http://{}", server.addr()).parse().unwrap())
        .expect("valid backend");
    let gateway = Gateway::builder(GREETER_PB.to_vec())
        .backend(client)
        .build()
        .expect("gateway builds");

    // Serve the gateway over a front socket (Tier-1 byte stream).
    let front = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front_addr = front.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = front.accept().await.unwrap();
        // Returns an error once the client disconnects — expected.
        let _ = grpc_gw::serve_connection(stream, gateway).await;
    });

    // Send a raw HTTP/1.1 request the backend will hang on (never responds).
    let mut stream = TcpStream::connect(front_addr).await.unwrap();
    stream
        .write_all(
            b"POST /greeter.v1.Greeter/Ping HTTP/1.1\r\n\
              Host: localhost\r\n\
              Content-Type: application/json\r\n\
              Content-Length: 2\r\n\r\n{}",
        )
        .await
        .unwrap();
    stream.flush().await.unwrap();

    // The proxied call must actually reach the backend first — otherwise the
    // cancellation assertion would be vacuous.
    tokio::time::timeout(Duration::from_secs(5), started_rx)
        .await
        .expect("backend should receive the proxied call within 5s")
        .expect("started channel not dropped");

    // Drop the inbound connection mid-call.
    drop(stream);

    // The gateway must reset the upstream gRPC stream, dropping the backend
    // handler future (no leaked call).
    tokio::time::timeout(Duration::from_secs(5), cancelled_rx)
        .await
        .expect("backend handler must be cancelled when the client disconnects")
        .expect("cancel channel not dropped");
}
