//! Co-hosting the dynamic gateway and a **real tonic gRPC server** in a single
//! process, behind a single port.
//!
//! This realises the "Recommended shape" from
//! `docs/design/co-hosting-with-tonic.md`:
//!
//! ```text
//!             :PORT  (hyper auto h1+h2)
//!                │
//!           Steer by content-type
//!         ┌──────┴───────┐
//!    application/grpc   else
//!         │              │
//!    tonic Greeter   grpc-gw Gateway ──► gRPC client ─┐ (dials :PORT)
//!         ▲                                           │
//!         └──────────── same in-process tonic ◄───────┘
//! ```
//!
//! A [`tower::steer::Steer`] front door keys on `content-type: application/grpc`
//! to send gRPC traffic to the generated [`GreeterServer`] and everything else
//! to the [`Gateway`]. Both arms are unified to `Response<tonic::body::Body>`
//! and boxed so `Steer` can hold them. The whole thing is served over one
//! ephemeral TCP port with `hyper_util`'s h1+h2 auto connection.
//!
//! The test then drives the **same port** two ways:
//! - a typed tonic [`GreeterClient`] (native gRPC), and
//! - a plain hyper HTTP/1.1 JSON client (REST),
//!
//! proving both protocols are served by one process on one port. The gateway's
//! backend hop loops back into the same port, so a REST request is transcoded
//! to gRPC and handled by the in-process tonic service.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, Request, Response, Uri};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tonic::body::Body as TonicBody;
use tonic::transport::{Channel, Endpoint};
use tower::steer::Steer;
use tower::ServiceExt;

use grpc_gw::{Gateway, GatewayOptions, GrpcClient};
use grpc_gw_tests::greeter::greeter_client::GreeterClient;
use grpc_gw_tests::greeter::greeter_server::{Greeter, GreeterServer};
use grpc_gw_tests::greeter::{
    HelloReply, HelloRequest, PingReply, PingRequest, SearchRequest, SearchResponse,
    UpdateGreetingRequest,
};
use grpc_gw_tests::GREETER_PB;

/// A real tonic service. Each method echoes its request into the reply so the
/// REST-side assertions can observe exactly what the gateway bound from the
/// path/body/query (mirrors the fake backend used by the unary tests).
#[derive(Default)]
struct GreeterImpl;

#[tonic::async_trait]
impl Greeter for GreeterImpl {
    async fn say_hello(
        &self,
        request: tonic::Request<HelloRequest>,
    ) -> Result<tonic::Response<HelloReply>, tonic::Status> {
        let name = request.into_inner().name;
        Ok(tonic::Response::new(HelloReply {
            message: format!("hello {name}"),
        }))
    }

    async fn update_greeting(
        &self,
        request: tonic::Request<UpdateGreetingRequest>,
    ) -> Result<tonic::Response<HelloReply>, tonic::Status> {
        let req = request.into_inner();
        Ok(tonic::Response::new(HelloReply {
            message: format!("{}: {}", req.name, req.greeting),
        }))
    }

    async fn ping(
        &self,
        _request: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PingReply>, tonic::Status> {
        Ok(tonic::Response::new(PingReply {
            pong: "pong!".to_owned(),
        }))
    }

    async fn search(
        &self,
        request: tonic::Request<SearchRequest>,
    ) -> Result<tonic::Response<SearchResponse>, tonic::Status> {
        let req = request.into_inner();
        Ok(tonic::Response::new(SearchResponse {
            result: Some(HelloReply {
                message: format!("{}/{}/{}", req.category, req.q, req.limit),
            }),
        }))
    }

    async fn echo(
        &self,
        request: tonic::Request<grpc_gw_tests::greeter::Kitchen>,
    ) -> Result<tonic::Response<grpc_gw_tests::greeter::Kitchen>, tonic::Status> {
        Ok(tonic::Response::new(request.into_inner()))
    }
}

/// The boxed, unified front-door service type Steer holds: takes an inbound
/// `Request<Incoming>` and yields `Response<tonic::body::Body>`, infallibly.
type FrontSvc = tower::util::BoxCloneService<Request<Incoming>, Response<TonicBody>, Infallible>;

/// True when a request is gRPC (`content-type: application/grpc[...]`).
fn is_grpc(req: &Request<Incoming>) -> bool {
    req.headers()
        .get(CONTENT_TYPE)
        .map(|v| v.as_bytes().starts_with(b"application/grpc"))
        .unwrap_or(false)
}

/// A running co-hosted server. Dropping it aborts the accept loop and all
/// in-flight connections (the test owns the lifetime via this guard).
struct Cohosted {
    addr: SocketAddr,
    accept: tokio::task::JoinHandle<()>,
}

impl Drop for Cohosted {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

/// Build the Steer front door (gRPC → tonic, else → gateway) and serve it on an
/// ephemeral port. The gateway's backend hop dials that same port.
async fn spawn_cohosted() -> Cohosted {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // The gateway dials the very port we're about to serve: its transcoded gRPC
    // call re-enters the front door and is steered to the in-process tonic svc.
    let backend = GrpcClient::plaintext(format!("http://{addr}").parse().unwrap()).unwrap();
    let gateway = Gateway::builder(GREETER_PB.to_vec())
        .backend(backend)
        .options(GatewayOptions::default())
        .build()
        .expect("gateway builds");

    // Arm 0: the tonic gRPC server, adapted to the unified front-door
    // signature. The generated `Service` is generic over the request body, so
    // we pin it to `Request<Incoming>` and re-wrap its `Response` body as
    // `tonic::body::Body` through a small adapter (avoids `map_response`
    // body-type inference ambiguity).
    let grpc: FrontSvc = tower::util::BoxCloneService::new(GrpcSvc {
        inner: GreeterServer::new(GreeterImpl),
    });

    // Arm 1: the gateway, adapted to the same signature. Wrap its
    // `Response<Full<Bytes>>` into `tonic::body::Body`.
    let rest: FrontSvc = tower::util::BoxCloneService::new(GatewaySvc { gateway });

    let steer = Steer::new(
        [grpc, rest],
        |req: &Request<Incoming>, _arms: &[FrontSvc]| usize::from(!is_grpc(req)),
    );

    let accept = tokio::spawn(async move {
        let mut conns = JoinSet::new();
        loop {
            let (stream, _peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            let svc = steer.clone();
            conns.spawn(async move {
                // Each connection gets a `service_fn` cloning the steer service
                // and oneshot-ing it per request (Steer is `Clone`, not `&mut`
                // reusable across concurrent calls without readiness juggling).
                let service = hyper::service::service_fn(move |req: Request<Incoming>| {
                    let svc = svc.clone();
                    async move { Ok::<_, Infallible>(svc.oneshot(req).await.expect("infallible")) }
                });
                let _ = auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });

    Cohosted { addr, accept }
}

/// Open a typed tonic client to the co-hosted port (native gRPC over h2c).
async fn grpc_client(addr: SocketAddr) -> GreeterClient<tonic::transport::Channel> {
    GreeterClient::connect(format!("http://{addr}"))
        .await
        .expect("tonic client connects")
}

/// Send one HTTP/1.1 request to the co-hosted port and return (status, body).
async fn rest_call(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Bytes,
) -> (http::StatusCode, Bytes) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
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

#[tokio::test]
async fn cohosted_server_serves_grpc_and_rest_on_one_port() {
    let server = spawn_cohosted().await;
    let addr = server.addr;

    // --- gRPC path: a native tonic client hits the same port directly. ---
    let mut client = grpc_client(addr).await;

    let reply = client
        .say_hello(HelloRequest {
            name: "grpc".to_owned(),
        })
        .await
        .expect("grpc say_hello ok")
        .into_inner();
    assert_eq!(reply.message, "hello grpc");

    let pong = client
        .ping(PingRequest {})
        .await
        .expect("grpc ping ok")
        .into_inner();
    assert_eq!(pong.pong, "pong!");

    // --- REST path: a JSON client hits the same port; the gateway transcodes
    // and loops back into the in-process tonic service over that same port. ---
    let (status, body) = rest_call(addr, "GET", "/v1/greeter/ada", Bytes::new()).await;
    assert_eq!(status, 200);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["message"], "hello ada");

    // Unannotated method via the synthesized default binding (POST JSON).
    let (status, body) = rest_call(
        addr,
        "POST",
        "/greeter.v1.Greeter/Ping",
        Bytes::from_static(b"{}"),
    )
    .await;
    assert_eq!(status, 200);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["pong"], "pong!");

    // A path-template + query + response_body REST call, end to end through the
    // real tonic Search method.
    let (status, body) =
        rest_call(addr, "GET", "/v1/search/books?q=rust&limit=5", Bytes::new()).await;
    assert_eq!(status, 200);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["message"], "books/rust/5");

    // --- Interleave: the gRPC client still works after the REST traffic,
    // proving both arms stay live on the one port. ---
    let reply = client
        .say_hello(HelloRequest {
            name: "again".to_owned(),
        })
        .await
        .expect("grpc say_hello ok (post-rest)")
        .into_inner();
    assert_eq!(reply.message, "hello again");

    drop(server);
}

/// A `tower::Service` adapter wrapping the gateway: `Request<Incoming>` →
/// `Response<tonic::body::Body>`, matching the gRPC arm so `Steer` can unify
/// them. Collects the body, calls [`Gateway::handle`], and re-wraps the
/// `Full<Bytes>` reply as a `tonic::body::Body`.
#[derive(Clone)]
struct GatewaySvc {
    gateway: Gateway,
}

impl tower::Service<Request<Incoming>> for GatewaySvc {
    type Response = Response<TonicBody>;
    type Error = Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        let gateway = self.gateway.clone();
        Box::pin(async move {
            let (parts, body) = req.into_parts();
            let method = parts.method.as_str().to_owned();
            let path = parts
                .uri
                .path_and_query()
                .map(|pq| pq.as_str().to_owned())
                .unwrap_or_else(|| parts.uri.path().to_owned());
            let bytes = body
                .collect()
                .await
                .map(|c| c.to_bytes())
                .unwrap_or_default();

            let resp = gateway.handle(&method, &path, &parts.headers, bytes).await;
            Ok(resp.map(TonicBody::new))
        })
    }
}

/// A `tower::Service` adapter pinning the generated [`GreeterServer`]'s
/// body-generic `Service` impl to `Request<Incoming>`. The inner service
/// already yields `Response<tonic::body::Body>` with `Error = Infallible`, so
/// this just fixes the input body type and delegates — letting the gRPC arm
/// share the exact signature of [`GatewaySvc`] for `Steer`.
#[derive(Clone)]
struct GrpcSvc {
    inner: GreeterServer<GreeterImpl>,
}

impl tower::Service<Request<Incoming>> for GrpcSvc {
    type Response = Response<TonicBody>;
    type Error = Infallible;
    type Future = <GreeterServer<GreeterImpl> as tower::Service<Request<Incoming>>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <GreeterServer<GreeterImpl> as tower::Service<Request<Incoming>>>::poll_ready(
            &mut self.inner,
            cx,
        )
    }

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        <GreeterServer<GreeterImpl> as tower::Service<Request<Incoming>>>::call(
            &mut self.inner,
            req,
        )
    }
}

/// An abort-on-drop guard for the in-memory duplex `GreeterServer` task. Holds
/// the connection's [`JoinHandle`] so the server is stopped explicitly when the
/// test ends (mirrors the [`Cohosted`] guard), rather than relying on
/// channel-EOF / runtime-drop teardown.
struct DuplexServer(tokio::task::JoinHandle<()>);

impl Drop for DuplexServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Serve the tonic `GreeterServer` over an in-memory [`tokio::io::duplex`] pipe
/// and return a tonic [`Channel`] whose connector hands back the client end of
/// that pipe, plus a [`DuplexServer`] guard that aborts the server task on drop.
/// This is the doc's **recommended "in-memory duplex" backend hop**: the gRPC
/// call rides an in-process pipe with no OS socket and no second trip through
/// the front door — only HTTP/2 framing + protobuf encode/decode.
///
/// The connector yields the client half exactly once; a single multiplexed h2
/// connection then serves every call made on the returned `Channel` (so a
/// native client and the gateway's backend hop can share one `Channel`).
fn inprocess_greeter_channel() -> (Channel, DuplexServer) {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);

    // Serve the GreeterServer over the single server-side pipe end. `auto`
    // detects the HTTP/2 connection preface (gRPC is h2 prior-knowledge), the
    // same way the front door serves gRPC.
    let server_task = tokio::spawn(async move {
        let server = GreeterServer::new(GreeterImpl);
        let service = hyper::service::service_fn(move |req: Request<Incoming>| {
            let mut server = server.clone();
            async move {
                <GreeterServer<GreeterImpl> as tower::Service<Request<Incoming>>>::call(
                    &mut server,
                    req,
                )
                .await
            }
        });
        let _ = auto::Builder::new(TokioExecutor::new())
            .serve_connection(TokioIo::new(server_io), service)
            .await;
    });

    // A `tower` connector that returns the client half on first connect. Shared
    // via `Arc<Mutex<Option<_>>>` so the connector is `Clone` (tonic requires
    // it) while still moving the single `DuplexStream` out exactly once.
    let slot = Arc::new(Mutex::new(Some(client_io)));
    let connector = tower::service_fn(move |_: Uri| {
        let slot = slot.clone();
        async move {
            match slot.lock().unwrap().take() {
                Some(io) => Ok::<_, std::io::Error>(TokioIo::new(io)),
                None => Err(std::io::Error::other("duplex backend already connected")),
            }
        }
    });

    // The authority is irrelevant (the custom connector ignores it); it only
    // populates the `:authority` header.
    let channel =
        Endpoint::from_static("http://greeter.invalid").connect_with_connector_lazy(connector);
    (channel, DuplexServer(server_task))
}

#[tokio::test]
async fn cohosted_inprocess_duplex_backend_has_no_socket_hop() {
    // One in-memory pipe to the tonic service, shared by both a native gRPC
    // client and the gateway's backend hop — no OS socket on the gRPC path. The
    // `_server` guard aborts the duplex server task when the test ends.
    let (channel, _server) = inprocess_greeter_channel();

    // --- Native gRPC, fully in-process over the duplex pipe. ---
    let mut client = GreeterClient::new(channel.clone());
    let reply = client
        .say_hello(HelloRequest {
            name: "dup".to_owned(),
        })
        .await
        .expect("grpc say_hello ok")
        .into_inner();
    assert_eq!(reply.message, "hello dup");

    // --- REST → gRPC, the gateway's backend hop riding the *same* in-memory
    // pipe via `GrpcClient::with_channel` (no loopback socket). ---
    let gateway = Gateway::builder(GREETER_PB.to_vec())
        .backend(GrpcClient::with_channel(channel.clone()))
        .options(GatewayOptions::default())
        .build()
        .expect("gateway builds");

    let resp = gateway
        .handle("GET", "/v1/greeter/ada", &HeaderMap::new(), Bytes::new())
        .await;
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["message"], "hello ada");

    // Path var + query + response_body, end to end through the in-process tonic
    // Search method.
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
    assert_eq!(json["message"], "books/rust/5");

    // The native gRPC client still works after the REST traffic, proving both
    // ride the one shared in-memory connection.
    let pong = client
        .ping(PingRequest {})
        .await
        .expect("grpc ping ok (post-rest)")
        .into_inner();
    assert_eq!(pong.pong, "pong!");

    drop(_server);
}
