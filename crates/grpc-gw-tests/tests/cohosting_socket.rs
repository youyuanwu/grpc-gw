//! Co-hosting the dynamic gateway and a **real tonic gRPC server** in a single
//! process, behind a single port — using **stock tonic APIs**.
//!
//! The tonic `GreeterServer` is turned into a [`tonic::service::Routes`] (an
//! `axum::Router`); the [`Gateway`] — itself a `tower::Service` — is mounted as
//! that router's `fallback_service`, and the combined router is wrapped back
//! into a `Routes` so tonic's own HTTP/2 server runs it on one ephemeral port:
//!
//! ```text
//!             :PORT  (tonic Server, HTTP/2)
//!                │
//!          axum::Router  (path routing)
//!        ┌───────┴────────────────┐
//!  /greeter.v1.Greeter/*    else → fallback_service
//!        │                        │
//!   tonic Greeter          grpc-gw Gateway ──► gRPC client ─┐ (dials :PORT)
//!        ▲                                                  │
//!        └──────────── same in-process tonic ◄──────────────┘
//! ```
//!
//! Routing is by **path**: native gRPC owns the `/pkg.Svc/Method` paths, and the
//! gateway serves the REST `/v1/...` bindings via the fallback. (A JSON *default
//! binding* on a gRPC method path can't be co-hosted this way — that shares one
//! path between JSON and gRPC and needs content-type steering, e.g.
//! `tower::steer::Steer`.)
//!
//! tonic's transport server is HTTP/2 only, so native gRPC and REST JSON both
//! ride h2c on the one port. The test drives it two ways — a typed tonic
//! [`GreeterClient`] (native gRPC) and an HTTP/2 JSON client (REST) — and the
//! gateway's backend hop loops back into the same port.
//!
//! The in-memory backend variant (no loopback socket on the gRPC hop) lives in
//! `cohosting_inprocess.rs`.

mod common;

use bytes::Bytes;
use tokio::sync::oneshot;
use tonic::transport::server::TcpIncoming;

use grpc_gw::{Gateway, GatewayOptions, GrpcClient};
use grpc_gw_tests::greeter::greeter_server::GreeterServer;
use grpc_gw_tests::greeter::{HelloRequest, PingRequest};
use grpc_gw_tests::GREETER_PB;

use common::{grpc_client, rest_call, Cohosted, GreeterImpl};

/// Stand up the co-hosted server with **stock tonic APIs** and serve it on an
/// ephemeral port. The tonic `GreeterServer` becomes a
/// [`tonic::service::Routes`] (an `axum::Router`); the [`Gateway`] is the
/// router's `fallback_service`, and the whole thing is wrapped back into a
/// `Routes` and served by tonic's HTTP/2 server. The gateway's backend hop
/// dials that same port.
async fn spawn_cohosted() -> Cohosted {
    let tcp = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind tcp");
    let addr = tcp.local_addr().expect("local addr");

    // The gateway dials the very port we're about to serve: its transcoded gRPC
    // call re-enters the router and is path-routed to the in-process tonic svc.
    let backend = GrpcClient::plaintext(format!("http://{addr}").parse().unwrap()).unwrap();
    let gateway = Gateway::builder(GREETER_PB.to_vec())
        .backend(backend)
        .options(GatewayOptions::default())
        .build()
        .expect("gateway builds");

    // `Routes` registers the gRPC method paths (`/greeter.v1.Greeter/*`) on an
    // axum router; mounting the gateway as `fallback_service` sends everything
    // else (the REST `/v1/...` bindings) to the transcoder. Wrapping the router
    // back into `Routes` lets tonic's own server drive it.
    let app = tonic::service::Routes::new(GreeterServer::new(GreeterImpl))
        .into_axum_router()
        .fallback_service(gateway);
    let routes = tonic::service::Routes::from(app);

    // tonic's transport server is HTTP/2 only, so native gRPC (h2c) and REST
    // JSON (h2c) share the one port over HTTP/2. A oneshot drives graceful
    // shutdown via `serve_with_incoming_shutdown`.
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let accept = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .serve_with_incoming_shutdown(routes, tcp, async {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    Cohosted::new(addr, shutdown_tx, accept)
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

    // NOTE: the gateway's JSON *default binding* for unannotated methods lives
    // on the gRPC method path (`POST /greeter.v1.Greeter/Ping`), which path
    // routing hands to the native tonic service instead. Co-hosting that JSON
    // binding on a shared gRPC path needs content-type steering, so it isn't
    // exercised here — the gateway serves the annotated `/v1/...` REST surface.

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

    // Graceful shutdown: drop the client so its connection closes, then signal
    // the server and await a clean drain.
    drop(client);
    server.shutdown().await;
}
