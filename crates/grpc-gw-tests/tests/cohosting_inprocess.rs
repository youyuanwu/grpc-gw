//! Co-hosting with an **in-memory backend hop**: the REST front door runs on a
//! real TCP port, but the gateway's transcoded gRPC call reaches the tonic
//! service over an in-memory duplex pipe ([`grpc_gw_util::in_process_channel`])
//! — so there is no loopback socket on the REST→gRPC path.
//!
//! A single `GreeterServer` is shared (it is `Clone`) between the in-memory
//! channel and the TCP front door, and native gRPC is served by the front
//! itself. The inbound request is the only OS socket on the REST path:
//!
//! ```text
//!   gRPC client ──TCP──┐
//!                      ▼
//!   REST client ──TCP──► :PORT (tonic Server, HTTP/2)
//!                          ├─ /pkg.Svc/* ───► tonic Greeter  (on the front)
//!                          └─ else → gateway ──► gRPC client
//!                                                  │ (in-memory duplex)
//!                                    tonic Greeter ◄┘  (no socket)
//! ```
//!
//! The single-port, loopback-socket backend variant lives in
//! `cohosting_socket.rs`.

mod common;

use bytes::Bytes;
use tokio::sync::oneshot;
use tonic::transport::server::TcpIncoming;

use grpc_gw::{Gateway, GatewayOptions, GrpcClient};
use grpc_gw_tests::greeter::greeter_server::GreeterServer;
use grpc_gw_tests::greeter::{HelloRequest, PingRequest};
use grpc_gw_tests::GREETER_PB;

use common::{grpc_client, rest_call, Cohosted, GreeterImpl};

/// Serve a **co-hosted** front door (native gRPC + REST) for `gateway` on an
/// ephemeral **TCP** port via stock tonic. The provided `greeter` (a tonic
/// `GreeterServer`) owns the gRPC method paths and `gateway` is the
/// `fallback_service` for the REST surface; the combined router is served by
/// tonic's HTTP/2 server. Unlike the socket-backed variant, the gateway's
/// backend is wired to an in-memory pipe by the caller, so the only socket is
/// the inbound one.
async fn spawn_rest_front(greeter: GreeterServer<GreeterImpl>, gateway: Gateway) -> Cohosted {
    let tcp = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind tcp");
    let addr = tcp.local_addr().expect("local addr");
    let app = tonic::service::Routes::new(greeter)
        .into_axum_router()
        .fallback_service(gateway);
    let routes = tonic::service::Routes::from(app);
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
async fn cohosted_inprocess_duplex_backend_has_no_socket_hop() {
    // A single `GreeterServer` instance is shared (it is `Clone`) by both the
    // in-memory duplex channel and the TCP front door, so native gRPC and the
    // gateway's REST backend hit the very same service object.
    let greeter = GreeterServer::new(GreeterImpl);

    // An in-memory duplex transport (no TCP). `in_process_channel` hands back
    // the client `Channel` plus the `Incoming` connection stream; the caller
    // decides how to serve it. `Incoming` is a `Stream` of connections, so we
    // serve the shared `greeter` over it with tonic's own server (graceful
    // shutdown wired in by the helper).
    let (channel, incoming) = grpc_gw_util::in_process_channel();
    let inproc = common::spawn_inprocess_backend(greeter.clone(), incoming);

    // The gateway's backend rides that in-memory pipe, so a transcoded REST
    // request reaches gRPC without a second (loopback) TCP hop.
    let gateway = Gateway::builder(GREETER_PB.to_vec())
        .backend(GrpcClient::with_channel(channel))
        .options(GatewayOptions::default())
        .build()
        .expect("gateway builds");

    // Co-hosted TCP front door: native gRPC *and* REST share the one port. The
    // front serves gRPC itself (the same `greeter`); REST is transcoded by the
    // gateway and hops to the in-memory tonic service. The inbound request is
    // the only OS socket on the REST path.
    let front = spawn_rest_front(greeter, gateway).await;
    let addr = front.addr;

    // --- Native gRPC over the front TCP port (served on the front itself). ---
    let mut client = grpc_client(addr).await;
    let reply = client
        .say_hello(HelloRequest {
            name: "grpc".to_owned(),
        })
        .await
        .expect("grpc say_hello ok")
        .into_inner();
    assert_eq!(reply.message, "hello grpc");

    // --- REST over TCP → gateway → in-memory pipe → tonic SayHello. ---
    let (status, body) = rest_call(addr, "GET", "/v1/greeter/ada", Bytes::new()).await;
    assert_eq!(status, 200);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["message"], "hello ada");

    // Path var + query + response_body, end to end through the in-memory tonic
    // Search method — REST over TCP but no TCP on the backend hop.
    let (status, body) =
        rest_call(addr, "GET", "/v1/search/books?q=rust&limit=5", Bytes::new()).await;
    assert_eq!(status, 200);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["message"], "books/rust/5");

    // The native gRPC client still works after the REST traffic, proving both
    // the front's gRPC arm and the gateway's in-memory backend stay live.
    let pong = client
        .ping(PingRequest {})
        .await
        .expect("grpc ping ok (post-rest)")
        .into_inner();
    assert_eq!(pong.pong, "pong!");

    // Graceful shutdown: drop the client, then stop the front (which drops the
    // gateway and its backend channel, closing the in-memory connection), then
    // drain the in-process backend.
    drop(client);
    front.shutdown().await;
    inproc.shutdown().await;
}
