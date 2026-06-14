//! A minimal co-hosting example: run a real tonic gRPC server and the dynamic
//! `grpc-gw` JSON↔gRPC gateway in **one process on one port**, then call it
//! both ways.
//!
//! Run with:
//!
//! ```sh
//! cargo run -p grpc-gw-tests --example cohosting
//! ```
//!
//! `tonic::service::Routes` puts the gRPC methods on an axum router; the
//! gateway is mounted as the `fallback_service` for the REST surface; tonic's
//! HTTP/2 server serves the lot. The gateway's backend dials the same port, so
//! a REST request is transcoded to gRPC and handled by the in-process service.

use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http::Request;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpStream;
use tonic::transport::server::TcpIncoming;

use grpc_gw::{Gateway, GatewayOptions, GrpcClient};
use grpc_gw_tests::greeter::greeter_client::GreeterClient;
use grpc_gw_tests::greeter::greeter_server::GreeterServer;
use grpc_gw_tests::greeter::HelloRequest;
use grpc_gw_tests::{GreeterImpl, GREETER_PB};

#[tokio::main]
async fn main() {
    // Bind an ephemeral port for the co-hosted server.
    let tcp = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
    let addr = tcp.local_addr().expect("addr");

    // The gateway transcodes REST → gRPC and dials the very port we serve, so
    // its call loops back into the in-process tonic service.
    let backend = GrpcClient::plaintext(format!("http://{addr}").parse().unwrap()).unwrap();
    let gateway = Gateway::builder(GREETER_PB.to_vec())
        .backend(backend)
        .options(GatewayOptions::default())
        .build()
        .expect("gateway builds");

    // gRPC method paths go to the tonic service; everything else (the REST
    // `/v1/...` bindings) falls back to the gateway.
    let app = tonic::service::Routes::new(GreeterServer::new(GreeterImpl))
        .into_axum_router()
        .fallback_service(gateway);
    let routes = tonic::service::Routes::from(app);

    let server = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .serve_with_incoming(routes, tcp)
            .await;
    });

    println!("co-hosted gRPC + REST gateway listening on http://{addr}\n");

    // 1) Call it as native gRPC with the generated client.
    let mut client = GreeterClient::connect(format!("http://{addr}"))
        .await
        .expect("grpc connect");
    let reply = client
        .say_hello(HelloRequest {
            name: "grpc".to_owned(),
        })
        .await
        .expect("say_hello")
        .into_inner();
    println!("gRPC  SayHello(name=grpc) -> {}", reply.message);

    // 2) Call the same port as REST JSON; the gateway transcodes it.
    let body = rest_get(addr, "/v1/greeter/rest").await;
    println!("REST  GET /v1/greeter/rest -> {body}");

    server.abort();
}

/// Minimal h2c JSON GET against the co-hosted port (tonic's server is HTTP/2
/// only, so REST rides h2c too).
async fn rest_get(addr: std::net::SocketAddr, path: &str) -> String {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let req = Request::builder()
        .method("GET")
        .uri(path)
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8_lossy(&bytes).into_owned()
}
