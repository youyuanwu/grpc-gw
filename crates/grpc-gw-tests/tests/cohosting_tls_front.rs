//! Co-hosting with **TLS terminated at the TCP front door** (openssl) and a
//! **plaintext in-process backend hop**.
//!
//! This is the realistic arrangement: TLS protects the network-facing socket,
//! and the gateway's transcoded gRPC call reaches the tonic service over an
//! in-memory [`tokio::io::duplex`] pipe in plaintext (encrypting a stream that
//! never leaves the process buys nothing).
//!
//! The front door is served by `tonic`'s own HTTP/2 server over a
//! [`tonic-tls`] openssl `TlsIncoming`, so the native gRPC service and the
//! gateway's REST surface share one TLS port (HTTP/2). Because tonic's
//! transport server is HTTP/2-only, the REST client speaks JSON over HTTP/2 TLS
//! too. The cert is generated at test time with [`rcgen`].
//!
//! ```text
//!   gRPC client ──TLS/h2──┐
//!                         ▼
//!   REST client ──TLS/h2──► :PORT (tonic Server + openssl)
//!                             ├─ /pkg.Svc/* ───► tonic Greeter (on the front)
//!                             └─ else → gateway ──► gRPC client
//!                                                    │ (in-memory duplex, plaintext)
//!                                      tonic Greeter ◄┘  (no socket)
//! ```

mod common;

use std::net::SocketAddr;
use std::pin::Pin;

use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use openssl::pkey::{PKey, Private};
use openssl::ssl::{
    select_next_proto, AlpnError, SslAcceptor, SslConnector, SslMethod, SslVerifyMode,
};
use openssl::x509::X509;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tonic::transport::server::TcpIncoming;
use tonic::transport::Endpoint;
use tonic_tls::openssl::ALPN_H2_WIRE;

use grpc_gw::{Gateway, GatewayOptions, GrpcClient};
use grpc_gw_tests::greeter::greeter_client::GreeterClient;
use grpc_gw_tests::greeter::greeter_server::GreeterServer;
use grpc_gw_tests::greeter::{HelloRequest, PingRequest};
use grpc_gw_tests::GREETER_PB;

use common::{Cohosted, GreeterImpl};

/// Generate a self-signed `localhost` cert with rcgen and load it into openssl.
fn make_cert() -> (X509, PKey<Private>) {
    let key_pair = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert = X509::from_pem(key_pair.cert.pem().as_bytes()).unwrap();
    let key = PKey::private_key_from_pem(key_pair.signing_key.serialize_pem().as_bytes()).unwrap();
    (cert, key)
}

/// An openssl server acceptor that presents `cert`/`key` and negotiates h2.
fn make_acceptor(cert: &X509, key: &PKey<Private>) -> SslAcceptor {
    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    builder.set_private_key(key).unwrap();
    builder.set_certificate(cert).unwrap();
    builder.check_private_key().unwrap();
    builder.set_alpn_select_callback(|_ssl, alpn| {
        select_next_proto(ALPN_H2_WIRE, alpn).ok_or(AlpnError::NOACK)
    });
    builder.build()
}

/// An openssl client connector that offers h2 over ALPN. The test cert is
/// self-signed, so verification is disabled.
fn make_ssl_connector() -> SslConnector {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_alpn_protos(ALPN_H2_WIRE).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    builder.build()
}

/// Open a typed tonic client to the TLS front door (native gRPC over h2 + TLS).
async fn tls_grpc_client(addr: SocketAddr) -> GreeterClient<tonic::transport::Channel> {
    let ep = Endpoint::from_shared(format!("https://localhost:{}", addr.port())).unwrap();
    let transport = tonic_tls::TcpTransport::from_endpoint(&ep);
    let channel = ep
        .connect_with_connector(tonic_tls::openssl::TlsConnector::new(
            transport,
            make_ssl_connector(),
            "localhost".to_owned(),
        ))
        .await
        .expect("tls grpc channel connects");
    GreeterClient::new(channel)
}

/// Send one JSON request to the TLS front door over HTTP/2 and return
/// (status, body). The front is tonic's h2-only server, so REST also rides h2.
async fn rest_call_tls(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Bytes,
) -> (StatusCode, Bytes) {
    let tcp = TcpStream::connect(addr).await.unwrap();
    let ssl = make_ssl_connector()
        .configure()
        .unwrap()
        .into_ssl("localhost")
        .unwrap();
    let mut tls = tokio_openssl::SslStream::new(ssl, tcp).unwrap();
    Pin::new(&mut tls).connect().await.expect("tls handshake");

    let (mut sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(tls))
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let uri = format!("https://localhost:{}{}", addr.port(), path);
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(body))
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, bytes)
}

/// Serve the co-hosted front door (native gRPC + REST) for `gateway` on an
/// ephemeral **TLS** TCP port: `tonic`'s HTTP/2 server over a tonic-tls openssl
/// `TlsIncoming`. The gRPC method paths go to `greeter`; everything else falls
/// back to the gateway.
async fn spawn_tls_front(
    greeter: GreeterServer<GreeterImpl>,
    gateway: Gateway,
    acceptor: SslAcceptor,
) -> Cohosted {
    let tcp = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind tcp");
    let addr = tcp.local_addr().expect("local addr");

    // The combined service: gRPC routes + gateway as the REST fallback, as an
    // axum router, wrapped back into `tonic::service::Routes` so tonic's server
    // can serve it.
    let app = tonic::service::Routes::new(greeter)
        .into_axum_router()
        .fallback_service(gateway);
    let routes = tonic::service::Routes::from(app);

    let tls_incoming = tonic_tls::openssl::TlsIncoming::new(tcp, acceptor);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let accept = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .serve_with_incoming_shutdown(routes, tls_incoming, async {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    Cohosted::new(addr, shutdown_tx, accept)
}

#[tokio::test]
async fn cohosted_tls_front_with_plaintext_inprocess_backend() {
    let (cert, key) = make_cert();
    let acceptor = make_acceptor(&cert, &key);

    // A single `GreeterServer` shared (it is `Clone`) by the in-memory backend
    // and the TLS front door.
    let greeter = GreeterServer::new(GreeterImpl);

    // Plaintext in-memory backend hop: serve the shared `greeter` over the
    // `Incoming` with tonic's own server (graceful shutdown wired in by the
    // helper).
    let (channel, incoming) = grpc_gw_util::in_process_channel();
    let inproc = common::spawn_inprocess_backend(greeter.clone(), incoming);

    // The gateway's backend rides that plaintext in-memory pipe.
    let gateway = Gateway::builder(GREETER_PB.to_vec())
        .backend(GrpcClient::with_channel(channel))
        .options(GatewayOptions::default())
        .build()
        .expect("gateway builds");

    // TLS front door (native gRPC + REST), TLS terminated here.
    let front = spawn_tls_front(greeter, gateway, acceptor).await;
    let addr = front.addr;

    // --- Native gRPC over the TLS front port. ---
    let mut client = tls_grpc_client(addr).await;
    let reply = client
        .say_hello(HelloRequest {
            name: "tls".to_owned(),
        })
        .await
        .expect("grpc say_hello over tls ok")
        .into_inner();
    assert_eq!(reply.message, "hello tls");

    // --- REST over TLS/h2 → gateway → plaintext in-memory pipe → tonic. ---
    let (status, body) = rest_call_tls(addr, "GET", "/v1/greeter/ada", Bytes::new()).await;
    assert_eq!(status, 200);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["message"], "hello ada");

    // Path var + query + response_body, end to end over the TLS front.
    let (status, body) =
        rest_call_tls(addr, "GET", "/v1/search/books?q=rust&limit=5", Bytes::new()).await;
    assert_eq!(status, 200);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["message"], "books/rust/5");

    // --- Native gRPC over TLS still works after the REST traffic. ---
    let pong = client
        .ping(PingRequest {})
        .await
        .expect("grpc ping over tls ok (post-rest)")
        .into_inner();
    assert_eq!(pong.pong, "pong!");

    // Graceful shutdown: drop the client, stop the TLS front (which drops the
    // gateway and its in-memory backend channel), then drain the backend.
    drop(client);
    front.shutdown().await;
    inproc.shutdown().await;
}
