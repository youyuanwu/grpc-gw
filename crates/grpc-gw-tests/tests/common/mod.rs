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
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

use grpc_gw_tests::greeter::greeter_client::GreeterClient;
use grpc_gw_tests::greeter::greeter_server::Greeter;
use grpc_gw_tests::greeter::{
    HelloReply, HelloRequest, PingReply, PingRequest, SearchRequest, SearchResponse,
    UpdateGreetingRequest,
};

/// A real tonic service. Each method echoes its request into the reply so the
/// REST-side assertions can observe exactly what the gateway bound from the
/// path/body/query (mirrors the fake backend used by the unary tests).
#[derive(Default)]
pub struct GreeterImpl;

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

/// A running co-hosted server. Dropping it aborts the `axum::serve` task (and
/// thus all in-flight connections); the test owns the lifetime via this guard.
pub struct Cohosted {
    pub addr: SocketAddr,
    pub accept: tokio::task::JoinHandle<()>,
}

impl Drop for Cohosted {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

/// Open a typed tonic client to the co-hosted port (native gRPC over h2c).
pub async fn grpc_client(addr: SocketAddr) -> GreeterClient<tonic::transport::Channel> {
    GreeterClient::connect(format!("http://{addr}"))
        .await
        .expect("tonic client connects")
}

/// Send one HTTP/1.1 request to the co-hosted port and return (status, body).
pub async fn rest_call(
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
