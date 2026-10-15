//! Test fixtures for grpc-gw.
//!
//! This crate exists to compile the `proto/` fixtures into a
//! `FileDescriptorSet` at build time (see `build.rs`) and expose the bytes to
//! the integration tests under `tests/`, so the generated `.pb` is never
//! committed and the main `grpc-gw` crate needs no `protoc` build dependency.
//!
//! It also hosts the shared [`backend`] harness (frame helpers, [`FrameList`]
//! body, and the abort-on-drop [`Backend`] guard) used by the integration
//! tests to stand up minimal in-process gRPC backends.

pub mod backend;

pub use backend::{deframe, frame, Backend, FrameList};

/// Typed tonic Greeter stubs (server + client) generated from
/// `proto/greeter.proto` by `build.rs`. Used by the co-hosting integration test
/// to run a real tonic gRPC server in the same process as the dynamic gateway.
pub mod greeter {
    tonic::include_proto!("greeter.v1");
}

/// A real tonic [`Greeter`](greeter::greeter_server::Greeter) service shared by
/// the co-hosting tests and examples. Each method echoes its request into the
/// reply so REST-side assertions can observe exactly what the gateway bound
/// from the path/body/query.
#[derive(Default)]
pub struct GreeterImpl;

#[tonic::async_trait]
impl greeter::greeter_server::Greeter for GreeterImpl {
    async fn say_hello(
        &self,
        request: tonic::Request<greeter::HelloRequest>,
    ) -> Result<tonic::Response<greeter::HelloReply>, tonic::Status> {
        let name = request.into_inner().name;
        Ok(tonic::Response::new(greeter::HelloReply {
            message: format!("hello {name}"),
        }))
    }

    async fn update_greeting(
        &self,
        request: tonic::Request<greeter::UpdateGreetingRequest>,
    ) -> Result<tonic::Response<greeter::HelloReply>, tonic::Status> {
        let req = request.into_inner();
        Ok(tonic::Response::new(greeter::HelloReply {
            message: format!("{}: {}", req.name, req.greeting),
        }))
    }

    async fn ping(
        &self,
        _request: tonic::Request<greeter::PingRequest>,
    ) -> Result<tonic::Response<greeter::PingReply>, tonic::Status> {
        Ok(tonic::Response::new(greeter::PingReply {
            pong: "pong!".to_owned(),
        }))
    }

    async fn search(
        &self,
        request: tonic::Request<greeter::SearchRequest>,
    ) -> Result<tonic::Response<greeter::SearchResponse>, tonic::Status> {
        let req = request.into_inner();
        Ok(tonic::Response::new(greeter::SearchResponse {
            result: Some(greeter::HelloReply {
                message: format!("{}/{}/{}", req.category, req.q, req.limit),
            }),
        }))
    }

    async fn echo(
        &self,
        request: tonic::Request<greeter::Kitchen>,
    ) -> Result<tonic::Response<greeter::Kitchen>, tonic::Status> {
        Ok(tonic::Response::new(request.into_inner()))
    }
}

/// The compiled `greeter.proto` fixture as a serialized `FileDescriptorSet`,
/// built with `--include_imports` (so `google.api.http` and
/// `google.protobuf.Timestamp` resolve in the pool).
pub const GREETER_PB: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/greeter.pb"));
