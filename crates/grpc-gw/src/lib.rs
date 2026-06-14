//! grpc-gw — a dynamic gRPC↔JSON transcoding reverse proxy.
//!
//! See `docs/design/grpc-gateway-design.md` for the architecture and
//! `docs/design/m1-scope.md` for the current (M1) build scope.
//!
//! At this early stage the crate exposes [`descriptor`] (the Spike 0 proof:
//! extracting `google.api.http` annotations from a runtime-loaded
//! `FileDescriptorSet` with no generated annotation types), [`routes`] (the
//! M1 route table lowered from those annotations plus synthesized defaults),
//! [`transcode`] (JSON ⇄ dynamic message), [`status`] (gRPC code → HTTP
//! mapping and the error envelope), [`proxy`] (a tonic-backed gRPC client
//! driven over a `Channel`), and [`gateway`] (the `Gateway` service that ties
//! them together, plus the Tier-1 `serve_connection` entry point).

pub mod descriptor;
pub mod gateway;
pub mod proxy;
pub mod routes;
pub mod status;
pub mod template;
pub mod transcode;

pub use descriptor::{extract_http_rules, DescriptorError, HttpPattern, HttpRule, MethodHttp};
pub use gateway::{
    serve_connection, BuildError, Gateway, GatewayBuilder, GatewayOptions, ServeError,
};
pub use proxy::{GrpcClient, GrpcReply, ProxyError};
pub use routes::{BodySelector, Route, RouteBinding, RouteConflict, RouteTable};
pub use status::{Code, ErrorEnvelope};
pub use template::{PathTemplate, TemplateError};
pub use transcode::{
    bind_field_path, decode_request_body, encode_response_json, BindError, BindMode, JsonOptions,
    TranscodeError,
};
