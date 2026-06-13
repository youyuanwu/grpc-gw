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
//! mapping and the error envelope), and [`proxy`] (gRPC framing + the
//! `hyper-util` h2 client).

pub mod descriptor;
pub mod proxy;
pub mod routes;
pub mod status;
pub mod transcode;

pub use descriptor::{extract_http_rules, DescriptorError, HttpPattern, HttpRule, MethodHttp};
pub use proxy::{
    decode_single_frame, encode_frame, FramingError, GrpcClient, GrpcReply, ProxyError,
};
pub use routes::{BodySelector, Route, RouteBinding, RouteConflict, RouteTable};
pub use status::{Code, ErrorEnvelope};
pub use transcode::{decode_request_body, encode_response_json, JsonOptions, TranscodeError};
