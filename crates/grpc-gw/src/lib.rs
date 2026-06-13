//! grpc-gw — a dynamic gRPC↔JSON transcoding reverse proxy.
//!
//! See `docs/design/grpc-gateway-design.md` for the architecture and
//! `docs/design/m1-scope.md` for the current (M1) build scope.
//!
//! At this early stage the crate exposes [`descriptor`] (the Spike 0 proof:
//! extracting `google.api.http` annotations from a runtime-loaded
//! `FileDescriptorSet` with no generated annotation types) and [`routes`] (the
//! M1 route table lowered from those annotations plus synthesized defaults).

pub mod descriptor;
pub mod routes;

pub use descriptor::{extract_http_rules, DescriptorError, HttpPattern, HttpRule, MethodHttp};
pub use routes::{BodySelector, Route, RouteBinding, RouteConflict, RouteTable};
