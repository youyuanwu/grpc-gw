//! grpc-gw — a dynamic gRPC↔JSON transcoding reverse proxy.
//!
//! See `docs/design/grpc-gateway-design.md` for the architecture and
//! `docs/design/m1-scope.md` for the current (M1) build scope.
//!
//! At this early stage the crate exposes only [`descriptor`], which contains
//! the Spike 0 proof: extracting `google.api.http` annotations from a
//! runtime-loaded `FileDescriptorSet` with no generated annotation types.

pub mod descriptor;

pub use descriptor::{extract_http_rules, HttpPattern, HttpRule, MethodHttp};
