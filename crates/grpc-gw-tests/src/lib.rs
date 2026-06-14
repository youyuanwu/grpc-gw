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

/// The compiled `greeter.proto` fixture as a serialized `FileDescriptorSet`,
/// built with `--include_imports` (so `google.api.http` and
/// `google.protobuf.Timestamp` resolve in the pool).
pub const GREETER_PB: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/greeter.pb"));
