//! Build script: compile the proto fixtures into a self-contained
//! `FileDescriptorSet` (`greeter.pb`) under `OUT_DIR` using `protoc`.
//!
//! The result is consumed by `src/lib.rs` via `include_bytes!`, so the binary
//! descriptor blob is generated on every build instead of being committed.
//!
//! It additionally generates typed tonic Greeter server/client stubs
//! (`greeter.v1.rs` under `OUT_DIR`, via `tonic-prost-build`) consumed by the
//! co-hosting integration test, which runs a real tonic gRPC server in the same
//! process as the dynamic gateway.
//!
//! `protoc` must be on `PATH` (or pointed to by the `PROTOC` env var). It
//! bundles the well-known types (`google/protobuf/*.proto`), so only our own
//! `proto/` include path is needed; `--include_imports` makes the set
//! self-contained (it carries `google/api/annotations.proto` so the
//! `google.api.http` extension resolves, plus `google/protobuf/timestamp.proto`).

use std::path::PathBuf;
use std::process::Command;

fn main() {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let proto_dir = manifest_dir.join("proto");
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let descriptor_path = out_dir.join("greeter.pb");

    // Rebuild whenever any proto in the tree changes.
    println!("cargo:rerun-if-changed={}", proto_dir.display());
    println!("cargo:rerun-if-env-changed=PROTOC");

    let protoc = std::env::var("PROTOC").unwrap_or_else(|_| "protoc".to_string());

    let status = Command::new(&protoc)
        .arg("-I")
        .arg(&proto_dir)
        .arg("--include_imports")
        .arg("--include_source_info")
        .arg("-o")
        .arg(&descriptor_path)
        .arg(proto_dir.join("greeter.proto"))
        .status()
        .unwrap_or_else(|e| panic!("failed to run `{protoc}` (set PROTOC or install protoc): {e}"));

    assert!(
        status.success(),
        "`{protoc}` failed to build the descriptor set (exit: {status})"
    );

    // Also generate typed tonic Greeter server/client stubs for the co-hosting
    // integration test (a real tonic server next to the dynamic gateway). This
    // uses tonic-prost-build's bundled protoc; it is independent of the raw
    // descriptor set above (which the gateway loads dynamically).
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto_dir.join("greeter.proto")], &[proto_dir])
        .expect("tonic-prost-build failed to generate Greeter stubs");
}
