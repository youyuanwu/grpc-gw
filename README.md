# grpc-gw

A dynamic **gRPC ↔ JSON transcoding reverse proxy** in the spirit of Go's
[grpc-gateway](https://github.com/grpc-ecosystem/grpc-gateway), written in Rust.

Point it at an existing gRPC backend and a protobuf descriptor set, and it
serves a REST/JSON front door — **no per-service codegen**. The same binary
transcodes any backend whose `FileDescriptorSet` it is handed, driven entirely
by runtime reflection via [`prost-reflect`](https://crates.io/crates/prost-reflect).

## Features

- **Zero codegen** — drop in a pre-built `.pb` descriptor set and go; no
  `protoc` at runtime, no generated annotation types.
- **`google.api.http` aware** — path templates, `additional_bindings`, `body`
  selectors, and query-parameter field-path expansion, matching grpc-gateway.
- **Canonical proto3 JSON** — int64-as-string, `SCREAMING_SNAKE_CASE` enums,
  RFC 3339 timestamps, well-known-type encodings.
- **Library-first** — embed it as a `tower::Service`, or co-host it in the same
  process as a `tonic` gRPC server (see [co-hosting](#co-hosting-with-tonic)).
- **Introspectable** — resolved route tables can be printed and descriptor sets
  validated offline.

> **Scope (M1):** unary RPCs only. Server-streaming returns `501`;
> client/bidi streaming and gRPC-Web are out of scope for now.

## Embedding (library)

`grpc-gw` is a `tower::Service<http::Request<B>>`, so it composes with hyper,
axum, or any tower stack:

```rust
use grpc_gw::{Gateway, GatewayOptions, GrpcClient};

let client = GrpcClient::plaintext("http://127.0.0.1:50051".parse()?)?;
let gateway = Gateway::builder(descriptor_bytes)
    .backend(client)
    .options(GatewayOptions::default())
    .build()?;

// `gateway` is now a tower::Service / axum fallback you can mount and serve.
```

## Co-hosting with tonic

The `grpc-gw-util` crate lets the gateway talk to a `tonic` server in the same
process over an in-memory pipe (no socket hop), so a single port can serve both
native gRPC and transcoded REST:

```rust
use grpc_gw_util::in_process_channel;

// `channel` dials the in-process backend; serve your tonic service on `incoming`.
let (channel, incoming) = in_process_channel();
let gateway = Gateway::builder(descriptor_bytes)
    .backend(GrpcClient::with_channel(channel))
    .build()?;
```

See [crates/grpc-gw-tests/examples/cohosting.rs](crates/grpc-gw-tests/examples/cohosting.rs) for a runnable demo:

```sh
cargo run -p grpc-gw-tests --example cohosting
```

## Repository layout

| Crate           | Description                                                      |
| --------------- | -------------------------------------------------------------- |
| `grpc-gw`       | The library and `grpc-gw` binary (gateway core + CLI).          |
| `grpc-gw-util`  | In-process transport helpers for co-hosting with `tonic`.       |
| `grpc-gw-tests` | Integration tests, fixtures, and the co-hosting example.        |

Design notes live under [`docs/design/`](docs/design/), notably
[`grpc-gateway-design.md`](docs/design/grpc-gateway-design.md) and
[`co-hosting-with-tonic.md`](docs/design/co-hosting-with-tonic.md).

## License

[MIT](LICENSE)
