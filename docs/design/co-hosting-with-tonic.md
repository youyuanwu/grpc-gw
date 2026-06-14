# Co-hosting grpc-gw with tonic in one process

> **Status: implemented.** The primary
> [grpc-gw design](./grpc-gateway-design.md) treats the gateway as an
> out-of-process reverse proxy. This note captures how to co-host the gateway
> and a tonic gRPC server in a *single* process. The in-process transport seam
> ships in the [`grpc-gw-util`](../../crates/grpc-gw-util) crate
> (`in_process_channel` / `in_process_transport`); a runnable end-to-end demo
> lives in [`crates/grpc-gw-tests/examples/cohosting.rs`](../../crates/grpc-gw-tests/examples/cohosting.rs)
> (`cargo run -p grpc-gw-tests --example cohosting`).

## Two independent routing decisions

Running both in one process splits into two separate problems:

1. **Front-door routing** — how a client request on a port is dispatched to
   either the gRPC server or the REST gateway.
2. **Backend hop** — how the gateway reaches the in-process gRPC service when
   transcoding a REST call.

## Decision 1 — Front-door: one port or two?

gRPC and REST are distinguishable per-request:

- **gRPC** = HTTP/2, `POST`, `content-type: application/grpc[-web | +proto]`.
- **REST** = any method, `content-type: application/json` (or none), over
  HTTP/1.1 *or* HTTP/2.

### Option A — Two ports (simplest)

tonic on `:50051`, gateway on `:8080`. No multiplexing logic; loses the
single-endpoint convenience. Good default when operational simplicity wins.

### Option B — Single port, path fallback (shipped)

Both tonic and the gateway are `tower::Service<http::Request<_>>`, so they
compose with stock APIs — no hand-rolled `Steer`. `tonic::service::Routes` is
an `axum::Router` that registers the gRPC method paths (`/pkg.Svc/{*rest}`);
mount the [`Gateway`](./grpc-gateway-design.md#tier-2--decoded-messages--frames)
as its `fallback_service`, so any request that isn't a known gRPC method path
(the REST `/v1/...` bindings) falls through to the gateway:

```rust
let app = tonic::service::Routes::new(GreeterServer::new(svc))
    .into_axum_router()
    .fallback_service(gateway);   // gateway is a tower::Service
let routes = tonic::service::Routes::from(app);

tonic::transport::Server::builder()
    .serve_with_incoming(routes, incoming)   // one listener, HTTP/2
    .await?;
```

Clients hit one port for both gRPC and JSON. Because tonic's server is
**HTTP/2-only** (`serve_with_incoming` does no h1→h2 upgrade), the REST surface
rides **h2c** (or h2-over-TLS) on this port — there is no HTTP/1.1 REST without
fronting it with `axum::serve` instead. Path fallback also can't disambiguate a
JSON request whose *default* binding lands on a real gRPC method path; route
those via an explicit `google.api.http` binding (or content-type steering)
rather than the default `POST /pkg.Svc/Method`.

## Decision 2 — Backend hop: how the gateway reaches the in-process service

The primary design has the gateway open an HTTP/2 gRPC client to the backend.
In one process there are three options, best-to-worst on overhead:

| Option | How | Cost | Keeps dynamic design? |
| ------ | --- | ---- | --------------------- |
| **In-memory duplex** (recommended) | Serve tonic `Routes` over a `tokio::io::duplex` pipe; gateway client connects via `Endpoint::connect_with_connector` returning the other half | HTTP/2 framing + protobuf encode/decode, **no TCP/socket** | yes |
| **Loopback TCP** | Gateway dials `http://127.0.0.1:50051` like any backend | Full localhost TCP + h2 + encode/decode | yes |
| **Direct trait dispatch** | Gateway calls the tonic service trait in-process, no gRPC at all | lowest framing, but see below | **no** |

### Why not direct trait dispatch

It looks like the cheapest option but fights the architecture: tonic handlers
expect **typed prost messages**, while `grpc-gw` builds **`prost-reflect`
dynamic messages** via reflection. Bridging them means serializing the dynamic
message to bytes and re-parsing into the prost type anyway — exactly what the
gRPC hop already does, minus the transport. You save the framing, not the
encode/decode, and you give up the fully dynamic, descriptor-driven design.

### Why in-memory duplex is the sweet spot

You keep the dynamic gateway (no per-service codegen, no prost coupling) and
only swap the transport from TCP to an in-process pipe. The gateway code is
unchanged — only the client *connector* differs. This is what
[`grpc-gw-util`](../../crates/grpc-gw-util) packages:

```rust
use grpc_gw_util::in_process_channel;

// `channel` dials an in-memory `tokio::io::duplex` backend; serve your tonic
// service over `incoming`. No socket, no port for the backend hop.
let (channel, incoming) = in_process_channel();
let gateway = Gateway::builder(descriptor_bytes)
    .backend(GrpcClient::with_channel(channel))
    .build()?;

tonic::transport::Server::builder()
    .serve_with_incoming(Routes::new(GreeterServer::new(svc)), incoming)
    .await?;
```

`in_process_transport()` exposes the lower-level `(Incoming, Connector)` pair
if you want to drive the `tonic::transport::Endpoint` yourself.

## Recommended shape

```text
            one port  (tonic HTTP/2 server)
               │
        tonic Routes (gRPC method paths)
        ┌──────┴───────┐
   /pkg.Svc/Method    else (REST /v1/... bindings)
        │              │
   tonic service   Gateway (fallback_service) ──► gRPC client over tokio::duplex ─┐
        ▲                                                                         │
        └──────────── same in-process tonic service (in_process_channel) ◄────────┘
```

One listener, gRPC method paths handled directly by tonic with the gateway as
the path `fallback_service`, and the gateway loops back into the *same*
in-process tonic service over an in-memory pipe.

## Caveats

- The single-port front is **HTTP/2-only** (tonic's server). gRPC needs h2
  prior-knowledge anyway, and the REST surface co-hosts as h2c (or h2-TLS); an
  HTTP/1.1 REST front requires `axum::serve` instead of tonic's server.
- gRPC-Web is a *third* content-type (`application/grpc-web`) — route it to
  tonic with the `tonic-web` layer if needed.
- In-memory duplex means the gateway and backend share the process's failure
  domain — fine for co-hosting, but you lose the independent-restart property
  of a true sidecar.
- Graceful shutdown ordering matters: drop the gateway/front (which releases
  the duplex client) before draining the backend, or an idle keep-alive h2
  connection can stall the drain. See the cohosting tests for the pattern.

## Reference

- Primary design: [grpc-gateway-design.md](./grpc-gateway-design.md)
- Shipped helper: [`grpc-gw-util`](../../crates/grpc-gw-util)
  (`in_process_channel` / `in_process_transport`)
- Runnable demo: [`crates/grpc-gw-tests/examples/cohosting.rs`](../../crates/grpc-gw-tests/examples/cohosting.rs)
- [`tokio::io::duplex`](https://docs.rs/tokio/latest/tokio/io/fn.duplex.html)
- [`tonic::transport::Endpoint::connect_with_connector`](https://docs.rs/tonic/latest/tonic/transport/struct.Endpoint.html)
