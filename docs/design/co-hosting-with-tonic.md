# Co-hosting grpc-gw with tonic in one process

> **Status: lower priority / future work.** The primary
> [grpc-gw design](./grpc-gateway-design.md) treats the gateway as an
> out-of-process reverse proxy. This note captures how to co-host the gateway
> and a tonic gRPC server in a *single* process when that's desired. It is not
> required for the initial milestones.

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

### Option B — Single port, content-type multiplexing (best UX)

Both tonic and `grpc-gw-server` are `tower::Service<Request<Body>>`. Combine
them with [`tower::steer::Steer`](https://docs.rs/tower/latest/tower/steer/),
keyed on whether the request is gRPC, and serve the combined service over an
HTTP/1+2 auto connection (`hyper_util::server::conn::auto::Builder`, already
the gateway's server foundation):

```rust
// Both arms must share Request/Response/Error types — box them to unify.
let grpc = /* tonic Routes as a tower service */;
let rest = grpc_gw::server::router(registry, backend, &cfg);

let svc = Steer::new(
    [BoxCloneService::new(grpc), BoxCloneService::new(rest)],
    |req: &Request<Body>, _services: &[_]| {
        let is_grpc = req
            .headers()
            .get(CONTENT_TYPE)
            .map(|v| v.as_bytes().starts_with(b"application/grpc"))
            .unwrap_or(false);
        if is_grpc { 0 } else { 1 }
    },
);
// serve `svc` with hyper_util auto::Builder (h1 + h2) on one listener
```

Clients hit one port for both gRPC and JSON.

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
expect **typed prost messages**, while `grpc-gw` builds **rust-protobuf
dynamic messages** via reflection. Bridging them means serializing the dynamic
message to bytes and re-parsing into the prost type anyway — exactly what the
gRPC hop already does, minus the transport. You save the framing, not the
encode/decode, and you give up the fully dynamic, descriptor-driven design.

### Why in-memory duplex is the sweet spot

You keep the dynamic gateway (no per-service codegen, no prost coupling) and
only swap the transport from TCP to an in-process pipe. The gateway code is
unchanged — only the client *connector* differs.

## Recommended shape

```text
            :8080  (hyper auto h1+h2)
               │
          Steer by content-type
        ┌──────┴───────┐
   application/grpc   else
        │              │
   tonic Routes   grpc-gw-server ──► gRPC client over tokio::duplex ─┐
        ▲                                                            │
        └──────────────── same in-process tonic Routes ◄────────────┘
```

One listener, content-type steering at the front, and the gateway loops back
into the *same* tonic `Routes` over an in-memory pipe.

## Caveats

- `Steer` requires both arms to share `Request`/`Response`/`Error` types — box
  them (`BoxCloneService`) to unify.
- gRPC needs HTTP/2; ensure the auto connection allows h2 prior-knowledge
  (gRPC clients don't perform an h1→h2 upgrade).
- gRPC-Web is a *third* content-type (`application/grpc-web`) — add it to the
  steering predicate and route to tonic with the `tonic-web` layer.
- In-memory duplex means the gateway and backend share the process's failure
  domain — fine for co-hosting, but you lose the independent-restart property
  of a true sidecar.

## Reference

- Primary design: [grpc-gateway-design.md](./grpc-gateway-design.md)
- [`tower::steer`](https://docs.rs/tower/latest/tower/steer/)
- [`tokio::io::duplex`](https://docs.rs/tokio/latest/tokio/io/fn.duplex.html)
- [`tonic::transport::Endpoint::connect_with_connector`](https://docs.rs/tonic/latest/tonic/transport/struct.Endpoint.html)
