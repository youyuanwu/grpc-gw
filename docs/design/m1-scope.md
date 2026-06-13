# grpc-gw — M1 scope (unary happy path)

> Implementer-facing companion to the [architecture design](./grpc-gateway-design.md).
> The design doc spans M1–M4; **this doc is the buildable M1 slice** — what
> ships, what explicitly does not, the acceptance bar, and the one spike that
> gates everything else. When the two disagree, the design doc is authoritative
> on *architecture* and this doc is authoritative on *M1 boundaries*.

## Goal

Stand up the smallest end-to-end gateway that transcodes **unary** RPCs over
JSON and is wire-compatible with Go grpc-gateway for that subset: load a
descriptor set (file *or* reflection), expose every method as a JSON endpoint,
forward to the backend over h2, and render canonical proto3 JSON back.

The litmus test: a client written against a Go grpc-gateway fronting the same
proto gets byte-compatible responses from grpc-gw for unary, unannotated and
`body:"*"`-annotated methods.

## In scope

| Area | M1 cut | Design reference |
| ---- | ------ | ---------------- |
| Descriptor load | `.pb` file **and** gRPC server reflection → `DescriptorRegistry` | [Descriptor loading](./grpc-gateway-design.md#descriptor-loading) |
| Route table | Primary bindings with `body:"*"` + the synthesized default unbound-method binding (`POST /pkg.Svc/Method`) | [Default binding policy](./grpc-gateway-design.md#default-binding-policy-unannotated-methods) |
| Request transcode | Whole JSON body → input dynamic message (`merge_from_str`) | [Request transcoding](./grpc-gateway-design.md#request-transcoding) |
| gRPC client | `hyper-util` h2 `Client` + 5-byte gRPC framing, metadata forward, trailer read | [gRPC client & framing](./grpc-gateway-design.md#grpc-client--framing) |
| Response transcode | Output dynamic message → canonical proto3 JSON (`200 application/json`) | [Response transcoding](./grpc-gateway-design.md#response-transcoding) |
| Status mapping | gRPC code → HTTP + Status-proto JSON error envelope (all 16 codes) | [Status & error mapping](./grpc-gateway-design.md#status--error-mapping) |
| Header forwarding | Static default allow-list only (no custom matchers yet) | [Header / metadata forwarding](./grpc-gateway-design.md#header--metadata-forwarding) |
| API boundary | Tier-1 `serve_connection` + Tier-3 `Gateway::builder` construction | [Library API boundary](./grpc-gateway-design.md#library-api-boundary-streams-not-config) |
| Introspection | `grpc-gw routes` and `grpc-gw check` | [Introspection & validation](./grpc-gateway-design.md#introspection--validation) |
| Binary | `bin/grpc-gw` with the [config sketch](./grpc-gateway-design.md#configuration-sketch) (descriptor + backend + listen) | — |

## Explicitly NOT in M1

Deferred to keep the first cut honest — these are real features, just later:

- **No path templates** beyond the literal gRPC wire path. No single-/multi-
  segment captures, field-path captures, or custom verbs → **M2**.
- **No `body:"field"` / `response_body` selectors**, **no query-param field-path
  expansion**, **no path-variable binding** → **M2**.
- **No `additional_bindings`** → **M2**.
- **No pluggable hooks** (custom `Marshaler`, `ErrorHandler`, header matchers,
  `Metadata`, `Grpc-Metadata-*`/`Grpc-Trailer-*` passthrough) → **M2**.
- **No streaming** (server-streaming NDJSON/SSE) → **M3**. M1 handles unary
  methods only; a `server_streaming: true` method is loaded but its endpoint
  returns `501 Not Implemented`.
- **No observability stack** (OpenTelemetry, Prometheus) and **no hot reload**
  → **M4**. Basic startup/error logging is fine.
- **No TLS convenience wiring** in the binary; embedders can still wrap streams
  per Tier 1, but the `tls` feature is **M4** polish.
- **No OpenAPI emit** (separate `grpc-gw-openapi` binary) → see
  [openapi-generation.md](./openapi-generation.md).

## Spike 0 — de-risk first (blocks the route table)

Before any routing work, confirm we can **decode the `google.api.http`
extension** (`MethodOptions` extension field **72295728**) from a descriptor
set via `protobuf::ext`. The entire route table depends on it.

- **Success:** read the `HttpRule` (at least `post`/`get` + `body`) off a real
  annotated `MethodDescriptor`.
- **Fallback if `protobuf::ext` is awkward:** decode the raw `UnknownFields` of
  `MethodOptions` for field 72295728 and parse the `HttpRule` message manually.
- **Output:** a one-paragraph note in the PR confirming which path works, so M2
  template work builds on a known-good extraction.

This is task zero; nothing else in M1 is unblocked until it resolves.

### Spike 0 — findings (resolved 2026-06-13)

**Outcome: the `UnknownFields` path works; `protobuf::ext` is not viable for a
dynamic gateway.** The `protobuf` crate's `ext` module (3.7.2) is a documented
"stopgap" that requires a generated `ExtFieldOptional<MethodOptions, HttpRule>`
constant — i.e. codegen of `google/api/annotations.proto` — which conflicts
with the design's "no generated annotations" goal. Instead, when the crate
parses a `FileDescriptorSet`, the `(google.api.http)` option is unknown to the
generated `MethodOptions` struct and is preserved as a length-delimited blob in
`MethodOptions.special_fields.unknown_fields().get(72295728)`. That blob is the
serialized `HttpRule`, which we hand-decode (its `pattern` oneof, `body`,
`response_body`, and `additional_bindings`) with a `CodedInputStream` — no
`protoc`, no generated annotation types. Implemented in
[`crates/grpc-gw/src/descriptor.rs`](../../crates/grpc-gw/src/descriptor.rs) and
verified by [`tests/spike_http_rule.rs`](../../crates/grpc-gw/tests/spike_http_rule.rs)
(GET + path template, POST + `body` + `additional_bindings`, and an unannotated
method with no rule). M2 path-template work can build on this extraction.


## Acceptance criteria

M1 is done when all hold:

1. **Conformance vs. Go.** For a fixture proto with (a) an unannotated method
   and (b) a `body:"*"`-annotated method, responses from grpc-gw and a Go
   grpc-gateway over the same backend are byte-identical for a representative
   set of messages (scalars, nested, repeated, map, enum, int64, a WKT
   timestamp).
2. **Both descriptor sources.** The same suite passes whether the registry was
   loaded from a `.pb` file or via `--reflection` against the live backend.
3. **Status mapping.** Each of the 16 gRPC codes returns the correct HTTP
   status and a Status-proto JSON envelope; `grpc-status-details-bin` details
   are rendered.
4. **Introspection.** `grpc-gw routes` lists every method's resolved endpoint;
   `grpc-gw check` exits `0` on a valid set and non-zero (with a useful message)
   on a set with a route conflict or unresolved field path.
5. **Embedding.** A ~5-line `Gateway::builder(...).backend(...).build()` example
   serves a request through Tier-1 `serve_connection` in a test.
6. **Cancellation.** Dropping the inbound connection cancels the upstream gRPC
   stream (no leaked backend call) — verified with a hanging backend method.

## Suggested task order

1. **Spike 0** — `google.api.http` extension decoding.
2. Descriptor load: `.pb` parse → registry; then reflection client → same
   registry. `grpc-gw check` falls out of the validation pass.
3. Route table for default + `body:"*"` bindings; `grpc-gw routes`.
4. Request transcode (whole-body) + gRPC client/framing + response transcode.
5. Status & error mapping.
6. Tier-3 builder + Tier-1 `serve_connection` + thin `bin/grpc-gw`.
7. Conformance harness against Go grpc-gateway; close the acceptance list.

## References

- Architecture: [grpc-gateway-design.md](./grpc-gateway-design.md)
- OpenAPI (separate track): [openapi-generation.md](./openapi-generation.md)
- Co-hosting (later): [co-hosting-with-tonic.md](./co-hosting-with-tonic.md)
