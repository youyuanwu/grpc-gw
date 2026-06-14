# grpc-gw — M2.1 path templates (routing & binding)

> Implementer-facing companion to the [architecture design](./grpc-gateway-design.md),
> the first buildable slice of **M2**. The design doc is authoritative on
> *architecture*; this doc is authoritative on the *path-template boundary* —
> what the parser/matcher/binder do, what they explicitly defer, and the
> acceptance bar.
>
> Sibling M2 slices (gRPC server reflection source, the Go cross-conformance
> harness, pluggable hooks) are independent and tracked separately. This slice
> is the keystone: `body:"field"`, query expansion, and `additional_bindings`
> all depend on the matcher + variable binder built here.

## Goal

Turn the route table's **opaque literal** paths into a real router. Today an
annotated `GET /v1/greeter/{name}` only string-equals the literal
`"/v1/greeter/{name}"` ([routes.rs](../../crates/grpc-gw/src/routes.rs) treats
`http_path` as opaque, and the gateway answers `501`/`404` for anything
templated). After this slice, `GET /v1/greeter/alice` matches that route,
captures `name = "alice"`, and binds it into the request `DynamicMessage`
before the upstream call — wire-compatible with Go grpc-gateway and the
[google.api.http transcoding spec](https://cloud.google.com/endpoints/docs/grpc/transcoding).

The litmus test: for a proto using path variables, `body:"field"`, query
parameters, and `additional_bindings`, grpc-gw routes and binds exactly as Go
grpc-gateway would, asserted against grpc-gw's own expected outputs.

## Prior art: `tonic-rest` (and why we can't reuse it)

[`tonic-rest`](https://github.com/zs-dima/tonic-rest) (`zs-dima`, 0.1.x) does
implement `google.api.http` REST transcoding for tonic — but as a **build-time
codegen** tool: its headline property is *"zero runtime reflection — all
handler code is generated at build time by `tonic-rest-build"`*. It emits Axum
handlers and `Router` routes per service, with `#[serde(with)]` adapters for
the WKTs and a `define_enum_serde!` macro, all bound to the server's generated
prost structs.

That is the **opposite architecture** to grpc-gw:

| | `tonic-rest` | grpc-gw |
| --- | --- | --- |
| When templates are resolved | Build time (codegen) | Runtime (descriptor-driven) |
| Path matching engine | Axum/`matchit` `Router` over generated routes | Our own matcher over the descriptor set |
| Coupling | Generated, per-service, in-process with the gRPC server | Dynamic, any `.pb`, out-of-process reverse proxy |
| Message access | Monomorphized prost structs + serde | `DynamicMessage` via `prost-reflect` |
| Field-path capture `{a.b.c}`, sub-patterns `{x=a/*/**}` | Lowered to Axum patterns at codegen | Must be matched + bound dynamically |

So `tonic-rest` is a useful **reference for the grammar and the canonical JSON
edge cases**, but its router is Axum-route emission tied to codegen; grpc-gw
routes a request against a descriptor set it loaded at startup, with no
generated types to hang routes on. We implement the matcher ourselves. (We also
can't lean on Axum/`matchit` directly: it models literal/`:single`/`*catchall`
segments but not field-path variable *names* like `{user.id}`, variables bound
to multi-segment sub-patterns like `{name=shelves/*/books/*}`, or custom verbs
`:undelete` — all of which the google.api.http grammar requires.)

## In scope

| Area | M2.1 cut |
| ---- | -------- |
| Template grammar | Full google.api.http grammar: literals, `*`, `**`, `{var}`, `{var=sub/pattern}`, field-path var names `{a.b.c}`, custom verbs `:verb` |
| Matcher | Per-HTTP-method router built once at startup; O(path segments) lookup; **declaration-order, first-match-wins** (Go grpc-gateway semantics) |
| Path-variable binding | Captured segments written into the input `DynamicMessage` by field path, with scalar coercion (number/bool/enum/string) |
| `body:"field"` | Parse the JSON body into the single named sub-field (not the whole message) |
| `response_body` | Render only the named field of the response as the JSON body |
| Query-param expansion | `?a.b=c&tags=x&tags=y` → fields not already bound by path or body, by field path, with repeated-field support |
| `additional_bindings` | Each registered as its own matchable binding on the same route |
| Conflict detection | Exact duplicate `(method, path)` is an error (as M1); structural **overlap** is, by default, a *shadowing warning* (the earlier-declared route wins, matching Go). A `strict_routes` flag promotes shadowing warnings to errors. |
| Introspection | `grpc-gw routes` shows resolved templates + which vars bind which fields |

## Explicitly NOT in M2.1

- **No streaming** — server-streaming endpoints still `501` (→ M3).
- **No pluggable hooks** (custom marshaler, header matchers, metadata) — that's
  a separate M2 slice; this slice keeps the M1 static header allow-list.
- **No gRPC server reflection source / conformance harness** — separate M2
  slices; this slice is validated against grpc-gw's own expected outputs.
- **No OpenAPI emit** — separate track.
- **No `google.protobuf.Any` resolution** beyond what M1 already does.

## Grammar

The canonical grammar (from `google/api/http.proto`):

```ebnf
Template  = "/" Segments [ Verb ] ;
Segments  = Segment { "/" Segment } ;
Segment   = "*" | "**" | LITERAL | Variable ;
Variable  = "{" FieldPath [ "=" Segments ] "}" ;
FieldPath = IDENT { "." IDENT } ;
Verb      = ":" LITERAL ;
```

Semantics we must honour:

- A bare `{name}` is sugar for `{name=*}` — captures exactly **one** segment.
- `*` matches one segment; `**` matches **zero or more** segments and may only
  appear as the final segment (in the path, or as the tail of a variable's
  sub-pattern).
- A variable's value is its matched segment(s): a `*`/literal capture is one
  decoded segment; a `**` capture is the remaining segments joined by `/`.
- Variables may not nest. A field path may be one or more identifiers; the last
  identifier names a leaf field, earlier ones traverse nested messages.
- A trailing `:verb` is a literal custom verb on the final segment.

### AST

```rust
// crates/grpc-gw/src/template.rs
pub struct PathTemplate {
    pub segments: Vec<Segment>,
    pub verb: Option<String>,          // trailing ":verb"
    pub vars: Vec<VarSpec>,            // flattened captures, in path order
}

pub enum Segment {
    Literal(String),
    Single,                            // *
    CatchAll,                          // **
    Variable(usize),                   // index into `vars`; expands to its sub-pattern
}

pub struct VarSpec {
    pub field_path: Vec<String>,       // {a.b.c} -> ["a","b","c"]
    pub start: usize,                  // first segment index this var covers
    pub segment_count: SegmentCount,   // One | Range (for **-containing subpatterns)
}
```

`PathTemplate::parse(&str) -> Result<PathTemplate, TemplateError>` is a small
hand-written recursive-descent parser (no regex, no codegen). Errors are
descriptive and surfaced by `grpc-gw check` (e.g. `** not in final position`,
`nested variable`, `empty field path`).

## Matcher

Build a router **once at startup**, keyed by HTTP method, holding the compiled
templates for that method **in declaration order**. A request `(method, path)`
resolves in O(path segments):

- Split the request path into URL-decoded segments; peel a trailing `:verb` if
  present.
- Walk the method's templates **in registration order** and take the **first**
  one that matches — exactly Go grpc-gateway's `ServeMux.ServeHTTP` loop
  (`for _, h := range s.handlers[r.Method] { if h.pat.Match(...) { return } }`).
  Registration order follows proto declaration order, so an earlier-declared
  `/v1/{x}` will match `/v1/foo` before a later-declared literal `/v1/foo` is
  ever tried. We deliberately **do not** reorder by specificity: the project's
  goal is wire-compatibility with Go grpc-gateway, and Go is first-match-wins.
- On a match, return the route's binding plus the captured variable segments.

> Note: this differs from Envoy's `grpc-httpjson-transcoding` / Cloud Endpoints,
> which pick the *most specific* template. We match Go, not Envoy. If a
> specificity mode is ever wanted it should be an explicit opt-in, not the
> default.

No per-request regex compilation; templates compile at build time, lookups are
segment walks. Per-route descriptor lookups (input/output message, field
descriptors for each var path) are resolved at build time and cached on the
binding, so the hot path does no name resolution.

### Conflict detection

Go grpc-gateway does **not** treat overlapping templates as an error — it just
resolves them by declaration order (first match wins), so an "overlap" is
**shadowing**, not a conflict. The **default** matches Go (silent at runtime),
with `grpc-gw check`:

- **Errors** on an *exact* duplicate `(method, path)` template (two handlers
  that are indistinguishable) — same hard failure M1's
  `RouteTable::conflicts()` already produces. Always an error, in every mode.
- **Warns** (does not fail) when a later-declared template is **shadowed** by an
  earlier one it structurally overlaps, naming both gRPC paths and the
  declaration order, so operators can spot an unreachable route without
  rejecting a config Go would accept.

#### `strict_routes` flag (opt-in)

Go's declaration-order routing means an overlapping route can be silently
unreachable — a real footgun. A config flag **`strict_routes`** (default
`false`, i.e. Go-compatible) **promotes the shadowing warning to a hard error**:

- `strict_routes = false` (default): silent shadowing at runtime; `check`
  *warns* on shadowed routes and exits `0`. **Byte-for-byte Go parity.**
- `strict_routes = true`: a shadowed (structurally overlapped) route is a
  **build error** — `Gateway::builder(...).build()` returns `BuildError`, and
  `grpc-gw check` exits non-zero, naming both routes. Runtime routing is
  unchanged (still first-match); the flag only governs whether shadowing is
  *tolerated* or *rejected* at construction/validation time.

The flag never changes *which* handler wins a match (always declaration order);
it only decides whether an unreachable route is allowed to exist. This keeps the
default wire-compatible with Go while giving operators a way to fail fast on
the shadowing footgun. It pairs naturally with the existing `unbound_methods`
toggle as a route-table construction option.

## Variable binding (path → message)

For each matched `VarSpec`, write the captured value into the input
`DynamicMessage` at its field path:

1. Traverse `field_path[..last]`, materializing intermediate message fields
   (create-if-absent), erroring if a path component is not a message.
2. Coerce the captured string to the **leaf field's** proto kind — reuse the
   JSON scalar conversion already behind `transcode.rs` (numbers, bool,
   enum-by-name-or-number, string; `**` captures stay strings). This keeps path
   values and JSON-body values converging on the same canonical parsing.

Precedence when several sources can set a field (grpc-gateway order):
**path variables → body → query**. Mechanically Go decodes the body first, then
overlays path variables (so path wins), then applies query parameters to fields
not already bound by a path variable; the net result is path > body > query,
which is what we implement.

## `body` and `response_body` selectors

- `body: "*"` (M1): whole JSON body → whole input message. Unchanged.
- `body: "field"`: parse the JSON body into the message located at that field
  path (the rest of the input is populated by path vars + query). Replaces the
  current `501` stub.
- `body: ""` (empty): no body read; the entire input comes from path + query
  (typical `GET`/`DELETE`).
- `response_body: "field"`: render only that field of the response message as
  the JSON body, instead of the whole message.

## Query-parameter expansion

After path + body binding, remaining URL query parameters populate fields **not
already bound by the path or body**:

- `?user.id=7&tags=a&tags=b` → set `user.id = 7`, append `a`,`b` to repeated
  `tags`.
- Field paths traverse nested messages; repeated keys append; scalar coercion
  matches the path-binding rules.
- Skipped entirely when `body: "*"` (the body already owns the whole message).
- Unknown field paths are ignored by default (configurable strictness can come
  with the hooks slice).

## `additional_bindings`

The route table already **decodes** `additional_bindings`
([descriptor.rs](../../crates/grpc-gw/src/descriptor.rs)); this slice
**registers** each as its own binding on the same `Route`, compiled into the
matcher alongside the primary. Each additional binding has its own method,
template, `body`, and `response_body`. (The grammar forbids recursive
`additional_bindings`; we only flatten one level, matching the spec.)

## Integration points

- **`crates/grpc-gw/src/template.rs`** (new): grammar AST, `parse`, the matcher,
  and `bind` (segment captures → field-path writes). Pure, unit-test-heavy.
- **`routes.rs`**: `RouteBinding` gains a compiled `PathTemplate` (replacing the
  opaque `http_path` string for matching; the literal is kept for display).
  `conflicts()` switches to structural overlap, classifying each as an *exact
  duplicate* (always an error) or *shadowing* (warning by default, error under
  `strict_routes`). `BodySelector::Field` becomes a live path, not a deferral
  marker. The route-table builder gains a `strict_routes` option alongside
  `unbound_methods`.
- **`gateway.rs`**: the exact `(method, path)` `HashMap` becomes a
  method-keyed matcher; `handle()` resolves via the matcher, applies
  path→body→query binding, calls upstream, then applies `response_body`. The
  `body:"field"` and templated-path `501` branches are removed. `BuildError`
  gains a shadowed-route variant returned when `strict_routes` is set and a
  shadowing overlap exists.
- **`bin/grpc-gw.rs`**: `routes` output gains the resolved template + var→field
  mapping; `check` reports template parse errors, exact-duplicate errors, and
  shadowing (warning by default, error with `--strict-routes`); a
  `--strict-routes` flag / `strict_routes` config field threads through to the
  builder.

## Test plan

- **Parser unit tests** (`template.rs`): every grammar form + error cases
  (`**` not final, nested `{}`, empty field path, bad verb).
- **Matcher unit tests**: declaration-order first-match (`/v1/{x}` declared
  before `/v1/foo` wins for `/v1/foo`), `*` vs `**` capture extents, custom-verb
  splitting, no-match paths.
- **Binding unit tests**: single-segment, field-path, multi-segment `**`, enum
  coercion, repeated query keys, precedence (path > body > query).
- **Integration** (`grpc-gw-tests`, reuse the shared `Backend` harness): extend
  the fixture so `SayHello` exercises `GET /v1/greeter/{name}`, `UpdateGreeting`
  exercises `PATCH` `additional_binding` + `body:"greeting"`, plus a query-param
  case and a `response_body` case. End-to-end JSON-in/JSON-out assertions.
- **Conflict / shadowing test**: an exact duplicate makes `grpc-gw check` exit
  non-zero in every mode; a deliberately *overlapping* (shadowing) pair only
  *warns* by default (exit `0`) but errors under `strict_routes` /
  `--strict-routes`, naming both paths.

## Acceptance criteria

M2.1 is done when all hold:

1. **Path variables.** `GET /v1/greeter/{name}` routes a real request and binds
   `name` into the input message; nested `{a.b}` and multi-segment `{x=**}`
   bind correctly.
2. **Declaration-order routing.** When two templates can match the same request,
   the **earlier-declared** one wins (Go grpc-gateway first-match semantics),
   deterministically — *not* the most specific one.
3. **`body:"field"` / `response_body`.** A field-scoped body parses into the
   named field; a `response_body` selector renders only the named response
   field. No `501`.
4. **Query expansion.** Query parameters populate unbound fields by field path,
   with repeated-field and scalar-coercion support, and are skipped under
   `body:"*"`.
5. **`additional_bindings`.** A method with a primary + additional binding is
   reachable via both, each with its own method/template/body.
6. **Introspection & conflicts.** `grpc-gw routes` shows resolved templates and
   var→field mappings; `grpc-gw check` errors on exact duplicate templates and
   warns on shadowed (overlapping) routes, exiting non-zero only on the former.
7. **`strict_routes` flag.** With the default (`false`), a shadowing overlap
   builds and runs (Go parity), `check` warns and exits `0`. With
   `strict_routes`/`--strict-routes`, the same overlap fails the build
   (`BuildError`) and `check` exits non-zero; runtime match order is unchanged.

## Suggested task order

1. `template.rs`: grammar AST + `parse` + parser unit tests.
2. Matcher (method-keyed, **declaration-order / first-match-wins**) + matcher
   unit tests.
3. Variable binding (field-path writes + scalar coercion) reusing
   `transcode.rs`’ scalar parsing + binding unit tests.
4. Wire into `routes.rs` (compiled template on the binding; structural
   `conflicts()`).
5. Wire into `gateway.rs` (`handle()` via matcher; remove `501` stubs); add
   `body:"field"` + `response_body`.
6. Query-param expansion.
7. `additional_bindings` registration.
8. Extend the fixture + integration tests; update `routes`/`check` output;
   close the acceptance list.

## References

- Architecture: [grpc-gateway-design.md](./grpc-gateway-design.md)
  (§ [Route table & path templates](./grpc-gateway-design.md))
- M1 buildable scope: [m1-scope.md](./m1-scope.md)
- [google.api.http transcoding spec](https://cloud.google.com/endpoints/docs/grpc/transcoding)
  and the [`google/api/http.proto`](https://github.com/googleapis/googleapis/blob/master/google/api/http.proto)
  grammar
- Prior art: [`tonic-rest`](https://github.com/zs-dima/tonic-rest) (build-time
  codegen; grammar/JSON reference only) and
  [grpc-gateway](https://github.com/grpc-ecosystem/grpc-gateway) (Go, the parity
  target)
