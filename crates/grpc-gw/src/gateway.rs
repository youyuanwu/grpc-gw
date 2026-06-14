//! The `Gateway` service and the Tier-1 byte-stream entry point.
//!
//! This assembly ties the pieces together: it resolves an incoming
//! `(method, path)` against the [route table](crate::routes) using the M2
//! [path-template matcher](crate::template), builds the input dynamic message
//! from the path variables, body, and query ([`transcode`](crate::transcode)),
//! forwards the call to the backend ([`GrpcClient`](crate::proxy)), and renders
//! the response (or a [status](crate::status) error envelope) back as JSON.
//!
//! See `docs/design/grpc-gateway-design.md#library-api-boundary-streams-not-config`
//! and `docs/design/m2-path-templates.md`.
//!
//! **Current boundaries:**
//!
//! - **Routing** is declaration-order, first-match-wins over compiled path
//!   templates (Go grpc-gateway semantics); `strict_routes` rejects shadowed
//!   routes at build time.
//! - **Body** handles `body: "*"` (whole body), `body: "field"` (a sub-field),
//!   and body-less (`GET`/`DELETE`) methods. Path variables and query
//!   parameters fill the rest (precedence path > body > query).
//! - **Server-streaming** methods load but their endpoint returns `501` (M3).
//! - **Header forwarding** is a static default allow-list (custom matchers are
//!   a later M2 slice).

use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::header::{HeaderName, CONTENT_TYPE};
use http::{HeaderMap, Response};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, ReflectMessage};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::descriptor::DescriptorError;
use crate::proxy::{GrpcClient, GrpcReply};
use crate::routes::{BodySelector, RouteTable};
use crate::status::{Code, ErrorEnvelope};
use crate::template::{Captures, PathTemplate, TemplateError};
use crate::transcode::{
    bind_field_path, decode_request_body, encode_response_json, BindMode, JsonOptions,
};

/// The default inbound HTTP headers copied into gRPC request metadata. A static
/// allow-list; pluggable matchers are a later M2 slice.
const FORWARDED_HEADERS: &[&str] = &[
    "authorization",
    "user-agent",
    "x-forwarded-for",
    "x-real-ip",
    "grpc-timeout",
    "x-request-id",
];

/// Routing/transcoding knobs for a [`Gateway`]. Defaults match grpc-gateway's
/// canonical behaviour.
#[derive(Debug, Clone)]
pub struct GatewayOptions {
    /// Synthesize a default `POST /pkg.Svc/Method` binding for methods lacking
    /// a `google.api.http` annotation. Default `true`.
    pub unbound_methods: bool,
    /// Reject a shadowed (structurally overlapping) route at build time instead
    /// of silently letting the earlier-declared route win. Default `false`,
    /// matching Go grpc-gateway's first-match routing.
    pub strict_routes: bool,
    /// JSON marshaling options for request/response transcoding.
    pub json: JsonOptions,
}

impl Default for GatewayOptions {
    fn default() -> Self {
        GatewayOptions {
            unbound_methods: true,
            strict_routes: false,
            json: JsonOptions::default(),
        }
    }
}

/// One resolved endpoint: a compiled template mapping to a backend gRPC call.
struct Entry {
    template: PathTemplate,
    grpc_path: String,
    input: MessageDescriptor,
    output: MessageDescriptor,
    server_streaming: bool,
    body: BodySelector,
    response_body: Option<String>,
}

struct Inner {
    /// Uppercase HTTP method → endpoints in declaration order (first match wins).
    routes: HashMap<String, Vec<Entry>>,
    backend: GrpcClient,
    json: JsonOptions,
}

/// A transcoding gateway: maps inbound JSON HTTP requests to backend gRPC
/// calls. Cheap to [`clone`](Clone) (an `Arc` inside) and serve concurrently.
#[derive(Clone)]
pub struct Gateway {
    inner: Arc<Inner>,
}

/// Failure building a [`Gateway`] from a descriptor set.
#[derive(Debug)]
pub enum BuildError {
    /// The descriptor set failed to decode.
    Descriptor(DescriptorError),
    /// No backend [`GrpcClient`] was supplied to the builder.
    MissingBackend,
    /// A binding's path template failed to parse.
    Template(TemplateError),
    /// `strict_routes` is set and a route is shadowed by an earlier one (or an
    /// exact duplicate exists). The string is the human-readable conflict.
    ShadowedRoute(String),
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::Descriptor(e) => write!(f, "{e}"),
            BuildError::MissingBackend => {
                write!(f, "no backend was configured on the gateway builder")
            }
            BuildError::Template(e) => write!(f, "{e}"),
            BuildError::ShadowedRoute(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for BuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BuildError::Descriptor(e) => Some(e),
            BuildError::Template(e) => Some(e),
            BuildError::MissingBackend | BuildError::ShadowedRoute(_) => None,
        }
    }
}

impl From<DescriptorError> for BuildError {
    fn from(e: DescriptorError) -> Self {
        BuildError::Descriptor(e)
    }
}

impl From<TemplateError> for BuildError {
    fn from(e: TemplateError) -> Self {
        BuildError::Template(e)
    }
}

/// Builder for a [`Gateway`]; see [`Gateway::builder`].
pub struct GatewayBuilder {
    descriptor_set: Vec<u8>,
    options: GatewayOptions,
    backend: Option<GrpcClient>,
}

impl GatewayBuilder {
    /// Set the backend gRPC client (required).
    pub fn backend(mut self, backend: GrpcClient) -> Self {
        self.backend = Some(backend);
        self
    }

    /// Override the routing/transcoding options.
    pub fn options(mut self, options: GatewayOptions) -> Self {
        self.options = options;
        self
    }

    /// Resolve the descriptor set into a route table and build the gateway.
    pub fn build(self) -> Result<Gateway, BuildError> {
        let backend = self.backend.ok_or(BuildError::MissingBackend)?;
        let pool = DescriptorPool::decode(self.descriptor_set.as_slice())
            .map_err(|e| BuildError::Descriptor(DescriptorError::from(e)))?;
        let table = RouteTable::build(&self.descriptor_set, self.options.unbound_methods)?;

        // Exact duplicates always fail the build; shadowing fails only under
        // `strict_routes` (otherwise the earlier route silently wins, as in Go).
        for conflict in table.conflicts() {
            if conflict.is_error(self.options.strict_routes) {
                return Err(BuildError::ShadowedRoute(conflict.to_string()));
            }
        }

        // Index every method's input/output descriptors by gRPC wire path.
        let mut methods = HashMap::new();
        for service in pool.services() {
            for method in service.methods() {
                let grpc_path = format!("/{}/{}", service.full_name(), method.name());
                methods.insert(
                    grpc_path,
                    (
                        method.input(),
                        method.output(),
                        method.is_server_streaming(),
                    ),
                );
            }
        }

        // Compile each binding's template into a method-keyed, declaration-order
        // list of endpoints.
        let mut routes: HashMap<String, Vec<Entry>> = HashMap::new();
        for route in &table.routes {
            let Some((input, output, server_streaming)) = methods.get(&route.grpc_path) else {
                continue;
            };
            for binding in &route.bindings {
                let template = PathTemplate::parse(&binding.http_path)?;
                routes
                    .entry(binding.http_method.clone())
                    .or_default()
                    .push(Entry {
                        template,
                        grpc_path: route.grpc_path.clone(),
                        input: input.clone(),
                        output: output.clone(),
                        server_streaming: *server_streaming,
                        body: binding.body.clone(),
                        response_body: binding.response_body.clone(),
                    });
            }
        }

        Ok(Gateway {
            inner: Arc::new(Inner {
                routes,
                backend,
                json: self.options.json,
            }),
        })
    }
}

impl Gateway {
    /// Start building a gateway from a serialized `FileDescriptorSet` (`.pb`),
    /// built with `protoc --include_imports` so `google.api.http` resolves.
    pub fn builder(descriptor_set: impl Into<Vec<u8>>) -> GatewayBuilder {
        GatewayBuilder {
            descriptor_set: descriptor_set.into(),
            options: GatewayOptions::default(),
            backend: None,
        }
    }

    /// Handle one request from its parts. This is the transport-agnostic core:
    /// embedders with their own HTTP stack can call it directly, while
    /// [`serve_connection`] drives it over a byte stream.
    ///
    /// `method` is the HTTP method (case-insensitive); `path` is the request
    /// target and may include a `?query` string (used for query-parameter field
    /// expansion). The returned response is always rendered (gRPC/HTTP errors
    /// become JSON envelopes), so this is infallible from the caller's view.
    pub async fn handle(
        &self,
        method: &str,
        path: &str,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Response<Full<Bytes>> {
        let (path_only, query) = match path.split_once('?') {
            Some((p, q)) => (p, Some(q)),
            None => (path, None),
        };
        let (segments, verb) = crate::template::split_request_path(path_only);

        // Find the first declaration-order endpoint on this method whose
        // template matches (Go first-match-wins).
        let method_upper = method.to_ascii_uppercase();
        let Some((entry, captures)) = self.inner.routes.get(&method_upper).and_then(|entries| {
            entries
                .iter()
                .find_map(|e| e.template.matches(&segments, verb).map(|c| (e, c)))
        }) else {
            return error_response(Code::NotFound, format!("no route for {method} {path}"));
        };

        if entry.server_streaming {
            return error_response(
                Code::Unimplemented,
                "server-streaming methods are not supported yet",
            );
        }

        // REST → gRPC: translate body + path captures + query into the input
        // message, then encode it to the wire.
        let input_msg = match entry.build_request(&captures, query, &body, &self.inner.json) {
            Ok(msg) => msg,
            Err((code, message)) => return error_response(code, message),
        };
        let request_bytes = Bytes::from(input_msg.encode_to_vec());
        let metadata = forwarded_metadata(headers);

        // The gRPC call.
        let reply = match self
            .inner
            .backend
            .unary(&entry.grpc_path, request_bytes, metadata)
            .await
        {
            Ok(reply) => reply,
            Err(e) => return error_response(Code::Unavailable, e.to_string()),
        };

        // gRPC → REST: render the reply (or its status) back as JSON.
        entry.render_reply(reply, &self.inner.json)
    }
}

impl Entry {
    /// REST → gRPC: build the input [`DynamicMessage`] from the request body,
    /// path-template captures, and query string, applying grpc-gateway's
    /// precedence (path variables > body > query). Returns `(Code, message)` on
    /// a translation failure for the caller to render.
    fn build_request(
        &self,
        captures: &Captures,
        query: Option<&str>,
        body: &[u8],
        json: &JsonOptions,
    ) -> Result<DynamicMessage, (Code, String)> {
        // 1. Body: whole message (`*`), a named sub-field, or none.
        let mut msg = match &self.body {
            BodySelector::Wildcard => decode_request_body(&self.input, body, json)
                .map_err(|e| (Code::InvalidArgument, e.to_string()))?,
            BodySelector::None => DynamicMessage::new(self.input.clone()),
            BodySelector::Field(field) => decode_body_into_field(&self.input, field, body, json)
                .map_err(|e| match e {
                    Resolve::Transcode(e) => (Code::InvalidArgument, e.to_string()),
                    Resolve::Field(message) => (Code::Internal, message),
                })?,
        };

        // 2. Path variables (highest precedence — overwrite body/query).
        for (field_path, value) in &captures.vars {
            bind_field_path(&mut msg, field_path, value, BindMode::Overwrite)
                .map_err(|e| (Code::InvalidArgument, e.to_string()))?;
        }

        // 3. Query parameters fill fields not already set, unless the whole body
        //    was consumed by `body: "*"`.
        if !matches!(self.body, BodySelector::Wildcard) {
            if let Some(query) = query {
                bind_query(&mut msg, query).map_err(|e| (Code::InvalidArgument, e))?;
            }
        }

        Ok(msg)
    }

    /// gRPC → REST: render a backend [`GrpcReply`] as the HTTP/JSON response —
    /// a non-OK status becomes an error envelope, otherwise the output message
    /// (optionally narrowed by `response_body`) is encoded as canonical JSON.
    fn render_reply(&self, reply: GrpcReply, json: &JsonOptions) -> Response<Full<Bytes>> {
        if reply.status != Code::Ok {
            let env = ErrorEnvelope::new(reply.status, reply.message);
            return json_response(env.http_status(), env.to_json());
        }

        let payload = reply.message_bytes.unwrap_or_default();
        let output_msg = match DynamicMessage::decode(self.output.clone(), payload) {
            Ok(msg) => msg,
            Err(e) => {
                return error_response(
                    Code::Internal,
                    format!("failed to decode backend response: {e}"),
                )
            }
        };

        // Optionally narrow the response to a single field (`response_body`).
        let encoded = match &self.response_body {
            None => encode_response_json(&output_msg, json),
            Some(field) => match output_msg.descriptor().get_field_by_name(field) {
                Some(fd) => {
                    let value = output_msg.get_field(&fd);
                    encode_field_json(&fd, &value, json)
                }
                None => {
                    return error_response(
                        Code::Internal,
                        format!("response_body field {field:?} not found"),
                    )
                }
            },
        };

        match encoded {
            Ok(json) => json_response(200, json),
            Err(e) => error_response(Code::Internal, e.to_string()),
        }
    }
}

/// Either a transcode error or a field-resolution error message.
enum Resolve {
    Transcode(crate::transcode::TranscodeError),
    Field(String),
}

/// Decode a JSON body into the field named by a `body: "field"` selector,
/// leaving the rest of the input message for path/query binding. Works for any
/// field kind (scalar or message): the body is wrapped as `{"field": <body>}`
/// and parsed through the canonical decoder. An empty body leaves the field
/// unset.
fn decode_body_into_field(
    input: &MessageDescriptor,
    field: &str,
    body: &[u8],
    opts: &JsonOptions,
) -> Result<DynamicMessage, Resolve> {
    if input.get_field_by_name(field).is_none() {
        return Err(Resolve::Field(format!("body field {field:?} not found")));
    }
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(DynamicMessage::new(input.clone()));
    }
    let body_value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| Resolve::Transcode(crate::transcode::TranscodeError::RequestJson(e)))?;
    let mut map = serde_json::Map::new();
    map.insert(field.to_owned(), body_value);
    let wrapper = serde_json::to_vec(&serde_json::Value::Object(map))
        .map_err(|e| Resolve::Transcode(crate::transcode::TranscodeError::RequestJson(e)))?;
    decode_request_body(input, &wrapper, opts).map_err(Resolve::Transcode)
}

/// Encode a single message field as JSON (for the `response_body` selector).
/// A singular message field is rendered as its own JSON object; scalars render
/// as their canonical proto3 JSON value.
fn encode_field_json(
    field: &prost_reflect::FieldDescriptor,
    value: &prost_reflect::Value,
    opts: &JsonOptions,
) -> Result<Vec<u8>, crate::transcode::TranscodeError> {
    use prost_reflect::{Kind, Value};
    use serde_json::Value as Json;

    // A singular message field: serialize the sub-message directly so nested
    // WKTs / enums use the same canonical mapping as a whole-message response.
    if let Value::Message(sub) = value {
        return encode_response_json(sub, opts);
    }

    let stringify = opts.stringify_64_bit_integers;
    let json = match value {
        Value::Bool(b) => Json::Bool(*b),
        Value::I32(n) => Json::from(*n),
        Value::U32(n) => Json::from(*n),
        Value::F32(n) => Json::from(*n),
        Value::F64(n) => Json::from(*n),
        Value::I64(n) if stringify => Json::String(n.to_string()),
        Value::I64(n) => Json::from(*n),
        Value::U64(n) if stringify => Json::String(n.to_string()),
        Value::U64(n) => Json::from(*n),
        Value::String(s) => Json::String(s.clone()),
        Value::EnumNumber(n) => match (&opts.use_enum_numbers, field.kind()) {
            (false, Kind::Enum(desc)) => match desc.get_value(*n) {
                Some(v) => Json::String(v.name().to_owned()),
                None => Json::from(*n),
            },
            _ => Json::from(*n),
        },
        // bytes / list / map response_body selectors are uncommon; defer them.
        other => {
            return Err(crate::transcode::TranscodeError::ResponseJson(
                serde::ser::Error::custom(format!(
                    "response_body for this field kind is unsupported: {other:?}"
                )),
            ))
        }
    };
    serde_json::to_vec(&json).map_err(crate::transcode::TranscodeError::ResponseJson)
}

/// Parse a `&`-separated query string and bind each parameter into the message
/// by field path (filling only fields not already set). Returns an error
/// message on a type-coercion failure (unknown fields are ignored).
fn bind_query(msg: &mut DynamicMessage, query: &str) -> Result<(), String> {
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        let key = query_decode(key);
        let value = query_decode(value);
        let field_path: Vec<String> = key.split('.').map(|s| s.to_owned()).collect();
        match bind_field_path(msg, &field_path, &value, BindMode::FillIfUnset) {
            Ok(()) => {}
            // Unknown query fields are ignored (grpc-gateway default leniency);
            // a real type-coercion failure is a 400.
            Err(crate::transcode::BindError::UnknownField { .. }) => {}
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(())
}

/// Percent/`+` decode a query component.
fn query_decode(s: &str) -> String {
    let s = s.replace('+', " ");
    if !s.contains('%') {
        return s;
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or(s)
}

/// Copy the default allow-list of inbound headers into a gRPC metadata map.
fn forwarded_metadata(headers: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for &name in FORWARDED_HEADERS {
        let key = HeaderName::from_static(name);
        for value in headers.get_all(&key) {
            out.append(key.clone(), value.clone());
        }
    }
    out
}

/// Build a JSON error response from a gRPC code + message.
fn error_response(code: Code, message: impl Into<String>) -> Response<Full<Bytes>> {
    let env = ErrorEnvelope::new(code, message);
    json_response(env.http_status(), env.to_json())
}

/// Build an `application/json` response with the given HTTP status and body.
fn json_response(status: u16, body: Vec<u8>) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
        .expect("static response builder is valid")
}

/// A failure serving an inbound connection.
#[derive(Debug)]
pub struct ServeError(Box<dyn std::error::Error + Send + Sync>);

impl std::fmt::Display for ServeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to serve connection: {}", self.0)
    }
}

impl std::error::Error for ServeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Serve a single inbound connection (Tier 1): the gateway decodes HTTP off the
/// byte stream and drives [`Gateway::handle`]. `io` is any
/// `AsyncRead + AsyncWrite` — a TCP socket, a TLS stream, an in-memory duplex,
/// or a custom transport; the gateway never opens a socket or owns TLS itself.
pub async fn serve_connection<IO>(io: IO, gateway: Gateway) -> Result<(), ServeError>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |req: Request<Incoming>| {
        let gateway = gateway.clone();
        async move { Ok::<_, Infallible>(dispatch(&gateway, req).await) }
    });

    hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
        .serve_connection(TokioIo::new(io), service)
        .await
        .map_err(ServeError)
}

/// [`Gateway`] is a [`tower::Service`] over any [`http::Request`] whose body is
/// a [`hyper::body::Body`] of [`Bytes`] — so it drops straight into a tower or
/// hyper stack, mounts on an axum router (e.g.
/// `Router::new().fallback_service(gateway)`), or joins a tonic server's router
/// via `tonic::service::Routes`. The reply is the same rendered
/// `Response<Full<Bytes>>` as [`Gateway::handle`], and the call is infallible.
impl<B> tower::Service<Request<B>> for Gateway
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
{
    type Response = Response<Full<Bytes>>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let gateway = self.clone();
        Box::pin(async move { Ok(dispatch(&gateway, req).await) })
    }
}

/// Adapt an `http::Request` into the parts [`Gateway::handle`] needs. Generic
/// over the request body so it serves hyper's [`Incoming`], axum's `Body`, or
/// any other [`hyper::body::Body`] of [`Bytes`].
async fn dispatch<B>(gateway: &Gateway, req: Request<B>) -> Response<Full<Bytes>>
where
    B: hyper::body::Body<Data = Bytes>,
{
    let (parts, body) = req.into_parts();
    let method = parts.method.as_str().to_owned();
    // Pass path *and* query so `handle` can do query-parameter field expansion.
    let path = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str().to_owned())
        .unwrap_or_else(|| parts.uri.path().to_owned());

    let body = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return error_response(Code::Internal, "failed to read request body"),
    };

    gateway.handle(&method, &path, &parts.headers, body).await
}
