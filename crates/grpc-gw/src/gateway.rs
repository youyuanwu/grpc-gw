//! The `Gateway` service and the Tier-1 byte-stream entry point.
//!
//! This is the M1 assembly that ties the pieces together: it resolves an
//! incoming `(method, path)` against the [route table](crate::routes), builds
//! the input dynamic message ([`transcode`](crate::transcode)), forwards the
//! call to the backend ([`GrpcClient`](crate::proxy)), and renders the
//! response (or a [status](crate::status) error envelope) back as JSON.
//!
//! See `docs/design/grpc-gateway-design.md#library-api-boundary-streams-not-config`.
//!
//! **M1 cut / boundaries** (everything here is honest about what M1 does *not*
//! do yet):
//!
//! - **Routing is exact** `(method, path)` matching — path templates are opaque
//!   literals, so annotated routes with captures (`{name}`) only match their
//!   literal template until M2. The workhorse is the synthesized default
//!   binding `POST /pkg.Svc/Method`.
//! - **Body** handles `body: "*"` (whole body) and body-less (`GET`/`DELETE`)
//!   methods. A `body: "field"` selector returns `501` (M2).
//! - **Server-streaming** methods load but their endpoint returns `501` (M3).
//! - **Header forwarding** is a static default allow-list (custom matchers are
//!   M2).

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use http::header::{HeaderName, CONTENT_TYPE};
use http::{HeaderMap, Response};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::descriptor::DescriptorError;
use crate::proxy::GrpcClient;
use crate::routes::{BodySelector, RouteTable};
use crate::status::{Code, ErrorEnvelope};
use crate::transcode::{decode_request_body, encode_response_json, JsonOptions};

/// The default inbound HTTP headers copied into gRPC request metadata. A static
/// allow-list in M1; pluggable matchers are M2.
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
    /// JSON marshaling options for request/response transcoding.
    pub json: JsonOptions,
}

impl Default for GatewayOptions {
    fn default() -> Self {
        GatewayOptions {
            unbound_methods: true,
            json: JsonOptions::default(),
        }
    }
}

/// One resolved endpoint: a `(method, path)` mapping to a backend gRPC call.
struct Entry {
    grpc_path: String,
    input: MessageDescriptor,
    output: MessageDescriptor,
    server_streaming: bool,
    body: BodySelector,
}

struct Inner {
    /// `(uppercase HTTP method, path)` → resolved endpoint.
    routes: HashMap<(String, String), Entry>,
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
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::Descriptor(e) => write!(f, "{e}"),
            BuildError::MissingBackend => {
                write!(f, "no backend was configured on the gateway builder")
            }
        }
    }
}

impl std::error::Error for BuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BuildError::Descriptor(e) => Some(e),
            BuildError::MissingBackend => None,
        }
    }
}

impl From<DescriptorError> for BuildError {
    fn from(e: DescriptorError) -> Self {
        BuildError::Descriptor(e)
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

        // Flatten the route table into an exact `(method, path)` index.
        let mut routes = HashMap::new();
        for route in &table.routes {
            let Some((input, output, server_streaming)) = methods.get(&route.grpc_path) else {
                continue;
            };
            for binding in &route.bindings {
                routes.insert(
                    (binding.http_method.clone(), binding.http_path.clone()),
                    Entry {
                        grpc_path: route.grpc_path.clone(),
                        input: input.clone(),
                        output: output.clone(),
                        server_streaming: *server_streaming,
                        body: binding.body.clone(),
                    },
                );
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
    /// path. The returned response is always rendered (gRPC/HTTP errors become
    /// JSON envelopes), so this is infallible from the caller's perspective.
    pub async fn handle(
        &self,
        method: &str,
        path: &str,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Response<Full<Bytes>> {
        let key = (method.to_ascii_uppercase(), path.to_owned());
        let Some(entry) = self.inner.routes.get(&key) else {
            return error_response(Code::NotFound, format!("no route for {method} {path}"));
        };

        if entry.server_streaming {
            return error_response(
                Code::Unimplemented,
                "server-streaming methods are not supported in M1",
            );
        }

        // Build the input dynamic message from the body.
        let input_msg = match &entry.body {
            BodySelector::Wildcard => {
                match decode_request_body(&entry.input, &body, &self.inner.json) {
                    Ok(msg) => msg,
                    Err(e) => return error_response(Code::InvalidArgument, e.to_string()),
                }
            }
            BodySelector::None => DynamicMessage::new(entry.input.clone()),
            BodySelector::Field(field) => {
                return error_response(
                    Code::Unimplemented,
                    format!("body field selector \"{field}\" is not supported in M1"),
                );
            }
        };

        let request_bytes = Bytes::from(input_msg.encode_to_vec());
        let metadata = forwarded_metadata(headers);

        let reply = match self
            .inner
            .backend
            .unary(&entry.grpc_path, request_bytes, metadata)
            .await
        {
            Ok(reply) => reply,
            Err(e) => return error_response(Code::Unavailable, e.to_string()),
        };

        if reply.status != Code::Ok {
            let env = ErrorEnvelope::new(reply.status, reply.message);
            return json_response(env.http_status(), env.to_json());
        }

        let payload = reply.message_bytes.unwrap_or_default();
        let output_msg = match DynamicMessage::decode(entry.output.clone(), payload) {
            Ok(msg) => msg,
            Err(e) => {
                return error_response(
                    Code::Internal,
                    format!("failed to decode backend response: {e}"),
                )
            }
        };

        match encode_response_json(&output_msg, &self.inner.json) {
            Ok(json) => json_response(200, json),
            Err(e) => error_response(Code::Internal, e.to_string()),
        }
    }
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
        async move { Ok::<_, std::convert::Infallible>(dispatch(&gateway, req).await) }
    });

    hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
        .serve_connection(TokioIo::new(io), service)
        .await
        .map_err(ServeError)
}

/// Adapt an `http::Request` into the parts [`Gateway::handle`] needs.
async fn dispatch(gateway: &Gateway, req: Request<Incoming>) -> Response<Full<Bytes>> {
    let (parts, body) = req.into_parts();
    let method = parts.method.as_str().to_owned();
    let path = parts.uri.path().to_owned();

    let body = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return error_response(Code::Internal, "failed to read request body"),
    };

    gateway.handle(&method, &path, &parts.headers, body).await
}
