//! gRPC client & framing: speak raw gRPC over HTTP/2 with no typed stubs.
//!
//! See `docs/design/grpc-gateway-design.md#grpc-client--framing`. The proxy
//! does **not** hand-roll an h2 client — it drives a
//! [`hyper_util::client::legacy::Client`] in `http2_only` mode (which owns the
//! handshake, per-authority pooling, and stream multiplexing) and adds only
//! the 5-byte gRPC length-prefixed framing on top.
//!
//! This is the **M1 cut**: unary calls only. The transport is pluggable via
//! the connector type parameter — TCP (the default [`GrpcClient::plaintext`]),
//! TLS, Unix sockets, or an in-memory duplex are all just connectors.

use bytes::{BufMut, Bytes, BytesMut};
use http::header::{HeaderMap, HeaderName, CONTENT_TYPE, TE};
use http::{Method, Request, Uri};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::connect::{Connect, HttpConnector};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

use crate::status::Code;

/// gRPC frame header length: 1 compression byte + 4-byte big-endian length.
const FRAME_HEADER_LEN: usize = 5;

/// `grpc-status` trailer/header name.
static GRPC_STATUS: HeaderName = HeaderName::from_static("grpc-status");
/// `grpc-message` trailer/header name.
static GRPC_MESSAGE: HeaderName = HeaderName::from_static("grpc-message");
/// `grpc-status-details-bin` trailer/header name (base64 `google.rpc.Status`).
static GRPC_STATUS_DETAILS_BIN: HeaderName = HeaderName::from_static("grpc-status-details-bin");

/// A framing-level failure decoding a gRPC data frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FramingError {
    /// Fewer than 5 bytes — no complete frame header.
    ShortHeader,
    /// The declared payload length exceeds the bytes available.
    ShortPayload { declared: usize, available: usize },
    /// The compression flag was non-zero; M1 does not decompress.
    Compressed,
    /// A unary response carried no message frame.
    NoMessage,
}

impl std::fmt::Display for FramingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FramingError::ShortHeader => write!(f, "gRPC frame header is incomplete"),
            FramingError::ShortPayload {
                declared,
                available,
            } => write!(
                f,
                "gRPC frame declares {declared} bytes but only {available} are available"
            ),
            FramingError::Compressed => {
                write!(f, "compressed gRPC frames are not supported in M1")
            }
            FramingError::NoMessage => write!(f, "unary gRPC response carried no message"),
        }
    }
}

impl std::error::Error for FramingError {}

/// Prefix `payload` with the 5-byte gRPC frame header (uncompressed).
pub fn encode_frame(payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(FRAME_HEADER_LEN + payload.len());
    buf.put_u8(0); // compression flag: none
    buf.put_u32(payload.len() as u32); // big-endian length
    buf.put_slice(payload);
    buf.freeze()
}

/// Decode exactly one gRPC frame from the front of `buf`, returning its
/// payload. Used for unary responses, which carry a single message frame.
pub fn decode_single_frame(buf: &[u8]) -> Result<Bytes, FramingError> {
    if buf.len() < FRAME_HEADER_LEN {
        return Err(FramingError::ShortHeader);
    }
    let compression = buf[0];
    if compression != 0 {
        return Err(FramingError::Compressed);
    }
    let declared = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    let body = &buf[FRAME_HEADER_LEN..];
    if body.len() < declared {
        return Err(FramingError::ShortPayload {
            declared,
            available: body.len(),
        });
    }
    Ok(Bytes::copy_from_slice(&body[..declared]))
}

/// The outcome of a unary gRPC call: either a message payload (status `OK`) or
/// a non-OK status with its message and optional binary details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcReply {
    pub status: Code,
    /// `grpc-message`, percent-decoded.
    pub message: String,
    /// The single response message frame's payload, present iff `status` is
    /// `Ok`.
    pub message_bytes: Option<Bytes>,
    /// Raw `grpc-status-details-bin` (`google.rpc.Status`), if the backend set
    /// it. Decoding into the error envelope `details` is a later step.
    pub status_details_bin: Option<Bytes>,
}

/// A transport- or framing-level proxy failure (distinct from a non-OK gRPC
/// status, which is a *successful* call that [`GrpcReply`] carries).
#[derive(Debug)]
pub enum ProxyError {
    /// Building the backend request URI failed.
    Uri(http::Error),
    /// The connection or request failed at the hyper-util layer.
    Transport(hyper_util::client::legacy::Error),
    /// Reading the response body/trailers failed.
    Body(hyper::Error),
    /// The response framing was malformed.
    Framing(FramingError),
    /// The backend returned no `grpc-status` in headers or trailers.
    MissingStatus,
}

impl std::fmt::Display for ProxyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProxyError::Uri(e) => write!(f, "failed to build backend URI: {e}"),
            ProxyError::Transport(e) => write!(f, "backend transport error: {e}"),
            ProxyError::Body(e) => write!(f, "failed to read backend response: {e}"),
            ProxyError::Framing(e) => write!(f, "malformed gRPC response: {e}"),
            ProxyError::MissingStatus => {
                write!(f, "backend response carried no grpc-status")
            }
        }
    }
}

impl std::error::Error for ProxyError {}

impl From<FramingError> for ProxyError {
    fn from(e: FramingError) -> Self {
        ProxyError::Framing(e)
    }
}

/// A gRPC-over-HTTP/2 client for one backend authority, generic over its
/// connector so callers can plug in TLS, Unix, or custom transports.
#[derive(Clone)]
pub struct GrpcClient<C> {
    client: Client<C, Full<Bytes>>,
    /// Backend base URI (scheme + authority); per-call paths are appended.
    backend: Uri,
}

impl GrpcClient<HttpConnector> {
    /// Build a plaintext (h2c) client for `backend` over TCP. `backend` must
    /// carry a scheme and authority, e.g. `http://127.0.0.1:50051`.
    pub fn plaintext(backend: Uri) -> Self {
        let mut connector = HttpConnector::new();
        connector.enforce_http(false);
        Self::with_connector(connector, backend)
    }
}

impl<C> GrpcClient<C>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    /// Build a client over an arbitrary connector (TLS, Unix, duplex, …).
    pub fn with_connector(connector: C, backend: Uri) -> Self {
        let client = Client::builder(TokioExecutor::new())
            .http2_only(true)
            .build(connector);
        GrpcClient { client, backend }
    }

    /// Perform a unary call: frame `message`, POST it to `grpc_path` with the
    /// forwarded `metadata`, then read the single response frame and trailing
    /// status.
    pub async fn unary(
        &self,
        grpc_path: &str,
        message: &[u8],
        metadata: HeaderMap,
    ) -> Result<GrpcReply, ProxyError> {
        let uri = self.build_uri(grpc_path)?;
        let mut req = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header(CONTENT_TYPE, "application/grpc+proto")
            .header(TE, "trailers")
            .body(Full::new(encode_frame(message)))
            .map_err(ProxyError::Uri)?;
        // Forward caller-supplied metadata (the gateway applies its allow-list
        // before calling us).
        for (name, value) in metadata.iter() {
            req.headers_mut().append(name.clone(), value.clone());
        }

        let resp = self
            .client
            .request(req)
            .await
            .map_err(ProxyError::Transport)?;
        // Trailers-only responses carry grpc-status/message in the *header*
        // map, so capture them before consuming the body.
        let header_status = parse_status_code(resp.headers());
        let header_message = parse_status_message(resp.headers());

        let collected = resp.into_body().collect().await.map_err(ProxyError::Body)?;
        let trailers = collected.trailers().cloned().unwrap_or_default();
        let body = collected.to_bytes();

        let status = parse_status_code(&trailers)
            .or(header_status)
            .ok_or(ProxyError::MissingStatus)?;
        let message = parse_status_message(&trailers)
            .or(header_message)
            .unwrap_or_default();
        let status_details_bin = trailers
            .get(&GRPC_STATUS_DETAILS_BIN)
            .and_then(|v| decode_base64(v.as_bytes()))
            .map(Bytes::from);

        if status == Code::Ok {
            let payload = decode_single_frame(&body).map_err(|e| match e {
                FramingError::ShortHeader if body.is_empty() => {
                    ProxyError::Framing(FramingError::NoMessage)
                }
                other => ProxyError::Framing(other),
            })?;
            Ok(GrpcReply {
                status,
                message,
                message_bytes: Some(payload),
                status_details_bin,
            })
        } else {
            Ok(GrpcReply {
                status,
                message,
                message_bytes: None,
                status_details_bin,
            })
        }
    }

    fn build_uri(&self, grpc_path: &str) -> Result<Uri, ProxyError> {
        let mut parts = self.backend.clone().into_parts();
        parts.path_and_query = Some(
            grpc_path
                .parse()
                .map_err(|e: http::uri::InvalidUri| ProxyError::Uri(e.into()))?,
        );
        Uri::from_parts(parts).map_err(|e| ProxyError::Uri(e.into()))
    }
}

/// Read and parse the numeric `grpc-status` from a header/trailer map.
fn parse_status_code(map: &HeaderMap) -> Option<Code> {
    let raw = map.get(&GRPC_STATUS)?;
    let n: i32 = raw.to_str().ok()?.trim().parse().ok()?;
    Some(Code::from_i32(n))
}

/// Read and percent-decode `grpc-message` from a header/trailer map.
fn parse_status_message(map: &HeaderMap) -> Option<String> {
    let raw = map.get(&GRPC_MESSAGE)?;
    Some(percent_decode(raw.as_bytes()))
}

/// Percent-decode a `grpc-message` value (`%XX` escapes, otherwise literal).
/// Invalid escapes are passed through verbatim, matching lenient gRPC clients.
fn percent_decode(input: &[u8]) -> String {
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        if input[i] == b'%' && i + 2 < input.len() {
            if let (Some(hi), Some(lo)) = (hex_val(input[i + 1]), hex_val(input[i + 2])) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(input[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Decode a standard base64 (`grpc-status-details-bin` is base64, no padding
/// required). Returns `None` on malformed input.
fn decode_base64(input: &[u8]) -> Option<Vec<u8>> {
    // Tolerate both padded and unpadded standard alphabets.
    let input: Vec<u8> = input.iter().copied().filter(|&b| b != b'=').collect();
    let mut out = Vec::with_capacity(input.len() / 4 * 3);
    let mut acc: u32 = 0;
    let mut bits = 0u32;
    for &b in &input {
        let v = base64_val(b)?;
        acc = (acc << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

fn base64_val(b: u8) -> Option<u8> {
    match b {
        b'A'..=b'Z' => Some(b - b'A'),
        b'a'..=b'z' => Some(b - b'a' + 26),
        b'0'..=b'9' => Some(b - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::HeaderValue;

    #[test]
    fn encode_then_decode_round_trips() {
        let payload = b"hello protobuf";
        let framed = encode_frame(payload);
        assert_eq!(framed.len(), FRAME_HEADER_LEN + payload.len());
        assert_eq!(framed[0], 0);
        assert_eq!(&framed[1..5], &(payload.len() as u32).to_be_bytes());

        let decoded = decode_single_frame(&framed).expect("decodes");
        assert_eq!(&decoded[..], payload);
    }

    #[test]
    fn encode_empty_payload() {
        let framed = encode_frame(b"");
        assert_eq!(framed.len(), FRAME_HEADER_LEN);
        assert_eq!(decode_single_frame(&framed).unwrap().len(), 0);
    }

    #[test]
    fn decode_rejects_short_header() {
        assert_eq!(
            decode_single_frame(&[0, 0, 0]),
            Err(FramingError::ShortHeader)
        );
    }

    #[test]
    fn decode_rejects_compressed() {
        let mut framed = encode_frame(b"x").to_vec();
        framed[0] = 1; // flip compression flag
        assert_eq!(decode_single_frame(&framed), Err(FramingError::Compressed));
    }

    #[test]
    fn decode_rejects_truncated_payload() {
        let framed = encode_frame(b"abcdef");
        let truncated = &framed[..framed.len() - 2];
        assert_eq!(
            decode_single_frame(truncated),
            Err(FramingError::ShortPayload {
                declared: 6,
                available: 4
            })
        );
    }

    #[test]
    fn parses_status_from_trailers() {
        let mut map = HeaderMap::new();
        map.insert(&GRPC_STATUS, HeaderValue::from_static("5"));
        assert_eq!(parse_status_code(&map), Some(Code::NotFound));
    }

    #[test]
    fn missing_status_is_none() {
        assert_eq!(parse_status_code(&HeaderMap::new()), None);
    }

    #[test]
    fn percent_decodes_grpc_message() {
        // "a b" with the space percent-encoded.
        assert_eq!(percent_decode(b"a%20b"), "a b");
        // Literal passthrough.
        assert_eq!(percent_decode(b"plain"), "plain");
        // Malformed escape passes through.
        assert_eq!(percent_decode(b"50%"), "50%");
    }

    #[test]
    fn base64_decodes_details() {
        // "Man" is the canonical base64 example -> "TWFu".
        assert_eq!(decode_base64(b"TWFu").as_deref(), Some(&b"Man"[..]));
        // Unpadded "any carnal pleasure." prefix "any" -> "YW55".
        assert_eq!(decode_base64(b"YW55").as_deref(), Some(&b"any"[..]));
    }
}
