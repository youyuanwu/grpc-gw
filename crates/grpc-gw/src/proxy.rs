//! gRPC client: speak raw gRPC over HTTP/2 with no typed stubs.
//!
//! See `docs/design/grpc-gateway-design.md#grpc-client--framing`. The proxy
//! delegates the entire gRPC wire layer to **tonic**: framing, `content-type`,
//! `te: trailers`, status-trailer parsing, `grpc-status-details-bin`, and
//! deadline/compression handling all live in tonic's
//! [`Grpc`](tonic::client::Grpc) client driven over a
//! [`Channel`](tonic::transport::Channel). We add only a trivial passthrough
//! [`BytesCodec`] so messages cross the boundary as raw bytes — the transcoder
//! owns JSON ⇄ protobuf, tonic owns the wire.
//!
//! This is the **M1 cut**: unary calls only. The transport is fully pluggable
//! without any generic surface on our side: callers build a [`Channel`] however
//! they like (TLS, Unix socket, in-memory duplex, load-balanced) via tonic's
//! [`Endpoint`](tonic::transport::Endpoint) and hand it to
//! [`GrpcClient::with_channel`]. [`GrpcClient::plaintext`] is the h2c
//! convenience constructor.

use bytes::{Buf, BufMut, Bytes};
use http::header::HeaderMap;
use http::Uri;
use tonic::client::Grpc;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::transport::{Channel, Endpoint};
use tonic::Status;

use crate::status::Code;

/// The outcome of a unary gRPC call: either a message payload (status `OK`) or
/// a non-OK status with its message and optional binary details.
///
/// A non-OK gRPC status is a *successful* call at the transport level — the
/// backend answered — so it is carried here rather than as a [`ProxyError`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcReply {
    pub status: Code,
    /// `grpc-message` (already unescaped by tonic).
    pub message: String,
    /// The response message bytes, present iff `status` is `Ok`.
    pub message_bytes: Option<Bytes>,
    /// Raw `google.rpc.Status` (`grpc-status-details-bin`), already base64-
    /// decoded by tonic, if the backend set it. Lowering into the error
    /// envelope `details` is a later step.
    pub status_details_bin: Option<Bytes>,
}

/// A transport-level proxy failure (distinct from a non-OK gRPC status, which a
/// successful [`GrpcReply`] carries).
#[derive(Debug)]
pub enum ProxyError {
    /// The backend URI could not be turned into a tonic [`Endpoint`].
    Endpoint(tonic::transport::Error),
    /// The gRPC method path was not a valid URI path.
    InvalidPath(http::uri::InvalidUri),
    /// The channel never became ready (e.g. the backend is unreachable).
    NotReady(tonic::transport::Error),
}

impl std::fmt::Display for ProxyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProxyError::Endpoint(e) => write!(f, "invalid backend endpoint: {e}"),
            ProxyError::InvalidPath(e) => write!(f, "invalid gRPC method path: {e}"),
            ProxyError::NotReady(e) => write!(f, "backend channel not ready: {e}"),
        }
    }
}

impl std::error::Error for ProxyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProxyError::Endpoint(e) | ProxyError::NotReady(e) => Some(e),
            ProxyError::InvalidPath(e) => Some(e),
        }
    }
}

/// A gRPC-over-HTTP/2 client for one backend, backed by a tonic [`Channel`].
///
/// The `Channel` erases its connector, so this type carries no generic
/// transport parameter: plug in TLS/Unix/duplex by constructing the channel
/// yourself and passing it to [`GrpcClient::with_channel`].
#[derive(Clone)]
pub struct GrpcClient {
    channel: Channel,
}

impl GrpcClient {
    /// Build a plaintext (h2c) client that lazily connects to `backend` over
    /// TCP. `backend` must carry a scheme and authority, e.g.
    /// `http://127.0.0.1:50051`.
    pub fn plaintext(backend: Uri) -> Result<Self, ProxyError> {
        let endpoint = Endpoint::from_shared(backend.to_string()).map_err(ProxyError::Endpoint)?;
        Ok(GrpcClient {
            channel: endpoint.connect_lazy(),
        })
    }

    /// Build a client over a pre-constructed tonic [`Channel`]. This is the
    /// pluggable-transport seam: TLS, Unix sockets, in-memory duplex, and
    /// load-balanced channels are all built via tonic's
    /// [`Endpoint`](tonic::transport::Endpoint) and handed in here.
    pub fn with_channel(channel: Channel) -> Self {
        GrpcClient { channel }
    }

    /// Perform a unary call: send `message` to `grpc_path` with the forwarded
    /// `metadata`, returning the single response message or the gRPC status.
    pub async fn unary(
        &self,
        grpc_path: &str,
        message: Bytes,
        metadata: HeaderMap,
    ) -> Result<GrpcReply, ProxyError> {
        let path = grpc_path.parse().map_err(ProxyError::InvalidPath)?;

        let mut grpc = Grpc::new(self.channel.clone());
        grpc.ready().await.map_err(ProxyError::NotReady)?;

        let mut request = tonic::Request::new(message);
        *request.metadata_mut() = tonic::metadata::MetadataMap::from_headers(metadata);

        match grpc.unary(request, path, BytesCodec).await {
            Ok(response) => Ok(GrpcReply {
                status: Code::Ok,
                message: String::new(),
                message_bytes: Some(response.into_inner()),
                status_details_bin: None,
            }),
            Err(status) => Ok(reply_from_status(status)),
        }
    }
}

/// Lower a tonic [`Status`] (a backend-reported non-OK result) into a
/// [`GrpcReply`].
fn reply_from_status(status: Status) -> GrpcReply {
    let details = status.details();
    GrpcReply {
        status: Code::from_i32(i32::from(status.code())),
        message: status.message().to_owned(),
        message_bytes: None,
        status_details_bin: (!details.is_empty()).then(|| Bytes::copy_from_slice(details)),
    }
}

/// A passthrough gRPC [`Codec`]: messages are raw [`Bytes`] in both directions.
/// tonic handles all framing around it; we just hand bytes through.
#[derive(Default, Clone, Copy)]
struct BytesCodec;

impl Codec for BytesCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = BytesCodec;
    type Decoder = BytesCodec;

    fn encoder(&mut self) -> Self::Encoder {
        BytesCodec
    }

    fn decoder(&mut self) -> Self::Decoder {
        BytesCodec
    }
}

impl Encoder for BytesCodec {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put_slice(&item);
        Ok(())
    }
}

impl Decoder for BytesCodec {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        // tonic delivers exactly one message's bytes per call.
        let len = src.remaining();
        Ok(Some(src.copy_to_bytes(len)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_maps_to_reply_with_details() {
        // Attach opaque details bytes (already-decoded google.rpc.Status).
        let status = Status::with_details(
            tonic::Code::NotFound,
            "greeter not found",
            Bytes::from_static(b"\x08\x05"),
        );
        let reply = reply_from_status(status);

        assert_eq!(reply.status, Code::NotFound);
        assert_eq!(reply.message, "greeter not found");
        assert!(reply.message_bytes.is_none());
        assert_eq!(reply.status_details_bin.as_deref(), Some(&b"\x08\x05"[..]));
    }

    #[test]
    fn ok_status_has_no_details() {
        let reply = reply_from_status(Status::new(tonic::Code::Ok, ""));
        assert_eq!(reply.status, Code::Ok);
        assert!(reply.status_details_bin.is_none());
    }
}
