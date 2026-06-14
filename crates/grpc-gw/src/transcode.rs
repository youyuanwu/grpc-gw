//! Request/response transcoding: JSON ⇄ dynamic protobuf message.
//!
//! See `docs/design/grpc-gateway-design.md#request-transcoding` and
//! `#response-transcoding`. This is the **M1 cut**:
//!
//! - Request: whole-body (`body: "*"`) JSON → input [`DynamicMessage`]. The
//!   `body`/`response_body` field selectors, query-param expansion, and
//!   path-variable binding are M2.
//! - Response: output [`DynamicMessage`] → canonical proto3 JSON.
//!
//! Both directions are descriptor-driven via `prost-reflect`'s `serde`
//! support, so no message type is known at compile time. Serialization
//! defaults to the canonical proto3 JSON mapping (64-bit ints as strings,
//! `lowerCamelCase` field names, enums as names, default-valued fields
//! skipped); [`JsonOptions`] exposes the knobs grpc-gateway also exposes.

use prost_reflect::{DeserializeOptions, DynamicMessage, MessageDescriptor, SerializeOptions};

/// JSON marshaling knobs, mirroring the subset of grpc-gateway's `JSONPb`
/// options M1 supports. Defaults are the canonical proto3 JSON mapping.
#[derive(Debug, Clone)]
pub struct JsonOptions {
    /// Emit fields holding their default value (grpc-gateway `EmitDefaults`).
    /// Default `false` (canonical proto3 JSON omits them).
    pub emit_default_fields: bool,
    /// Use the proto field name instead of `lowerCamelCase` (`json_name`).
    /// Default `false`.
    pub use_proto_field_names: bool,
    /// Serialize enum values as their integer instead of their name.
    /// Default `false`.
    pub use_enum_numbers: bool,
    /// Encode 64-bit integers as JSON strings (spec-required to avoid
    /// precision loss). Default `true`.
    pub stringify_64_bit_integers: bool,
    /// Reject unknown fields when decoding a request body. Default `true`.
    pub deny_unknown_fields: bool,
}

impl Default for JsonOptions {
    fn default() -> Self {
        JsonOptions {
            emit_default_fields: false,
            use_proto_field_names: false,
            use_enum_numbers: false,
            stringify_64_bit_integers: true,
            deny_unknown_fields: true,
        }
    }
}

impl JsonOptions {
    fn serialize_options(&self) -> SerializeOptions {
        SerializeOptions::new()
            .skip_default_fields(!self.emit_default_fields)
            .use_proto_field_name(self.use_proto_field_names)
            .use_enum_numbers(self.use_enum_numbers)
            .stringify_64_bit_integers(self.stringify_64_bit_integers)
    }

    fn deserialize_options(&self) -> DeserializeOptions {
        DeserializeOptions::new().deny_unknown_fields(self.deny_unknown_fields)
    }
}

/// A transcoding failure on either edge of the gateway.
#[derive(Debug)]
pub enum TranscodeError {
    /// The HTTP request body was not valid proto3 JSON for the input message.
    RequestJson(serde_json::Error),
    /// The output message could not be rendered as JSON.
    ResponseJson(serde_json::Error),
}

impl std::fmt::Display for TranscodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TranscodeError::RequestJson(e) => write!(f, "invalid JSON request body: {e}"),
            TranscodeError::ResponseJson(e) => write!(f, "failed to encode JSON response: {e}"),
        }
    }
}

impl std::error::Error for TranscodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TranscodeError::RequestJson(e) | TranscodeError::ResponseJson(e) => Some(e),
        }
    }
}

/// Decode an HTTP JSON request body into the input [`DynamicMessage`]
/// (whole-body `body: "*"` mapping).
///
/// An empty body yields the default (all-fields-unset) message, matching
/// grpc-gateway's treatment of an absent body as the zero message.
pub fn decode_request_body(
    input: &MessageDescriptor,
    body: &[u8],
    opts: &JsonOptions,
) -> Result<DynamicMessage, TranscodeError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(DynamicMessage::new(input.clone()));
    }
    let mut de = serde_json::Deserializer::from_slice(body);
    let msg = DynamicMessage::deserialize_with_options(
        input.clone(),
        &mut de,
        &opts.deserialize_options(),
    )
    .map_err(TranscodeError::RequestJson)?;
    de.end().map_err(TranscodeError::RequestJson)?;
    Ok(msg)
}

/// Encode an output [`DynamicMessage`] as canonical proto3 JSON bytes
/// (`application/json` response body).
pub fn encode_response_json(
    msg: &DynamicMessage,
    opts: &JsonOptions,
) -> Result<Vec<u8>, TranscodeError> {
    let mut buf = Vec::new();
    let mut ser = serde_json::Serializer::new(&mut buf);
    msg.serialize_with_options(&mut ser, &opts.serialize_options())
        .map_err(TranscodeError::ResponseJson)?;
    Ok(buf)
}
