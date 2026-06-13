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

#[cfg(test)]
mod tests {
    use super::*;
    use prost_reflect::DescriptorPool;

    const GREETER_PB: &[u8] = include_bytes!("../tests/fixtures/greeter.pb");

    fn message(name: &str) -> MessageDescriptor {
        DescriptorPool::decode(GREETER_PB)
            .expect("descriptor set decodes")
            .get_message_by_name(name)
            .unwrap_or_else(|| panic!("message {name} not found"))
    }

    #[test]
    fn decodes_request_body_into_dynamic_message() {
        let desc = message("greeter.v1.UpdateGreetingRequest");
        let msg = decode_request_body(
            &desc,
            br#"{"name":"ada","greeting":"hi"}"#,
            &JsonOptions::default(),
        )
        .expect("valid request");

        let name = msg.get_field_by_name("name").unwrap();
        assert_eq!(name.as_str(), Some("ada"));
        let greeting = msg.get_field_by_name("greeting").unwrap();
        assert_eq!(greeting.as_str(), Some("hi"));
    }

    #[test]
    fn empty_body_yields_default_message() {
        let desc = message("greeter.v1.PingRequest");
        let msg = decode_request_body(&desc, b"", &JsonOptions::default())
            .expect("empty body is the default message");
        assert_eq!(
            encode_response_json(&msg, &JsonOptions::default()).unwrap(),
            b"{}"
        );
    }

    #[test]
    fn rejects_unknown_fields_by_default() {
        let desc = message("greeter.v1.HelloRequest");
        let err = decode_request_body(&desc, br#"{"nope":1}"#, &JsonOptions::default())
            .expect_err("unknown field is rejected");
        assert!(matches!(err, TranscodeError::RequestJson(_)));
    }

    #[test]
    fn rejects_malformed_json() {
        let desc = message("greeter.v1.HelloRequest");
        let err = decode_request_body(&desc, b"{not json", &JsonOptions::default())
            .expect_err("malformed JSON is rejected");
        assert!(matches!(err, TranscodeError::RequestJson(_)));
    }

    #[test]
    fn encodes_response_as_canonical_json() {
        let desc = message("greeter.v1.HelloReply");
        let mut msg = DynamicMessage::new(desc.clone());
        msg.set_field_by_name(
            "message",
            prost_reflect::Value::String("hello, ada".to_owned()),
        );

        let bytes = encode_response_json(&msg, &JsonOptions::default()).expect("encodes");
        assert_eq!(bytes, br#"{"message":"hello, ada"}"#);
    }

    #[test]
    fn skips_default_fields_by_default() {
        let desc = message("greeter.v1.HelloReply");
        let msg = DynamicMessage::new(desc); // message field is empty/default
        let bytes = encode_response_json(&msg, &JsonOptions::default()).expect("encodes");
        assert_eq!(bytes, b"{}");
    }

    #[test]
    fn emit_default_fields_includes_empties() {
        let desc = message("greeter.v1.HelloReply");
        let msg = DynamicMessage::new(desc);
        let opts = JsonOptions {
            emit_default_fields: true,
            ..JsonOptions::default()
        };
        let bytes = encode_response_json(&msg, &opts).expect("encodes");
        assert_eq!(bytes, br#"{"message":""}"#);
    }

    #[test]
    fn round_trips_request_to_response_shape() {
        let req_desc = message("greeter.v1.UpdateGreetingRequest");
        let req =
            decode_request_body(&req_desc, br#"{"name":"x"}"#, &JsonOptions::default()).unwrap();
        // greeting defaults to "" and is skipped in canonical JSON.
        let bytes = encode_response_json(&req, &JsonOptions::default()).unwrap();
        assert_eq!(bytes, br#"{"name":"x"}"#);
    }
}
