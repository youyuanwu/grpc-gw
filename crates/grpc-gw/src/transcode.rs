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

use prost_reflect::{
    DeserializeOptions, DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, ReflectMessage,
    SerializeOptions, Value,
};

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

/// A failure binding a string value (from a path variable or query parameter)
/// into a message field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindError {
    /// The field path named a field that does not exist on the message.
    UnknownField { path: String, field: String },
    /// A non-leaf path component is not a message (cannot descend into it).
    NotAMessage { path: String, field: String },
    /// The value could not be coerced to the leaf field's proto type.
    InvalidValue {
        path: String,
        kind: String,
        value: String,
    },
    /// The leaf field's proto type is not supported for path/query binding
    /// (e.g. `bytes`, nested `message`, or `map`).
    UnsupportedField { path: String, kind: String },
}

impl std::fmt::Display for BindError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BindError::UnknownField { path, field } => {
                write!(f, "unknown field {field:?} in field path {path:?}")
            }
            BindError::NotAMessage { path, field } => {
                write!(f, "field {field:?} in path {path:?} is not a message")
            }
            BindError::InvalidValue { path, kind, value } => {
                write!(f, "cannot bind {value:?} to {kind} field {path:?}")
            }
            BindError::UnsupportedField { path, kind } => {
                write!(f, "binding into {kind} field {path:?} is unsupported")
            }
        }
    }
}

impl std::error::Error for BindError {}

/// How a binding interacts with a field that is already set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindMode {
    /// Always set the value (path variables win over body/query).
    Overwrite,
    /// Only set if the field is currently unset (query fills, never overwrites).
    FillIfUnset,
}

/// Bind a single string `value` into `msg` at `field_path`, descending through
/// nested messages and coercing the value to the leaf field's proto type.
///
/// Repeated leaf fields **append**; singular leaf fields honour `mode`. Used for
/// both path-variable binding ([`BindMode::Overwrite`]) and query-parameter
/// expansion ([`BindMode::FillIfUnset`]).
pub fn bind_field_path(
    msg: &mut DynamicMessage,
    field_path: &[String],
    value: &str,
    mode: BindMode,
) -> Result<(), BindError> {
    bind_inner(msg, field_path, field_path, value, mode)
}

fn bind_inner(
    msg: &mut DynamicMessage,
    full_path: &[String],
    remaining: &[String],
    value: &str,
    mode: BindMode,
) -> Result<(), BindError> {
    let path_str = full_path.join(".");
    let (head, rest) = remaining
        .split_first()
        .expect("field path is non-empty by construction");
    let field =
        msg.descriptor()
            .get_field_by_name(head)
            .ok_or_else(|| BindError::UnknownField {
                path: path_str.clone(),
                field: head.clone(),
            })?;

    if rest.is_empty() {
        bind_leaf(msg, &field, &path_str, value, mode)
    } else {
        // Descend into a nested message field; `get_field_mut` materializes the
        // default (an empty message) if the field is currently unset.
        if !matches!(field.kind(), Kind::Message(_)) || field.is_list() || field.is_map() {
            return Err(BindError::NotAMessage {
                path: path_str,
                field: head.clone(),
            });
        }
        let child =
            msg.get_field_mut(&field)
                .as_message_mut()
                .ok_or_else(|| BindError::NotAMessage {
                    path: path_str.clone(),
                    field: head.clone(),
                })?;
        bind_inner(child, full_path, rest, value, mode)
    }
}

fn bind_leaf(
    msg: &mut DynamicMessage,
    field: &FieldDescriptor,
    path_str: &str,
    value: &str,
    mode: BindMode,
) -> Result<(), BindError> {
    let kind = field.kind();
    if field.is_map() {
        return Err(BindError::UnsupportedField {
            path: path_str.to_owned(),
            kind: "map".to_owned(),
        });
    }
    let coerced = coerce_scalar(&kind, value, path_str)?;
    if field.is_list() {
        match msg.get_field_mut(field) {
            Value::List(list) => list.push(coerced),
            other => *other = Value::List(vec![coerced]),
        }
    } else {
        if mode == BindMode::FillIfUnset && msg.has_field(field) {
            return Ok(());
        }
        msg.set_field(field, coerced);
    }
    Ok(())
}

/// Coerce a string to a scalar [`Value`] of the given proto [`Kind`].
fn coerce_scalar(kind: &Kind, value: &str, path: &str) -> Result<Value, BindError> {
    let invalid = || BindError::InvalidValue {
        path: path.to_owned(),
        kind: format!("{kind:?}").to_lowercase(),
        value: value.to_owned(),
    };
    let v = match kind {
        Kind::Bool => Value::Bool(match value {
            "true" | "1" | "t" | "T" | "TRUE" | "True" => true,
            "false" | "0" | "f" | "F" | "FALSE" | "False" => false,
            _ => return Err(invalid()),
        }),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
            Value::I32(value.parse().map_err(|_| invalid())?)
        }
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
            Value::I64(value.parse().map_err(|_| invalid())?)
        }
        Kind::Uint32 | Kind::Fixed32 => Value::U32(value.parse().map_err(|_| invalid())?),
        Kind::Uint64 | Kind::Fixed64 => Value::U64(value.parse().map_err(|_| invalid())?),
        Kind::Float => Value::F32(value.parse().map_err(|_| invalid())?),
        Kind::Double => Value::F64(value.parse().map_err(|_| invalid())?),
        Kind::String => Value::String(value.to_owned()),
        Kind::Enum(desc) => {
            // Accept the enum value by number or by name (canonical proto3 JSON).
            if let Ok(n) = value.parse::<i32>() {
                Value::EnumNumber(n)
            } else if let Some(v) = desc.get_value_by_name(value) {
                Value::EnumNumber(v.number())
            } else {
                return Err(invalid());
            }
        }
        Kind::Bytes => {
            return Err(BindError::UnsupportedField {
                path: path.to_owned(),
                kind: "bytes".to_owned(),
            })
        }
        Kind::Message(_) => {
            return Err(BindError::UnsupportedField {
                path: path.to_owned(),
                kind: "message".to_owned(),
            })
        }
    };
    Ok(v)
}
