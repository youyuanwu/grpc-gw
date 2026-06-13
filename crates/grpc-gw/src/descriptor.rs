//! Descriptor loading and `google.api.http` extraction.
//!
//! This module is the home of **Spike 0** (see `docs/design/m1-scope.md`):
//! reading the `google.api.http` annotation off a runtime-loaded descriptor
//! set *without* any generated `annotations.rs`.
//!
//! ## Why we decode unknown fields instead of using `protobuf::ext`
//!
//! `protobuf::ext` is a "stopgap" that needs a generated
//! `ExtFieldOptional<MethodOptions, HttpRule>` constant — i.e. codegen of
//! `google/api/annotations.proto`. That conflicts with the design goal of
//! depending only on the stable `protobuf::descriptor` types. When the
//! `protobuf` crate parses a `FileDescriptorSet`, the `(google.api.http)`
//! option (`MethodOptions` extension field **72295728**) is unknown to the
//! generated `MethodOptions` struct, so it is preserved verbatim in that
//! message's `UnknownFields` as a length-delimited blob — the serialized
//! `HttpRule`. We read that blob and hand-decode the few `HttpRule` fields we
//! need. No `protoc`, no generated annotations.

use protobuf::descriptor::FileDescriptorSet;
use protobuf::CodedInputStream;
use protobuf::Message;
use protobuf::UnknownValueRef;

/// `MethodOptions` extension field number for `google.api.http`.
pub const HTTP_RULE_FIELD_NUMBER: u32 = 72295728;

/// The HTTP method + path lowered from an `HttpRule` `pattern` oneof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpPattern {
    Get(String),
    Put(String),
    Post(String),
    Delete(String),
    Patch(String),
    /// A custom verb: `(kind, path)`.
    Custom(String, String),
}

impl HttpPattern {
    /// The path template of this pattern.
    pub fn path(&self) -> &str {
        match self {
            HttpPattern::Get(p)
            | HttpPattern::Put(p)
            | HttpPattern::Post(p)
            | HttpPattern::Delete(p)
            | HttpPattern::Patch(p)
            | HttpPattern::Custom(_, p) => p,
        }
    }

    /// The HTTP method as an uppercase string (custom verbs return their kind).
    pub fn method(&self) -> &str {
        match self {
            HttpPattern::Get(_) => "GET",
            HttpPattern::Put(_) => "PUT",
            HttpPattern::Post(_) => "POST",
            HttpPattern::Delete(_) => "DELETE",
            HttpPattern::Patch(_) => "PATCH",
            HttpPattern::Custom(kind, _) => kind,
        }
    }
}

/// A decoded `google.api.http` `HttpRule` (the subset M1/M2 need).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HttpRule {
    pub selector: String,
    pub pattern: Option<HttpPattern>,
    pub body: String,
    pub response_body: String,
    pub additional_bindings: Vec<HttpRule>,
}

/// One method's identity plus its (optional) decoded `HttpRule`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MethodHttp {
    /// Fully-qualified service name, e.g. `greeter.v1.Greeter`.
    pub service: String,
    /// Method short name, e.g. `SayHello`.
    pub method: String,
    /// gRPC wire path, e.g. `/greeter.v1.Greeter/SayHello`.
    pub grpc_path: String,
    pub server_streaming: bool,
    /// `Some` if the method carries a `google.api.http` annotation.
    pub http_rule: Option<HttpRule>,
}

/// Parse a serialized `FileDescriptorSet` and extract every method's
/// `google.api.http` rule (when present).
///
/// This is the Spike 0 entry point: it proves we can route off annotations
/// loaded from a `.pb` with no generated annotation types.
pub fn extract_http_rules(descriptor_set: &[u8]) -> protobuf::Result<Vec<MethodHttp>> {
    let fds = FileDescriptorSet::parse_from_bytes(descriptor_set)?;
    let mut out = Vec::new();

    for file in &fds.file {
        let package = file.package();
        for service in &file.service {
            let service_fqn = if package.is_empty() {
                service.name().to_string()
            } else {
                format!("{}.{}", package, service.name())
            };

            for method in &service.method {
                let grpc_path = format!("/{}/{}", service_fqn, method.name());
                let http_rule = method
                    .options
                    .as_ref()
                    .and_then(|opts| {
                        opts.special_fields
                            .unknown_fields()
                            .get(HTTP_RULE_FIELD_NUMBER)
                    })
                    .map(|value| match value {
                        UnknownValueRef::LengthDelimited(bytes) => decode_http_rule(bytes),
                        other => Err(protobuf::Error::from(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "google.api.http (field {HTTP_RULE_FIELD_NUMBER}) had unexpected \
                                 wire type: {other:?}"
                            ),
                        ))),
                    })
                    .transpose()?;

                out.push(MethodHttp {
                    service: service_fqn.clone(),
                    method: method.name().to_string(),
                    grpc_path,
                    server_streaming: method.server_streaming(),
                    http_rule,
                });
            }
        }
    }

    Ok(out)
}

/// Hand-decode an `HttpRule` message from its raw protobuf bytes.
///
/// `HttpRule` field numbers (from `google/api/http.proto`):
/// 1 selector, 2 get, 3 put, 4 post, 5 delete, 6 patch, 7 body,
/// 8 custom (CustomHttpPattern), 11 additional_bindings, 12 response_body.
fn decode_http_rule(bytes: &[u8]) -> protobuf::Result<HttpRule> {
    let mut is = CodedInputStream::from_bytes(bytes);
    let mut rule = HttpRule::default();

    while let Some(tag) = is.read_raw_tag_or_eof()? {
        let field_number = tag >> 3;
        let wire_type = tag & 0x7;
        match field_number {
            1 => rule.selector = is.read_string()?,
            2 => rule.pattern = Some(HttpPattern::Get(is.read_string()?)),
            3 => rule.pattern = Some(HttpPattern::Put(is.read_string()?)),
            4 => rule.pattern = Some(HttpPattern::Post(is.read_string()?)),
            5 => rule.pattern = Some(HttpPattern::Delete(is.read_string()?)),
            6 => rule.pattern = Some(HttpPattern::Patch(is.read_string()?)),
            7 => rule.body = is.read_string()?,
            8 => {
                let nested = is.read_bytes()?;
                let (kind, path) = decode_custom_pattern(&nested)?;
                rule.pattern = Some(HttpPattern::Custom(kind, path));
            }
            11 => {
                let nested = is.read_bytes()?;
                rule.additional_bindings.push(decode_http_rule(&nested)?);
            }
            12 => rule.response_body = is.read_string()?,
            _ => skip_field(&mut is, wire_type)?,
        }
    }

    Ok(rule)
}

/// Decode `CustomHttpPattern { string kind = 1; string path = 2; }`.
fn decode_custom_pattern(bytes: &[u8]) -> protobuf::Result<(String, String)> {
    let mut is = CodedInputStream::from_bytes(bytes);
    let mut kind = String::new();
    let mut path = String::new();

    while let Some(tag) = is.read_raw_tag_or_eof()? {
        let field_number = tag >> 3;
        let wire_type = tag & 0x7;
        match field_number {
            1 => kind = is.read_string()?,
            2 => path = is.read_string()?,
            _ => skip_field(&mut is, wire_type)?,
        }
    }

    Ok((kind, path))
}

/// Skip a field whose number we do not handle, by wire type.
fn skip_field(is: &mut CodedInputStream, wire_type: u32) -> protobuf::Result<()> {
    match wire_type {
        0 => {
            is.read_raw_varint64()?;
        }
        1 => {
            is.read_fixed64()?;
        }
        2 => {
            is.read_bytes()?;
        }
        5 => {
            is.read_fixed32()?;
        }
        other => {
            return Err(protobuf::Error::from(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported wire type {other} while skipping field"),
            )));
        }
    }
    Ok(())
}
