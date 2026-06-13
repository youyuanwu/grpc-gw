//! Descriptor loading and `google.api.http` extraction.
//!
//! This module is the home of **Spike 0** (see `docs/design/m1-scope.md`):
//! reading the `google.api.http` annotation off a runtime-loaded descriptor
//! set *without* any generated `annotations.rs`.
//!
//! With `prost-reflect`, this is a first-class reflection operation:
//!
//! - [`DescriptorPool::decode`] parses a `FileDescriptorSet` (`.pb`).
//! - [`prost_reflect::MethodDescriptor::options`] returns a [`DynamicMessage`]
//!   of the method's `MethodOptions`, **including extension fields**.
//! - The `(google.api.http)` option is resolved by name via
//!   [`DescriptorPool::get_extension_by_name`] and read with
//!   [`DynamicMessage::get_extension`].
//!
//! The descriptor set must include `google/api/annotations.proto` (build it
//! with `protoc --include_imports`) so the extension definition is present in
//! the pool. No `protoc` at runtime, no generated annotation types.

use prost_reflect::{DescriptorPool, DynamicMessage, ExtensionDescriptor};

/// Fully-qualified name of the `google.api.http` method extension.
pub const HTTP_RULE_EXTENSION: &str = "google.api.http";

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

/// Error returned while loading a descriptor set.
#[derive(Debug)]
pub enum DescriptorError {
    /// The `FileDescriptorSet` bytes failed to decode into a pool.
    Decode(prost_reflect::DescriptorError),
}

impl std::fmt::Display for DescriptorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DescriptorError::Decode(e) => write!(f, "failed to decode descriptor set: {e}"),
        }
    }
}

impl std::error::Error for DescriptorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DescriptorError::Decode(e) => Some(e),
        }
    }
}

impl From<prost_reflect::DescriptorError> for DescriptorError {
    fn from(e: prost_reflect::DescriptorError) -> Self {
        DescriptorError::Decode(e)
    }
}

/// Parse a serialized `FileDescriptorSet` and extract every method's
/// `google.api.http` rule (when present).
///
/// This is the Spike 0 entry point: it proves we can route off annotations
/// loaded from a `.pb` using `prost-reflect` reflection, with no generated
/// annotation types.
pub fn extract_http_rules(descriptor_set: &[u8]) -> Result<Vec<MethodHttp>, DescriptorError> {
    let pool = DescriptorPool::decode(descriptor_set)?;
    // The extension is resolved once from the pool; it is `None` if the set was
    // built without `google/api/annotations.proto` (no `--include_imports`).
    let http_ext = pool.get_extension_by_name(HTTP_RULE_EXTENSION);

    let mut out = Vec::new();
    for service in pool.services() {
        for method in service.methods() {
            let grpc_path = format!("/{}/{}", service.full_name(), method.name());
            let http_rule = http_ext
                .as_ref()
                .and_then(|ext| extract_rule(&method.options(), ext));

            out.push(MethodHttp {
                service: service.full_name().to_string(),
                method: method.name().to_string(),
                grpc_path,
                server_streaming: method.is_server_streaming(),
                http_rule,
            });
        }
    }

    Ok(out)
}

/// Read the `google.api.http` extension off a `MethodOptions` dynamic message,
/// returning `None` when the method is unannotated.
fn extract_rule(options: &DynamicMessage, ext: &ExtensionDescriptor) -> Option<HttpRule> {
    if !options.has_extension(ext) {
        return None;
    }
    let value = options.get_extension(ext);
    let rule_msg = value.as_message()?;
    Some(http_rule_from_message(rule_msg))
}

/// Lower an `HttpRule` `DynamicMessage` into our [`HttpRule`] (recursive for
/// `additional_bindings`).
fn http_rule_from_message(msg: &DynamicMessage) -> HttpRule {
    let mut rule = HttpRule {
        selector: string_field(msg, "selector"),
        body: string_field(msg, "body"),
        response_body: string_field(msg, "response_body"),
        ..HttpRule::default()
    };

    // The `pattern` oneof: exactly one of these is set on a valid rule.
    rule.pattern = pattern_field(msg, "get", HttpPattern::Get)
        .or_else(|| pattern_field(msg, "put", HttpPattern::Put))
        .or_else(|| pattern_field(msg, "post", HttpPattern::Post))
        .or_else(|| pattern_field(msg, "delete", HttpPattern::Delete))
        .or_else(|| pattern_field(msg, "patch", HttpPattern::Patch))
        .or_else(|| custom_pattern_field(msg));

    if msg.has_field_by_name("additional_bindings") {
        if let Some(value) = msg.get_field_by_name("additional_bindings") {
            if let Some(items) = value.as_list() {
                for item in items {
                    if let Some(nested) = item.as_message() {
                        rule.additional_bindings
                            .push(http_rule_from_message(nested));
                    }
                }
            }
        }
    }

    rule
}

/// Read a `string` field by name, defaulting to empty.
fn string_field(msg: &DynamicMessage, name: &str) -> String {
    msg.get_field_by_name(name)
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// If the named `string` pattern field is set, build the pattern via `ctor`.
fn pattern_field(
    msg: &DynamicMessage,
    name: &str,
    ctor: fn(String) -> HttpPattern,
) -> Option<HttpPattern> {
    if !msg.has_field_by_name(name) {
        return None;
    }
    let path = msg.get_field_by_name(name)?.as_str()?.to_owned();
    Some(ctor(path))
}

/// Read the `custom` `CustomHttpPattern { kind, path }` pattern, if set.
fn custom_pattern_field(msg: &DynamicMessage) -> Option<HttpPattern> {
    if !msg.has_field_by_name("custom") {
        return None;
    }
    let custom = msg.get_field_by_name("custom")?;
    let custom = custom.as_message()?;
    Some(HttpPattern::Custom(
        string_field(custom, "kind"),
        string_field(custom, "path"),
    ))
}
