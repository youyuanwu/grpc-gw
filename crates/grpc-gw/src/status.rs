//! gRPC status codes: code → HTTP status mapping and the grpc-gateway-style
//! error envelope.
//!
//! See `docs/design/grpc-gateway-design.md#status--error-mapping`. The HTTP
//! mapping follows grpc-gateway's `runtime.HTTPStatusFromCode` exactly, and the
//! error body is the Status-proto JSON shape
//! `{ "code": n, "message": "...", "details": [...] }` (not the Google-API
//! error shape).
//!
//! M1 renders `code` + `message`; decoding `grpc-status-details-bin`
//! (`google.rpc.Status`) into `details` lands with the proxy wiring (it needs
//! the descriptor pool to resolve packed `Any` type URLs).

use serde::Serialize;

/// A gRPC status code (`grpc-status` trailer value), 0–16.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Code {
    Ok,
    Cancelled,
    Unknown,
    InvalidArgument,
    DeadlineExceeded,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    OutOfRange,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
    Unauthenticated,
}

impl Code {
    /// Map a numeric `grpc-status` value to a [`Code`]; out-of-range values
    /// (a misbehaving backend) fold to [`Code::Unknown`], as grpc-gateway does.
    pub fn from_i32(value: i32) -> Code {
        match value {
            0 => Code::Ok,
            1 => Code::Cancelled,
            2 => Code::Unknown,
            3 => Code::InvalidArgument,
            4 => Code::DeadlineExceeded,
            5 => Code::NotFound,
            6 => Code::AlreadyExists,
            7 => Code::PermissionDenied,
            8 => Code::ResourceExhausted,
            9 => Code::FailedPrecondition,
            10 => Code::Aborted,
            11 => Code::OutOfRange,
            12 => Code::Unimplemented,
            13 => Code::Internal,
            14 => Code::Unavailable,
            15 => Code::DataLoss,
            16 => Code::Unauthenticated,
            _ => Code::Unknown,
        }
    }

    /// The numeric `grpc-status` value.
    pub fn as_i32(self) -> i32 {
        self as i32
    }

    /// The canonical screaming-snake-case name (e.g. `NOT_FOUND`).
    pub fn name(self) -> &'static str {
        match self {
            Code::Ok => "OK",
            Code::Cancelled => "CANCELLED",
            Code::Unknown => "UNKNOWN",
            Code::InvalidArgument => "INVALID_ARGUMENT",
            Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Code::NotFound => "NOT_FOUND",
            Code::AlreadyExists => "ALREADY_EXISTS",
            Code::PermissionDenied => "PERMISSION_DENIED",
            Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Code::FailedPrecondition => "FAILED_PRECONDITION",
            Code::Aborted => "ABORTED",
            Code::OutOfRange => "OUT_OF_RANGE",
            Code::Unimplemented => "UNIMPLEMENTED",
            Code::Internal => "INTERNAL",
            Code::Unavailable => "UNAVAILABLE",
            Code::DataLoss => "DATA_LOSS",
            Code::Unauthenticated => "UNAUTHENTICATED",
        }
    }

    /// The HTTP status code grpc-gateway maps this gRPC code to.
    pub fn http_status(self) -> u16 {
        match self {
            Code::Ok => 200,
            Code::Cancelled => 499, // client closed request (nginx convention)
            Code::Unknown => 500,
            Code::InvalidArgument => 400,
            Code::DeadlineExceeded => 504,
            Code::NotFound => 404,
            Code::AlreadyExists => 409,
            Code::PermissionDenied => 403,
            Code::ResourceExhausted => 429,
            Code::FailedPrecondition => 400,
            Code::Aborted => 409,
            Code::OutOfRange => 400,
            Code::Unimplemented => 501,
            Code::Internal => 500,
            Code::Unavailable => 503,
            Code::DataLoss => 500,
            Code::Unauthenticated => 401,
        }
    }
}

/// The grpc-gateway-compatible error envelope, serialized as
/// `{ "code": n, "message": "...", "details": [...] }`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ErrorEnvelope {
    /// Numeric gRPC status code.
    pub code: i32,
    /// Human-readable `grpc-message`.
    pub message: String,
    /// Decoded `google.rpc.Status` details (packed `Any`s). Empty in M1 until
    /// the proxy decodes `grpc-status-details-bin`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<serde_json::Value>,
}

impl ErrorEnvelope {
    /// Build an envelope for a code + message, with no details.
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        ErrorEnvelope {
            code: code.as_i32(),
            message: message.into(),
            details: Vec::new(),
        }
    }

    /// The HTTP status this envelope should be served with.
    pub fn http_status(&self) -> u16 {
        Code::from_i32(self.code).http_status()
    }

    /// Serialize the envelope to JSON bytes.
    pub fn to_json(&self) -> Vec<u8> {
        // The envelope is a closed shape of plain types — serialization here
        // cannot fail, so an unwrap is sound.
        serde_json::to_vec(self).expect("error envelope serializes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_all_codes_to_grpc_gateway_http_statuses() {
        let cases = [
            (Code::Ok, 200),
            (Code::Cancelled, 499),
            (Code::Unknown, 500),
            (Code::InvalidArgument, 400),
            (Code::DeadlineExceeded, 504),
            (Code::NotFound, 404),
            (Code::AlreadyExists, 409),
            (Code::PermissionDenied, 403),
            (Code::ResourceExhausted, 429),
            (Code::FailedPrecondition, 400),
            (Code::Aborted, 409),
            (Code::OutOfRange, 400),
            (Code::Unimplemented, 501),
            (Code::Internal, 500),
            (Code::Unavailable, 503),
            (Code::DataLoss, 500),
            (Code::Unauthenticated, 401),
        ];
        for (code, http) in cases {
            assert_eq!(code.http_status(), http, "{}", code.name());
        }
    }

    #[test]
    fn round_trips_numeric_codes() {
        for n in 0..=16 {
            assert_eq!(Code::from_i32(n).as_i32(), n);
        }
    }

    #[test]
    fn out_of_range_code_folds_to_unknown() {
        assert_eq!(Code::from_i32(42), Code::Unknown);
        assert_eq!(Code::from_i32(-1), Code::Unknown);
    }

    #[test]
    fn envelope_renders_status_proto_shape() {
        let env = ErrorEnvelope::new(Code::NotFound, "greeter not found");
        assert_eq!(env.http_status(), 404);
        assert_eq!(
            env.to_json(),
            br#"{"code":5,"message":"greeter not found"}"#
        );
    }

    #[test]
    fn envelope_includes_details_when_present() {
        let mut env = ErrorEnvelope::new(Code::InvalidArgument, "bad");
        env.details
            .push(serde_json::json!({"@type": "x", "field": "name"}));
        let json = String::from_utf8(env.to_json()).unwrap();
        assert!(json.contains("\"details\":["));
        assert!(json.contains("\"field\":\"name\""));
    }
}
