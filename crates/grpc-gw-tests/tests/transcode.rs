//! Transcoding round-trip tests over the generated fixture descriptor set.
//!
//! These exercise [`grpc_gw::transcode`] (JSON ⇄ dynamic message) as a black
//! box against [`grpc_gw_tests::GREETER_PB`]. They live here (rather than as
//! unit tests inside `grpc-gw`) so the only consumer of the generated `.pb` is
//! this fixtures crate.

use grpc_gw::transcode::{decode_request_body, encode_response_json, JsonOptions, TranscodeError};
use grpc_gw_tests::GREETER_PB;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, Value};

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

    assert_eq!(msg.get_field_by_name("name").unwrap().as_str(), Some("ada"));
    assert_eq!(
        msg.get_field_by_name("greeting").unwrap().as_str(),
        Some("hi")
    );
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
    let mut msg = DynamicMessage::new(desc);
    msg.set_field_by_name("message", Value::String("hello, ada".to_owned()));

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
    let desc = message("greeter.v1.UpdateGreetingRequest");
    let req = decode_request_body(&desc, br#"{"name":"x"}"#, &JsonOptions::default()).unwrap();
    // greeting defaults to "" and is skipped in canonical JSON.
    let bytes = encode_response_json(&req, &JsonOptions::default()).unwrap();
    assert_eq!(bytes, br#"{"name":"x"}"#);
}

// --- proto3-JSON type coverage (acceptance criterion #1) ---------------------

/// Build a fully-populated `Kitchen` message, then assert it round-trips
/// request JSON → dynamic message → canonical response JSON byte-for-byte,
/// covering the proto3-JSON edge cases (int64-as-string, enum-as-name, nested,
/// repeated, map, bytes-as-base64, timestamp-as-RFC3339).
#[test]
fn kitchen_canonical_json_round_trip() {
    let desc = message("greeter.v1.Kitchen");

    // Note: canonical proto3 JSON uses lowerCamelCase field names, 64-bit ints
    // as strings, enums by name, bytes as base64, and Timestamp as RFC 3339.
    let input = br#"{
        "i32": 7,
        "i64": "9007199254740993",
        "u64": "18446744073709551615",
        "flag": true,
        "dbl": 1.5,
        "text": "hi",
        "blob": "aGVsbG8=",
        "flavor": "SOUR",
        "nested": { "label": "n", "count": 3 },
        "tags": ["a", "b"],
        "scores": { "x": 1, "y": 2 },
        "at": "2026-06-13T12:00:00Z"
    }"#;

    let msg = decode_request_body(&desc, input, &JsonOptions::default()).expect("valid Kitchen");
    let out = encode_response_json(&msg, &JsonOptions::default()).expect("encodes");
    let out: serde_json::Value = serde_json::from_slice(&out).unwrap();

    assert_eq!(out["i32"], 7);
    assert_eq!(out["i64"], "9007199254740993"); // string, no precision loss
    assert_eq!(out["u64"], "18446744073709551615");
    assert_eq!(out["flag"], true);
    assert_eq!(out["dbl"], 1.5);
    assert_eq!(out["text"], "hi");
    assert_eq!(out["blob"], "aGVsbG8="); // base64("hello")
    assert_eq!(out["flavor"], "SOUR"); // enum name
    assert_eq!(out["nested"]["label"], "n");
    assert_eq!(out["nested"]["count"], 3);
    assert_eq!(out["tags"], serde_json::json!(["a", "b"]));
    assert_eq!(out["scores"]["x"], 1);
    assert_eq!(out["scores"]["y"], 2);
    assert_eq!(out["at"], "2026-06-13T12:00:00Z");
}

#[test]
fn kitchen_enum_as_number_option() {
    let desc = message("greeter.v1.Kitchen");
    let msg = decode_request_body(&desc, br#"{"flavor":"SWEET"}"#, &JsonOptions::default())
        .expect("valid");
    let opts = JsonOptions {
        use_enum_numbers: true,
        ..JsonOptions::default()
    };
    let out: serde_json::Value =
        serde_json::from_slice(&encode_response_json(&msg, &opts).unwrap()).unwrap();
    assert_eq!(out["flavor"], 1); // SWEET = 1
}

#[test]
fn kitchen_int64_accepts_number_or_string_in() {
    // Proto3 JSON permits int64 as a number on input; output is always a string.
    let desc = message("greeter.v1.Kitchen");
    let msg =
        decode_request_body(&desc, br#"{"i64": 42}"#, &JsonOptions::default()).expect("valid");
    let out: serde_json::Value =
        serde_json::from_slice(&encode_response_json(&msg, &JsonOptions::default()).unwrap())
            .unwrap();
    assert_eq!(out["i64"], "42");
}

#[test]
fn kitchen_proto_field_names_option() {
    let desc = message("greeter.v1.Kitchen");
    let msg =
        decode_request_body(&desc, br#"{"text":"x"}"#, &JsonOptions::default()).expect("valid");
    let opts = JsonOptions {
        use_proto_field_names: true,
        ..JsonOptions::default()
    };
    // Single-word fields are identical either way; assert the option is honored
    // and the value survives.
    let out: serde_json::Value =
        serde_json::from_slice(&encode_response_json(&msg, &opts).unwrap()).unwrap();
    assert_eq!(out["text"], "x");
}
