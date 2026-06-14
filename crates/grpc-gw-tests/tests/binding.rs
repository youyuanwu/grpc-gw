//! Field-path binding tests: drive [`grpc_gw::transcode::bind_field_path`]
//! against the generated fixture descriptor set, covering the scalar coercions
//! (number/bool/enum/string), nested-message descent, repeated-field append,
//! and the [`BindMode`] precedence used by path-variable vs. query binding.

use grpc_gw::transcode::{bind_field_path, BindError, BindMode};
use grpc_gw_tests::GREETER_PB;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};

fn message(name: &str) -> MessageDescriptor {
    DescriptorPool::decode(GREETER_PB)
        .expect("descriptor set decodes")
        .get_message_by_name(name)
        .unwrap_or_else(|| panic!("message {name} not found"))
}

fn kitchen() -> DynamicMessage {
    DynamicMessage::new(message("greeter.v1.Kitchen"))
}

fn path(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| s.to_string()).collect()
}

#[test]
fn binds_scalar_string() {
    let mut msg = kitchen();
    bind_field_path(&mut msg, &path(&["text"]), "hi", BindMode::Overwrite).unwrap();
    assert_eq!(msg.get_field_by_name("text").unwrap().as_str(), Some("hi"));
}

#[test]
fn coerces_numbers_and_bool() {
    let mut msg = kitchen();
    bind_field_path(&mut msg, &path(&["i32"]), "7", BindMode::Overwrite).unwrap();
    bind_field_path(
        &mut msg,
        &path(&["i64"]),
        "9007199254740993",
        BindMode::Overwrite,
    )
    .unwrap();
    bind_field_path(&mut msg, &path(&["u64"]), "42", BindMode::Overwrite).unwrap();
    bind_field_path(&mut msg, &path(&["flag"]), "true", BindMode::Overwrite).unwrap();
    bind_field_path(&mut msg, &path(&["dbl"]), "1.5", BindMode::Overwrite).unwrap();

    assert_eq!(msg.get_field_by_name("i32").unwrap().as_i32(), Some(7));
    assert_eq!(
        msg.get_field_by_name("i64").unwrap().as_i64(),
        Some(9007199254740993)
    );
    assert_eq!(msg.get_field_by_name("u64").unwrap().as_u64(), Some(42));
    assert_eq!(msg.get_field_by_name("flag").unwrap().as_bool(), Some(true));
    assert_eq!(msg.get_field_by_name("dbl").unwrap().as_f64(), Some(1.5));
}

#[test]
fn coerces_enum_by_name_and_number() {
    let mut by_name = kitchen();
    bind_field_path(
        &mut by_name,
        &path(&["flavor"]),
        "SOUR",
        BindMode::Overwrite,
    )
    .unwrap();
    assert_eq!(
        by_name
            .get_field_by_name("flavor")
            .unwrap()
            .as_enum_number(),
        Some(2)
    );

    let mut by_num = kitchen();
    bind_field_path(&mut by_num, &path(&["flavor"]), "1", BindMode::Overwrite).unwrap();
    assert_eq!(
        by_num.get_field_by_name("flavor").unwrap().as_enum_number(),
        Some(1)
    );
}

#[test]
fn descends_into_nested_message() {
    let mut msg = kitchen();
    bind_field_path(
        &mut msg,
        &path(&["nested", "label"]),
        "n",
        BindMode::Overwrite,
    )
    .unwrap();
    bind_field_path(
        &mut msg,
        &path(&["nested", "count"]),
        "3",
        BindMode::Overwrite,
    )
    .unwrap();

    let nested = msg.get_field_by_name("nested").unwrap();
    let nested = nested.as_message().unwrap();
    assert_eq!(
        nested.get_field_by_name("label").unwrap().as_str(),
        Some("n")
    );
    assert_eq!(nested.get_field_by_name("count").unwrap().as_i32(), Some(3));
}

#[test]
fn appends_to_repeated_field() {
    let mut msg = kitchen();
    bind_field_path(&mut msg, &path(&["tags"]), "a", BindMode::FillIfUnset).unwrap();
    bind_field_path(&mut msg, &path(&["tags"]), "b", BindMode::FillIfUnset).unwrap();

    let tags = msg.get_field_by_name("tags").unwrap();
    let list = tags.as_list().unwrap();
    let got: Vec<&str> = list.iter().map(|v| v.as_str().unwrap()).collect();
    assert_eq!(got, vec!["a", "b"]);
}

#[test]
fn overwrite_vs_fill_if_unset() {
    let mut msg = kitchen();
    bind_field_path(&mut msg, &path(&["text"]), "first", BindMode::Overwrite).unwrap();

    // FillIfUnset must not clobber an already-set field (path > query).
    bind_field_path(&mut msg, &path(&["text"]), "second", BindMode::FillIfUnset).unwrap();
    assert_eq!(
        msg.get_field_by_name("text").unwrap().as_str(),
        Some("first")
    );

    // Overwrite replaces it.
    bind_field_path(&mut msg, &path(&["text"]), "third", BindMode::Overwrite).unwrap();
    assert_eq!(
        msg.get_field_by_name("text").unwrap().as_str(),
        Some("third")
    );
}

#[test]
fn unknown_field_errors() {
    let mut msg = kitchen();
    let err = bind_field_path(&mut msg, &path(&["nope"]), "x", BindMode::Overwrite).unwrap_err();
    assert!(matches!(err, BindError::UnknownField { .. }), "{err:?}");
}

#[test]
fn invalid_number_errors() {
    let mut msg = kitchen();
    let err = bind_field_path(
        &mut msg,
        &path(&["i32"]),
        "not-a-number",
        BindMode::Overwrite,
    )
    .unwrap_err();
    assert!(matches!(err, BindError::InvalidValue { .. }), "{err:?}");
}

#[test]
fn bytes_field_is_unsupported() {
    let mut msg = kitchen();
    let err = bind_field_path(&mut msg, &path(&["blob"]), "aGk=", BindMode::Overwrite).unwrap_err();
    assert!(matches!(err, BindError::UnsupportedField { .. }), "{err:?}");
}

#[test]
fn descending_through_non_message_errors() {
    let mut msg = kitchen();
    // `text` is a scalar; treating it as an intermediate message must error.
    let err =
        bind_field_path(&mut msg, &path(&["text", "x"]), "y", BindMode::Overwrite).unwrap_err();
    assert!(matches!(err, BindError::NotAMessage { .. }), "{err:?}");
}
