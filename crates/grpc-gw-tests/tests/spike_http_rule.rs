//! Spike 0 verification: load the generated fixture descriptor set and assert
//! that `google.api.http` annotations are extracted correctly — including the
//! `body` selector, a custom-ish additional binding, and the unannotated
//! method that has no rule.
//!
//! The descriptor set is built by this crate's `build.rs` (via `protoc`) and
//! exposed as [`grpc_gw_tests::GREETER_PB`].

use grpc_gw::{extract_http_rules, HttpPattern};
use grpc_gw_tests::GREETER_PB;

fn method<'a>(
    rules: &'a [grpc_gw::MethodHttp],
    service_suffix: &str,
    method: &str,
) -> &'a grpc_gw::MethodHttp {
    rules
        .iter()
        .find(|m| m.service.ends_with(service_suffix) && m.method == method)
        .unwrap_or_else(|| panic!("method {service_suffix}/{method} not found"))
}

#[test]
fn extracts_get_binding_with_path_template() {
    let rules = extract_http_rules(GREETER_PB).expect("descriptor set parses");
    let say_hello = method(&rules, "Greeter", "SayHello");

    assert_eq!(say_hello.grpc_path, "/greeter.v1.Greeter/SayHello");
    assert!(!say_hello.server_streaming);

    let rule = say_hello.http_rule.as_ref().expect("SayHello is annotated");
    assert_eq!(
        rule.pattern,
        Some(HttpPattern::Get("/v1/greeter/{name}".to_string()))
    );
    assert_eq!(rule.pattern.as_ref().unwrap().method(), "GET");
    assert_eq!(rule.body, "");
    assert!(rule.additional_bindings.is_empty());
}

#[test]
fn extracts_post_binding_with_body_and_additional_binding() {
    let rules = extract_http_rules(GREETER_PB).expect("descriptor set parses");
    let update = method(&rules, "Greeter", "UpdateGreeting");

    let rule = update
        .http_rule
        .as_ref()
        .expect("UpdateGreeting is annotated");
    assert_eq!(
        rule.pattern,
        Some(HttpPattern::Post("/v1/greeter/{name}/greeting".to_string()))
    );
    assert_eq!(rule.body, "greeting");

    assert_eq!(rule.additional_bindings.len(), 1, "one additional binding");
    let extra = &rule.additional_bindings[0];
    assert_eq!(
        extra.pattern,
        Some(HttpPattern::Patch(
            "/v1/greeter/{name}/greeting".to_string()
        ))
    );
    assert_eq!(extra.body, "greeting");
}

#[test]
fn unannotated_method_has_no_rule() {
    let rules = extract_http_rules(GREETER_PB).expect("descriptor set parses");
    let ping = method(&rules, "Greeter", "Ping");

    assert_eq!(ping.grpc_path, "/greeter.v1.Greeter/Ping");
    assert!(
        ping.http_rule.is_none(),
        "Ping has no google.api.http annotation"
    );
}

#[test]
fn all_methods_present() {
    let rules = extract_http_rules(GREETER_PB).expect("descriptor set parses");
    let greeter: Vec<_> = rules
        .iter()
        .filter(|m| m.service.ends_with("Greeter"))
        .map(|m| m.method.as_str())
        .collect();
    assert_eq!(greeter, vec!["SayHello", "UpdateGreeting", "Ping", "Echo"]);
}
