//! M1 route table: lower decoded `google.api.http` rules (and synthesized
//! defaults for unannotated methods) into a flat, introspectable set of
//! HTTP→gRPC bindings.
//!
//! See `docs/design/grpc-gateway-design.md#route-table--path-templates` and
//! `#default-binding-policy-unannotated-methods`. This is the **M1 cut**:
//!
//! - Primary bindings only — `additional_bindings` are decoded but not yet
//!   registered (M2).
//! - Path templates are treated as **opaque literals**; the template grammar
//!   (captures, multi-segment, custom verbs) lands in M2.
//! - Unannotated methods get the synthesized default `POST /pkg.Svc/Method`
//!   with `body: "*"` when `unbound_methods` is enabled (Go grpc-gateway's
//!   `generate_unbound_methods` behaviour).

use serde::Serialize;

use crate::descriptor::{extract_http_rules, DescriptorError, HttpPattern, HttpRule, MethodHttp};

/// Where the request body comes from for a binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "field")]
pub enum BodySelector {
    /// `body: "*"` — the whole request message is parsed from the JSON body.
    Wildcard,
    /// No body is read (typical for `GET`/`DELETE`).
    None,
    /// `body: "field"` — a single field is parsed from the body (M2).
    Field(String),
}

/// One resolved HTTP entry point for a method.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteBinding {
    /// HTTP method, uppercase (`POST`, `GET`, …; custom verbs use their kind).
    pub http_method: String,
    /// Path template, an opaque literal in M1.
    pub http_path: String,
    pub body: BodySelector,
    /// `response_body` field selector, if any (M2 narrowing).
    pub response_body: Option<String>,
    /// `true` when this binding was synthesized as the unbound default,
    /// `false` when it came from a `google.api.http` annotation.
    pub synthesized: bool,
}

/// A method and the HTTP bindings that reach it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Route {
    /// Fully-qualified service name, e.g. `greeter.v1.Greeter`.
    pub service: String,
    /// Method short name, e.g. `SayHello`.
    pub method: String,
    /// gRPC wire path, e.g. `/greeter.v1.Greeter/SayHello`.
    pub grpc_path: String,
    pub server_streaming: bool,
    pub bindings: Vec<RouteBinding>,
}

/// The resolved set of routes for a descriptor set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteTable {
    pub routes: Vec<Route>,
}

/// A route conflict: two bindings that match the same HTTP method + path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteConflict {
    pub http_method: String,
    pub http_path: String,
    /// gRPC paths of the methods that collide on this `(method, path)`.
    pub grpc_paths: Vec<String>,
}

impl std::fmt::Display for RouteConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} is claimed by {}",
            self.http_method,
            self.http_path,
            self.grpc_paths.join(", ")
        )
    }
}

impl RouteTable {
    /// Build the route table directly from a serialized `FileDescriptorSet`.
    ///
    /// `unbound_methods` mirrors the config flag of the same name: when `true`
    /// (the default), every method lacking a primary `google.api.http` rule
    /// receives the synthesized `POST /pkg.Svc/Method` default binding.
    pub fn build(descriptor_set: &[u8], unbound_methods: bool) -> Result<Self, DescriptorError> {
        let methods = extract_http_rules(descriptor_set)?;
        Ok(Self::from_methods(methods, unbound_methods))
    }

    /// Build the route table from already-extracted [`MethodHttp`]s.
    pub fn from_methods(methods: Vec<MethodHttp>, unbound_methods: bool) -> Self {
        let routes = methods
            .into_iter()
            .map(|m| lower_method(m, unbound_methods))
            .collect();
        RouteTable { routes }
    }

    /// Total number of HTTP bindings across all routes.
    pub fn binding_count(&self) -> usize {
        self.routes.iter().map(|r| r.bindings.len()).sum()
    }

    /// Detect bindings that collide on the same `(http_method, http_path)`.
    ///
    /// In M1 path templates are opaque literals, so this is exact-string
    /// matching; M2's template matcher will subsume this with structural
    /// overlap detection.
    pub fn conflicts(&self) -> Vec<RouteConflict> {
        use std::collections::BTreeMap;

        // (method, path) → ordered, de-duplicated list of grpc paths.
        let mut seen: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
        for route in &self.routes {
            for binding in &route.bindings {
                let key = (binding.http_method.clone(), binding.http_path.clone());
                let entry = seen.entry(key).or_default();
                if !entry.contains(&route.grpc_path) {
                    entry.push(route.grpc_path.clone());
                }
            }
        }

        seen.into_iter()
            .filter(|(_, grpc_paths)| grpc_paths.len() > 1)
            .map(|((http_method, http_path), grpc_paths)| RouteConflict {
                http_method,
                http_path,
                grpc_paths,
            })
            .collect()
    }
}

/// Lower one method into a [`Route`], applying the M1 binding policy.
fn lower_method(method: MethodHttp, unbound_methods: bool) -> Route {
    let bindings = match &method.http_rule {
        Some(rule) => primary_binding(rule).into_iter().collect(),
        None if unbound_methods => vec![default_binding(&method.grpc_path)],
        None => Vec::new(),
    };

    Route {
        service: method.service,
        method: method.method,
        grpc_path: method.grpc_path,
        server_streaming: method.server_streaming,
        bindings,
    }
}

/// The synthesized unbound default: `POST /pkg.Svc/Method`, `body: "*"`.
fn default_binding(grpc_path: &str) -> RouteBinding {
    RouteBinding {
        http_method: "POST".to_owned(),
        http_path: grpc_path.to_owned(),
        body: BodySelector::Wildcard,
        response_body: None,
        synthesized: true,
    }
}

/// Lower the primary pattern of an annotated rule into a binding.
///
/// Returns `None` for a rule with no `pattern` set (a malformed `HttpRule`),
/// which `check` surfaces as an unresolved binding.
fn primary_binding(rule: &HttpRule) -> Option<RouteBinding> {
    let pattern = rule.pattern.as_ref()?;
    Some(RouteBinding {
        http_method: pattern.method().to_owned(),
        http_path: pattern.path().to_owned(),
        body: body_selector(pattern, &rule.body),
        response_body: (!rule.response_body.is_empty()).then(|| rule.response_body.clone()),
        synthesized: false,
    })
}

/// Map a rule's `body` string to a [`BodySelector`], defaulting body-less
/// methods (`GET`/`DELETE` with an empty `body`) to [`BodySelector::None`].
fn body_selector(pattern: &HttpPattern, body: &str) -> BodySelector {
    match body {
        "*" => BodySelector::Wildcard,
        "" => match pattern {
            HttpPattern::Get(_) | HttpPattern::Delete(_) => BodySelector::None,
            _ => BodySelector::Wildcard,
        },
        field => BodySelector::Field(field.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn method(grpc_path: &str, rule: Option<HttpRule>) -> MethodHttp {
        MethodHttp {
            service: "greeter.v1.Greeter".to_owned(),
            method: "M".to_owned(),
            grpc_path: grpc_path.to_owned(),
            server_streaming: false,
            http_rule: rule,
        }
    }

    #[test]
    fn unannotated_gets_default_post_binding() {
        let m = method("/greeter.v1.Greeter/Ping", None);
        let table = RouteTable::from_methods(vec![m], true);

        let route = &table.routes[0];
        assert_eq!(route.bindings.len(), 1);
        let b = &route.bindings[0];
        assert_eq!(b.http_method, "POST");
        assert_eq!(b.http_path, "/greeter.v1.Greeter/Ping");
        assert_eq!(b.body, BodySelector::Wildcard);
        assert!(b.synthesized);
    }

    #[test]
    fn unannotated_skipped_when_unbound_disabled() {
        let m = method("/greeter.v1.Greeter/Ping", None);
        let table = RouteTable::from_methods(vec![m], false);
        assert!(table.routes[0].bindings.is_empty());
    }

    #[test]
    fn annotated_get_has_no_body() {
        let rule = HttpRule {
            pattern: Some(HttpPattern::Get("/v1/greeter/{name}".to_owned())),
            ..HttpRule::default()
        };
        let table = RouteTable::from_methods(vec![method("/x/Y", Some(rule))], true);

        let b = &table.routes[0].bindings[0];
        assert_eq!(b.http_method, "GET");
        assert_eq!(b.http_path, "/v1/greeter/{name}");
        assert_eq!(b.body, BodySelector::None);
        assert!(!b.synthesized);
    }

    #[test]
    fn annotated_post_wildcard_body() {
        let rule = HttpRule {
            pattern: Some(HttpPattern::Post("/v1/greeter".to_owned())),
            body: "*".to_owned(),
            ..HttpRule::default()
        };
        let table = RouteTable::from_methods(vec![method("/x/Y", Some(rule))], true);

        let b = &table.routes[0].bindings[0];
        assert_eq!(b.http_method, "POST");
        assert_eq!(b.body, BodySelector::Wildcard);
    }

    #[test]
    fn field_body_selector_is_preserved() {
        let rule = HttpRule {
            pattern: Some(HttpPattern::Post("/v1/greeter".to_owned())),
            body: "greeting".to_owned(),
            response_body: "result".to_owned(),
            ..HttpRule::default()
        };
        let table = RouteTable::from_methods(vec![method("/x/Y", Some(rule))], true);

        let b = &table.routes[0].bindings[0];
        assert_eq!(b.body, BodySelector::Field("greeting".to_owned()));
        assert_eq!(b.response_body.as_deref(), Some("result"));
    }

    #[test]
    fn detects_conflicting_bindings() {
        let table = RouteTable::from_methods(
            vec![
                method("/x/A", None),
                MethodHttp {
                    method: "B".to_owned(),
                    grpc_path: "/x/B".to_owned(),
                    // Annotate B onto A's default path → conflict on POST /x/A.
                    http_rule: Some(HttpRule {
                        pattern: Some(HttpPattern::Post("/x/A".to_owned())),
                        body: "*".to_owned(),
                        ..HttpRule::default()
                    }),
                    ..method("/x/B", None)
                },
            ],
            true,
        );

        let conflicts = table.conflicts();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].http_method, "POST");
        assert_eq!(conflicts[0].http_path, "/x/A");
        assert_eq!(conflicts[0].grpc_paths, vec!["/x/A", "/x/B"]);
    }

    #[test]
    fn no_conflict_for_distinct_paths() {
        let table =
            RouteTable::from_methods(vec![method("/x/A", None), method("/x/B", None)], true);
        // Each defaults to POST on its own distinct gRPC path.
        assert!(table.conflicts().is_empty());
    }
}
