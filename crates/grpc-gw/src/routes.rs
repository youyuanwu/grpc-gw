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
use crate::template::{PathTemplate, Segment};

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

/// How two overlapping bindings relate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictKind {
    /// The two bindings have the *same* `(method, path)` template — one is an
    /// exact duplicate of the other. Always an error (the later is unreachable
    /// and indistinguishable).
    Duplicate,
    /// The later binding is structurally **shadowed** by an earlier one that
    /// overlaps it: some concrete request path matches both, and the
    /// earlier-declared one wins (Go grpc-gateway first-match semantics). The
    /// later route is unreachable. A warning by default; an error under
    /// `strict_routes`.
    Shadowed,
}

/// A route overlap: a binding that collides with an earlier-declared one on the
/// same HTTP method (an exact [`Duplicate`](ConflictKind::Duplicate) or a
/// [`Shadowed`](ConflictKind::Shadowed) structural overlap).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteConflict {
    pub kind: ConflictKind,
    pub http_method: String,
    /// The later (shadowed/duplicated) binding's path template.
    pub http_path: String,
    /// gRPC paths of the colliding methods: `[earlier, later]`.
    pub grpc_paths: Vec<String>,
}

impl RouteConflict {
    /// Whether this overlap is a hard error given the strictness setting. Exact
    /// duplicates are always errors; shadowing is an error only under
    /// `strict_routes`.
    pub fn is_error(&self, strict_routes: bool) -> bool {
        self.kind == ConflictKind::Duplicate || strict_routes
    }
}

impl std::fmt::Display for RouteConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            ConflictKind::Duplicate => write!(
                f,
                "{} {} is claimed by {}",
                self.http_method,
                self.http_path,
                self.grpc_paths.join(", ")
            ),
            ConflictKind::Shadowed => write!(
                f,
                "{} {} ({}) is shadowed by earlier route {}",
                self.http_method,
                self.http_path,
                self.grpc_paths.last().map(String::as_str).unwrap_or(""),
                self.grpc_paths.first().map(String::as_str).unwrap_or(""),
            ),
        }
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

    /// Detect bindings that structurally overlap on the same HTTP method.
    ///
    /// Templates are compiled and compared structurally (literals, `*`, `**`,
    /// and variable sub-patterns), so `/v1/{a}` and `/v1/foo` are reported as a
    /// [`Shadowed`](ConflictKind::Shadowed) overlap even though their literal
    /// strings differ. The earlier-declared binding wins at runtime (Go
    /// first-match semantics); each later binding is reported against the first
    /// earlier binding it collides with. An identical `(method, path)` is a
    /// [`Duplicate`](ConflictKind::Duplicate). Bindings whose templates fail to
    /// parse are skipped here (surfaced separately as parse errors).
    pub fn conflicts(&self) -> Vec<RouteConflict> {
        // Flatten bindings in declaration order, compiling each template.
        let mut entries: Vec<(String, String, String, PathTemplate)> = Vec::new();
        for route in &self.routes {
            for binding in &route.bindings {
                if let Ok(template) = PathTemplate::parse(&binding.http_path) {
                    entries.push((
                        binding.http_method.clone(),
                        binding.http_path.clone(),
                        route.grpc_path.clone(),
                        template,
                    ));
                }
            }
        }

        let mut conflicts = Vec::new();
        for j in 1..entries.len() {
            for i in 0..j {
                let (mi, _, gi, ti) = &entries[i];
                let (mj, pj, gj, tj) = &entries[j];
                if mi != mj || ti.verb != tj.verb {
                    continue;
                }
                if templates_overlap(ti, tj) {
                    let kind = if entries[i].1 == *pj {
                        ConflictKind::Duplicate
                    } else {
                        ConflictKind::Shadowed
                    };
                    conflicts.push(RouteConflict {
                        kind,
                        http_method: mj.clone(),
                        http_path: pj.clone(),
                        grpc_paths: vec![gi.clone(), gj.clone()],
                    });
                    break; // shadowed by the first earlier match
                }
            }
        }
        conflicts
    }
}

/// A structural position in a flattened template: a literal, a single-segment
/// wildcard, or a catch-all (the rest).
enum Slot {
    Lit(String),
    Any,
    Rest,
}

/// Flatten a template (expanding variable sub-patterns) into structural slots.
fn template_slots(t: &PathTemplate) -> Vec<Slot> {
    fn push_seg(out: &mut Vec<Slot>, t: &PathTemplate, seg: &Segment) {
        match seg {
            Segment::Literal(s) => out.push(Slot::Lit(s.clone())),
            Segment::Single => out.push(Slot::Any),
            Segment::CatchAll => out.push(Slot::Rest),
            Segment::Variable(idx) => {
                for sub in &t.vars[*idx].sub {
                    push_seg(out, t, sub);
                }
            }
        }
    }
    let mut out = Vec::new();
    for seg in &t.segments {
        push_seg(&mut out, t, seg);
    }
    out
}

/// Whether two templates could both match some concrete request path.
fn templates_overlap(a: &PathTemplate, b: &PathTemplate) -> bool {
    slots_overlap(&template_slots(a), &template_slots(b))
}

fn slots_overlap(a: &[Slot], b: &[Slot]) -> bool {
    match (a.split_first(), b.split_first()) {
        (None, None) => true,
        // `**` is always the final slot and matches the remainder (incl. empty).
        (Some((Slot::Rest, _)), _) | (_, Some((Slot::Rest, _))) => true,
        (None, Some(_)) | (Some(_), None) => false,
        (Some((x, ar)), Some((y, br))) => {
            let compatible = match (x, y) {
                (Slot::Lit(l), Slot::Lit(r)) => l == r,
                _ => true, // a wildcard matches a literal or another wildcard
            };
            compatible && slots_overlap(ar, br)
        }
    }
}

/// Lower one method into a [`Route`], applying the binding policy.
fn lower_method(method: MethodHttp, unbound_methods: bool) -> Route {
    let bindings = match &method.http_rule {
        Some(rule) => lower_rule(rule),
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

/// Lower a rule's primary pattern plus any `additional_bindings` into bindings,
/// in declaration order (primary first). `additional_bindings` may not nest, so
/// only one level is flattened (matching the google.api.http spec).
fn lower_rule(rule: &HttpRule) -> Vec<RouteBinding> {
    let mut bindings = Vec::new();
    if let Some(b) = primary_binding(rule) {
        bindings.push(b);
    }
    for additional in &rule.additional_bindings {
        if let Some(b) = primary_binding(additional) {
            bindings.push(b);
        }
    }
    bindings
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

    #[test]
    fn lowers_additional_bindings() {
        let rule = HttpRule {
            pattern: Some(HttpPattern::Post("/v1/greeter/{name}/greeting".to_owned())),
            body: "greeting".to_owned(),
            additional_bindings: vec![HttpRule {
                pattern: Some(HttpPattern::Patch("/v1/greeter/{name}/greeting".to_owned())),
                body: "greeting".to_owned(),
                ..HttpRule::default()
            }],
            ..HttpRule::default()
        };
        let table = RouteTable::from_methods(vec![method("/x/Update", Some(rule))], true);

        let bindings = &table.routes[0].bindings;
        assert_eq!(bindings.len(), 2, "primary + 1 additional");
        assert_eq!(bindings[0].http_method, "POST");
        assert_eq!(bindings[1].http_method, "PATCH");
        assert_eq!(bindings[1].http_path, "/v1/greeter/{name}/greeting");
    }

    #[test]
    fn detects_structural_shadowing() {
        // An earlier `/v1/{x}` variable route shadows a later literal `/v1/foo`.
        let shadower = MethodHttp {
            method: "A".to_owned(),
            grpc_path: "/x/A".to_owned(),
            http_rule: Some(HttpRule {
                pattern: Some(HttpPattern::Get("/v1/{x}".to_owned())),
                ..HttpRule::default()
            }),
            ..method("/x/A", None)
        };
        let shadowed = MethodHttp {
            method: "B".to_owned(),
            grpc_path: "/x/B".to_owned(),
            http_rule: Some(HttpRule {
                pattern: Some(HttpPattern::Get("/v1/foo".to_owned())),
                ..HttpRule::default()
            }),
            ..method("/x/B", None)
        };
        let table = RouteTable::from_methods(vec![shadower, shadowed], false);

        let conflicts = table.conflicts();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].kind, ConflictKind::Shadowed);
        assert_eq!(conflicts[0].grpc_paths, vec!["/x/A", "/x/B"]);
        // Shadowing is a warning by default, an error only under strict_routes.
        assert!(!conflicts[0].is_error(false));
        assert!(conflicts[0].is_error(true));
    }

    #[test]
    fn distinct_literals_do_not_shadow() {
        let a = MethodHttp {
            method: "A".to_owned(),
            grpc_path: "/x/A".to_owned(),
            http_rule: Some(HttpRule {
                pattern: Some(HttpPattern::Get("/v1/foo".to_owned())),
                ..HttpRule::default()
            }),
            ..method("/x/A", None)
        };
        let b = MethodHttp {
            method: "B".to_owned(),
            grpc_path: "/x/B".to_owned(),
            http_rule: Some(HttpRule {
                pattern: Some(HttpPattern::Get("/v1/bar".to_owned())),
                ..HttpRule::default()
            }),
            ..method("/x/B", None)
        };
        assert!(RouteTable::from_methods(vec![a, b], false)
            .conflicts()
            .is_empty());
    }
}
