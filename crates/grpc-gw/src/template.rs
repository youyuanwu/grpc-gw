//! M2 path templates: parse the `google.api.http` path-template grammar into a
//! matchable form, route a request path against it (Go grpc-gateway
//! declaration-order, first-match-wins semantics), and capture the variable
//! segments for field-path binding.
//!
//! See `docs/design/m2-path-templates.md`. The grammar (from
//! `google/api/http.proto`):
//!
//! ```ebnf
//! Template  = "/" Segments [ Verb ] ;
//! Segments  = Segment { "/" Segment } ;
//! Segment   = "*" | "**" | LITERAL | Variable ;
//! Variable  = "{" FieldPath [ "=" Segments ] "}" ;
//! FieldPath = IDENT { "." IDENT } ;
//! Verb      = ":" LITERAL ;
//! ```
//!
//! Semantics honoured here:
//! - a bare `{name}` is sugar for `{name=*}` (captures exactly one segment);
//! - `*` matches one segment, `**` matches zero-or-more and may only appear as
//!   the final segment (of the path, or of a variable's sub-pattern);
//! - a variable's value is its matched segment(s): a single/literal capture is
//!   one segment, a `**` capture is the remaining segments joined by `/`;
//! - variables may not nest; a field path is one-or-more identifiers.

use std::fmt;

/// A single element of a compiled path template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segment {
    /// A literal path segment, matched exactly (e.g. `v1`).
    Literal(String),
    /// `*` — matches exactly one path segment, any value.
    Single,
    /// `**` — matches zero or more path segments (only valid as the final
    /// segment of a template or of a variable sub-pattern).
    CatchAll,
    /// A `{field.path=sub/pattern}` capture; indexes into [`PathTemplate::vars`].
    Variable(usize),
}

/// How many path segments a variable's sub-pattern consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentCount {
    /// A fixed number of segments (no `**` in the sub-pattern).
    Exact(usize),
    /// At least `min` segments, unbounded above (the sub-pattern ends in `**`).
    AtLeast(usize),
}

/// A captured variable: its message field path and the span of the template's
/// flattened segment list that it covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VarSpec {
    /// The field path, e.g. `{user.id}` → `["user", "id"]`.
    pub field_path: Vec<String>,
    /// Sub-pattern segments this variable expands to (each `Single`, `CatchAll`,
    /// or `Literal`; never a nested `Variable`).
    pub sub: Vec<Segment>,
    /// Number of request path segments this variable consumes.
    pub count: SegmentCount,
}

/// A compiled path template: a flat segment list (variables expanded inline via
/// `Segment::Variable(idx)`) plus the captured variables and an optional verb.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathTemplate {
    /// The literal template string, kept for display / introspection.
    pub raw: String,
    /// The top-level segment sequence (variables appear as `Variable(idx)`).
    pub segments: Vec<Segment>,
    /// Captured variables, in left-to-right order.
    pub vars: Vec<VarSpec>,
    /// A trailing custom verb (`:verb`), if present.
    pub verb: Option<String>,
}

/// A parse error with a human-readable reason (surfaced by `grpc-gw check`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateError {
    pub template: String,
    pub reason: String,
}

impl fmt::Display for TemplateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid path template {:?}: {}",
            self.template, self.reason
        )
    }
}

impl std::error::Error for TemplateError {}

/// A successful match: each variable's captured value (decoded path segments
/// joined by `/`), paired with the variable's field path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Captures {
    /// `(field_path, value)` for each variable, in template order.
    pub vars: Vec<(Vec<String>, String)>,
}

impl PathTemplate {
    /// Parse a `google.api.http` path template.
    pub fn parse(template: &str) -> Result<PathTemplate, TemplateError> {
        Parser::new(template).parse()
    }

    /// Match a request path (already split into raw, still-percent-encoded
    /// segments) plus an optional trailing verb against this template.
    ///
    /// Returns `Some(Captures)` on a full match. Captured segment values are
    /// percent-decoded; literals and `*`/`**` structural segments are matched
    /// on the decoded value.
    pub fn matches(&self, path_segments: &[&str], verb: Option<&str>) -> Option<Captures> {
        if self.verb.as_deref() != verb {
            return None;
        }
        let mut captures = Vec::with_capacity(self.vars.len());
        if self.match_segments(&self.segments, path_segments, &mut captures) {
            Some(Captures { vars: captures })
        } else {
            None
        }
    }

    /// Recursively match `segments` against `input`, accumulating variable
    /// captures. Returns `true` only on an exact, full consumption of `input`.
    fn match_segments(
        &self,
        segments: &[Segment],
        input: &[&str],
        captures: &mut Vec<(Vec<String>, String)>,
    ) -> bool {
        match segments.split_first() {
            None => input.is_empty(),
            Some((first, rest)) => match first {
                Segment::Literal(lit) => match input.split_first() {
                    Some((head, tail)) if decode(head) == *lit => {
                        self.match_segments(rest, tail, captures)
                    }
                    _ => false,
                },
                Segment::Single => match input.split_first() {
                    Some((_, tail)) => self.match_segments(rest, tail, captures),
                    None => false,
                },
                Segment::CatchAll => {
                    // `**` only ever appears last (enforced at parse time), so it
                    // greedily consumes the remainder.
                    rest.is_empty()
                }
                Segment::Variable(idx) => self.match_variable(*idx, rest, input, captures),
            },
        }
    }

    /// Match the variable at `vars[idx]` against the head of `input`, then the
    /// remaining `rest` template segments against the tail.
    fn match_variable(
        &self,
        idx: usize,
        rest: &[Segment],
        input: &[&str],
        captures: &mut Vec<(Vec<String>, String)>,
    ) -> bool {
        let var = &self.vars[idx];
        let take = match var.count {
            SegmentCount::Exact(n) => n,
            // A `**`-tailed variable is by construction the final template
            // segment (`rest` is empty), so it greedily consumes the remainder.
            SegmentCount::AtLeast(min) => {
                if input.len() < min {
                    return false;
                }
                input.len()
            }
        };
        if input.len() < take {
            return false;
        }
        let (head, tail) = input.split_at(take);
        // Validate the captured slice against the variable's sub-pattern and
        // build the decoded value (segments joined by `/`).
        if !sub_pattern_matches(&var.sub, head) {
            return false;
        }
        let value = head.iter().map(|s| decode(s)).collect::<Vec<_>>().join("/");
        captures.push((var.field_path.clone(), value));
        self.match_segments(rest, tail, captures)
    }
}

/// Check a captured segment slice against a variable's sub-pattern segments.
/// `**` in the sub-pattern matches the remaining captured segments.
fn sub_pattern_matches(sub: &[Segment], input: &[&str]) -> bool {
    match sub.split_first() {
        None => input.is_empty(),
        Some((first, rest)) => match first {
            Segment::Literal(lit) => match input.split_first() {
                Some((head, tail)) if decode(head) == *lit => sub_pattern_matches(rest, tail),
                _ => false,
            },
            Segment::Single => match input.split_first() {
                Some((_, tail)) => sub_pattern_matches(rest, tail),
                None => false,
            },
            Segment::CatchAll => rest.is_empty(),
            Segment::Variable(_) => false, // variables never nest
        },
    }
}

/// Minimal percent-decoding for a single path segment. Leaves invalid escapes
/// untouched (the matcher only needs `%XX` style decoding for values).
fn decode(segment: &str) -> String {
    if !segment.contains('%') {
        return segment.to_owned();
    }
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| segment.to_owned())
}

/// Recursive-descent parser for the path-template grammar.
struct Parser<'a> {
    raw: &'a str,
    /// The remaining unparsed tail.
    rest: &'a str,
    vars: Vec<VarSpec>,
}

impl<'a> Parser<'a> {
    fn new(raw: &'a str) -> Self {
        Parser {
            raw,
            rest: raw,
            vars: Vec::new(),
        }
    }

    fn err(&self, reason: impl Into<String>) -> TemplateError {
        TemplateError {
            template: self.raw.to_owned(),
            reason: reason.into(),
        }
    }

    fn parse(mut self) -> Result<PathTemplate, TemplateError> {
        if !self.rest.starts_with('/') {
            return Err(self.err("must start with '/'"));
        }
        // Split off a trailing verb (`:verb`) before segment parsing. The verb
        // is the text after the final ':' that is not inside a `{}` block.
        let verb = self.split_verb()?;

        self.rest = &self.rest[1..]; // consume leading '/'
        let segments = self.parse_segments(false)?;
        if !self.rest.is_empty() {
            return Err(self.err(format!("unexpected trailing input {:?}", self.rest)));
        }
        if segments.is_empty() {
            return Err(self.err("empty path"));
        }
        Ok(PathTemplate {
            raw: self.raw.to_owned(),
            segments,
            vars: self.vars,
            verb,
        })
    }

    /// Detect and strip a trailing `:verb`. Returns the verb (without the
    /// colon) if present. A colon inside `{}` is not a verb separator.
    fn split_verb(&mut self) -> Result<Option<String>, TemplateError> {
        let mut depth = 0usize;
        let mut verb_colon = None;
        for (i, c) in self.rest.char_indices() {
            match c {
                '{' => depth += 1,
                '}' => depth = depth.saturating_sub(1),
                ':' if depth == 0 => verb_colon = Some(i),
                _ => {}
            }
        }
        if let Some(idx) = verb_colon {
            let verb = &self.rest[idx + 1..];
            if verb.is_empty() {
                return Err(self.err("empty verb after ':'"));
            }
            if verb.contains('/') {
                return Err(self.err("verb may not contain '/'"));
            }
            self.rest = &self.rest[..idx];
            Ok(Some(verb.to_owned()))
        } else {
            Ok(None)
        }
    }

    /// Parse `Segment { "/" Segment }`. `in_var` is true when parsing a
    /// variable's sub-pattern (where nested variables are forbidden). Both the
    /// top level and a sub-pattern continue on `/`; a sub-pattern additionally
    /// stops at `}`/`=`, the top level stops at end-of-input.
    fn parse_segments(&mut self, in_var: bool) -> Result<Vec<Segment>, TemplateError> {
        let mut segments = Vec::new();
        loop {
            let seg = self.parse_segment(in_var)?;
            let is_catch_all = matches!(seg, Segment::CatchAll)
                || matches!(&seg, Segment::Variable(i)
                    if matches!(self.vars[*i].count, SegmentCount::AtLeast(_)));
            segments.push(seg);

            if self.rest.starts_with('/') {
                if is_catch_all {
                    return Err(self.err("'**' must be the final segment"));
                }
                self.rest = &self.rest[1..]; // consume '/' and parse another
                continue;
            }
            break;
        }
        Ok(segments)
    }

    fn parse_segment(&mut self, in_var: bool) -> Result<Segment, TemplateError> {
        if self.rest.starts_with("**") {
            self.rest = &self.rest[2..];
            Ok(Segment::CatchAll)
        } else if self.rest.starts_with('*') {
            self.rest = &self.rest[1..];
            Ok(Segment::Single)
        } else if self.rest.starts_with('{') {
            if in_var {
                return Err(self.err("nested variable '{' inside a variable"));
            }
            self.parse_variable()
        } else {
            self.parse_literal()
        }
    }

    fn parse_literal(&mut self) -> Result<Segment, TemplateError> {
        // A literal runs until the next '/', '{', '}', '=' or end.
        let end = self
            .rest
            .find(['/', '{', '}', '='])
            .unwrap_or(self.rest.len());
        let lit = &self.rest[..end];
        if lit.is_empty() {
            return Err(self.err("empty path segment"));
        }
        if lit.contains('*') {
            return Err(self.err(format!("invalid literal segment {lit:?}")));
        }
        self.rest = &self.rest[end..];
        Ok(Segment::Literal(lit.to_owned()))
    }

    fn parse_variable(&mut self) -> Result<Segment, TemplateError> {
        self.rest = &self.rest[1..]; // consume '{'
        let field_path = self.parse_field_path()?;

        let sub = if self.rest.starts_with('=') {
            self.rest = &self.rest[1..]; // consume '='
            self.parse_segments(true)?
        } else {
            // Bare `{name}` == `{name=*}`.
            vec![Segment::Single]
        };

        if !self.rest.starts_with('}') {
            return Err(self.err("unterminated variable (expected '}')"));
        }
        self.rest = &self.rest[1..]; // consume '}'

        // A `**` may only be the final segment of the sub-pattern.
        if let Some(pos) = sub.iter().position(|s| matches!(s, Segment::CatchAll)) {
            if pos != sub.len() - 1 {
                return Err(self.err("'**' must be the final segment of a variable"));
            }
        }
        let count = segment_count(&sub);
        let idx = self.vars.len();
        self.vars.push(VarSpec {
            field_path,
            sub,
            count,
        });
        Ok(Segment::Variable(idx))
    }

    fn parse_field_path(&mut self) -> Result<Vec<String>, TemplateError> {
        let end = self
            .rest
            .find(['=', '}'])
            .ok_or_else(|| self.err("unterminated variable field path"))?;
        let raw = &self.rest[..end];
        self.rest = &self.rest[end..];
        if raw.is_empty() {
            return Err(self.err("empty variable field path"));
        }
        let parts: Vec<String> = raw.split('.').map(|s| s.to_owned()).collect();
        if parts.iter().any(|p| p.is_empty()) {
            return Err(self.err(format!("invalid field path {raw:?}")));
        }
        Ok(parts)
    }
}

/// Count the request segments a sub-pattern consumes.
fn segment_count(sub: &[Segment]) -> SegmentCount {
    let mut fixed = 0usize;
    let mut unbounded = false;
    for s in sub {
        match s {
            Segment::CatchAll => unbounded = true,
            _ => fixed += 1,
        }
    }
    if unbounded {
        SegmentCount::AtLeast(fixed)
    } else {
        SegmentCount::Exact(fixed)
    }
}

/// Split a request path into segments plus an optional trailing verb. The verb
/// is the suffix after the final ':' in the last segment.
///
/// Returns `(segments, verb)`. The leading '/' is required and stripped.
pub fn split_request_path(path: &str) -> (Vec<&str>, Option<&str>) {
    let path = path.strip_prefix('/').unwrap_or(path);
    let mut segments: Vec<&str> = path.split('/').collect();
    let mut verb = None;
    if let Some(last) = segments.last_mut() {
        if let Some(idx) = last.rfind(':') {
            verb = Some(&last[idx + 1..]);
            *last = &last[..idx];
        }
    }
    (segments, verb)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(t: &str) -> PathTemplate {
        PathTemplate::parse(t).unwrap_or_else(|e| panic!("parse {t:?}: {e}"))
    }

    fn caps(t: &str, path: &str) -> Option<Vec<(Vec<String>, String)>> {
        let tmpl = parse(t);
        let (segs, verb) = split_request_path(path);
        tmpl.matches(&segs, verb).map(|c| c.vars)
    }

    #[test]
    fn parses_literal_only() {
        let t = parse("/v1/greeter/hello");
        assert_eq!(
            t.segments,
            vec![
                Segment::Literal("v1".into()),
                Segment::Literal("greeter".into()),
                Segment::Literal("hello".into()),
            ]
        );
        assert!(t.vars.is_empty());
        assert!(t.verb.is_none());
    }

    #[test]
    fn parses_single_capture() {
        let t = parse("/v1/greeter/{name}");
        assert_eq!(t.vars.len(), 1);
        assert_eq!(t.vars[0].field_path, vec!["name".to_string()]);
        assert_eq!(t.vars[0].count, SegmentCount::Exact(1));
        assert_eq!(t.vars[0].sub, vec![Segment::Single]);
    }

    #[test]
    fn parses_field_path_capture() {
        let t = parse("/v1/{user.profile.id}");
        assert_eq!(
            t.vars[0].field_path,
            vec!["user".to_string(), "profile".into(), "id".into()]
        );
    }

    #[test]
    fn parses_multi_segment_subpattern() {
        let t = parse("/v1/{name=shelves/*/books/*}");
        assert_eq!(t.vars[0].count, SegmentCount::Exact(4));
    }

    #[test]
    fn parses_catch_all_variable() {
        let t = parse("/v1/{path=**}");
        assert_eq!(t.vars[0].count, SegmentCount::AtLeast(0));
    }

    #[test]
    fn parses_verb() {
        let t = parse("/v1/messages/{id}:undelete");
        assert_eq!(t.verb.as_deref(), Some("undelete"));
        assert_eq!(t.vars[0].field_path, vec!["id".to_string()]);
    }

    #[test]
    fn rejects_catch_all_not_final() {
        let e = PathTemplate::parse("/v1/**/x").unwrap_err();
        assert!(e.reason.contains("final segment"), "{}", e.reason);
    }

    #[test]
    fn rejects_nested_variable() {
        let e = PathTemplate::parse("/v1/{a={b}}").unwrap_err();
        assert!(e.reason.contains("nested"), "{}", e.reason);
    }

    #[test]
    fn rejects_empty_field_path() {
        let e = PathTemplate::parse("/v1/{}").unwrap_err();
        assert!(e.reason.contains("field path"), "{}", e.reason);
    }

    #[test]
    fn rejects_missing_leading_slash() {
        let e = PathTemplate::parse("v1/x").unwrap_err();
        assert!(e.reason.contains("start with"), "{}", e.reason);
    }

    #[test]
    fn rejects_empty_verb() {
        let e = PathTemplate::parse("/v1/x:").unwrap_err();
        assert!(e.reason.contains("verb"), "{}", e.reason);
    }

    #[test]
    fn matches_literal() {
        assert_eq!(caps("/v1/greeter/hello", "/v1/greeter/hello"), Some(vec![]));
        assert_eq!(caps("/v1/greeter/hello", "/v1/greeter/world"), None);
    }

    #[test]
    fn matches_single_capture() {
        assert_eq!(
            caps("/v1/greeter/{name}", "/v1/greeter/alice"),
            Some(vec![(vec!["name".into()], "alice".into())])
        );
        // single capture does not span multiple segments
        assert_eq!(caps("/v1/greeter/{name}", "/v1/greeter/a/b"), None);
    }

    #[test]
    fn matches_field_path_capture() {
        assert_eq!(
            caps("/v1/{user.id}", "/v1/42"),
            Some(vec![(vec!["user".into(), "id".into()], "42".into())])
        );
    }

    #[test]
    fn matches_multi_segment_subpattern() {
        assert_eq!(
            caps("/v1/{name=shelves/*/books/*}", "/v1/shelves/7/books/9"),
            Some(vec![(vec!["name".into()], "shelves/7/books/9".into())])
        );
        assert_eq!(
            caps("/v1/{name=shelves/*/books/*}", "/v1/shelves/7/books"),
            None
        );
    }

    #[test]
    fn matches_catch_all_variable() {
        assert_eq!(
            caps("/v1/{path=**}", "/v1/a/b/c"),
            Some(vec![(vec!["path".into()], "a/b/c".into())])
        );
        // `**` matches zero segments too
        assert_eq!(
            caps("/v1/{path=**}", "/v1"),
            Some(vec![(vec!["path".into()], "".into())])
        );
    }

    #[test]
    fn matches_verb() {
        assert_eq!(
            caps("/v1/messages/{id}:undelete", "/v1/messages/7:undelete"),
            Some(vec![(vec!["id".into()], "7".into())])
        );
        // wrong verb does not match
        assert_eq!(caps("/v1/messages/{id}:undelete", "/v1/messages/7"), None);
        // verb required: template without verb must not match a verb request
        assert_eq!(caps("/v1/messages/{id}", "/v1/messages/7:undelete"), None);
    }

    #[test]
    fn matches_star_segment() {
        assert_eq!(caps("/v1/*/info", "/v1/anything/info"), Some(vec![]));
        assert_eq!(caps("/v1/*/info", "/v1/a/b/info"), None);
    }

    #[test]
    fn decodes_captured_value() {
        assert_eq!(
            caps("/v1/greeter/{name}", "/v1/greeter/a%20b"),
            Some(vec![(vec!["name".into()], "a b".into())])
        );
    }
}
