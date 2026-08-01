//! Structured-filter evaluation for gateway records.
//!
//! Two pure helpers consuming the [`crate::query::GatewayQuery`] struct:
//!
//! - [`record_matches`] — does a single record's `fields` map satisfy the
//!   query's `filters` + `ranges` + typed `negations`?
//! - [`query_accepts_plugin`] — could a plugin advertising the given
//!   `served_file_types` / `served_content_kinds` possibly produce a record
//!   that satisfies the query? Used by plugins as a cheap early-return.
//!
//! The dispatcher applies `record_matches` to every plugin's return value, and
//! meta-share applies the same algorithm consumer-side to defend against stale
//! gateway peers — both tiers must agree on outcomes for any (record, query)
//! pair. See the original gateway crate docs for the full semantics.

use std::collections::BTreeMap;

use crate::query::GatewayQuery;

/// True iff a record's `fields` satisfies every filter, range, and typed
/// negation in `query`.
pub fn record_matches(fields: &BTreeMap<String, String>, query: &GatewayQuery) -> bool {
    for (key, allowed) in &query.filters {
        if key == "languages" {
            if !languages_filter_matches(fields, allowed) {
                return false;
            }
            continue;
        }
        // `genres` is a key-set too (`genres/<Name> = "true"`, METADATA_KEYS
        // §`genres/{name}`) on everything the card tier writes. Without this arm
        // a `genres:` filter looks up a flat `genres` field, finds nothing on a
        // card, and drops it — so a genre-scoped row would return empty.
        if key == "genres" {
            if !genres_filter_matches(fields, allowed) {
                return false;
            }
            continue;
        }
        let Some(field_value) = fields.get(key) else {
            return false;
        };
        if !any_csv_value_matches(field_value, allowed) {
            return false;
        }
    }

    for range in &query.ranges {
        let Some(field_value) = fields.get(&range.field) else {
            return false;
        };
        let Ok(n) = field_value.parse::<i64>() else {
            return false;
        };
        if let Some(lo) = range.lo {
            if n < lo {
                return false;
            }
        }
        if let Some(hi) = range.hi {
            if n > hi {
                return false;
            }
        }
    }

    for negation in &query.negations {
        let Some(name) = negation.field.as_deref() else {
            // Bare-word negation — not enforced here.
            continue;
        };
        let Some(field_value) = fields.get(name) else {
            // Missing field ⇒ negation passes (nothing to exclude).
            continue;
        };
        if any_csv_value_matches(field_value, std::slice::from_ref(&negation.value)) {
            return false;
        }
    }

    true
}

/// True iff a plugin advertising the given `served_file_types` /
/// `served_content_kinds` could possibly produce a record satisfying
/// `query.filters["fileType" | "contentKind"]`.
pub fn query_accepts_plugin(
    query: &GatewayQuery,
    served_file_types: &[&str],
    served_content_kinds: &[&str],
) -> bool {
    accepts_axis(query.filters.get("fileType"), served_file_types)
        && accepts_axis(query.filters.get("contentKind"), served_content_kinds)
}

fn accepts_axis(requested: Option<&Vec<String>>, served: &[&str]) -> bool {
    let Some(requested) = requested else {
        return true;
    };
    if requested.is_empty() {
        return true;
    }
    // Wildcard: a plugin that advertises `"*"` on an axis serves every value on
    // that axis (e.g. tribler is a free-text overlay search with no inherent
    // type limitation). It accepts any requested fileType/contentKind.
    if served.contains(&"*") {
        return true;
    }
    requested
        .iter()
        .any(|v| served.iter().any(|s| s.eq_ignore_ascii_case(v)))
}

/// Resolve a `languages` filter against the key-set storage form
/// (`languages/<lang3> = "true"`). **Fails open** on anything we can't
/// confidently exclude. A record matches when:
/// 1. it carries a requested code as a truthy key-set member (OR across codes);
/// 2. a legacy flat `languages` csv field contains a requested code;
/// 3. it carries `mul` (multiple — may contain a requested one) or `und`
///    (undetermined); or
/// 4. it carries no language information at all.
///
/// Only a record with a *concrete*, non-matching language (e.g. `jpn`-only
/// under an `eng` filter) is dropped. Kept byte-identical in logic with
/// meta-search's copy (and the client's `passesLangFilter`) — language narrows
/// the result set without ever hiding unknown/undetermined/multi releases.
fn languages_filter_matches(fields: &BTreeMap<String, String>, allowed: &[String]) -> bool {
    for code in allowed {
        let key = format!("languages/{}", code.trim().to_ascii_lowercase());
        if fields.get(&key).is_some_and(|v| is_truthy_member(v)) {
            return true;
        }
    }
    if let Some(flat) = fields.get("languages") {
        if any_csv_value_matches(flat, allowed) {
            return true;
        }
    }
    // Fail open: `mul`/`und` are explicit "could be anything / unknown" markers,
    // and a record with no concrete language at all is equally unknown. Keep all
    // three; only a specific, non-matching language reaches the `false` below.
    if fields.get("languages/mul").is_some_and(|v| is_truthy_member(v))
        || fields.get("languages/und").is_some_and(|v| is_truthy_member(v))
    {
        return true;
    }
    !record_has_concrete_language(fields)
}

/// True iff the record states a *concrete* language: a truthy `languages/<code>`
/// key-set member other than the `und`/`mul` non-codes, or a non-empty legacy
/// flat `languages` field. Distinguishes "a specific language that didn't match
/// ⇒ drop" from "no language info ⇒ keep" for [`languages_filter_matches`].
fn record_has_concrete_language(fields: &BTreeMap<String, String>) -> bool {
    if fields.get("languages").is_some_and(|v| !v.trim().is_empty()) {
        return true;
    }
    fields.iter().any(|(k, v)| {
        is_truthy_member(v)
            && k.starts_with("languages/")
            && k != "languages/und"
            && k != "languages/mul"
    })
}

/// Key-set members are written as the literal `"true"`; tolerate a couple of
/// obvious truthy spellings and treat anything else as absent.
/// Fold a genre label to its comparison form: lowercase, alphanumerics only.
///
/// Genre display names carry spaces and ampersands (`"Action & Adventure"`,
/// `"Sci-Fi & Fantasy"`, `"Science Fiction"`), and **space is the query DSL's
/// token separator** — so a filter can never spell a genre the way the record
/// stores it. Callers pass a slug (`genres:action-adventure`); folding both
/// sides makes them one value without forcing the stored name to be
/// machine-shaped.
fn genre_fold(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
}

/// True iff the record carries one of the requested genres, in **either**
/// encoding: a truthy `genres/<Name>` key-set member (the card tier's shape) or
/// the legacy flat comma-joined `genres` field. OR across the requested genres,
/// both sides folded — see [`genre_fold`].
///
/// **Fails closed, unlike [`languages_filter_matches`].** A language filter is a
/// viewer *preference* narrowing a set, so an unknown-language release is kept.
/// A genre filter *defines what a row is*: a record with no genre information is
/// not known to be Action, so putting it in an Action row states something
/// untrue. Upstream has already filtered by genre, so this is a re-validation.
fn genres_filter_matches(fields: &BTreeMap<String, String>, allowed: &[String]) -> bool {
    let wanted: Vec<String> = allowed.iter().map(|g| genre_fold(g)).collect();
    if wanted.iter().all(|g| g.is_empty()) {
        return true; // a filter with no usable value constrains nothing
    }
    for (key, value) in fields {
        let Some(name) = key.strip_prefix("genres/") else {
            continue;
        };
        if is_truthy_member(value) && wanted.contains(&genre_fold(name)) {
            return true;
        }
    }
    if let Some(flat) = fields.get("genres") {
        for member in flat.split(|c| c == ',' || c == '|') {
            if wanted.contains(&genre_fold(member)) {
                return true;
            }
        }
    }
    false
}

fn is_truthy_member(v: &str) -> bool {
    matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes")
}

/// True iff `field_value` (treated as a csv set when it contains commas)
/// contains at least one case-insensitive match for any item in `allowed`.
fn any_csv_value_matches(field_value: &str, allowed: &[String]) -> bool {
    for piece in field_value.split(',') {
        let piece = piece.trim();
        if piece.is_empty() {
            continue;
        }
        if allowed.iter().any(|v| v.eq_ignore_ascii_case(piece)) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{Negation, RangeFilter};

    fn empty_query() -> GatewayQuery {
        GatewayQuery {
            raw_text: String::new(),
            free_text: String::new(),
            filters: BTreeMap::new(),
            ranges: Vec::new(),
            negations: Vec::new(),
        }
    }

    fn with_filter(field: &str, values: &[&str]) -> GatewayQuery {
        let mut q = empty_query();
        q.filters.insert(
            field.to_string(),
            values.iter().map(|s| s.to_string()).collect(),
        );
        q
    }

    fn fields(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn filter_rejects_mismatched_filetype() {
        let q = with_filter("fileType", &["video"]);
        assert!(!record_matches(&fields(&[("fileType", "image")]), &q));
    }

    #[test]
    fn filter_accepts_matching_filetype() {
        let q = with_filter("fileType", &["video"]);
        assert!(record_matches(&fields(&[("fileType", "video")]), &q));
    }

    #[test]
    fn filter_rejects_record_missing_field() {
        let q = with_filter("fileType", &["video"]);
        assert!(!record_matches(&fields(&[]), &q));
    }

    #[test]
    fn filter_is_case_insensitive() {
        let q = with_filter("fileType", &["VIDEO"]);
        assert!(record_matches(&fields(&[("fileType", "video")]), &q));
        let q = with_filter("fileType", &["video"]);
        assert!(record_matches(&fields(&[("fileType", "VIDEO")]), &q));
    }

    #[test]
    fn filter_ors_within_key() {
        let q = with_filter("fileType", &["video", "audio"]);
        assert!(record_matches(&fields(&[("fileType", "audio")]), &q));
        assert!(record_matches(&fields(&[("fileType", "video")]), &q));
        assert!(!record_matches(&fields(&[("fileType", "image")]), &q));
    }

    #[test]
    fn filter_ands_across_keys() {
        let mut q = with_filter("fileType", &["video"]);
        q.filters.insert("genres".into(), vec!["Action".into()]);
        assert!(!record_matches(
            &fields(&[("fileType", "video"), ("genres", "Drama")]),
            &q
        ));
        assert!(record_matches(
            &fields(&[("fileType", "video"), ("genres", "Action")]),
            &q
        ));
    }

    #[test]
    fn csv_field_matches_one_of_its_values() {
        let q = with_filter("genres", &["Anime"]);
        assert!(record_matches(
            &fields(&[("genres", "Animation,Anime,Drama")]),
            &q
        ));
    }

    #[test]
    fn csv_field_with_whitespace_is_trimmed() {
        let q = with_filter("genres", &["Anime"]);
        assert!(record_matches(
            &fields(&[("genres", "Animation, Anime , Drama")]),
            &q
        ));
    }

    #[test]
    fn languages_keyset_member_matches() {
        let q = with_filter("languages", &["eng"]);
        assert!(record_matches(&fields(&[("languages/eng", "true")]), &q));
    }

    #[test]
    fn languages_filter_is_or_across_codes() {
        let q = with_filter("languages", &["eng", "fre"]);
        assert!(record_matches(&fields(&[("languages/eng", "true")]), &q));
        assert!(record_matches(&fields(&[("languages/fre", "true")]), &q));
        assert!(!record_matches(&fields(&[("languages/jpn", "true")]), &q));
    }

    #[test]
    fn languages_filter_fails_open_on_unknown() {
        let q = with_filter("languages", &["eng"]);
        // No language info at all ⇒ kept (unknown, not excluded).
        assert!(record_matches(&fields(&[("fileType", "video")]), &q));
        // `mul` (multi) may contain the requested language ⇒ kept.
        assert!(record_matches(&fields(&[("languages/mul", "true")]), &q));
        // `und` (undetermined) ⇒ kept.
        assert!(record_matches(&fields(&[("languages/und", "true")]), &q));
        // A specific language alongside `mul` is still kept (multi).
        assert!(record_matches(
            &fields(&[("languages/jpn", "true"), ("languages/mul", "true")]),
            &q
        ));
    }

    #[test]
    fn languages_concrete_mismatch_still_drops() {
        let q = with_filter("languages", &["eng"]);
        // A concrete, non-matching language ⇒ dropped (language narrows).
        assert!(!record_matches(&fields(&[("languages/jpn", "true")]), &q));
        // A tombstoned/false member is treated as absent ⇒ unknown ⇒ kept.
        assert!(record_matches(&fields(&[("languages/eng", "false")]), &q));
    }

    #[test]
    fn languages_legacy_flat_csv_fallback() {
        let q = with_filter("languages", &["eng"]);
        assert!(record_matches(&fields(&[("languages", "jpn,eng")]), &q));
        assert!(!record_matches(&fields(&[("languages", "jpn,ger")]), &q));
    }

    #[test]
    fn range_in_bounds() {
        let mut q = empty_query();
        q.ranges.push(RangeFilter {
            field: "movieYear".into(),
            lo: Some(2010),
            hi: Some(2020),
        });
        assert!(record_matches(&fields(&[("movieYear", "2015")]), &q));
    }

    #[test]
    fn range_out_of_bounds_rejected() {
        let mut q = empty_query();
        q.ranges.push(RangeFilter {
            field: "movieYear".into(),
            lo: Some(2010),
            hi: Some(2020),
        });
        assert!(!record_matches(&fields(&[("movieYear", "2025")]), &q));
        assert!(!record_matches(&fields(&[("movieYear", "2005")]), &q));
    }

    #[test]
    fn range_open_upper_bound() {
        let mut q = empty_query();
        q.ranges.push(RangeFilter {
            field: "movieYear".into(),
            lo: Some(2010),
            hi: None,
        });
        assert!(record_matches(&fields(&[("movieYear", "2025")]), &q));
    }

    #[test]
    fn range_unparseable_value_rejected() {
        let mut q = empty_query();
        q.ranges.push(RangeFilter {
            field: "movieYear".into(),
            lo: Some(2010),
            hi: None,
        });
        assert!(!record_matches(&fields(&[("movieYear", "circa 2015")]), &q));
    }

    #[test]
    fn typed_negation_excludes_match() {
        let mut q = empty_query();
        q.negations.push(Negation {
            field: Some("fileType".into()),
            value: "image".into(),
        });
        assert!(!record_matches(&fields(&[("fileType", "image")]), &q));
        assert!(record_matches(&fields(&[("fileType", "video")]), &q));
    }

    #[test]
    fn typed_negation_passes_when_field_missing() {
        let mut q = empty_query();
        q.negations.push(Negation {
            field: Some("fileType".into()),
            value: "image".into(),
        });
        assert!(record_matches(&fields(&[]), &q));
    }

    #[test]
    fn bare_word_negation_is_not_enforced() {
        let mut q = empty_query();
        q.negations.push(Negation {
            field: None,
            value: "alice".into(),
        });
        assert!(record_matches(
            &fields(&[("title", "alice in wonderland")]),
            &q
        ));
    }

    #[test]
    fn empty_query_matches_anything() {
        assert!(record_matches(&fields(&[]), &empty_query()));
        assert!(record_matches(
            &fields(&[("fileType", "video"), ("title", "x")]),
            &empty_query()
        ));
    }

    #[test]
    fn accepts_returns_true_when_no_type_filter() {
        let q = empty_query();
        assert!(query_accepts_plugin(&q, &["video"], &["movie"]));
        assert!(query_accepts_plugin(&q, &[], &[]));
    }

    #[test]
    fn accepts_rejects_disjoint_filetype() {
        let q = with_filter("fileType", &["video"]);
        assert!(!query_accepts_plugin(&q, &["image"], &["gif"]));
        assert!(query_accepts_plugin(&q, &["video"], &[]));
    }

    #[test]
    fn accepts_rejects_disjoint_contentkind() {
        let q = with_filter("contentKind", &["movie"]);
        assert!(!query_accepts_plugin(
            &q,
            &["video", "image"],
            &["paper", "book"]
        ));
        assert!(query_accepts_plugin(&q, &["video"], &["movie", "episode"]));
    }

    #[test]
    fn accepts_requires_both_axes_when_both_filtered() {
        let mut q = with_filter("fileType", &["video"]);
        q.filters.insert("contentKind".into(), vec!["movie".into()]);
        assert!(query_accepts_plugin(&q, &["video"], &["movie"]));
        assert!(!query_accepts_plugin(&q, &["video"], &["paper"]));
        assert!(!query_accepts_plugin(&q, &["image"], &["movie"]));
    }

    #[test]
    fn accepts_wildcard_matches_any_requested_type() {
        // A plugin advertising "*" (e.g. tribler) accepts any fileType /
        // contentKind filter on the wildcarded axis.
        let q = with_filter("fileType", &["archive"]);
        assert!(query_accepts_plugin(&q, &["*"], &[]));
        let q = with_filter("contentKind", &["audiobook"]);
        assert!(query_accepts_plugin(&q, &["*"], &["*"]));
        // Wildcard on one axis only: the non-wildcard axis still gates.
        let mut q = with_filter("fileType", &["audio"]);
        q.filters.insert("contentKind".into(), vec!["podcast".into()]);
        assert!(query_accepts_plugin(&q, &["*"], &["*"]));
        assert!(!query_accepts_plugin(&q, &["*"], &["movie"]));
    }
}
