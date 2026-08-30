//! `contentKind` → `domain` — the routing table from METADATA_KEYS.md §1.
//!
//! `domain` answers **"which application is this record destined for?"**. It
//! is the coarsest facet in the registry and the key every client filters its
//! catalogue on:
//!
//! ```text
//! meta-watch   →  domain:film|tv
//! meta-listen  →  domain:music
//! meta-read    →  domain:literature
//! ```
//!
//! # Why it is stored and not derived on read
//!
//! `contentKind`'s vocabulary is deliberately open — a new feeder may mint a
//! kind and ship without a fleet-wide update. That only stays safe because the
//! domain travels *with* the record: a peer that has never heard of
//! `podcastEpisode` still routes it correctly, because it never has to
//! interpret the token. Deriving on read would silently drop every record
//! whose kind the reader predates.
//!
//! # ⚠ The binding is permanent and one-way
//!
//! Add kinds freely; **never re-map an existing kind to a different domain**.
//! Because the value is stored, two peers on different versions that disagree
//! about where `audiobook` belongs produce a split-brain that redeploying
//! cannot heal — the wrong domain is already written into records across the
//! mesh. If a kind genuinely has to move, mint a new kind.
//!
//! # Why `domain` and not one kind-key per medium
//!
//! `record_matches` is AND-across-keys / OR-within-key, and a non-matching key
//! fails the record outright. A per-medium split (`videoKind` / `audioKind` /
//! `documentKind`) would force meta-listen to express "music video **or**
//! audio track" as an OR across two *different* keys — which the query
//! language cannot represent at all. One domain key, OR-ed within itself,
//! always can.

/// The `contentKind` → `domain` table. A kind absent from this list has no
/// domain and is **not independently routable** — correct for sidecars
/// (subtitles, artwork) and for un-anchored hits, a producer bug for anything
/// else.
///
/// ⚠ `pack` is deliberately absent. A season pack and an album release are the
/// same structural thing, so the kind alone cannot tell `tv` from `music`; the
/// writer stamps `pack`'s domain from its own context (the same reason
/// `derive_file_type` cannot have a second `pack` arm).
const DOMAIN_BY_CONTENT_KIND: &[(&str, &str)] = &[
    // film / tv
    ("movie", "film"),
    ("series", "tv"),
    ("episode", "tv"),
    // music
    ("track", "music"),
    ("album", "music"),
    ("artist", "music"),
    ("musicVideo", "music"),
    ("djMix", "music"),
    ("liveSet", "music"),
    // spoken
    ("podcast", "spoken"),
    ("podcastEpisode", "spoken"),
    // literature — an audiobook's identity is the book (same author, same ISBN
    // family, same series as the ebook), so meta-read groups the two editions
    // on one page. The player does not decide the domain.
    ("book", "literature"),
    ("audiobook", "literature"),
    ("comic", "literature"),
    ("manga", "literature"),
    ("magazine", "literature"),
    // science
    ("paper", "science"),
];

/// Resolve the domain a `contentKind` belongs to.
///
/// Returns `None` for `pack` (not derivable from the kind — the writer supplies
/// it), for format/sidecar kinds that route nowhere, and for any kind this
/// build has never heard of. A caller that already knows the domain from its
/// own context must prefer that over this table.
pub fn domain_for_content_kind(content_kind: &str) -> Option<&'static str> {
    let k = content_kind.trim();
    DOMAIN_BY_CONTENT_KIND
        .iter()
        .find(|(kind, _)| kind.eq_ignore_ascii_case(k))
        .map(|(_, domain)| *domain)
}

/// The inverse: every `contentKind` that belongs to `domain`.
///
/// Used at the gateway-routing boundary. A gateway declares the *content kinds*
/// it serves (`served_content_kinds`), not domains, so a `domain:` filter is
/// matched by expanding it here rather than by adding a `served_domains` field
/// to the heartbeat — which would be a wire-protocol change requiring every
/// feeder manifest and every peer to move at once.
pub fn content_kinds_for_domain(domain: &str) -> Vec<&'static str> {
    let d = domain.trim();
    DOMAIN_BY_CONTENT_KIND
        .iter()
        .filter(|(_, dom)| dom.eq_ignore_ascii_case(d))
        .map(|(kind, _)| *kind)
        .collect()
}

/// Rewrite a query so a `domain:` filter can reach a plugin that has never
/// heard of the key.
///
/// Returns `None` when the query carries no `domain:` filter — the common
/// path, and cheaper than cloning.
///
/// # Why translate instead of teaching every plugin
///
/// `record_matches` fails a record outright when a filter key is missing from
/// its fields, and `domain` is missing from every record a not-yet-updated
/// feeder emits. Nine of the fifteen feeders pin the SDK by git tag, so they
/// cannot pick up a new key on our schedule. Translating `domain:literature`
/// into the `contentKind:` alternation it covers means the wire query a plugin
/// sees never mentions `domain` at all, and old plugins keep working unchanged
/// — while the dispatcher still enforces the real `domain:` filter afterwards,
/// against records it has just stamped.
///
/// An explicit `contentKind:` filter already in the query is left alone: it is
/// the narrower of the two, and the `domain` term is still enforced
/// post-stamp, so the AND semantics hold either way.
pub fn expand_domain_filter(query: &crate::query::GatewayQuery) -> Option<crate::query::GatewayQuery> {
    let domains = query.filters.get("domain")?;
    let mut out = query.clone();
    out.filters.remove("domain");
    if !out.filters.contains_key("contentKind") {
        let kinds: Vec<String> = domains
            .iter()
            .flat_map(|d| content_kinds_for_domain(d))
            .map(|k| k.to_string())
            .collect();
        // An unknown domain expands to nothing. Inserting an empty
        // alternation would drop every record at the plugin; leaving the
        // filter out lets the plugin answer and the post-stamp `domain:`
        // check do the (correct, empty) filtering.
        if !kinds.is_empty() {
            out.filters.insert("contentKind".to_string(), kinds);
        }
    }
    Some(out)
}

/// Stamp `domain` on a field map that already carries a `contentKind`.
///
/// The convention from METADATA_KEYS.md §1: **whoever writes `contentKind`
/// writes `domain` in the same breath**. An existing `domain` is never
/// overwritten — a writer that knows better than the table (a `pack`, say) has
/// already set it.
pub fn stamp_domain(fields: &mut std::collections::BTreeMap<String, String>) {
    if fields.contains_key("domain") {
        return;
    }
    let Some(kind) = fields.get("contentKind").cloned() else {
        return;
    };
    if let Some(domain) = domain_for_content_kind(&kind) {
        fields.insert("domain".to_string(), domain.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn kinds_map_to_their_domain() {
        assert_eq!(domain_for_content_kind("movie"), Some("film"));
        assert_eq!(domain_for_content_kind("episode"), Some("tv"));
        assert_eq!(domain_for_content_kind("track"), Some("music"));
        // The case that proves medium and domain are different axes.
        assert_eq!(domain_for_content_kind("musicVideo"), Some("music"));
        // The case that proves the player does not decide the domain.
        assert_eq!(domain_for_content_kind("audiobook"), Some("literature"));
    }

    #[test]
    fn pack_has_no_derivable_domain() {
        assert_eq!(domain_for_content_kind("pack"), None);
    }

    #[test]
    fn unknown_kind_is_none_not_a_guess() {
        assert_eq!(domain_for_content_kind("hologram"), None);
    }

    #[test]
    fn expansion_is_the_inverse() {
        let music = content_kinds_for_domain("music");
        assert!(music.contains(&"track"));
        assert!(music.contains(&"musicVideo"));
        assert!(!music.contains(&"audiobook"));
        for kind in content_kinds_for_domain("literature") {
            assert_eq!(domain_for_content_kind(kind), Some("literature"));
        }
    }

    fn q(filters: &[(&str, &[&str])]) -> crate::query::GatewayQuery {
        crate::query::GatewayQuery {
            raw_text: String::new(),
            free_text: String::new(),
            filters: filters
                .iter()
                .map(|(k, vs)| {
                    (
                        k.to_string(),
                        vs.iter().map(|v| v.to_string()).collect::<Vec<_>>(),
                    )
                })
                .collect(),
            ranges: Vec::new(),
            negations: Vec::new(),
        }
    }

    #[test]
    fn no_domain_filter_is_left_untouched() {
        assert!(expand_domain_filter(&q(&[("fileType", &["video"])])).is_none());
    }

    #[test]
    fn domain_becomes_the_content_kinds_it_covers() {
        let out = expand_domain_filter(&q(&[("domain", &["literature"])])).unwrap();
        // The plugin never sees `domain:` — that is the whole point.
        assert!(!out.filters.contains_key("domain"));
        let kinds = out.filters.get("contentKind").unwrap();
        assert!(kinds.contains(&"book".to_string()));
        assert!(kinds.contains(&"audiobook".to_string()));
        assert!(!kinds.contains(&"track".to_string()));
    }

    #[test]
    fn an_explicit_content_kind_filter_wins() {
        let out = expand_domain_filter(&q(&[
            ("domain", &["music"][..]),
            ("contentKind", &["track"][..]),
        ]))
        .unwrap();
        assert_eq!(
            out.filters.get("contentKind").map(Vec::as_slice),
            Some(&["track".to_string()][..])
        );
    }

    #[test]
    fn unknown_domain_adds_no_alternation() {
        // An empty alternation would drop every record at the plugin; the
        // post-stamp `domain:` check does the (correct, empty) filtering.
        let out = expand_domain_filter(&q(&[("domain", &["hologram"])])).unwrap();
        assert!(!out.filters.contains_key("contentKind"));
    }

    #[test]
    fn stamp_fills_in_and_never_overwrites() {
        let mut f: BTreeMap<String, String> = BTreeMap::new();
        f.insert("contentKind".into(), "episode".into());
        stamp_domain(&mut f);
        assert_eq!(f.get("domain").map(String::as_str), Some("tv"));

        // A pack writer that already knows its context keeps it.
        let mut p: BTreeMap<String, String> = BTreeMap::new();
        p.insert("contentKind".into(), "pack".into());
        p.insert("domain".into(), "music".into());
        stamp_domain(&mut p);
        assert_eq!(p.get("domain").map(String::as_str), Some("music"));

        // No contentKind → no domain invented.
        let mut n: BTreeMap<String, String> = BTreeMap::new();
        stamp_domain(&mut n);
        assert!(n.get("domain").is_none());
    }
}
