//! `contentKind` → `domain` / `workForm` — the routing tables from
//! METADATA_KEYS.md §1.
//!
//! `domain` answers **"which application is this record destined for?"**. It
//! is the coarsest facet in the registry and the key every client filters its
//! catalogue on — **one value per app**, no alternation:
//!
//! ```text
//! meta-watch   →  domain:screen
//! meta-listen  →  domain:music
//! meta-read    →  domain:literature
//! ```
//!
//! `workForm` answers the *other* question the old `film`/`tv` split was
//! secretly encoding: **is this work complete in itself, or one instalment of
//! a run?** (`standalone` | `serial`). It is cross-domain — a comic issue is
//! `serial` in `literature` exactly as an episode is `serial` in `screen` —
//! which is what lets a client ask for it independently of the wall.
//!
//! # Why they are stored and not derived on read
//!
//! `contentKind`'s vocabulary is deliberately open — a new feeder may mint a
//! kind and ship without a fleet-wide update. That only stays safe because the
//! domain travels *with* the record: a peer that has never heard of
//! `podcastEpisode` still routes it correctly, because it never has to
//! interpret the token. Deriving on read would silently drop every record
//! whose kind the reader predates. The same argument applies to `workForm`,
//! plus a second one: the two kinds that most need it (`pack` and `card`) are
//! exactly the two the kind alone cannot decide.
//!
//! # ⚠ The binding is permanent and one-way
//!
//! Add kinds freely; **never re-map an existing kind to a different domain**.
//! Because the value is stored, two peers on different versions that disagree
//! about where `audiobook` belongs produce a split-brain that redeploying
//! cannot heal — the wrong domain is already written into records across the
//! mesh. If a kind genuinely has to move, mint a new kind.
//!
//! The `film`+`tv` → `screen` merge (METADATA_KEYS.md §14.17) is the one
//! deliberate exception, taken once and early and paid for with a one-shot
//! meta-core corpus sweep. It is not a precedent.
//!
//! # Why `domain` and not one kind-key per medium
//!
//! `record_matches` is AND-across-keys / OR-within-key, and a non-matching key
//! fails the record outright. A per-medium split (`videoKind` / `audioKind` /
//! `documentKind`) would force meta-listen to express "music video **or**
//! audio track" as an OR across two *different* keys — which the query
//! language cannot represent at all. One domain key, OR-ed within itself,
//! always can. `workForm` is one key everywhere for the same reason.

/// The `contentKind` → `domain` table. A kind absent from this list has no
/// domain and is **not independently routable** — correct for sidecars
/// (subtitles, artwork) and for un-anchored hits, a producer bug for anything
/// else.
///
/// ⚠ `pack` is deliberately absent. A season pack and an album release are the
/// same structural thing, so the kind alone cannot tell `screen` from `music`;
/// the writer stamps `pack`'s domain from its own context (the same reason
/// `derive_file_type` cannot have a second `pack` arm).
///
/// ⚠ `podcast` / `podcastEpisode` are absent too: the `spoken` domain was
/// retired unused (METADATA_KEYS.md §14.17), and the PR that first *writes*
/// those kinds picks their domain.
const DOMAIN_BY_CONTENT_KIND: &[(&str, &str)] = &[
    // screen — everything meta-watch consumes, films and serials alike. The
    // standalone/serial split lives in `workForm`, not here.
    ("movie", "screen"),
    ("series", "screen"),
    ("episode", "screen"),
    // music
    ("track", "music"),
    ("album", "music"),
    ("artist", "music"),
    ("musicVideo", "music"),
    ("djMix", "music"),
    ("liveSet", "music"),
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

/// The `contentKind` → `workForm` table (METADATA_KEYS.md §1).
///
/// `standalone` = a work complete in itself. `serial` = an instalment of an
/// ongoing run, **or the container or headline for one** — which is why a
/// `series` card and an `episode` are both `serial`.
///
/// ⚠ `pack` is absent for the same reason it is absent above: a season pack is
/// `serial`, an album release is `standalone`, and only the writer knows which.
const WORK_FORM_BY_CONTENT_KIND: &[(&str, &str)] = &[
    // screen
    ("movie", "standalone"),
    ("series", "serial"),
    ("episode", "serial"),
    // music — an album is a closed work, not an ongoing run, so every rung of
    // the music ladder is standalone.
    ("track", "standalone"),
    ("album", "standalone"),
    ("artist", "standalone"),
    ("musicVideo", "standalone"),
    ("djMix", "standalone"),
    ("liveSet", "standalone"),
    // literature — the split that proves `workForm` is not a screen-only axis.
    ("book", "standalone"),
    ("audiobook", "standalone"),
    ("comic", "serial"),
    ("manga", "serial"),
    ("magazine", "serial"),
    // science
    ("paper", "standalone"),
];

/// Resolve the domain a `contentKind` belongs to.
///
/// Returns `None` for `pack` (not derivable from the kind — the writer supplies
/// it), for format/sidecar kinds that route nowhere, and for any kind this
/// build has never heard of. A caller that already knows the domain from its
/// own context must prefer that over this table.
pub fn domain_for_content_kind(content_kind: &str) -> Option<&'static str> {
    lookup(DOMAIN_BY_CONTENT_KIND, content_kind)
}

/// Resolve the work form a `contentKind` sits on. Same contract as
/// [`domain_for_content_kind`]: `None` means "not derivable", never a guess.
pub fn work_form_for_content_kind(content_kind: &str) -> Option<&'static str> {
    lookup(WORK_FORM_BY_CONTENT_KIND, content_kind)
}

fn lookup(table: &'static [(&'static str, &'static str)], key: &str) -> Option<&'static str> {
    let k = key.trim();
    table
        .iter()
        .find(|(kind, _)| kind.eq_ignore_ascii_case(k))
        .map(|(_, value)| *value)
}

/// Kinds whose `domain` the kind→domain table deliberately cannot decide, so
/// the **writer** supplies it (METADATA_KEYS.md §1). Today that is `pack`
/// alone: a season pack and an album release are the same structural thing, and
/// only the producer knows which — indexer-feeder's `bt.rs` reads the domain off
/// the per-file kind *before* overwriting it with `pack` precisely so the value
/// survives.
///
/// They must ride along in every `domain:` → `contentKind:` expansion. The
/// expansion is only ever a **pre-filter** — the real `domain:` term is enforced
/// afterwards by [`crate::query_eval::record_matches`] — so widening it is free,
/// while omitting these kinds would drop exactly the records whose `domain` is
/// the most authoritative one on the wire.
pub const WRITER_SUPPLIED_DOMAIN_KINDS: &[&str] = &["pack"];

/// The `workForm` twin of [`WRITER_SUPPLIED_DOMAIN_KINDS`], and for the same
/// reason: a season pack is `serial`, an album release is `standalone`.
pub const WRITER_SUPPLIED_WORK_FORM_KINDS: &[&str] = &["pack"];

/// The inverse: every `contentKind` that belongs to `domain`.
///
/// Used at the gateway-routing boundary. A gateway declares the *content kinds*
/// it serves (`served_content_kinds`), not domains, so a `domain:` filter is
/// matched by expanding it here rather than by adding a `served_domains` field
/// to the heartbeat — which would be a wire-protocol change requiring every
/// feeder manifest and every peer to move at once.
///
/// ⚠ Since the `film`+`tv` merge this returns **several** kinds for `screen`
/// (`movie`, `series`, `episode`) where the two old values were disjoint. A
/// caller that reads only the first element silently means "movie" — scan the
/// whole list, or narrow with a `workForm:` term first.
pub fn content_kinds_for_domain(domain: &str) -> Vec<&'static str> {
    inverse(DOMAIN_BY_CONTENT_KIND, domain)
}

/// The inverse of the `workForm` table. Same routing-boundary role, same
/// scan-the-whole-list warning as [`content_kinds_for_domain`].
pub fn content_kinds_for_work_form(work_form: &str) -> Vec<&'static str> {
    inverse(WORK_FORM_BY_CONTENT_KIND, work_form)
}

fn inverse(table: &'static [(&'static str, &'static str)], value: &str) -> Vec<&'static str> {
    let v = value.trim();
    table
        .iter()
        .filter(|(_, val)| val.eq_ignore_ascii_case(v))
        .map(|(kind, _)| *kind)
        .collect()
}

/// Rewrite a query so a `domain:` / `workForm:` filter can reach a plugin that
/// has never heard of either key.
///
/// Returns `None` when the query carries neither — the common path, and cheaper
/// than cloning.
///
/// # Why translate instead of teaching every plugin
///
/// `record_matches` fails a record outright when a filter key is missing from
/// its fields, and `domain` is missing from every record a not-yet-updated
/// feeder emits (`workForm` from every record written before it existed).
/// Most feeders pin the SDK by git tag, so they cannot pick up a new key on our
/// schedule. Translating `domain:literature` into the `contentKind:`
/// alternation it covers means the wire query a plugin sees never mentions
/// either key — old plugins keep working unchanged, while the dispatcher still
/// enforces the real filters afterwards, against records it has just stamped.
///
/// # The two axes intersect
///
/// `domain:screen workForm:serial` covers `series` and `episode` — the
/// **intersection**, not the union. Widening to the union would hand a serial
/// query the whole film corpus as a pre-filter; the post-stamp check would then
/// throw it away, having already paid for it upstream.
///
/// An explicit `contentKind:` filter already in the query is left alone: it is
/// the narrower of the three, and the `domain`/`workForm` terms are still
/// enforced post-stamp, so the AND semantics hold either way.
pub fn expand_routing_filters(
    query: &crate::query::GatewayQuery,
) -> Option<crate::query::GatewayQuery> {
    let domains = query.filters.get("domain");
    let work_forms = query.filters.get("workForm");
    if domains.is_none() && work_forms.is_none() {
        return None;
    }

    let by_domain = domains.map(|vs| expand_values(vs, content_kinds_for_domain));
    let by_work_form = work_forms.map(|vs| expand_values(vs, content_kinds_for_work_form));

    let mut out = query.clone();
    out.filters.remove("domain");
    out.filters.remove("workForm");
    if out.filters.contains_key("contentKind") {
        return Some(out);
    }

    let mut kinds: Vec<&'static str> = match (by_domain, by_work_form) {
        (Some(d), Some(w)) => d.into_iter().filter(|k| w.contains(k)).collect(),
        (Some(d), None) => d,
        (None, Some(w)) => w,
        (None, None) => unreachable!("guarded above"),
    };
    // Domain-ambiguous kinds come along whenever the expansion is non-empty
    // — see [`WRITER_SUPPLIED_DOMAIN_KINDS`]. Skipped when nothing mapped,
    // so an unknown value still expands to nothing and leaves the filter
    // out entirely (below) rather than narrowing to `contentKind:pack`.
    if !kinds.is_empty() {
        for k in WRITER_SUPPLIED_DOMAIN_KINDS
            .iter()
            .chain(WRITER_SUPPLIED_WORK_FORM_KINDS.iter())
        {
            if !kinds.contains(k) {
                kinds.push(k);
            }
        }
    }
    // An unknown value — or two axes with no kind in common — expands to
    // nothing. Inserting an empty alternation would drop every record at the
    // plugin; leaving the filter out lets the plugin answer and the post-stamp
    // check do the (correct, empty) filtering.
    if !kinds.is_empty() {
        out.filters.insert(
            "contentKind".to_string(),
            kinds.into_iter().map(|k| k.to_string()).collect(),
        );
    }
    Some(out)
}

/// Deprecated alias for [`expand_routing_filters`], kept so a caller pinned to
/// an older SDK keeps compiling. It expands `workForm:` too — the name is the
/// only thing that is stale.
#[deprecated(note = "renamed to expand_routing_filters; it now expands workForm: as well")]
pub fn expand_domain_filter(
    query: &crate::query::GatewayQuery,
) -> Option<crate::query::GatewayQuery> {
    expand_routing_filters(query)
}

fn expand_values(
    values: &[String],
    expand: fn(&str) -> Vec<&'static str>,
) -> Vec<&'static str> {
    let mut out: Vec<&'static str> = Vec::new();
    for v in values {
        for k in expand(v) {
            if !out.contains(&k) {
                out.push(k);
            }
        }
    }
    out
}

/// Stamp `domain` on a field map that already carries a `contentKind`.
///
/// The convention from METADATA_KEYS.md §1: **whoever writes `contentKind`
/// writes `domain` in the same breath**. An existing `domain` is never
/// overwritten — a writer that knows better than the table (a `pack`, say) has
/// already set it.
pub fn stamp_domain(fields: &mut std::collections::BTreeMap<String, String>) {
    stamp(fields, "domain", domain_for_content_kind);
}

/// Stamp `workForm` the same way, from the same `contentKind`. Call it
/// wherever [`stamp_domain`] is called — the registry treats the two as one
/// breath.
pub fn stamp_work_form(fields: &mut std::collections::BTreeMap<String, String>) {
    stamp(fields, "workForm", work_form_for_content_kind);
}

fn stamp(
    fields: &mut std::collections::BTreeMap<String, String>,
    key: &str,
    derive: fn(&str) -> Option<&'static str>,
) {
    if fields.contains_key(key) {
        return;
    }
    let Some(kind) = fields.get("contentKind").cloned() else {
        return;
    };
    if let Some(value) = derive(&kind) {
        fields.insert(key.to_string(), value.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn kinds_map_to_their_domain() {
        assert_eq!(domain_for_content_kind("movie"), Some("screen"));
        assert_eq!(domain_for_content_kind("episode"), Some("screen"));
        assert_eq!(domain_for_content_kind("track"), Some("music"));
        // The case that proves medium and domain are different axes.
        assert_eq!(domain_for_content_kind("musicVideo"), Some("music"));
        // The case that proves the player does not decide the domain.
        assert_eq!(domain_for_content_kind("audiobook"), Some("literature"));
    }

    #[test]
    fn film_and_tv_are_one_domain_now() {
        // The merge (METADATA_KEYS.md §14.17): meta-watch's wall is one term.
        assert_eq!(
            domain_for_content_kind("movie"),
            domain_for_content_kind("series")
        );
        // ...and the split it used to carry lives here instead.
        assert_eq!(work_form_for_content_kind("movie"), Some("standalone"));
        assert_eq!(work_form_for_content_kind("series"), Some("serial"));
        assert_eq!(work_form_for_content_kind("episode"), Some("serial"));
    }

    #[test]
    fn work_form_is_not_a_screen_only_axis() {
        // The point of the key: it means the same thing in every domain.
        assert_eq!(work_form_for_content_kind("comic"), Some("serial"));
        assert_eq!(work_form_for_content_kind("book"), Some("standalone"));
        assert_eq!(work_form_for_content_kind("album"), Some("standalone"));
    }

    #[test]
    fn the_spoken_domain_is_gone() {
        // Retired unused. The kinds stay reserved with no domain until the PR
        // that writes them picks one.
        assert_eq!(domain_for_content_kind("podcast"), None);
        assert_eq!(domain_for_content_kind("podcastEpisode"), None);
        assert!(content_kinds_for_domain("spoken").is_empty());
    }

    #[test]
    fn pack_has_no_derivable_domain_or_work_form() {
        assert_eq!(domain_for_content_kind("pack"), None);
        assert_eq!(work_form_for_content_kind("pack"), None);
    }

    #[test]
    fn unknown_kind_is_none_not_a_guess() {
        assert_eq!(domain_for_content_kind("hologram"), None);
        assert_eq!(work_form_for_content_kind("hologram"), None);
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
        for kind in content_kinds_for_work_form("serial") {
            assert_eq!(work_form_for_content_kind(kind), Some("serial"));
        }
    }

    #[test]
    fn screen_expands_to_every_rung_not_just_the_first() {
        // The regression the merge creates for any caller reading `.first()`.
        let screen = content_kinds_for_domain("screen");
        assert!(screen.contains(&"movie"));
        assert!(screen.contains(&"series"));
        assert!(screen.contains(&"episode"));
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
    fn no_routing_filter_is_left_untouched() {
        assert!(expand_routing_filters(&q(&[("fileType", &["video"])])).is_none());
    }

    #[test]
    fn domain_becomes_the_content_kinds_it_covers() {
        let out = expand_routing_filters(&q(&[("domain", &["literature"])])).unwrap();
        // The plugin never sees `domain:` — that is the whole point.
        assert!(!out.filters.contains_key("domain"));
        let kinds = out.filters.get("contentKind").unwrap();
        assert!(kinds.contains(&"book".to_string()));
        assert!(kinds.contains(&"audiobook".to_string()));
        assert!(!kinds.contains(&"track".to_string()));
    }

    #[test]
    fn work_form_alone_also_expands() {
        let out = expand_routing_filters(&q(&[("workForm", &["serial"])])).unwrap();
        assert!(!out.filters.contains_key("workForm"));
        let kinds = out.filters.get("contentKind").unwrap();
        assert!(kinds.contains(&"episode".to_string()));
        assert!(kinds.contains(&"comic".to_string()));
        assert!(!kinds.contains(&"movie".to_string()));
    }

    #[test]
    fn the_two_axes_intersect_they_do_not_union() {
        let out = expand_routing_filters(&q(&[
            ("domain", &["screen"][..]),
            ("workForm", &["serial"][..]),
        ]))
        .unwrap();
        let kinds = out.filters.get("contentKind").unwrap();
        assert!(kinds.contains(&"series".to_string()));
        assert!(kinds.contains(&"episode".to_string()));
        // The union would have dragged the whole film corpus in.
        assert!(!kinds.contains(&"movie".to_string()));
        // A `serial` comic is in `literature`, not on meta-watch's wall.
        assert!(!kinds.contains(&"comic".to_string()));
    }

    #[test]
    fn an_explicit_content_kind_filter_wins() {
        let out = expand_routing_filters(&q(&[
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
    fn expansion_carries_the_writer_supplied_kinds() {
        // `pack` has no domain in the table (a season pack and an album are the
        // same structural thing), so the writer stamps one. Leaving it out of
        // the alternation would make a `domain:screen` pre-filter drop the whole
        // torrent-pack corpus, which `record_matches` would then never see.
        let out = expand_routing_filters(&q(&[("domain", &["screen"])])).unwrap();
        let kinds = out.filters.get("contentKind").unwrap();
        assert!(kinds.contains(&"episode".to_string()));
        assert!(kinds.contains(&"pack".to_string()));
        // ...and it rides along on the `workForm` axis too, exactly once.
        let out = expand_routing_filters(&q(&[("workForm", &["serial"])])).unwrap();
        let kinds = out.filters.get("contentKind").unwrap();
        assert_eq!(kinds.iter().filter(|k| *k == "pack").count(), 1);
    }

    #[test]
    fn unknown_value_adds_no_alternation() {
        // An empty alternation would drop every record at the plugin; the
        // post-stamp check does the (correct, empty) filtering.
        let out = expand_routing_filters(&q(&[("domain", &["hologram"])])).unwrap();
        // Not even `pack`: a lone `contentKind:pack` would be a NARROWING of a
        // filter that should widen to nothing.
        assert!(!out.filters.contains_key("contentKind"));
    }

    #[test]
    fn two_axes_with_nothing_in_common_widen_rather_than_narrow() {
        // No music kind is `serial`. The pre-filter drops out; the post-stamp
        // check still enforces both terms.
        let out = expand_routing_filters(&q(&[
            ("domain", &["music"][..]),
            ("workForm", &["serial"][..]),
        ]))
        .unwrap();
        assert!(!out.filters.contains_key("contentKind"));
    }

    #[test]
    fn stamp_fills_in_and_never_overwrites() {
        let mut f: BTreeMap<String, String> = BTreeMap::new();
        f.insert("contentKind".into(), "episode".into());
        stamp_domain(&mut f);
        stamp_work_form(&mut f);
        assert_eq!(f.get("domain").map(String::as_str), Some("screen"));
        assert_eq!(f.get("workForm").map(String::as_str), Some("serial"));

        // A pack writer that already knows its context keeps it.
        let mut p: BTreeMap<String, String> = BTreeMap::new();
        p.insert("contentKind".into(), "pack".into());
        p.insert("domain".into(), "music".into());
        p.insert("workForm".into(), "standalone".into());
        stamp_domain(&mut p);
        stamp_work_form(&mut p);
        assert_eq!(p.get("domain").map(String::as_str), Some("music"));
        assert_eq!(p.get("workForm").map(String::as_str), Some("standalone"));

        // No contentKind → nothing invented.
        let mut n: BTreeMap<String, String> = BTreeMap::new();
        stamp_domain(&mut n);
        stamp_work_form(&mut n);
        assert!(n.get("domain").is_none());
        assert!(n.get("workForm").is_none());
    }
}
