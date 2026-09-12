//! The `openlibrary` upstream — Open Library work cards, the book identity tier.
//!
//! `TMDB : video :: AniList : comics :: Open Library : books`. It names works and
//! nothing else: no bytes, no editions, no availability. Byte tiers (`gutenberg`
//! in this same binary, torznab and others elsewhere) supply the editions and
//! join them to a card through `openlibraryid`.
//!
//! ## The one contract that must not drift
//!
//! The card address is `compute_card_cid("openlibrary", "OL…W")` — the bare work
//! id, **no kind prefix** — byte-identical to `meta-read/src/card_cid.rs`'s
//! `openlibrary` (reading-model §2.3). Pinned by golden vectors below. Get it
//! wrong and every card lands on a different tile from the one the client
//! routes to.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use meta_feeder_sdk::budget::RateBudget;
use meta_feeder_sdk::config::ConfigSchema;
use meta_feeder_sdk::hash::compute_card_cid;
use meta_feeder_sdk::plugin::{ConfigError, FeederPlugin, HashKind, HashOutcome};
use meta_feeder_sdk::query::GatewayQuery;
use meta_feeder_sdk::types::{DiscoveryRecord, GatewayError, Hash, PluginHealth};
use tracing::{debug, warn};

use crate::openlibrary_client::{
    self as client, OpenLibraryClient, SearchDoc, BROWSE_DEADLINE, SEARCH_DEADLINE,
};

/// This upstream's id, and the card locator's source namespace.
///
/// ⚠ Cross-repo contract — see the module doc.
pub const UPSTREAM: &str = "openlibrary";

/// Default result sizing. `top_n` answers the search box; `discovery_n` fills a
/// curated row.
const DEFAULT_TOP_N: usize = 12;
const DEFAULT_DISCOVERY_N: usize = 20;

/// Over-fetch factors. The card gate (cover AND description) drops a share of
/// every list — most of a `sort=new` list, which is largely catalogue stubs —
/// and a row that asked for twenty should not render seven.
const SEARCH_OVERFETCH: usize = 2;
const FRESH_OVERFETCH: usize = 4;
const MAX_LIMIT: usize = 100;

/// Work JSON fetches in flight at once. The rate budget is the real limiter;
/// this only bounds how many leases one row holds while waiting.
const DESCRIPTION_CONCURRENCY: usize = 4;

/// The card locator for an Open Library work id. `None` for anything that is
/// not a bare `OL…W` work id.
pub fn card_cid(work_id: &str) -> Option<String> {
    if !client::is_work_id(work_id) {
        return None;
    }
    compute_card_cid(UPSTREAM, work_id)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

/// First non-empty value for `key`.
fn first_filter(q: &GatewayQuery, key: &str) -> Option<String> {
    q.filters
        .get(key)
        .and_then(|v| v.first())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn filter_is_true(q: &GatewayQuery, key: &str) -> bool {
    q.filters
        .get(key)
        .map(|v| v.iter().any(|x| x.eq_ignore_ascii_case("true")))
        .unwrap_or(false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Browse {
    Popular,
    Fresh,
}

/// A keyword-less curated row: `popular:true` / `fresh:true` and no free text.
fn browse_kind(q: &GatewayQuery) -> Option<Browse> {
    if !q.free_text.trim().is_empty() {
        return None;
    }
    if filter_is_true(q, "fresh") {
        return Some(Browse::Fresh);
    }
    if filter_is_true(q, "popular") || filter_is_true(q, "trending") {
        return Some(Browse::Popular);
    }
    None
}

/// The browse markers a card echoes. See [`to_record`].
const BROWSE_MARKERS: &[&str] = &["popular", "fresh", "trending"];

/// Identity and instalment filters this feeder cannot answer.
///
/// ⚠ An id-anchored query this upstream does not understand must return
/// NOTHING — never fall through to the free-text branch, which would search on
/// the query's title and fill a work page with plausible-looking strangers
/// (measured on the music tier: an `mbReleaseGroupId` query fell through and a
/// BTS album page filled with sixty Grateful Dead recordings).
///
/// - `gutenbergid` / `isbn` CAN lead to a work (reading-model §2.4), but the
///   binding belongs on the **edition**, where the `gutenberg` upstream writes
///   it. A card answered here carries neither key, so the gateway's
///   `record_matches` would drop it anyway — the only effect of answering is
///   spent budget.
/// - `workcid` names one site's comic; `chapterNumber` & co. scope an
///   instalment. A card is never chapter N (see meta-feeder-anilist's list for
///   the stored-echo incident this guards against).
const FOREIGN_IDENTITY_FILTERS: &[&str] = &[
    "anilistid",
    "workcid",
    "chapterNumber",
    "volumeNumber",
    "chapterStart",
    "chapterEnd",
    "tmdbid",
    "imdbid",
    "tvdbid",
    "gutenbergid",
    "isbn",
    "olEditionId",
    "isrc",
    "doi",
];

/// The foreign filter on `q`, if any. Every MusicBrainz id (`mbReleaseGroupId`,
/// `mbArtistId`, `mbRecordingId`, …) counts, by prefix, so a new one is
/// declined without a list edit.
fn foreign_identity_filter(q: &GatewayQuery) -> Option<&str> {
    q.filters.keys().map(String::as_str).find(|k| {
        FOREIGN_IDENTITY_FILTERS.contains(k)
            || (k.starts_with("mb") && k[2..].starts_with(|c: char| c.is_ascii_uppercase()))
    })
}

/// Project one doc plus its work's description into a card record.
///
/// `None` when the work is not a card:
/// - no work id → no card cid → no `cids/` member, and a record without one is
///   silently dropped by the gateway's store-back while the coverage gate still
///   marks the query covered;
/// - **no cover or no description** — an anchor card promises to describe a
///   work (reading-model §2.5). Open Library holds millions of stub works with
///   neither, and publishing them would fill the Books rows with blank tiles.
///
/// ⚠ Echoes only the browse markers, not every filter. `record_matches` fails a
/// record that lacks a filter key, so a `popular:true` row needs its cards to
/// carry `popular`; but echoing *all* filters (the AniList tier's approach)
/// would stamp e.g. a `licence:pd` query onto a card — and the gateway stores
/// what passes, permanently. A card that lacks another filter's key does not
/// match it, which is the truth.
pub fn to_record(doc: &SearchDoc, description: &str, query: &GatewayQuery) -> Option<DiscoveryRecord> {
    let id = doc.work_id()?;
    let cid = card_cid(id)?;
    let title = doc.title.as_deref().map(str::trim).filter(|t| !t.is_empty())?;
    let poster_url = doc.cover_url()?;
    let description = description.trim();
    if description.is_empty() {
        return None;
    }

    let mut fields: BTreeMap<String, String> = BTreeMap::new();

    // ── The classification axes ──────────────────────────────────────────────
    //
    // Whoever writes contentKind writes domain AND workForm in the same breath
    // (METADATA_KEYS.md convention 6). A book is a work complete in itself.
    fields.insert("fileType".into(), "card".into());
    fields.insert("contentKind".into(), "book".into());
    fields.insert("domain".into(), "literature".into());
    fields.insert("workForm".into(), "standalone".into());

    // ── Identity ─────────────────────────────────────────────────────────────
    //
    // ⚠ Exactly ONE cids/ member. `url_key_for` takes the lexicographic .min()
    // of them, so a second member would make the store key arbitrary.
    fields.insert(format!("cids/{cid}"), "true".into());
    // Bare `OL…W`, the same string that is inside the cid.
    fields.insert("openlibraryid".into(), id.to_string());
    fields.insert("title".into(), title.to_string());

    // `description/{lang3}`, never a flat `description` — the flat key is indexed
    // but never written (METADATA_KEYS.md §14.6).
    fields.insert("description/eng".into(), description.to_string());

    let mut seen = HashSet::new();
    let authors: Vec<&str> = doc
        .author_name
        .iter()
        .map(|a| a.trim())
        .filter(|a| !a.is_empty() && seen.insert(*a))
        .collect();
    for a in &authors {
        fields.insert(format!("authors/{a}"), "true".into());
    }
    if !authors.is_empty() {
        fields.insert("author".into(), authors.join(", "));
    }

    // `first_publish_year` is the WORK's year (1813 for Pride and Prejudice),
    // which is what a card names. A bare year: the upstream has no finer value.
    if let Some(y) = doc.first_publish_year.filter(|y| *y > 0) {
        fields.insert("releasedate".into(), format!("{y:04}"));
    }

    // ⚠ `poster_url`, NOT a pre-hashed `poster`: the gateway's SEEDABLE_FIELDS
    // path fetches it, seeds the bytes and writes the content cid back.
    fields.insert("poster_url".into(), poster_url);

    fields.insert(
        "sourceUrl".into(),
        format!("https://openlibrary.org/works/{id}"),
    );
    fields.insert(format!("source/gateway:{UPSTREAM}"), "true".into());

    for marker in BROWSE_MARKERS {
        if filter_is_true(query, marker) {
            fields.insert((*marker).to_string(), "true".into());
        }
    }

    Some(DiscoveryRecord {
        upstream_id: UPSTREAM.into(),
        record_id: id.to_string(),
        fields,
    })
}

/// Turn docs into at most `n` cards, fetching each candidate's description.
///
/// Candidates without a work id, a title or a cover are dropped BEFORE any work
/// JSON is fetched — each fetch spends budget, and those docs cannot become
/// cards whatever the description says. The stream is lazy, so once `n` cards
/// exist no further descriptions are requested.
async fn project(
    client: &OpenLibraryClient,
    docs: Vec<SearchDoc>,
    query: &GatewayQuery,
    n: usize,
    deadline: Duration,
) -> Vec<DiscoveryRecord> {
    let mut seen = HashSet::new();
    let candidates: Vec<SearchDoc> = docs
        .into_iter()
        .filter(|d| {
            d.work_id().is_some()
                && d.cover_url().is_some()
                && d.title.as_deref().is_some_and(|t| !t.trim().is_empty())
        })
        .filter(|d| seen.insert(d.key.clone()))
        .collect();

    stream::iter(candidates)
        .map(|doc| async move {
            let id = doc.work_id().unwrap_or_default().to_string();
            let text = client.work_text(&id, deadline).await;
            (doc, text)
        })
        .buffered(DESCRIPTION_CONCURRENCY)
        .filter_map(|(doc, text)| async move {
            match text {
                Ok(Some(w)) => {
                    // ⚠ A merged-away id. The index still lists it, but minting a
                    // card for it would be a second tile for a work that lives
                    // under another id — which the index usually lists too.
                    if Some(w.id.as_str()) != doc.work_id() {
                        debug!(from = doc.key, to = w.id, "openlibrary: merged work; skipped");
                        return None;
                    }
                    to_record(&doc, w.description.as_deref()?, query)
                }
                Ok(None) => None,
                Err(e) => {
                    debug!(key = doc.key, error = ?e, "openlibrary: work lookup failed; no card");
                    None
                }
            }
        })
        .take(n)
        .collect()
        .await
}

pub struct OpenLibraryPlugin {
    client: Option<OpenLibraryClient>,
    top_n: usize,
    discovery_n: usize,
    contact: Option<String>,
    /// Shared with the `gutenberg` plugin by `main.rs`: both call the same
    /// upstream, so they draw on one bucket.
    budget: Option<Arc<RateBudget>>,
    /// Test hook: point the client at a mock server. Never set in production.
    api_base: Option<String>,
}

impl Default for OpenLibraryPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenLibraryPlugin {
    pub fn new() -> Self {
        Self {
            client: None,
            top_n: env_usize("OPENLIBRARY_TOP_N", DEFAULT_TOP_N),
            discovery_n: env_usize("OPENLIBRARY_DISCOVERY_N", DEFAULT_DISCOVERY_N),
            contact: client::env_contact(),
            budget: None,
            api_base: None,
        }
    }

    /// Construct against a specific API base — tests only.
    pub fn with_api_base(api_base: impl Into<String>) -> Self {
        Self {
            api_base: Some(api_base.into()),
            ..Self::new()
        }
    }

    /// Draw on a budget shared with the other Open Library caller in this
    /// process instead of building one at `configure()`.
    pub fn with_budget(mut self, budget: Arc<RateBudget>) -> Self {
        self.budget = Some(budget);
        self
    }
}

#[async_trait]
impl FeederPlugin for OpenLibraryPlugin {
    fn upstream_id(&self) -> &'static str {
        UPSTREAM
    }

    /// ⚠ `card` only, and `book` only.
    ///
    /// This is the identity tier: it names works, it does not serve bytes. A
    /// `document` axis here would route edition queries — the ones that ask for
    /// an actual epub — to an upstream that has none.
    fn served_file_types(&self) -> &'static [&'static str] {
        &["card"]
    }

    fn served_content_kinds(&self) -> &'static [&'static str] {
        &["book"]
    }

    fn configure(&mut self, cache_dir: &Path) -> Result<(), ConfigError> {
        let cache = meta_feeder_sdk::common::open_midhash_cache(cache_dir, UPSTREAM)?;
        let contact = client::load_contact(cache_dir);
        let budget = self
            .budget
            .clone()
            .unwrap_or_else(|| client::budget(contact.is_some()));
        self.client = Some(OpenLibraryClient::new(
            budget,
            Some(cache),
            self.api_base.clone(),
            contact.as_deref(),
        ));
        self.contact = contact;
        Ok(())
    }

    fn health(&self) -> PluginHealth {
        if self.client.is_some() {
            PluginHealth::Ok
        } else {
            PluginHealth::Degraded {
                reason: "not configured".into(),
            }
        }
    }

    async fn handle_query(
        &self,
        query: &GatewayQuery,
        max_results: usize,
    ) -> Result<Vec<DiscoveryRecord>, GatewayError> {
        // Layer A — the routing gate, before any network.
        if !meta_feeder_sdk::query_eval::query_accepts_plugin(
            query,
            self.served_file_types(),
            self.served_content_kinds(),
        ) {
            // A browse row rejected here is an empty wall with no error anywhere
            // — the operator's only clue. Say it loudly.
            if browse_kind(query).is_some() {
                warn!(
                    filters = ?query.filters.keys().collect::<Vec<_>>(),
                    "openlibrary: a browse row was routed here but does not match \
                     served axes (card × book); the row will render empty"
                );
            }
            return Ok(Vec::new());
        }

        let Some(client) = self.client.as_ref() else {
            return Ok(Vec::new());
        };

        // A foreign identity anchor: decline rather than degrade.
        if let Some(k) = foreign_identity_filter(query) {
            debug!(filter = k, "openlibrary: foreign identity anchor; returning nothing");
            return Ok(Vec::new());
        }

        let cap = max_results.max(1);

        // Our own anchor: resolve exactly one card. An id that is not a work id
        // answers nothing — it must not fall through to the free-text branch.
        if let Some(raw) = first_filter(query, "openlibraryid") {
            let id = raw.strip_prefix("/works/").unwrap_or(&raw);
            if !client::is_work_id(id) {
                return Ok(Vec::new());
            }
            let Some(doc) = client.doc_by_work_id(id, SEARCH_DEADLINE).await? else {
                return Ok(Vec::new());
            };
            return Ok(project(client, vec![doc], query, 1, SEARCH_DEADLINE).await);
        }

        // A curated row.
        if let Some(kind) = browse_kind(query) {
            let n = self.discovery_n.min(cap);
            let docs = match kind {
                Browse::Popular => {
                    client
                        .trending((n * SEARCH_OVERFETCH).min(MAX_LIMIT), BROWSE_DEADLINE)
                        .await?
                }
                Browse::Fresh => {
                    client
                        .fresh((n * FRESH_OVERFETCH).min(MAX_LIMIT), BROWSE_DEADLINE)
                        .await?
                }
            };
            return Ok(project(client, docs, query, n, BROWSE_DEADLINE).await);
        }

        // Free text.
        let text = query.free_text.trim();
        if text.is_empty() {
            // ⚠ Never `free_text_or_star()` here: `search.json` rejects `q=*`
            // with a 422, and a filters-only query is not a request for the
            // whole catalogue anyway.
            return Ok(Vec::new());
        }
        let n = self.top_n.min(cap);
        let docs = client
            .search(text, (n * SEARCH_OVERFETCH).min(MAX_LIMIT), SEARCH_DEADLINE)
            .await?;
        Ok(project(client, docs, query, n, SEARCH_DEADLINE).await)
    }

    /// A card has no bytes: the record IS the payload.
    ///
    /// `CardLocator` + `bytes: None` lands in the dispatcher's metadata-only
    /// branch. The cover travels as `poster_url` and the gateway's seed path
    /// owns it.
    async fn compute_outcomes(&self, record_id: &str) -> Result<Vec<HashOutcome>, GatewayError> {
        let id = record_id.trim();
        let cid = card_cid(id).ok_or_else(|| {
            GatewayError::Permanent(format!("openlibrary: bad work id {record_id}"))
        })?;

        // Re-resolve so the stored record is complete even when compute is
        // called cold. Cheap: the client caches. A record that resolved to a
        // different work (a merge) is not this cid's record.
        let record = match self.client.as_ref() {
            Some(c) => match c.doc_by_work_id(id, SEARCH_DEADLINE).await {
                Ok(Some(doc)) => project(c, vec![doc], &GatewayQuery::from_free_text(""), 1, SEARCH_DEADLINE)
                    .await
                    .into_iter()
                    .find(|r| r.record_id == id),
                _ => None,
            },
            None => None,
        };

        Ok(vec![HashOutcome {
            hash: Hash(cid),
            hash_kind: HashKind::CardLocator,
            bytes: None,
            record,
            file_extension: None,
        }])
    }

    async fn get_blob(&self, _cid: &str) -> Option<Vec<u8>> {
        None
    }

    fn config_schema(&self) -> ConfigSchema {
        ConfigSchema {
            fields: vec![client::contact_field()],
        }
    }

    fn config_values(&self) -> serde_json::Value {
        serde_json::json!({ "contact": self.contact.clone().unwrap_or_default() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{any, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn q(free: &str, filters: &[(&str, &str)]) -> GatewayQuery {
        let mut q = GatewayQuery::from_free_text(free);
        for (k, v) in filters {
            q.filters.insert((*k).into(), vec![(*v).into()]);
        }
        q
    }

    fn pride() -> SearchDoc {
        SearchDoc {
            key: "/works/OL66554W".into(),
            title: Some("Pride and Prejudice".into()),
            author_name: vec!["Jane Austen".into()],
            cover_i: Some(14348537),
            first_publish_year: Some(1813),
            id_project_gutenberg: vec![],
        }
    }

    fn doc_json(id: &str, title: &str, cover: Option<i64>) -> serde_json::Value {
        json!({
            "key": format!("/works/{id}"),
            "title": title,
            "author_name": ["Jane Austen"],
            "cover_i": cover,
            "first_publish_year": 1813,
            "language": ["eng"]
        })
    }

    async fn plugin_against(server: &MockServer) -> (OpenLibraryPlugin, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut p = OpenLibraryPlugin::with_api_base(server.uri())
            .with_budget(RateBudget::new(100.0, 100.0));
        p.configure(dir.path()).unwrap();
        (p, dir)
    }

    /// A CONFIGURED plugin whose API base fails the test if it is ever called.
    async fn plugin_that_must_not_call_out() -> (OpenLibraryPlugin, MockServer, tempfile::TempDir) {
        let server = MockServer::start().await;
        Mock::given(any())
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        let (p, dir) = plugin_against(&server).await;
        (p, server, dir)
    }

    /// ⚠ The cross-repo contract. These literals are what `meta-read`'s
    /// `card_cid::openlibrary` produces (OL45804W is pinned there). If this test
    /// fails, every card this feeder mints lands on a different tile from the
    /// one the client routes to. Do not "fix" it by updating the literal — find
    /// out which side moved.
    #[test]
    fn golden_vectors_match_meta_read() {
        assert_eq!(
            card_cid("OL45804W").as_deref(),
            Some("bagdsaaaubnxxazlonruwe4tboj4u6tbugu4dancx")
        );
        assert_eq!(
            card_cid("OL66554W").as_deref(),
            Some("bagdsaaaubnxxazlonruwe4tboj4u6tbwgy2tkncx")
        );
        // No kind prefix, and nothing that is not a work id.
        assert_eq!(card_cid("OL66554W"), compute_card_cid("openlibrary", "OL66554W"));
        assert_eq!(card_cid("OL12710531M"), None);
        assert_eq!(card_cid("work:OL66554W"), None);
    }

    #[test]
    fn a_card_carries_exactly_the_contract_fields() {
        let r = to_record(&pride(), "A novel of manners.", &q("pride", &[])).unwrap();
        assert_eq!(r.upstream_id, "openlibrary");
        assert_eq!(r.record_id, "OL66554W");
        let expected: BTreeMap<String, String> = [
            ("fileType", "card"),
            ("contentKind", "book"),
            ("domain", "literature"),
            ("workForm", "standalone"),
            ("cids/bagdsaaaubnxxazlonruwe4tboj4u6tbwgy2tkncx", "true"),
            ("openlibraryid", "OL66554W"),
            ("title", "Pride and Prejudice"),
            ("authors/Jane Austen", "true"),
            ("author", "Jane Austen"),
            ("description/eng", "A novel of manners."),
            ("poster_url", "https://covers.openlibrary.org/b/id/14348537-L.jpg"),
            ("releasedate", "1813"),
            ("sourceUrl", "https://openlibrary.org/works/OL66554W"),
            ("source/gateway:openlibrary", "true"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        // Exact equality: this is also the assertion that a card carries no
        // trust claim (`anchored`, `anchorSource`) — the card IS the anchor — and
        // no drifting counts.
        assert_eq!(r.fields, expected);
    }

    #[test]
    fn several_authors_are_a_key_set_and_a_joined_scalar() {
        let mut d = pride();
        d.author_name = vec!["A. Writer".into(), "B. Writer".into(), "A. Writer".into()];
        let r = to_record(&d, "x", &q("", &[])).unwrap();
        assert_eq!(r.fields["authors/A. Writer"], "true");
        assert_eq!(r.fields["authors/B. Writer"], "true");
        assert_eq!(r.fields["author"], "A. Writer, B. Writer");
    }

    #[test]
    fn no_card_without_a_cover_or_a_description() {
        let mut coverless = pride();
        coverless.cover_i = None;
        assert!(to_record(&coverless, "A novel.", &q("", &[])).is_none());
        assert!(to_record(&pride(), "   ", &q("", &[])).is_none());
        let mut edition = pride();
        edition.key = "/works/OL12710531M".into();
        assert!(to_record(&edition, "A novel.", &q("", &[])).is_none());
    }

    #[test]
    fn echoes_browse_markers_but_no_other_filter() {
        let r = to_record(
            &pride(),
            "A novel.",
            &q("", &[("popular", "true"), ("licence", "pd"), ("contentKind", "book")]),
        )
        .unwrap();
        assert_eq!(r.fields["popular"], "true");
        assert!(!r.fields.contains_key("licence"), "a card must not borrow a query's licence");
    }

    #[test]
    fn serves_the_card_tier_only() {
        let p = OpenLibraryPlugin::new();
        assert_eq!(p.served_file_types(), &["card"]);
        assert_eq!(p.served_content_kinds(), &["book"]);
    }

    #[test]
    fn browse_markers_map_to_rows() {
        assert_eq!(browse_kind(&q("", &[("popular", "true")])), Some(Browse::Popular));
        assert_eq!(browse_kind(&q("", &[("trending", "true")])), Some(Browse::Popular));
        assert_eq!(browse_kind(&q("", &[("fresh", "true")])), Some(Browse::Fresh));
        assert_eq!(browse_kind(&q("austen", &[("popular", "true")])), None);
        assert_eq!(browse_kind(&q("", &[])), None);
    }

    #[tokio::test]
    async fn unconfigured_answers_nothing_rather_than_erroring() {
        let p = OpenLibraryPlugin::new();
        let out = p.handle_query(&q("austen", &[("fileType", "card")]), 5).await.unwrap();
        assert!(out.is_empty());
    }

    /// Layer A: an edition query or a comic query never reaches Open Library.
    #[tokio::test]
    async fn a_query_for_other_axes_is_declined_before_any_request() {
        let (p, server, _dir) = plugin_that_must_not_call_out().await;
        for filters in [
            vec![("fileType", "document"), ("contentKind", "book")],
            vec![("fileType", "card"), ("contentKind", "comic")],
        ] {
            let out = p.handle_query(&q("pride", &filters), 5).await.unwrap();
            assert!(out.is_empty(), "{filters:?}");
        }
        server.verify().await;
    }

    #[tokio::test]
    async fn every_foreign_identity_anchor_is_declined_before_any_request() {
        let (p, server, _dir) = plugin_that_must_not_call_out().await;
        let mut keys: Vec<&str> = FOREIGN_IDENTITY_FILTERS.to_vec();
        keys.extend(["mbReleaseGroupId", "mbArtistId", "mbRecordingId"]);
        for key in keys {
            let query = q("Pride and Prejudice", &[("contentKind", "book"), (key, "1342")]);
            let out = p.handle_query(&query, 5).await.expect("declined, not failed");
            assert!(out.is_empty(), "{key} must not degrade to a free-text search");
        }
        server.verify().await;
    }

    #[tokio::test]
    async fn a_malformed_openlibraryid_answers_nothing_rather_than_searching() {
        let (p, server, _dir) = plugin_that_must_not_call_out().await;
        let query = q("Pride and Prejudice", &[("openlibraryid", "OL66554M")]);
        assert!(p.handle_query(&query, 5).await.unwrap().is_empty());
        server.verify().await;
    }

    #[tokio::test]
    async fn a_filters_only_query_without_a_browse_marker_answers_nothing() {
        let (p, server, _dir) = plugin_that_must_not_call_out().await;
        let query = q("", &[("domain", "literature"), ("contentKind", "book"), ("licence", "pd")]);
        assert!(p.handle_query(&query, 5).await.unwrap().is_empty());
        server.verify().await;
    }

    #[tokio::test]
    async fn a_search_mints_one_card_per_work_with_a_cover_and_a_description() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search.json"))
            .and(query_param("q", "pride and prejudice"))
            .and(query_param("fields", "key,title,author_name,cover_i,first_publish_year,language"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "numFound": 4,
                "docs": [
                    doc_json("OL66554W", "Pride and Prejudice", Some(14348537)),
                    // No cover: no card, and no work JSON is fetched for it.
                    doc_json("OL15165350W", "Pride and Prejudice", None),
                    // A work with no description: no card.
                    doc_json("OL1W", "Stub", Some(5)),
                    // An edition key under /works/: never a card.
                    { "key": "/works/OL12710531M", "title": "Stub", "cover_i": 7 }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/works/OL66554W.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "key": "/works/OL66554W",
                "description": "Pride and Prejudice is an 1813 novel.\r\n\r\n----------\r\nAlso contained in: …"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/works/OL1W.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "key": "/works/OL1W", "title": "Stub" })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/works/OL15165350W.json"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;

        let (p, _dir) = plugin_against(&server).await;
        let query = q("pride and prejudice", &[("fileType", "card")]);
        let out = p.handle_query(&query, 10).await.unwrap();
        assert_eq!(out.len(), 1);
        let f = &out[0].fields;
        assert_eq!(f["openlibraryid"], "OL66554W");
        assert_eq!(f["description/eng"], "Pride and Prejudice is an 1813 novel.");
        assert_eq!(f["cids/bagdsaaaubnxxazlonruwe4tboj4u6tbwgy2tkncx"], "true");

        // The repeat is served from the cache: every mock above is `expect(1)`.
        let again = p.handle_query(&query, 10).await.unwrap();
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].fields, out[0].fields);
        server.verify().await;
    }

    #[tokio::test]
    async fn an_openlibraryid_resolves_exactly_one_card_and_reads_a_typed_description() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search.json"))
            .and(query_param("q", "key:/works/OL66554W"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "numFound": 1,
                "docs": [doc_json("OL66554W", "Pride and Prejudice", Some(14348537))]
            })))
            .mount(&server)
            .await;
        // The free text must NOT be searched when the anchor is present.
        Mock::given(method("GET"))
            .and(path("/search.json"))
            .and(query_param("q", "Pride and Prejudice"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/works/OL66554W.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "key": "/works/OL66554W",
                "description": { "type": "/type/text", "value": "A novel of manners." }
            })))
            .mount(&server)
            .await;

        let (p, _dir) = plugin_against(&server).await;
        let out = p
            .handle_query(
                &q("Pride and Prejudice", &[("openlibraryid", "OL66554W"), ("contentKind", "book")]),
                10,
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].fields["description/eng"], "A novel of manners.");
        assert_eq!(out[0].fields["releasedate"], "1813");
        server.verify().await;
    }

    /// A work the search index does not hold still resolves, from the work JSON
    /// and its author records — with no year, rather than an edition's.
    #[tokio::test]
    async fn an_unindexed_work_falls_back_to_the_work_json_and_its_authors() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search.json"))
            .and(query_param("q", "key:/works/OL45804W"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "numFound": 0, "docs": [] })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/works/OL45804W.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "key": "/works/OL45804W",
                "title": "Fantastic Mr Fox",
                "covers": [-1, 6498519],
                "authors": [{ "author": { "key": "/authors/OL34184A" }, "type": { "key": "/type/author_role" } }],
                "first_publish_date": "1988",
                "description": "A clever fox outwits three farmers."
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/authors/OL34184A.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "name": "Roald Dahl" })))
            .mount(&server)
            .await;

        let (p, _dir) = plugin_against(&server).await;
        let out = p
            .handle_query(&q("", &[("openlibraryid", "OL45804W")]), 10)
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        let f = &out[0].fields;
        assert_eq!(f["title"], "Fantastic Mr Fox");
        assert_eq!(f["author"], "Roald Dahl");
        assert_eq!(f["poster_url"], "https://covers.openlibrary.org/b/id/6498519-L.jpg");
        assert_eq!(f["cids/bagdsaaaubnxxazlonruwe4tboj4u6tbugu4dancx"], "true");
        assert!(!f.contains_key("releasedate"), "an edition's date is not the work's");
    }

    /// ⚠ A merged work answers 200 with a redirect body. The card must be minted
    /// once, under the id the work lives under.
    #[tokio::test]
    async fn a_merged_work_is_not_minted_under_its_old_id() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search.json"))
            .and(query_param("q", "mr fox"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "numFound": 2,
                "docs": [
                    doc_json("OL45883W", "Fantastic Mr Fox", Some(1)),
                    doc_json("OL45804W", "Fantastic Mr Fox", Some(6498519))
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/works/OL45883W.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "key": "/works/OL45883W",
                "type": { "key": "/type/redirect" },
                "location": "/works/OL45804W"
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/works/OL45804W.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "key": "/works/OL45804W",
                "description": "A clever fox."
            })))
            .mount(&server)
            .await;

        let (p, _dir) = plugin_against(&server).await;
        let out = p.handle_query(&q("mr fox", &[]), 10).await.unwrap();
        let ids: Vec<&str> = out.iter().map(|r| r.record_id.as_str()).collect();
        assert_eq!(ids, vec!["OL45804W"]);
    }

    #[tokio::test]
    async fn the_popular_row_reads_trending_and_echoes_its_marker() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/trending/daily.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "query": "/trending/daily",
                "works": [doc_json("OL66554W", "Pride and Prejudice", Some(14348537))]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/search.json"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/works/OL66554W.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "key": "/works/OL66554W", "description": "A novel." })))
            .mount(&server)
            .await;

        let (p, _dir) = plugin_against(&server).await;
        let row = q("", &[("domain", "literature"), ("contentKind", "book"), ("popular", "true")]);
        let out = p.handle_query(&row, 20).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].fields["popular"], "true");
        server.verify().await;
    }

    #[tokio::test]
    async fn the_fresh_row_sorts_search_by_new() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search.json"))
            .and(query_param("sort", "new"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "numFound": 1,
                "docs": [doc_json("OL66554W", "Pride and Prejudice", Some(14348537))]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/works/OL66554W.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "key": "/works/OL66554W", "description": "A novel." })))
            .mount(&server)
            .await;

        let (p, _dir) = plugin_against(&server).await;
        let out = p.handle_query(&q("", &[("fresh", "true")]), 20).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].fields["fresh"], "true");
        server.verify().await;
    }

    #[tokio::test]
    async fn a_rate_limited_search_surfaces_as_rate_limited() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search.json"))
            .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "30"))
            .mount(&server)
            .await;
        let (p, _dir) = plugin_against(&server).await;
        let err = p.handle_query(&q("austen", &[]), 5).await.expect_err("429");
        assert!(
            matches!(err, GatewayError::RateLimited { retry_after_s: 30 }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn compute_outcomes_is_a_byteless_card_locator() {
        let p = OpenLibraryPlugin::new();
        let out = p.compute_outcomes("OL66554W").await.unwrap();
        assert_eq!(out.len(), 1);
        assert!(out[0].bytes.is_none(), "a card has no bytes");
        assert!(out[0].file_extension.is_none(), "must be None when bytes is None");
        assert_eq!(out[0].hash_kind, HashKind::CardLocator);
        assert_eq!(out[0].hash.0, "bagdsaaaubnxxazlonruwe4tboj4u6tbwgy2tkncx");
    }

    #[tokio::test]
    async fn compute_outcomes_rejects_a_non_work_id() {
        let p = OpenLibraryPlugin::new();
        assert!(p.compute_outcomes("OL12710531M").await.is_err());
        assert!(p.compute_outcomes("1342").await.is_err());
    }
}
