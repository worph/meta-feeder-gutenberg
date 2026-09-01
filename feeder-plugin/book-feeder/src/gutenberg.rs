//! Project Gutenberg bridge via the [Gutendex](https://gutendex.com) JSON API.
//!
//! - Search: `GET /books?search={q}` → JSON list of book records.
//! - Fetch one: `GET /books/{id}` → single book record with a `formats` map.
//! - Canonical file format for midhash: `application/epub+zip`. Books that
//!   don't expose an epub variant fail `compute_outcomes` with a permanent
//!   error (operator-visible; very rare in practice).
//!
//! No auth, no API key, no rate-limit configuration. Moved verbatim from the
//! gateway crate's `plugins/gutenberg.rs`; only the import paths changed
//! (`crate::*`/`super::common` → `meta_feeder_sdk::*`, `GatewayPlugin` →
//! `FeederPlugin`).

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use async_trait::async_trait;
use meta_feeder_sdk::cache::MidhashCache;
use meta_feeder_sdk::common;
#[cfg(test)]
use meta_feeder_sdk::plugin::HashKind;
use meta_feeder_sdk::plugin::{upstream_id_field, ConfigError, FeederPlugin, HashOutcome};
use meta_feeder_sdk::query::GatewayQuery;
use meta_feeder_sdk::types::{DiscoveryRecord, GatewayError, PluginHealth};
use serde::Deserialize;
use tracing::{debug, warn};

/// Canonical Gutendex base URL. Overridable via [`GutenbergPlugin::with_base_url`]
/// for tests + private mirrors.
const DEFAULT_BASE_URL: &str = "https://gutendex.com";

/// Format we treat as the canonical hashable bytes for a Gutenberg record.
const EPUB_FORMAT: &str = "application/epub+zip";

/// HTTP timeout for each upstream call.
const HTTP_TIMEOUT_SECS: u64 = 30;

/// User-Agent string sent on every Gutendex request.
const USER_AGENT: &str = concat!("meta-share/", env!("CARGO_PKG_VERSION"), " (gateway:gutenberg)");

/// Attempts per Gutendex call before a transport failure is reported as
/// transient.
///
/// ⚠ **A browse row has no way to say "the upstream blinked".** It renders the
/// records it got, and one dropped connection turns "Free & Public Domain" into
/// an empty shelf that reads as *there are no public-domain books* — the exact
/// impression this whole tier exists to prevent. Measured against the live
/// service from this stack, roughly one call in three failed at the transport
/// layer (`error sending request`) while the very next succeeded, so a single
/// attempt is not enough to put a stable row on screen.
///
/// Three, not more: the failures are connection-level and fast, and the caller
/// is a live user-facing fan-out already bounded by a 15 s idle cutoff. This is
/// deliberately NOT a retry of 4xx/5xx or rate-limiting — `map_status` still
/// classifies those, and retrying a 429 would be the wrong thing entirely.
const SEARCH_ATTEMPTS: usize = 3;

/// Backoff between those attempts. Short: a transport blink recovers
/// immediately, and anything longer eats the caller's idle budget.
const RETRY_BACKOFF_MS: u64 = 250;

/// Gutenberg gateway plugin. Cheap to construct; `configure()` opens the
/// per-plugin redb cache.
pub struct GutenbergPlugin {
    http: reqwest::Client,
    base_url: String,
    cache: Option<MidhashCache>,
}

impl GutenbergPlugin {
    pub fn new() -> Self {
        Self::with_base_url(DEFAULT_BASE_URL.to_string())
    }

    /// Construct a plugin pointing at `base_url` instead of the public
    /// Gutendex. Used by tests (against a `wiremock::MockServer`) and by
    /// private-mirror deployments.
    pub fn with_base_url(base_url: String) -> Self {
        let http = common::build_http_client(HTTP_TIMEOUT_SECS, USER_AGENT, None);
        Self {
            http,
            base_url,
            cache: None,
        }
    }

    fn cache(&self) -> Result<&MidhashCache, GatewayError> {
        common::require_cache(self.cache.as_ref(), "gutenberg")
    }

    /// GET `url` (with optional query pairs), retrying a **transport** failure up
    /// to [`SEARCH_ATTEMPTS`] times.
    ///
    /// Only `reqwest::Error` from `send()` is retried — a connection reset, a DNS
    /// blip, a TLS handshake that died. A response that arrives is handed
    /// straight to `map_status`, so 404 stays `NotFound`, 429 stays
    /// `RateLimited`, and a 5xx stays a single transient rather than three
    /// hits on an upstream that is already struggling.
    async fn get_retrying(
        &self,
        url: &str,
        query: &[(&str, &str)],
    ) -> Result<reqwest::Response, GatewayError> {
        let mut last = String::new();
        for attempt in 1..=SEARCH_ATTEMPTS {
            match self.http.get(url).query(query).send().await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    last = e.to_string();
                    warn!(
                        target: "meta-share::gateway",
                        upstream = "gutenberg",
                        url,
                        attempt,
                        of = SEARCH_ATTEMPTS,
                        error = %last,
                        "gutendex transport failure; retrying"
                    );
                    if attempt < SEARCH_ATTEMPTS {
                        tokio::time::sleep(std::time::Duration::from_millis(RETRY_BACKOFF_MS))
                            .await;
                    }
                }
            }
        }
        Err(GatewayError::Transient(format!(
            "GET {url}: {last} (after {SEARCH_ATTEMPTS} attempts)"
        )))
    }

    /// GET `{base}/books/{id}/`; map HTTP/JSON failures to `GatewayError`.
    async fn fetch_book(&self, record_id: &str) -> Result<GutendexBook, GatewayError> {
        let url = format!("{}/books/{}", self.base_url.trim_end_matches('/'), record_id);
        let resp = self.get_retrying(&url, &[]).await?;
        common::map_status(&resp)?;
        resp.json::<GutendexBook>()
            .await
            .map_err(|e| GatewayError::Permanent(format!("decode gutendex book {record_id}: {e}")))
    }

    /// GET arbitrary upstream URL and return the body as bytes.
    async fn fetch_bytes(&self, url: &str) -> Result<bytes::Bytes, GatewayError> {
        common::fetch_bytes(&self.http, url).await
    }

    /// Fetch + parse each result's Project Gutenberg RDF/XML metadata for the
    /// richer per-edition fields the Gutendex JSON API doesn't carry. Cached
    /// aggressively by `record_id`. Failures degrade gracefully.
    async fn fetch_bibrec_fields(&self, books: &[GutendexBook]) -> Vec<BTreeMap<String, String>> {
        use futures::stream::{self, StreamExt};
        const BIBREC_CONCURRENCY: usize = 6;
        let cache = self.cache.clone();
        let http = self.http.clone();
        let ids: Vec<u64> = books.iter().map(|b| b.id).collect();
        stream::iter(ids.into_iter())
            .map(|id| {
                let cache = cache.clone();
                let http = http.clone();
                async move {
                    let record_id = id.to_string();
                    if let Some(cached) = cache
                        .as_ref()
                        .and_then(|c| c.get_bibrec(&record_id).ok().flatten())
                    {
                        return cached;
                    }
                    let url = format!("https://www.gutenberg.org/cache/epub/{id}/pg{id}.rdf");
                    let resp = tokio::time::timeout(
                        std::time::Duration::from_secs(8),
                        http.get(&url).send(),
                    )
                    .await;
                    let xml = match resp {
                        Ok(Ok(r)) if r.status().is_success() => r.text().await.ok(),
                        Ok(Ok(r)) => {
                            warn!(
                                target: "meta-share::gateway",
                                upstream = "gutenberg",
                                id,
                                status = %r.status(),
                                "rdf fetch non-2xx"
                            );
                            None
                        }
                        Ok(Err(e)) => {
                            warn!(
                                target: "meta-share::gateway",
                                upstream = "gutenberg",
                                id,
                                error = %e,
                                "rdf fetch failed"
                            );
                            None
                        }
                        Err(_) => {
                            warn!(
                                target: "meta-share::gateway",
                                upstream = "gutenberg",
                                id,
                                "rdf fetch timed out"
                            );
                            None
                        }
                    };
                    let Some(xml) = xml else {
                        return BTreeMap::new();
                    };
                    let fields = parse_pgterms_rdf(&xml);
                    if !fields.is_empty() {
                        if let Some(cache) = cache.as_ref() {
                            if let Err(e) = cache.put_bibrec(&record_id, &fields) {
                                warn!(
                                    target: "meta-share::gateway",
                                    upstream = "gutenberg",
                                    record_id = %record_id,
                                    error = %e,
                                    "bibrec put failed (non-fatal)"
                                );
                            }
                        }
                    }
                    fields
                }
            })
            .buffered(BIBREC_CONCURRENCY)
            .collect()
            .await
    }

    /// Download every result's cover image (where present), hash, and cache.
    /// Returns a `Vec<Option<String>>` of cover CIDs aligned with the input
    /// slice — `None` means no cover or download failure.
    async fn fetch_cover_cids(&self, books: &[GutendexBook]) -> Vec<Option<String>> {
        use futures::stream::{self, StreamExt};
        const COVER_CONCURRENCY: usize = 8;
        let cache = self.cache.clone();
        let plugin_http = self.http.clone();
        let to_process: Vec<(usize, String, Option<String>, Option<String>)> = books
            .iter()
            .enumerate()
            .map(|(idx, b)| {
                let record_id = b.id.to_string();
                let cover_url = b.formats.get("image/jpeg").cloned();
                let cached_cid = cache
                    .as_ref()
                    .and_then(|c| c.get_cover_cid(&record_id).ok().flatten());
                (idx, record_id, cover_url, cached_cid)
            })
            .collect();
        stream::iter(to_process.into_iter())
            .map(|(idx, record_id, cover_url, cached_cid)| {
                let cache = cache.clone();
                let http = plugin_http.clone();
                async move {
                    if let Some(cid) = cached_cid {
                        return Some(cid);
                    }
                    let url = cover_url?;
                    let bytes_res = tokio::time::timeout(
                        std::time::Duration::from_secs(8),
                        http.get(&url).send(),
                    )
                    .await;
                    let resp = match bytes_res {
                        Ok(Ok(r)) if r.status().is_success() => r,
                        Ok(Ok(r)) => {
                            warn!(
                                target: "meta-share::gateway",
                                upstream = "gutenberg",
                                book_id = idx,
                                status = %r.status(),
                                "cover fetch non-2xx"
                            );
                            return None;
                        }
                        Ok(Err(e)) => {
                            warn!(
                                target: "meta-share::gateway",
                                upstream = "gutenberg",
                                book_id = idx,
                                error = %e,
                                "cover fetch failed"
                            );
                            return None;
                        }
                        Err(_) => {
                            warn!(
                                target: "meta-share::gateway",
                                upstream = "gutenberg",
                                book_id = idx,
                                "cover fetch timed out"
                            );
                            return None;
                        }
                    };
                    let bytes = match resp.bytes().await {
                        Ok(b) => b,
                        Err(e) => {
                            warn!(
                                target: "meta-share::gateway",
                                upstream = "gutenberg",
                                book_id = idx,
                                error = %e,
                                "cover body read failed"
                            );
                            return None;
                        }
                    };
                    let cid = meta_feeder_sdk::hash::compute_ipfs_cid(&bytes);
                    if let Some(cache) = cache.as_ref() {
                        if let Err(e) = cache.put_blob(&cid, &bytes) {
                            warn!(
                                target: "meta-share::gateway",
                                upstream = "gutenberg",
                                cid = %cid,
                                error = %e,
                                "cover put_blob failed (non-fatal)"
                            );
                        }
                        if let Err(e) = cache.put_cover_cid(&record_id, &cid) {
                            warn!(
                                target: "meta-share::gateway",
                                upstream = "gutenberg",
                                record_id = %record_id,
                                error = %e,
                                "cover put_cover_cid failed (non-fatal)"
                            );
                        }
                    }
                    Some(cid)
                }
            })
            .buffered(COVER_CONCURRENCY)
            .collect()
            .await
    }
}

impl Default for GutenbergPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl FeederPlugin for GutenbergPlugin {
    fn upstream_id(&self) -> &'static str {
        "gutenberg"
    }

    fn configure(&mut self, cache_dir: &Path) -> Result<(), ConfigError> {
        self.cache = Some(common::open_midhash_cache(cache_dir, "gutenberg")?);
        Ok(())
    }

    async fn handle_query(
        &self,
        query: &GatewayQuery,
        max_results: usize,
    ) -> Result<Vec<DiscoveryRecord>, GatewayError> {
        // Layer A early-return: Gutenberg only serves `document` / `book`.
        if !meta_feeder_sdk::query_eval::query_accepts_plugin(
            query,
            self.served_file_types(),
            self.served_content_kinds(),
        ) {
            return Ok(Vec::new());
        }
        // ⚠ **The trailing slash is not cosmetic.** Gutendex 301s `/books` to
        // `/books/`, so every single search paid for two round trips and an extra
        // connection that could fail on its own. `/books/{id}` above already used
        // the canonical form; the search path did not.
        let url = format!("{}/books/", self.base_url.trim_end_matches('/'));

        // ⚠ **A browse query must NOT send `search=*`.**
        //
        // `free_text_or_star()` exists for upstreams where `*` means "everything".
        // Gutendex is not one of them: it treats the asterisk as a **literal**,
        // so `?search=*` returns the four books with a `*` in their title —
        // "The Seri Indians. (1898 N 17 / 1895-1896 (pages 1-344*))" and friends.
        // That is what the "Free & Public Domain" row had been asking for, which
        // is why a shelf backed by 79,296 public-domain books rendered as either
        // empty or three pieces of 19th-century ethnography.
        //
        // Omitting the parameter is the catalogue browse: `/books/` returns
        // everything, already ordered by download count (identical to
        // `?sort=popular`), so the row leads with Pride and Prejudice rather than
        // with whatever happens to contain punctuation.
        let q = query.free_text.trim();
        let params: Vec<(&str, &str)> = if q.is_empty() {
            Vec::new()
        } else {
            vec![("search", q)]
        };
        let resp = self.get_retrying(&url, &params).await?;
        common::map_status(&resp)?;
        let body: GutendexSearch = resp.json().await.map_err(|e| {
            GatewayError::Permanent(format!("decode gutendex search response: {e}"))
        })?;
        debug!(
            target: "meta-share::gateway",
            upstream = "gutenberg",
            query = if q.is_empty() { "<catalogue browse>" } else { q },
            count = body.results.len(),
            "search returned"
        );
        let raw: Vec<GutendexBook> = body.results.into_iter().take(max_results).collect();
        let (cover_cids, bibrecs) =
            tokio::join!(self.fetch_cover_cids(&raw), self.fetch_bibrec_fields(&raw),);
        Ok(raw
            .into_iter()
            .zip(cover_cids)
            .zip(bibrecs)
            .map(|((book, cover_cid), bibrec)| into_discovery_record(book, cover_cid, bibrec))
            .collect())
    }

    async fn compute_outcomes(&self, record_id: &str) -> Result<Vec<HashOutcome>, GatewayError> {
        let cache = self.cache()?;
        if let Some(hit) = common::cached_outcome(cache, record_id, "gutenberg")? {
            return Ok(hit);
        }

        let book = self.fetch_book(record_id).await?;
        let epub_url = book.formats.get(EPUB_FORMAT).cloned().ok_or_else(|| {
            GatewayError::Permanent(format!(
                "gutenberg record {record_id} has no {EPUB_FORMAT} format"
            ))
        })?;
        let bytes = self.fetch_bytes(&epub_url).await?;
        let cid = meta_feeder_sdk::hash::compute_ipfs_cid(&bytes);

        common::store_midhash(cache, record_id, "gutenberg", &cid);

        let cover_cid = cache.get_cover_cid(record_id).ok().flatten();
        let bibrec = cache
            .get_bibrec(record_id)
            .ok()
            .flatten()
            .unwrap_or_default();
        let record = into_discovery_record(book, cover_cid, bibrec);

        Ok(common::single_outcome(
            cid,
            bytes,
            record,
            Some("epub".to_string()),
        ))
    }

    async fn get_blob(&self, cid: &str) -> Option<Vec<u8>> {
        match self.cache.as_ref()?.get_blob(cid) {
            Ok(opt) => opt,
            Err(e) => {
                warn!(
                    target: "meta-share::gateway",
                    upstream = "gutenberg",
                    cid,
                    error = %e,
                    "get_blob: cache read failed"
                );
                None
            }
        }
    }

    fn health(&self) -> PluginHealth {
        if self.cache.is_some() {
            PluginHealth::Ok
        } else {
            PluginHealth::Degraded {
                reason: "configure() not yet called".to_string(),
            }
        }
    }

    fn served_file_types(&self) -> &'static [&'static str] {
        &["document"]
    }

    fn served_content_kinds(&self) -> &'static [&'static str] {
        &["book"]
    }
}

/// Convert a `GutendexBook` into our wire-level `DiscoveryRecord`.
fn into_discovery_record(
    book: GutendexBook,
    cover_cid: Option<String>,
    bibrec: BTreeMap<String, String>,
) -> DiscoveryRecord {
    let GutendexBook {
        id,
        title,
        authors,
        subjects,
        bookshelves,
        languages,
        copyright,
        media_type,
        formats,
        download_count,
    } = book;

    let record_id = id.to_string();
    let mut fields = BTreeMap::new();
    fields.insert("title".into(), title);
    fields.insert("fileType".into(), "document".into());
    fields.insert("contentKind".into(), "book".into());
    // A book's identity graph is Gutenberg/ISBN → `literature`
    // (METADATA_KEYS.md §1), and a book is a work complete in itself →
    // `standalone`. Written with the kind, never derived on read.
    fields.insert("domain".into(), "literature".into());
    fields.insert("workForm".into(), "standalone".into());
    fields.insert(
        "sourceUrl".into(),
        format!("https://www.gutenberg.org/ebooks/{id}"),
    );
    // Canonical `<upstream_id>id` field.
    fields.insert(upstream_id_field("gutenberg"), id.to_string());

    if !authors.is_empty() {
        fields.insert(
            "author".into(),
            authors
                .iter()
                .map(|a| a.name.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    if let Some(lang) = languages.first() {
        fields.insert("language".into(), iso639_1_to_2(lang).to_string());
    }
    if !subjects.is_empty() {
        fields.insert("subjects".into(), subjects.join(", "));
    }
    if !bookshelves.is_empty() {
        fields.insert("bookshelves".into(), bookshelves.join(", "));
    }
    if let Some(mt) = media_type {
        fields.insert("mediaType".into(), mt);
    }
    // ⚠ Deliberately NOT stamping a self-computed `poster`.
    //
    // This feeder hashes the cover itself and used to stamp the result. Two
    // things were wrong with that. The bytes only ever reached its own redb blob
    // cache, so the cid named data no peer on the mesh held — and the cid it
    // produced was malformed anyway: 50 characters where a raw sha2-256 CIDv1 is
    // 59, so `/api/image/<cid>` 504'd even once a real seed existed, because the
    // stale field won over the gateway's rewrite.
    //
    // `poster_url` below is the whole contract: the gateway's SEEDABLE_FIELDS
    // path fetches it, hashes the bytes, seeds them into the blockstore and
    // writes the resulting content cid back as `poster`.
    let _ = cover_cid;
    // ⚠ `poster` alone is a CID with no bytes behind it anywhere on the mesh.
    //
    // The cover is fetched and hashed here, but the bytes only ever land in this
    // feeder's own redb blob cache (served at `/blob/gutenberg/<cid>`). Nothing
    // pulls them from there into a blockstore, so every client asks for a CID no
    // peer holds and every cover renders blank — measured: `/api/image/<cid>`
    // → 504, and `.../raw` → 404 on both the client and the gateway peer.
    //
    // The gateway's seed path is keyed on the `*_url` convention:
    // `SEEDABLE_FIELDS` (meta-gateway `remote_feeder.rs:177`) maps
    // `poster_url` → fetch → hash → seed into the blockstore → rewrite the
    // record's `poster` to the resulting content cid. Emitting the upstream URL
    // is what enrols this feeder in that path; the `poster` above then gets
    // overwritten with an identical, and crucially *backed*, cid.
    if let Some(url) = formats.get("image/jpeg") {
        fields.insert("poster_url".into(), url.clone());
    }
    if let Some(epub_url) = formats.get(EPUB_FORMAT) {
        fields.insert("format".into(), "epub".into());
        // ⚠ Without a `cids/` member this record is SILENTLY DISCARDED.
        //
        // The gateway keys its store-back on `url_key_for` (meta-gateway
        // `dispatch.rs`) and skips any record that has none — while the
        // search-coverage gate still marks (gutenberg, query) covered for an
        // hour. Together that produced the tier's long-standing behaviour: the
        // first search answers from the live fan-out, nothing is ever
        // persisted, and every repeat inside the window is served from an empty
        // meta-core. Measured: the gateway's meta-core held zero /file records
        // ("meta-core poll: nothing new, scanned=0") after hundreds of results,
        // which also meant no poster was ever seeded and every cover was blank.
        //
        // A `0x1006` url-locator over the download URL is the fleet convention —
        // meta-feeder-internetarchive and -jamendo both stamp
        // `cids/<compute_url_cid(url)>`. It costs no I/O (the cid is a pure
        // function of the URL string) and doubles as the byte path: meta-share
        // redeems the locator on demand.
        //
        // ⚠ Exactly ONE `cids/` member. `url_key_for` takes the lexicographic
        // `.min()`, so a second cid would make the store key arbitrary.
        if let Some(cid) = meta_feeder_sdk::hash::compute_url_cid(epub_url) {
            fields.insert(format!("cids/{cid}"), "true".into());
        }
    }
    if let Some(c) = copyright {
        fields.insert("publicDomain".into(), (!c).to_string());
        // ⚠ `licence` is **required on every record from a free-tier upstream**
        // (METADATA_KEYS `licence`), and Gutenberg is the archetypal one. Omitting
        // it was not a cosmetic gap: consumers filter the free tier ON this field
        // — meta-read's "Free & Public Domain" row asks for `licence:pd` — so a
        // catalogue of 79,296 public-domain books was invisible to the one row
        // built to show it, while its own seeded fixture (which does carry the
        // field) sat there looking like the whole corpus.
        //
        // `PublicDomain` is the registry's own spelling. `publicDomain` above
        // stays: it is the upstream's raw boolean, and the registry is explicit
        // that absence of `licence` means *unknown*, never *unrestricted* — so a
        // book Gutendex reports as still in copyright gets neither field
        // asserting freedom.
        if !c {
            fields.insert("licence".into(), "PublicDomain".into());
        }
    }
    let _ = download_count;
    fields.insert("fileName".into(), format!("gutenberg-{id}.epub"));

    for (k, v) in bibrec {
        fields.entry(k).or_insert(v);
    }

    DiscoveryRecord {
        upstream_id: "gutenberg".into(),
        record_id,
        fields,
    }
}

/// Parse a Project Gutenberg `pg{id}.rdf` document for per-edition metadata.
fn parse_pgterms_rdf(xml: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();

    let pull = |tag: &str| extract_first_tag_text(xml, tag);

    if let Some(v) = pull("dcterms:issued") {
        out.insert("releaseDate".into(), v);
    }
    if let Some(v) = pull("dcterms:rights") {
        out.insert("copyrightStatus".into(), v);
    }
    if let Some(v) = pull("dcterms:alternative") {
        out.insert("alternateTitle".into(), v);
    }
    if let Some(v) = pull("dcterms:description") {
        if let Some(url) = v
            .split_whitespace()
            .find(|t| t.contains("wikipedia.org") && t.starts_with("http"))
        {
            out.insert("wikipediaUrl".into(), url.to_string());
        }
        out.insert("note".into(), v);
    }
    if let Some(v) = pull("pgterms:marc508") {
        out.insert("credits".into(), v);
    }
    if let Some(v) = pull("pgterms:marc520") {
        out.insert("summary".into(), v);
    }
    if let Some(v) = pull("pgterms:marc908") {
        out.insert("readingLevel".into(), v);
    }
    if let Some(url) = extract_resource_attr(xml, "pgterms:webpage") {
        out.insert("authorWebpage".into(), url);
    }

    out
}

/// Find the first `<tag ...>BODY</tag>` and return BODY trimmed.
fn extract_first_tag_text(xml: &str, tag: &str) -> Option<String> {
    let open_prefix = format!("<{tag}");
    let close = format!("</{tag}>");
    let open_start = xml.find(&open_prefix)?;
    let after_attrs = open_start + open_prefix.len();
    let rest = &xml[after_attrs..];
    let body_start = after_attrs + rest.find('>')? + 1;
    let body_end_rel = xml[body_start..].find(&close)?;
    let body = &xml[body_start..body_start + body_end_rel];
    let decoded = body
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'");
    let trimmed = decoded.trim().to_string();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

/// Find the first `<tag ... rdf:resource="URL"...` and return URL.
fn extract_resource_attr(xml: &str, tag: &str) -> Option<String> {
    let open_prefix = format!("<{tag} ");
    let open_start = xml.find(&open_prefix)?;
    let rest = &xml[open_start + open_prefix.len()..];
    let tag_end = rest.find('>')?;
    let inside = &rest[..tag_end];
    let needle = "rdf:resource=\"";
    let attr_start = inside.find(needle)?;
    let after = &inside[attr_start + needle.len()..];
    let url_end = after.find('"')?;
    Some(after[..url_end].to_string())
}

#[cfg(test)]
mod rdf_parser_tests {
    use super::*;

    #[test]
    fn extracts_release_date_and_copyright() {
        let xml = r#"<rdf:RDF>
            <pgterms:ebook rdf:about="ebooks/11">
              <dcterms:issued rdf:datatype="...">2008-06-27</dcterms:issued>
              <dcterms:rights>Public domain in the USA.</dcterms:rights>
            </pgterms:ebook>
          </rdf:RDF>"#;
        let f = parse_pgterms_rdf(xml);
        assert_eq!(f.get("releaseDate").map(String::as_str), Some("2008-06-27"));
        assert_eq!(
            f.get("copyrightStatus").map(String::as_str),
            Some("Public domain in the USA.")
        );
    }

    #[test]
    fn extracts_credits_summary_reading_level() {
        let xml = r#"<rdf:RDF>
            <pgterms:marc508>Arthur DiBianca and David Widger</pgterms:marc508>
            <pgterms:marc520>"Alice's..." short summary.</pgterms:marc520>
            <pgterms:marc908>Reading ease score: 87.5 (6th grade). Easy to read.</pgterms:marc908>
          </rdf:RDF>"#;
        let f = parse_pgterms_rdf(xml);
        assert_eq!(
            f.get("credits").map(String::as_str),
            Some("Arthur DiBianca and David Widger")
        );
        assert!(f.get("summary").unwrap().contains("short summary"));
        assert!(f.get("readingLevel").unwrap().contains("87.5"));
    }

    #[test]
    fn pulls_wikipedia_url_out_of_description() {
        let xml = r#"<rdf:RDF>
            <dcterms:description>Wikipedia page about this book: https://en.wikipedia.org/wiki/Alice%27s_Adventures_in_Wonderland</dcterms:description>
          </rdf:RDF>"#;
        let f = parse_pgterms_rdf(xml);
        assert_eq!(
            f.get("wikipediaUrl").map(String::as_str),
            Some("https://en.wikipedia.org/wiki/Alice%27s_Adventures_in_Wonderland")
        );
        assert!(f.get("note").unwrap().contains("Wikipedia"));
    }

    #[test]
    fn empty_when_not_an_rdf_doc() {
        let f = parse_pgterms_rdf("<html><body>not rdf</body></html>");
        assert!(f.is_empty());
    }
}

/// ISO 639-1 (two-letter) → ISO 639-2/T (three-letter) for the ~20 most common
/// Gutenberg languages. Unknown codes pass through unchanged.
fn iso639_1_to_2(code: &str) -> &str {
    match code {
        "en" => "eng",
        "fr" => "fra",
        "de" => "deu",
        "es" => "spa",
        "it" => "ita",
        "pt" => "por",
        "nl" => "nld",
        "la" => "lat",
        "el" => "ell",
        "ru" => "rus",
        "fi" => "fin",
        "sv" => "swe",
        "da" => "dan",
        "no" => "nor",
        "pl" => "pol",
        "cs" => "ces",
        "hu" => "hun",
        "ja" => "jpn",
        "zh" => "zho",
        "ar" => "ara",
        "ko" => "kor",
        "tr" => "tur",
        "ro" => "ron",
        "ca" => "cat",
        "is" => "isl",
        other => other,
    }
}

// -- Gutendex JSON shapes (subset). We only deserialise the fields we use.

#[derive(Debug, Deserialize)]
struct GutendexSearch {
    results: Vec<GutendexBook>,
}

#[derive(Debug, Deserialize)]
struct GutendexBook {
    id: u64,
    title: String,
    #[serde(default)]
    authors: Vec<GutendexPerson>,
    #[serde(default)]
    subjects: Vec<String>,
    #[serde(default)]
    bookshelves: Vec<String>,
    #[serde(default)]
    languages: Vec<String>,
    #[serde(default)]
    copyright: Option<bool>,
    #[serde(default)]
    media_type: Option<String>,
    #[serde(default)]
    formats: HashMap<String, String>,
    #[serde(default)]
    download_count: u64,
}

#[derive(Debug, Deserialize)]
struct GutendexPerson {
    name: String,
    // Gutendex also returns `birth_year` / `death_year`, but those are about
    // the author, not this book file, so we don't deserialize or surface them
    // (serde ignores the unknown JSON keys). See `into_discovery_record`.
}

#[cfg(test)]
mod tests {
    use super::*;
    use meta_feeder_sdk::types::PluginHealth;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn books_json() -> serde_json::Value {
        serde_json::json!({
            "count": 2,
            "next": null,
            "previous": null,
            "results": [
                {
                    "id": 1342,
                    "title": "Pride and Prejudice",
                    "authors": [{"name": "Austen, Jane", "birth_year": 1775, "death_year": 1817}],
                    "languages": ["en"],
                    // ⚠ The live API returns this on every result and the
                    // fixture omitted it, so `publicDomain` / `licence` were
                    // silently untested — the exact fields the free-tier rows
                    // filter on.
                    "copyright": false,
                    "formats": {
                        "application/epub+zip": "EPUB_URL_PLACEHOLDER",
                        "text/plain; charset=utf-8": "https://www.gutenberg.org/files/1342/1342-0.txt"
                    }
                },
                {
                    "id": 84,
                    "title": "Frankenstein; Or, The Modern Prometheus",
                    "authors": [{"name": "Shelley, Mary Wollstonecraft", "birth_year": 1797, "death_year": 1851}],
                    "languages": ["en"],
                    "copyright": false,
                    "formats": {
                        "application/epub+zip": "EPUB_URL_PLACEHOLDER",
                        "text/plain; charset=utf-8": "https://www.gutenberg.org/files/84/84-0.txt"
                    }
                }
            ]
        })
    }

    fn configured_plugin_against(server: &MockServer) -> (GutenbergPlugin, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut plugin = GutenbergPlugin::with_base_url(server.uri());
        plugin.configure(dir.path()).expect("configure");
        (plugin, dir)
    }

    /// ⚠ The search path must ask for the CANONICAL `/books/`.
    ///
    /// Gutendex 301s `/books` → `/books/`. Following a redirect is free in
    /// correctness terms and invisible in a test that mounts both, which is
    /// exactly why this asserts the path the client actually requests: two round
    /// trips per search, on a live user-facing fan-out, is a cost nobody would
    /// choose deliberately.
    #[tokio::test]
    async fn search_requests_the_canonical_slashed_path() {
        let server = MockServer::start().await;
        // ONLY the slashed form is mounted; the bare form 404s the way a
        // redirect-less client would notice.
        Mock::given(method("GET"))
            .and(path("/books/"))
            .and(query_param("search", "alice"))
            .respond_with(ResponseTemplate::new(200).set_body_json(books_json()))
            .mount(&server)
            .await;

        let (plugin, _dir) = configured_plugin_against(&server);
        let records = plugin
            .handle_query(&GatewayQuery::from_free_text("alice"), 50)
            .await
            .expect("handle_query against the slashed path");
        assert_eq!(records.len(), 2);
    }

    /// ⚠ Every public-domain record must carry `licence`.
    ///
    /// METADATA_KEYS makes it **required on every record from a free-tier
    /// upstream**, and consumers filter the free tier on it. Without it a
    /// public-domain catalogue is invisible to the very rows built to show it.
    #[tokio::test]
    async fn a_public_domain_book_carries_the_licence_field() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/books/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(books_json()))
            .mount(&server)
            .await;

        let (plugin, _dir) = configured_plugin_against(&server);
        let records = plugin
            .handle_query(&GatewayQuery::from_free_text("alice"), 50)
            .await
            .expect("handle_query");
        let f = &records[0].fields;
        // The registry's own spelling, not a private shorthand.
        assert_eq!(f.get("licence").map(String::as_str), Some("PublicDomain"));
        // The raw upstream boolean stays alongside it.
        assert_eq!(f.get("publicDomain").map(String::as_str), Some("true"));
    }

    /// ⚠ A browse query must not ask Gutendex for a literal asterisk.
    ///
    /// `free_text_or_star()` is right for upstreams where `*` means "everything".
    /// Gutendex matches it literally, so `?search=*` returns the four books with
    /// an asterisk in the title — which is what the "Free & Public Domain" row
    /// was rendering instead of a public-domain catalogue.
    #[tokio::test]
    async fn a_filters_only_query_browses_the_catalogue_instead_of_searching_for_a_star() {
        let server = MockServer::start().await;
        // ⚠ `query_param_is_missing` is the assertion. Mounting a catch-all and
        // checking the records would pass just as happily against `search=*`.
        Mock::given(method("GET"))
            .and(path("/books/"))
            .and(wiremock::matchers::query_param_is_missing("search"))
            .respond_with(ResponseTemplate::new(200).set_body_json(books_json()))
            .mount(&server)
            .await;

        let (plugin, _dir) = configured_plugin_against(&server);
        let browse = GatewayQuery {
            raw_text: "domain:literature contentKind:book licence:pd".to_string(),
            free_text: String::new(),
            filters: [("contentKind".to_string(), vec!["book".to_string()])]
                .into_iter()
                .collect(),
            ranges: Vec::new(),
            negations: Vec::new(),
        };
        let records = plugin.handle_query(&browse, 50).await.expect("browse");
        assert_eq!(records.len(), 2, "the catalogue browse must return records");
    }

    /// Whitespace-only free text is a browse, not a search for spaces.
    #[tokio::test]
    async fn blank_free_text_is_treated_as_a_browse() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/books/"))
            .and(wiremock::matchers::query_param_is_missing("search"))
            .respond_with(ResponseTemplate::new(200).set_body_json(books_json()))
            .mount(&server)
            .await;

        let (plugin, _dir) = configured_plugin_against(&server);
        let mut q = GatewayQuery::from_free_text("   ");
        q.filters
            .insert("contentKind".to_string(), vec!["book".to_string()]);
        assert_eq!(plugin.handle_query(&q, 50).await.expect("browse").len(), 2);
    }

    /// ⚠ One dropped connection must not empty a browse row.
    ///
    /// Measured against the live service, roughly one call in three failed at the
    /// transport layer while the next succeeded. Without a retry that renders as
    /// "there are no public-domain books", with nothing in the UI to say
    /// otherwise — a silent, wrong, and very believable empty shelf.
    #[tokio::test]
    async fn a_transport_failure_is_retried_rather_than_emptying_the_row() {
        // wiremock cannot sever a connection, so the failure is produced the one
        // way a client sees as a transport error rather than a status: the first
        // call goes to a dead port, the retry to the live server. Driving that
        // through `get_retrying` directly keeps the assertion on the retry
        // itself.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/books/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(books_json()))
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);

        // A port nothing is listening on: `send()` fails before any response.
        let dead = "http://127.0.0.1:1/books/";
        let err = plugin
            .get_retrying(dead, &[])
            .await
            .expect_err("a dead port cannot succeed");
        match err {
            GatewayError::Transient(m) => assert!(
                m.contains(&format!("after {SEARCH_ATTEMPTS} attempts")),
                "a transport failure must report as transient AFTER exhausting the \
                 budget, so the caller can tell a blink from an outage; got: {m}"
            ),
            other => panic!("expected Transient, got {other:?}"),
        }

        // And a reachable server still answers on the first attempt.
        let resp = plugin
            .get_retrying(&format!("{}/books/", server.uri()), &[])
            .await
            .expect("live server answers");
        assert!(resp.status().is_success());
    }

    /// ⚠ A 404 must NOT be retried three times.
    ///
    /// The retry exists for connections that never delivered a response. A
    /// response that arrives — 404, 429, 5xx — is `map_status`'s business, and
    /// retrying a rate-limit is actively harmful to the upstream that is already
    /// throttling us.
    #[tokio::test]
    async fn a_status_response_is_not_retried() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/books/404404"))
            .respond_with(ResponseTemplate::new(404))
            // `expect(1)` is the assertion: wiremock panics on drop if the
            // endpoint was hit more than once.
            .expect(1)
            .mount(&server)
            .await;

        let (plugin, _dir) = configured_plugin_against(&server);
        let err = plugin.fetch_book("404404").await.expect_err("404");
        assert!(matches!(err, GatewayError::NotFound), "got {err:?}");
    }

    #[tokio::test]
    async fn handle_query_maps_results_to_discovery_records() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/books/"))
            .and(query_param("search", "alice"))
            .respond_with(ResponseTemplate::new(200).set_body_json(books_json()))
            .mount(&server)
            .await;

        let (plugin, _dir) = configured_plugin_against(&server);
        let records = plugin
            .handle_query(&GatewayQuery::from_free_text("alice"), 50)
            .await
            .expect("handle_query");
        assert_eq!(records.len(), 2);

        let r0 = &records[0];
        assert_eq!(r0.upstream_id, "gutenberg");
        assert_eq!(r0.record_id, "1342");
        assert_eq!(
            r0.fields.get("title").map(String::as_str),
            Some("Pride and Prejudice")
        );
        assert_eq!(r0.fields.get("fileType").map(String::as_str), Some("document"));
        assert_eq!(r0.fields.get("contentKind").map(String::as_str), Some("book"));
        assert_eq!(r0.fields.get("gutenbergid").map(String::as_str), Some("1342"));
        assert_eq!(
            r0.fields.get("sourceUrl").map(String::as_str),
            Some("https://www.gutenberg.org/ebooks/1342")
        );
        assert_eq!(r0.fields.get("author").map(String::as_str), Some("Austen, Jane"));
        assert_eq!(r0.fields.get("language").map(String::as_str), Some("eng"));
        assert_eq!(r0.fields.get("format").map(String::as_str), Some("epub"));
        assert_eq!(
            r0.fields.get("fileName").map(String::as_str),
            Some("gutenberg-1342.epub")
        );
    }

    #[tokio::test]
    async fn handle_query_truncates_to_max_results() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/books/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(books_json()))
            .mount(&server)
            .await;

        let (plugin, _dir) = configured_plugin_against(&server);
        let records = plugin
            .handle_query(&GatewayQuery::from_free_text("alice"), 1)
            .await
            .expect("ok");
        assert_eq!(records.len(), 1);
    }

    #[tokio::test]
    async fn compute_outcomes_cache_hit_skips_http() {
        let server = MockServer::start().await;
        let (plugin, _dir) = configured_plugin_against(&server);
        plugin
            .cache
            .as_ref()
            .unwrap()
            .put_midhash("1342", "bafyCACHED")
            .unwrap();

        let outcomes = plugin.compute_outcomes("1342").await.expect("cache hit");
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].hash.as_str(), "bafyCACHED");
    }

    #[tokio::test]
    async fn compute_outcomes_fetches_epub_and_caches() {
        let server = MockServer::start().await;

        let epub_bytes: Vec<u8> = b"<<fake epub bytes for test>>".to_vec();
        let expected_cid = meta_feeder_sdk::hash::compute_ipfs_cid(&epub_bytes);

        let epub_path = "/ebooks/1342.epub.images";
        let book = serde_json::json!({
            "id": 1342,
            "title": "Pride and Prejudice",
            "authors": [{"name": "Austen, Jane"}],
            "languages": ["en"],
            "formats": {
                "application/epub+zip": format!("{}{epub_path}", server.uri()),
            }
        });
        Mock::given(method("GET"))
            .and(path("/books/1342"))
            .respond_with(ResponseTemplate::new(200).set_body_json(book))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(epub_path))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(epub_bytes.clone())
                    .insert_header("content-type", "application/epub+zip"),
            )
            .mount(&server)
            .await;

        let (plugin, _dir) = configured_plugin_against(&server);
        let outcomes = plugin.compute_outcomes("1342").await.expect("compute");
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].hash.as_str(), expected_cid);
        assert_eq!(outcomes[0].hash_kind, HashKind::Sha2_256);
        assert_eq!(
            plugin
                .cache
                .as_ref()
                .unwrap()
                .get_midhash("1342")
                .unwrap()
                .as_deref(),
            Some(expected_cid.as_str())
        );
    }

    #[tokio::test]
    async fn compute_outcomes_missing_epub_returns_permanent() {
        let server = MockServer::start().await;
        let book = serde_json::json!({
            "id": 99999,
            "title": "Audio-only Record",
            "authors": [],
            "languages": ["en"],
            "formats": {
                "text/plain; charset=utf-8": format!("{}/files/99999/99999-0.txt", server.uri()),
            }
        });
        Mock::given(method("GET"))
            .and(path("/books/99999"))
            .respond_with(ResponseTemplate::new(200).set_body_json(book))
            .mount(&server)
            .await;

        let (plugin, _dir) = configured_plugin_against(&server);
        let err = plugin
            .compute_outcomes("99999")
            .await
            .expect_err("should error");
        match err {
            GatewayError::Permanent(msg) => {
                assert!(msg.contains("epub"), "msg: {msg}");
                assert!(msg.contains("99999"), "msg: {msg}");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn upstream_404_maps_to_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/books/1"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let (plugin, _dir) = configured_plugin_against(&server);
        let err = plugin.compute_outcomes("1").await.expect_err("404");
        assert!(matches!(err, GatewayError::NotFound));
    }

    #[tokio::test]
    async fn upstream_5xx_maps_to_transient() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/books/"))
            .respond_with(ResponseTemplate::new(502))
            .mount(&server)
            .await;

        let (plugin, _dir) = configured_plugin_against(&server);
        let err = plugin
            .handle_query(&GatewayQuery::from_free_text("x"), 10)
            .await
            .expect_err("502");
        assert!(matches!(err, GatewayError::Transient(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn upstream_429_maps_to_rate_limited_with_retry_after() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/books/"))
            .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "90"))
            .mount(&server)
            .await;

        let (plugin, _dir) = configured_plugin_against(&server);
        let err = plugin
            .handle_query(&GatewayQuery::from_free_text("x"), 10)
            .await
            .expect_err("429");
        match err {
            GatewayError::RateLimited { retry_after_s } => {
                assert_eq!(retry_after_s, 90);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn health_reflects_configure_state() {
        let plugin = GutenbergPlugin::new();
        match plugin.health() {
            PluginHealth::Degraded { reason } => assert!(reason.contains("configure")),
            PluginHealth::Ok => panic!("unconfigured plugin should not be Ok"),
        }

        let dir = tempfile::tempdir().unwrap();
        let mut plugin = GutenbergPlugin::new();
        plugin.configure(dir.path()).unwrap();
        match plugin.health() {
            PluginHealth::Ok => {}
            PluginHealth::Degraded { reason } => {
                panic!("configured plugin should be Ok, got Degraded: {reason}")
            }
        }
    }
}
