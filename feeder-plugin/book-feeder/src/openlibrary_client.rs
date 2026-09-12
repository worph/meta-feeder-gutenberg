//! The Open Library HTTP client: rate budget, redb read-through cache, and the
//! calls the book tier makes.
//!
//! Shared by both upstreams this binary hosts:
//! - the `openlibrary` card plugin — search, trending, "new", work lookups;
//! - the `gutenberg` plugin — the `id_project_gutenberg` binding lookup
//!   (reading-model §2.4), which is what lets an edition join its work.
//!
//! Plain JSON over `reqwest`, like every feeder in the fleet. Shapes confirmed
//! against the live API on 2026-09-11: `search.json` answers `{numFound, docs}`,
//! `/trending/daily.json` answers `{works}` with the same doc shape (and honours
//! `fields=`), and a work JSON's `description` is either a bare string or a
//! `{type: "/type/text", value}` object — both occur in the catalogue.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use meta_feeder_sdk::budget::{Lease, RateBudget};
use meta_feeder_sdk::cache::MidhashCache;
use meta_feeder_sdk::config::ConfigField;
use meta_feeder_sdk::types::GatewayError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, warn};

/// Canonical Open Library base URL. Overridable for tests (wiremock).
pub const DEFAULT_API_BASE: &str = "https://openlibrary.org";

/// Cover image base. `-L` is the large rendition; the gateway seeds whatever
/// `poster_url` names, so one size is published, not three.
const COVERS_BASE: &str = "https://covers.openlibrary.org/b/id";

/// Config key (and env seed) for the operator contact sent in the User-Agent.
pub const CONTACT_KEY: &str = "contact";
pub const CONTACT_ENV: &str = "OPENLIBRARY_CONTACT";

/// How long a caller waits for a rate-budget lease.
///
/// Three callers, three patiences. A **search** has someone watching a spinner.
/// A **browse row** is painted by a timer and is worth less late than never
/// blocking the search behind it. A **binding lookup** rides on a Gutenberg
/// result that is already in hand, and must never make that result late: an
/// unbound edition is still a readable edition, and the next query binds it.
pub const SEARCH_DEADLINE: Duration = Duration::from_secs(20);
pub const BROWSE_DEADLINE: Duration = Duration::from_secs(5);
pub const BINDING_DEADLINE: Duration = Duration::from_secs(2);

/// HTTP timeout for a single upstream call.
const HTTP_TIMEOUT_SECS: u64 = 20;

/// Attempts per call when the request never produced a response. openlibrary.org
/// refused connections outright for a stretch while this tier was being planned;
/// one retry covers a blink without hammering an upstream that answered.
const SEND_ATTEMPTS: usize = 2;
const RETRY_BACKOFF_MS: u64 = 250;

/// The field selection for every doc-shaped request, written once so a card
/// resolved by search and the same card resolved by id are byte-identical.
const SEARCH_FIELDS: &str = "key,title,author_name,cover_i,first_publish_year,language";

/// Cache lifetimes.
///
/// ⚠ Unlike AniList's, these lists are not immutable. `trending/daily` changes
/// every day and `sort=new` every hour; a permanent cache would freeze the
/// Popular Books row on whatever it showed the first time. Work JSON and author
/// names are edited rarely and are cached long.
const DAY: u64 = 86_400;
const WORK_TTL_SECS: u64 = 30 * DAY;
const SEARCH_TTL_SECS: u64 = DAY;
const BROWSE_TTL_SECS: u64 = 6 * 3600;
/// A binding is a fact Open Library records; it is re-checked monthly. A miss is
/// re-checked weekly, because Open Library librarians add Gutenberg ids to
/// editions all the time and a permanent negative would never see them.
const BINDING_HIT_TTL_SECS: u64 = 30 * DAY;
const BINDING_MISS_TTL_SECS: u64 = 7 * DAY;

/// Authors resolved on the work-JSON fallback. Each is one more request.
const MAX_AUTHORS: usize = 4;

/// Build the process budget for Open Library.
///
/// Open Library asks API clients to identify themselves (app name + contact in
/// the User-Agent) and throttles anonymous traffic first, so an identified
/// client gets a larger bucket. The exact published ceiling could not be read
/// back from the developer page when this was written — an overestimate is
/// safe regardless: a 429 freezes the whole bucket for its `Retry-After` via
/// `note_throttled`, rather than each caller discovering it alone.
pub fn budget(identified: bool) -> Arc<RateBudget> {
    if identified {
        RateBudget::new(3.0, 6.0)
    } else {
        RateBudget::new(1.0, 3.0)
    }
}

/// The operator contact, file-or-env.
///
/// ⚠ **Looked up in both plugins' config dirs, not just the caller's.** The
/// SDK's `/config` page edits the *lowest* hosted upstream id, which in this
/// binary is `gutenberg`, so a contact saved through the dashboard lands in
/// `gateway/gutenberg/config.json` — yet both upstreams send requests to Open
/// Library and both must identify themselves. File wins over env, as for every
/// feeder config.
pub fn load_contact(cache_dir: &Path) -> Option<String> {
    let mut dirs = vec![cache_dir.to_path_buf()];
    if let Some(parent) = cache_dir.parent() {
        for id in ["gutenberg", "openlibrary"] {
            dirs.push(parent.join(id));
        }
    }
    for dir in dirs {
        let Ok(bytes) = std::fs::read(dir.join("config.json")) else {
            continue;
        };
        let Ok(v) = serde_json::from_slice::<Value>(&bytes) else {
            continue;
        };
        if let Some(c) = v.get(CONTACT_KEY).and_then(Value::as_str).and_then(sanitize_contact) {
            return Some(c);
        }
    }
    env_contact()
}

/// The first-boot seed: `OPENLIBRARY_CONTACT`.
pub fn env_contact() -> Option<String> {
    std::env::var(CONTACT_ENV).ok().as_deref().and_then(sanitize_contact)
}

/// The config field both plugins declare (see [`load_contact`] for why both).
pub fn contact_field() -> ConfigField {
    ConfigField::text(CONTACT_KEY, "Open Library contact").with_help(
        "An e-mail address or URL sent in the User-Agent of every Open Library request \
         (the openlibrary cards and the gutenberg → Open Library binding). Open Library \
         asks API clients to identify themselves; an identified client gets a larger \
         request budget.",
    )
}

/// Reduce a contact to something a header can carry.
///
/// `build_http_client` treats a client build failure as impossible and panics,
/// and an invalid header value is exactly such a failure — so a stray newline
/// in a config field must not reach it. Parentheses are dropped too: they would
/// close the User-Agent comment early.
fn sanitize_contact(raw: &str) -> Option<String> {
    let c: String = raw
        .chars()
        .filter(|c| c.is_ascii_graphic() || *c == ' ')
        .filter(|c| !matches!(c, '(' | ')'))
        .take(128)
        .collect();
    let c = c.trim();
    (!c.is_empty()).then(|| c.to_string())
}

fn user_agent(contact: Option<&str>) -> String {
    let base = concat!(
        "meta-feeder-book/",
        env!("CARGO_PKG_VERSION"),
        " (gateway:openlibrary; +https://github.com/worph/meta-feeder-book"
    );
    match contact {
        Some(c) => format!("{base}; {c})"),
        None => format!("{base})"),
    }
}

/// Is `s` a bare Open Library **work** id (`OL66554W`)?
///
/// ⚠ The `W` is the point. `OL…M` is an edition and `OL…A` an author, and the
/// search index does return edition keys under `/works/` for some junk records
/// (`/works/OL12710531M`, observed live on a `first_publish_year` query) — an
/// edition minted as a card would be a second tile for every printing.
pub fn is_work_id(s: &str) -> bool {
    s.len() > 3
        && s.starts_with("OL")
        && s.ends_with('W')
        && s[2..s.len() - 1].bytes().all(|b| b.is_ascii_digit())
}

/// One search/trending doc — the subset this feeder reads.
///
/// ⚠ Nothing mutable is read: no `edition_count`, `ratings_*`,
/// `want_to_read_count` or `ebook_access`. All drift without the work changing
/// (METADATA_KEYS rule #4).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SearchDoc {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub author_name: Vec<String>,
    #[serde(default)]
    pub cover_i: Option<i64>,
    #[serde(default)]
    pub first_publish_year: Option<i32>,
    #[serde(default)]
    pub id_project_gutenberg: Vec<String>,
}

impl SearchDoc {
    /// The bare work id, when the key names a work.
    pub fn work_id(&self) -> Option<&str> {
        self.key.strip_prefix("/works/").filter(|id| is_work_id(id))
    }

    /// The large cover URL. Open Library uses `-1` for "no cover" in `covers`
    /// arrays, so anything not positive is absent.
    pub fn cover_url(&self) -> Option<String> {
        self.cover_i
            .filter(|c| *c > 0)
            .map(|c| format!("{COVERS_BASE}/{c}-L.jpg"))
    }
}

/// What a work JSON contributes to a card.
#[derive(Debug, Clone)]
pub struct WorkText {
    /// The id the work JSON actually lives under — differs from the id asked
    /// for when that one was merged away (see [`OpenLibraryClient::work_json`]).
    pub id: String,
    pub description: Option<String>,
}

/// Cache envelope: the body plus when it was fetched, so lists can expire.
#[derive(Serialize, Deserialize)]
struct Cached {
    at: u64,
    body: Value,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub struct OpenLibraryClient {
    http: reqwest::Client,
    budget: Arc<RateBudget>,
    cache: Option<MidhashCache>,
    /// Overridable so tests can point the client at a wiremock server.
    api_base: String,
}

impl OpenLibraryClient {
    pub fn new(
        budget: Arc<RateBudget>,
        cache: Option<MidhashCache>,
        api_base: Option<String>,
        contact: Option<&str>,
    ) -> Self {
        Self {
            http: meta_feeder_sdk::common::build_http_client(
                HTTP_TIMEOUT_SECS,
                &user_agent(contact),
                None,
            ),
            budget,
            cache,
            api_base: api_base.unwrap_or_else(|| DEFAULT_API_BASE.to_string()),
        }
    }

    fn cache_get(&self, key: &str) -> Option<Cached> {
        let hit = self.cache.as_ref()?.get_misc(key).ok().flatten()?;
        serde_json::from_str::<Cached>(&hit).ok()
    }

    fn cache_put(&self, key: &str, body: &Value) {
        let Some(c) = &self.cache else { return };
        let envelope = Cached {
            at: now_secs(),
            body: body.clone(),
        };
        if let Ok(s) = serde_json::to_string(&envelope) {
            if let Err(e) = c.put_misc(key, &s) {
                warn!(upstream = "openlibrary", key, error = ?e, "cache put failed (non-fatal)");
            }
        }
    }

    /// One GET, budget-gated, never cached.
    async fn get(
        &self,
        path: &str,
        query: &[(&str, String)],
        deadline: Duration,
    ) -> Result<Value, GatewayError> {
        if matches!(self.budget.acquire(deadline).await, Lease::DeadlineExceeded) {
            // Transient, not Permanent: this upstream is busy, not broken.
            return Err(GatewayError::Transient(
                "openlibrary: rate budget deadline exceeded".into(),
            ));
        }
        let url = format!("{}{path}", self.api_base.trim_end_matches('/'));
        let mut last = String::new();
        let mut response = None;
        for attempt in 1..=SEND_ATTEMPTS {
            match self.http.get(&url).query(query).send().await {
                Ok(r) => {
                    response = Some(r);
                    break;
                }
                Err(e) => {
                    last = e.to_string();
                    warn!(upstream = "openlibrary", path, attempt, error = %last, "transport failure");
                    if attempt < SEND_ATTEMPTS {
                        tokio::time::sleep(Duration::from_millis(RETRY_BACKOFF_MS)).await;
                    }
                }
            }
        }
        let resp = response.ok_or_else(|| {
            GatewayError::Transient(format!("openlibrary: GET {path}: {last}"))
        })?;
        // 429 carries Retry-After; freeze the whole bucket, not just this call.
        if let Err(e) = meta_feeder_sdk::common::map_status(&resp) {
            if let GatewayError::RateLimited { retry_after_s } = &e {
                self.budget
                    .note_throttled(Duration::from_secs(*retry_after_s as u64));
            }
            return Err(e);
        }
        resp.json::<Value>()
            .await
            .map_err(|e| GatewayError::Transient(format!("openlibrary: bad json from {path}: {e}")))
    }

    /// [`Self::get`] behind the read-through cache. Only successes are cached:
    /// a transport blink or a 5xx must never be remembered as "nothing here".
    async fn get_cached(
        &self,
        key: &str,
        ttl_secs: u64,
        path: &str,
        query: &[(&str, String)],
        deadline: Duration,
    ) -> Result<Value, GatewayError> {
        if let Some(c) = self.cache_get(key) {
            if now_secs().saturating_sub(c.at) <= ttl_secs {
                return Ok(c.body);
            }
        }
        let body = self.get(path, query, deadline).await?;
        self.cache_put(key, &body);
        Ok(body)
    }

    /// Free-text search.
    pub async fn search(
        &self,
        text: &str,
        limit: usize,
        deadline: Duration,
    ) -> Result<Vec<SearchDoc>, GatewayError> {
        let normalised = text.split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase();
        let body = self
            .get_cached(
                &format!("openlibrary:search:{limit}:{normalised}"),
                SEARCH_TTL_SECS,
                "/search.json",
                &[
                    ("q", text.trim().to_string()),
                    ("fields", SEARCH_FIELDS.into()),
                    ("limit", limit.to_string()),
                ],
                deadline,
            )
            .await?;
        Ok(parse_docs(body.get("docs")))
    }

    /// The `popular:true` row: what Open Library readers opened today.
    ///
    /// The ordering is a popularity signal, but no count is read or stored.
    pub async fn trending(
        &self,
        limit: usize,
        deadline: Duration,
    ) -> Result<Vec<SearchDoc>, GatewayError> {
        let body = self
            .get_cached(
                &format!("openlibrary:trending:daily:{limit}"),
                BROWSE_TTL_SECS,
                "/trending/daily.json",
                &[("fields", SEARCH_FIELDS.into()), ("limit", limit.to_string())],
                deadline,
            )
            .await?;
        Ok(parse_docs(body.get("works")))
    }

    /// The `fresh:true` row: recently added works first published this year or
    /// last.
    ///
    /// ⚠ `search.json` rejects `q=*` (422, "Query too short"), and `sort=new`
    /// over a broad query is dominated by coverless catalogue stubs. The year
    /// window keeps the row about new books; the card gate (cover + description)
    /// drops the stubs that remain, so the caller over-fetches.
    pub async fn fresh(
        &self,
        limit: usize,
        deadline: Duration,
    ) -> Result<Vec<SearchDoc>, GatewayError> {
        let year = 1970 + now_secs() / 31_556_952;
        let body = self
            .get_cached(
                &format!("openlibrary:fresh:{year}:{limit}"),
                BROWSE_TTL_SECS,
                "/search.json",
                &[
                    ("q", format!("first_publish_year:[{} TO {year}]", year - 1)),
                    ("sort", "new".into()),
                    ("fields", SEARCH_FIELDS.into()),
                    ("limit", limit.to_string()),
                ],
                deadline,
            )
            .await?;
        Ok(parse_docs(body.get("docs")))
    }

    /// Resolve one work id to a doc — the anchored lookup.
    ///
    /// Search by key first, so the record matches what a search would have
    /// produced (same fields, same `first_publish_year`; the work JSON's
    /// `first_publish_date` is frequently an edition's date — `1853` for a novel
    /// first published in 1813). The work JSON plus its author records are the
    /// fallback for a work the search index does not hold; that path carries no
    /// year, because absent is better than wrong.
    pub async fn doc_by_work_id(
        &self,
        id: &str,
        deadline: Duration,
    ) -> Result<Option<SearchDoc>, GatewayError> {
        let body = self
            .get_cached(
                &format!("openlibrary:key:{id}"),
                WORK_TTL_SECS,
                "/search.json",
                &[
                    ("q", format!("key:/works/{id}")),
                    ("fields", SEARCH_FIELDS.into()),
                    ("limit", "1".into()),
                ],
                deadline,
            )
            .await?;
        if let Some(doc) = parse_docs(body.get("docs"))
            .into_iter()
            .find(|d| d.work_id() == Some(id))
        {
            return Ok(Some(doc));
        }

        let Some((work_id, work)) = self.work_json(id, deadline).await? else {
            return Ok(None);
        };
        let author_keys: Vec<String> = work
            .get("authors")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.pointer("/author/key").and_then(Value::as_str))
                    .filter_map(|k| k.strip_prefix("/authors/"))
                    .map(str::to_string)
                    .take(MAX_AUTHORS)
                    .collect()
            })
            .unwrap_or_default();
        let mut author_name = Vec::new();
        for author in author_keys {
            match self.author_name(&author, deadline).await {
                Ok(Some(name)) => author_name.push(name),
                Ok(None) => {}
                Err(e) => {
                    debug!(upstream = "openlibrary", author, error = ?e, "author lookup failed")
                }
            }
        }
        Ok(Some(SearchDoc {
            key: format!("/works/{work_id}"),
            title: work.get("title").and_then(Value::as_str).map(str::to_string),
            author_name,
            cover_i: work
                .get("covers")
                .and_then(Value::as_array)
                .and_then(|a| a.iter().filter_map(Value::as_i64).find(|c| *c > 0)),
            first_publish_year: None,
            id_project_gutenberg: Vec::new(),
        }))
    }

    async fn author_name(&self, id: &str, deadline: Duration) -> Result<Option<String>, GatewayError> {
        let body = match self
            .get_cached(
                &format!("openlibrary:author:{id}"),
                WORK_TTL_SECS,
                &format!("/authors/{id}.json"),
                &[],
                deadline,
            )
            .await
        {
            Ok(b) => b,
            Err(GatewayError::NotFound) => return Ok(None),
            Err(e) => return Err(e),
        };
        Ok(body
            .get("name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string))
    }

    /// The work JSON and the id it actually lives under, following one merge
    /// redirect.
    ///
    /// ⚠ A merged work answers **200** with `{"type": {"key": "/type/redirect"},
    /// "location": "/works/OL…W"}` (observed live: `OL45883W` → `OL45804W`) —
    /// not an HTTP redirect, so an HTTP client follows nothing and a naive
    /// reader finds no description.
    async fn work_json(
        &self,
        id: &str,
        deadline: Duration,
    ) -> Result<Option<(String, Value)>, GatewayError> {
        let mut id = id.to_string();
        for _ in 0..2 {
            let body = match self
                .get_cached(
                    &format!("openlibrary:work:{id}"),
                    WORK_TTL_SECS,
                    &format!("/works/{id}.json"),
                    &[],
                    deadline,
                )
                .await
            {
                Ok(b) => b,
                Err(GatewayError::NotFound) => return Ok(None),
                Err(e) => return Err(e),
            };
            if body.pointer("/type/key").and_then(Value::as_str) != Some("/type/redirect") {
                return Ok(Some((id, body)));
            }
            match body
                .get("location")
                .and_then(Value::as_str)
                .and_then(|l| l.strip_prefix("/works/"))
                .filter(|l| is_work_id(l))
            {
                Some(next) => id = next.to_string(),
                None => return Ok(None),
            }
        }
        Ok(None)
    }

    /// A work's canonical id and plain-text description.
    pub async fn work_text(&self, id: &str, deadline: Duration) -> Result<Option<WorkText>, GatewayError> {
        Ok(self.work_json(id, deadline).await?.map(|(id, work)| WorkText {
            id,
            description: text_value(work.get("description")).and_then(|d| clean_description(&d)),
        }))
    }

    /// The Open Library work that records Project Gutenberg etext `gutenberg_id`.
    ///
    /// `Some` only when **exactly one** work lists it — an identifier link Open
    /// Library itself records, which is what grades a binding Anchored
    /// (reading-model §2.4). Zero works is no binding; two or more is an
    /// unmerged duplicate in Open Library, and picking one would be a guess.
    ///
    /// ⚠ Never a title or author search. That is a Matched-grade binding and is
    /// out of scope here; emitting it under an anchored claim would promote a
    /// resemblance to a proof.
    pub async fn gutenberg_work(
        &self,
        gutenberg_id: u64,
        deadline: Duration,
    ) -> Result<Option<String>, GatewayError> {
        let key = format!("openlibrary:gutenberg:{gutenberg_id}");
        if let Some(c) = self.cache_get(&key) {
            let work = c.body.get("work").and_then(Value::as_str).map(str::to_string);
            let ttl = if work.is_some() {
                BINDING_HIT_TTL_SECS
            } else {
                BINDING_MISS_TTL_SECS
            };
            if now_secs().saturating_sub(c.at) <= ttl {
                return Ok(work);
            }
        }
        let body = self
            .get(
                "/search.json",
                &[
                    ("q", format!("id_project_gutenberg:{gutenberg_id}")),
                    ("fields", "key,id_project_gutenberg".into()),
                    // Two, not one: `limit=1` would make "exactly one" unprovable.
                    ("limit", "2".into()),
                ],
                deadline,
            )
            .await?;
        let work = single_gutenberg_work(&body, gutenberg_id);
        self.cache_put(&key, &serde_json::json!({ "work": work }));
        Ok(work)
    }
}

/// Decide a binding from a `q=id_project_gutenberg:<id>` response.
fn single_gutenberg_work(body: &Value, gutenberg_id: u64) -> Option<String> {
    let docs = parse_docs(body.get("docs"));
    let total = body
        .get("numFound")
        .or_else(|| body.get("num_found"))
        .and_then(Value::as_u64)
        .unwrap_or(docs.len() as u64);
    if total != 1 || docs.len() != 1 {
        return None;
    }
    let doc = &docs[0];
    // The doc must itself list the etext. The query is an exact field match, so
    // this only fails on an index quirk — and a binding graded Anchored cannot
    // rest on a quirk.
    let wanted = gutenberg_id.to_string();
    if !doc.id_project_gutenberg.iter().any(|g| g.trim() == wanted) {
        return None;
    }
    doc.work_id().map(str::to_string)
}

fn parse_docs(v: Option<&Value>) -> Vec<SearchDoc> {
    v.and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|d| serde_json::from_value::<SearchDoc>(d.clone()).ok())
                .collect()
        })
        .unwrap_or_default()
}

/// A text field that is either `"…"` or `{"type": "/type/text", "value": "…"}`.
fn text_value(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) => Some(s.clone()),
        Value::Object(o) => o.get("value").and_then(Value::as_str).map(str::to_string),
        _ => None,
    }
}

/// Community descriptions carry Windows line endings and, often, a trailing
/// `----------` rule followed by "Also contained in:" link lists. The rule and
/// what follows are catalogue furniture, not synopsis.
fn clean_description(raw: &str) -> Option<String> {
    let text = raw.replace("\r\n", "\n").replace('\r', "\n");
    let body = text.split("\n----").next().unwrap_or_default().trim();
    (!body.is_empty()).then(|| body.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_ids_are_works_only() {
        assert!(is_work_id("OL66554W"));
        assert!(!is_work_id("OL12710531M"), "an edition key is not a work");
        assert!(!is_work_id("OL21594A"), "an author key is not a work");
        assert!(!is_work_id("OLW"));
        assert!(!is_work_id("/works/OL66554W"));
    }

    #[test]
    fn description_reads_both_live_shapes() {
        let plain = serde_json::json!({ "description": "A novel." });
        let typed = serde_json::json!({ "description": { "type": "/type/text", "value": "A novel." } });
        assert_eq!(text_value(plain.get("description")).as_deref(), Some("A novel."));
        assert_eq!(text_value(typed.get("description")).as_deref(), Some("A novel."));
        assert_eq!(text_value(serde_json::json!({ "description": 3 }).get("description")), None);
    }

    #[test]
    fn description_drops_the_link_list_after_the_rule() {
        let raw = "First para.\r\n\r\nSecond para.\r\n\r\n----------\r\nAlso contained in:\r\n[x](/works/OL1W)";
        assert_eq!(
            clean_description(raw).as_deref(),
            Some("First para.\n\nSecond para.")
        );
        assert_eq!(clean_description("  \r\n "), None);
    }

    #[test]
    fn cover_minus_one_is_no_cover() {
        let mut d = SearchDoc {
            key: "/works/OL66554W".into(),
            cover_i: Some(-1),
            ..Default::default()
        };
        assert_eq!(d.cover_url(), None);
        d.cover_i = Some(14348537);
        assert_eq!(
            d.cover_url().as_deref(),
            Some("https://covers.openlibrary.org/b/id/14348537-L.jpg")
        );
    }

    #[test]
    fn a_binding_needs_exactly_one_work_that_lists_the_etext() {
        let one = serde_json::json!({ "numFound": 1, "docs": [
            { "key": "/works/OL66554W", "id_project_gutenberg": ["1342", "42671"] }
        ]});
        assert_eq!(single_gutenberg_work(&one, 1342).as_deref(), Some("OL66554W"));

        let none = serde_json::json!({ "numFound": 0, "docs": [] });
        assert_eq!(single_gutenberg_work(&none, 84), None);

        // `limit=2` returned two docs: an unmerged duplicate, not a binding.
        let two = serde_json::json!({ "numFound": 2, "docs": [
            { "key": "/works/OL1W", "id_project_gutenberg": ["11"] },
            { "key": "/works/OL2W", "id_project_gutenberg": ["11"] }
        ]});
        assert_eq!(single_gutenberg_work(&two, 11), None);

        // More found than returned also counts as ambiguous.
        let more = serde_json::json!({ "numFound": 3, "docs": [
            { "key": "/works/OL1W", "id_project_gutenberg": ["11"] }
        ]});
        assert_eq!(single_gutenberg_work(&more, 11), None);

        // A doc that does not list the etext itself proves nothing.
        let unlisted = serde_json::json!({ "numFound": 1, "docs": [
            { "key": "/works/OL1W", "id_project_gutenberg": ["999"] }
        ]});
        assert_eq!(single_gutenberg_work(&unlisted, 11), None);

        // An edition key is never bound as a work.
        let edition = serde_json::json!({ "numFound": 1, "docs": [
            { "key": "/works/OL12710531M", "id_project_gutenberg": ["11"] }
        ]});
        assert_eq!(single_gutenberg_work(&edition, 11), None);
    }

    #[test]
    fn contact_is_made_header_safe() {
        assert_eq!(
            sanitize_contact(" ops@example.org\r\n(x) ").as_deref(),
            Some("ops@example.orgx")
        );
        assert_eq!(sanitize_contact("\n"), None);
        assert!(user_agent(Some("ops@example.org")).ends_with("; ops@example.org)"));
        // Must build without panicking.
        let _ = OpenLibraryClient::new(budget(false), None, None, Some("a@b.c"));
    }

    #[test]
    fn a_dashboard_saved_contact_is_found_from_either_plugin_dir() {
        let root = tempfile::tempdir().unwrap();
        let gutenberg = root.path().join("gutenberg");
        let openlibrary = root.path().join("openlibrary");
        std::fs::create_dir_all(&gutenberg).unwrap();
        std::fs::create_dir_all(&openlibrary).unwrap();
        // The SDK's config page saves under the lowest upstream id.
        std::fs::write(
            gutenberg.join("config.json"),
            r#"{"contact":"ops@example.org"}"#,
        )
        .unwrap();
        assert_eq!(load_contact(&openlibrary).as_deref(), Some("ops@example.org"));
        assert_eq!(load_contact(&gutenberg).as_deref(), Some("ops@example.org"));
    }

    /// A cached miss is not re-asked inside its window, and a failure is never
    /// cached at all.
    #[tokio::test]
    async fn binding_lookups_cache_answers_but_never_failures() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search.json"))
            .and(query_param("q", "id_project_gutenberg:84"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "numFound": 0, "docs": [] })),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/search.json"))
            .and(query_param("q", "id_project_gutenberg:11"))
            .respond_with(ResponseTemplate::new(503))
            .expect(2)
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let cache = MidhashCache::open(dir.path()).unwrap();
        let c = OpenLibraryClient::new(RateBudget::new(100.0, 100.0), Some(cache), Some(server.uri()), None);

        assert_eq!(c.gutenberg_work(84, SEARCH_DEADLINE).await.unwrap(), None);
        assert_eq!(c.gutenberg_work(84, SEARCH_DEADLINE).await.unwrap(), None);
        assert!(c.gutenberg_work(11, SEARCH_DEADLINE).await.is_err());
        assert!(c.gutenberg_work(11, SEARCH_DEADLINE).await.is_err());
    }
}
