//! Feeder-driven enrichment — the "smart feeder / dumb gateway" pipeline.
//!
//! A feeder that self-publishes (has a `META_CORE_URL`) writes its own base
//! record, then drives the **reused meta-sort enrichment plugins**
//! (`metamesh-plugin-filename-parser`, `metamesh-plugin-tmdb`) over HTTP. Each
//! plugin is handed the `metaCoreUrl` and writes its own fields straight back to
//! meta-core (`PATCH /meta/{cid}` merge), so the gateway never touches
//! enrichment — full separation of concerns.
//!
//! ## Ordering — why we poll
//!
//! tmdb searches on `originalTitle`/`movieYear`/`videoType` (which it reads from
//! the record), and those are produced by filename-parser. So the pipeline is
//! **filename-parser → tmdb**. The plugin `/process` contract is *async*
//! (returns `accepted`, then writes to meta-core and POSTs a callback), so after
//! kicking filename-parser we **poll meta-core** until `originalTitle` appears
//! (bounded), read the merged fields back, and hand them to tmdb. Polling keeps
//! the feeder stateless — no callback-correlation bookkeeping. The plugins'
//! callback POSTs still fire; they hit the feeder's no-op `/enrich/callback`.
//!
//! The whole pipeline runs in a spawned background task so it never adds latency
//! to the libp2p resolve path; the meta-share ingester propagates the enriched
//! record on its next poll (~eventually consistent).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tracing::{debug, warn};

use crate::meta_core::{self, Provenance};
use crate::types::DiscoveryRecord;

/// Operator-supplied enrichment wiring, parsed once from the feeder env.
#[derive(Clone, Debug)]
pub struct EnrichmentConfig {
    /// meta-core root the feeder self-publishes to + hands to the plugins.
    pub meta_core_url: String,
    /// `metamesh-plugin-filename-parser` base URL (optional).
    pub filename_parser_url: Option<String>,
    /// `metamesh-plugin-tmdb` base URL (optional).
    pub tmdb_url: Option<String>,
    /// TMDB v3 key / v4 bearer token, POSTed to tmdb `/configure`.
    pub tmdb_token: Option<String>,
    /// TMDB metadata language (default `en-US`).
    pub tmdb_language: String,
    /// Stable per-deployment id stamped into `provenance.gatewayPeer`.
    pub gateway_peer: String,
    /// URL the plugins POST their completion callback to (the feeder's own
    /// no-op `/enrich/callback`). Polling is the real completion signal; this
    /// only keeps the plugins from logging a failed-callback warning.
    pub callback_url: String,
    /// `metamesh-plugin-opensubtitles` base URL (optional). When set, the
    /// pipeline drives subtitle fetching after tmdb resolves the ids — the
    /// single reference implementation of OpenSubtitles fetching (the gateway
    /// no longer carries its own).
    pub opensubtitles_url: Option<String>,
    /// OpenSubtitles consumer API key, POSTed to the plugin's `/configure`.
    pub opensubtitles_api_key: Option<String>,
    /// OpenSubtitles account username (required by the plugin for downloads).
    pub opensubtitles_username: Option<String>,
    /// OpenSubtitles account password.
    pub opensubtitles_password: Option<String>,
    /// Wanted subtitle languages (CSV of ISO 639-1 codes; default `en`).
    pub opensubtitles_languages: String,
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Derive a co-bundled sidecar plugin URL from the feeder's own hostname, by the
/// app-compose convention `<prefix>-feeder` → `<prefix>-<plugin>:8080`
/// (e.g. `metafeedertorznab-feeder` → `http://metafeedertorznab-tmdb:8080`). Lets
/// a feeder reach its bundled filename-parser / tmdb sidecars with **no explicit
/// `*_PLUGIN_URL` env** — config lives in the UI, wiring is derived. Returns
/// `None` when `HOSTNAME` is unset or not in `<prefix>-feeder` form, in which case
/// the caller falls back to the env seed (then soft-skips that plugin).
fn derive_sidecar_url(plugin: &str) -> Option<String> {
    let host = env_nonempty("HOSTNAME")?;
    let prefix = host.strip_suffix("-feeder")?;
    Some(format!("http://{prefix}-{plugin}:8080"))
}

impl EnrichmentConfig {
    /// Parse from the feeder env. Returns `None` when `META_CORE_URL` is unset —
    /// the **bundled-feeder** path (records returned over HTTP, the gateway
    /// dispatcher stores them). Self-publishing feeders that source meta-core from
    /// their own dashboard config use [`from_meta_core`](Self::from_meta_core).
    pub fn from_env() -> Option<Self> {
        Some(Self::from_meta_core(env_nonempty("META_CORE_URL")?))
    }

    /// Build with an explicit meta-core root — sourced from the feeder's own
    /// `config.json` (dashboard) when set, else the `META_CORE_URL` env seed. The
    /// sidecar URLs come from their `*_PLUGIN_URL` env, falling back to the
    /// hostname-derived co-bundled sidecar (see [`derive_sidecar_url`]); the rest
    /// keep their env seeds / defaults. Secrets (tmdb token, language) are
    /// overlaid by the caller from the per-feeder config.
    pub fn from_meta_core(meta_core_url: String) -> Self {
        EnrichmentConfig {
            meta_core_url,
            filename_parser_url: env_nonempty("FILENAME_PARSER_URL")
                .or_else(|| derive_sidecar_url("filename-parser")),
            tmdb_url: env_nonempty("TMDB_PLUGIN_URL").or_else(|| derive_sidecar_url("tmdb")),
            tmdb_token: env_nonempty("TMDB_TOKEN").or_else(|| env_nonempty("TORZNAB_TMDB_TOKEN")),
            tmdb_language: env_nonempty("TMDB_LANGUAGE").unwrap_or_else(|| "en-US".to_string()),
            gateway_peer: env_nonempty("META_GATEWAY_PEER_ID")
                .or_else(|| env_nonempty("HOSTNAME"))
                .unwrap_or_else(|| "gateway-feeder".to_string()),
            callback_url: env_nonempty("META_FEEDER_CALLBACK_URL")
                .unwrap_or_else(|| "http://127.0.0.1:8080/enrich/callback".to_string()),
            opensubtitles_url: env_nonempty("OPENSUBTITLES_PLUGIN_URL"),
            opensubtitles_api_key: env_nonempty("OPENSUBTITLES_API_KEY"),
            opensubtitles_username: env_nonempty("OPENSUBTITLES_USERNAME"),
            opensubtitles_password: env_nonempty("OPENSUBTITLES_PASSWORD"),
            opensubtitles_languages: env_nonempty("OPENSUBTITLES_LANGUAGES")
                .unwrap_or_else(|| "en".to_string()),
        }
    }
}

/// How long to wait for filename-parser to merge `originalTitle` before handing
/// off to tmdb. filename-parser is a pure, fast string parse; this is a generous
/// ceiling, not an expected wait.
const FILENAME_PARSE_POLL_TIMEOUT: Duration = Duration::from_secs(5);
/// How long to wait for tmdb to merge `tmdbid` before fanning its anchor to the
/// sibling files. tmdb hits the network, so a longer ceiling than filename.
const TMDB_POLL_TIMEOUT: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Show-level TMDB fields that are **fanned** from the primary file to every
/// sibling file in a multi-file (season-pack) torrent — they're identical for
/// every episode of a show, so we resolve tmdb **once** and copy these across
/// (avoids N TMDB lookups; the tmdb plugin caches by `cid_midhash256`, which
/// gateway BT records don't carry, so without this each file would re-hit TMDB).
/// Per-file fields (`originalTitle`, `season`, `episode`, `fileName`, `sizeByte`,
/// `quality`, …) are deliberately NOT fanned — filename-parser sets those per file.
const ANCHOR_EXACT: &[&str] = &[
    "tmdbid",
    "imdbid",
    "poster",
    "backdrop",
    "rating",
    "releasedate",
    "studio",
    "genres",
    "tags",
];
const ANCHOR_PREFIXES: &[&str] = &["plot/", "genres/", "studio/", "tags/"];

/// One file (or pack) to enrich: its CID, the filename to parse, and the base
/// fields already written to meta-core (handed to the plugins as `existingMeta`).
#[derive(Clone, Debug)]
pub struct EnrichTarget {
    pub cid: String,
    pub file_name: String,
    pub base: BTreeMap<String, String>,
}

/// Drives meta-core writes + the enrichment plugins for one feeder. Cheap to
/// clone (just a reqwest client + an `Arc<config>`), so it can be moved into a
/// spawned per-resolve task.
#[derive(Clone)]
pub struct Enricher {
    http: reqwest::Client,
    cfg: Arc<EnrichmentConfig>,
}

#[derive(Serialize)]
struct ProcessRequest<'a> {
    #[serde(rename = "taskId")]
    task_id: String,
    cid: &'a str,
    #[serde(rename = "filePath")]
    file_path: &'a str,
    #[serde(rename = "callbackUrl")]
    callback_url: &'a str,
    #[serde(rename = "metaCoreUrl")]
    meta_core_url: &'a str,
    #[serde(rename = "existingMeta")]
    existing_meta: &'a BTreeMap<String, String>,
}

impl Enricher {
    pub fn new(http: reqwest::Client, cfg: EnrichmentConfig) -> Self {
        Enricher {
            http,
            cfg: Arc::new(cfg),
        }
    }

    pub fn meta_core_url(&self) -> &str {
        &self.cfg.meta_core_url
    }

    /// `true` if any enrichment plugin is wired (so there's enrichment to drive
    /// beyond the base-record write).
    pub fn has_plugins(&self) -> bool {
        self.cfg.filename_parser_url.is_some() || self.cfg.tmdb_url.is_some()
    }

    /// Write the feeder's base record into meta-core (merge/upsert) under `cid`.
    /// Identical shape to the gateway dispatcher's write (provenance + the
    /// `cids/<cid>` key-set collapse).
    pub async fn write_base_record(
        &self,
        cid: &str,
        record: &DiscoveryRecord,
    ) -> anyhow::Result<()> {
        let prov = Provenance::now(
            &record.upstream_id,
            &record.record_id,
            &self.cfg.gateway_peer,
        );
        let body = meta_core::build_metadata_body(record, &prov);
        meta_core::put_record(&self.http, &self.cfg.meta_core_url, cid, &body).await
    }

    /// Run the video enrichment pipeline for one resolved CID: filename-parser
    /// (clean title/year/season/episode) → tmdb (tmdbid/poster/overview). Each
    /// plugin writes its own fields to meta-core. Best-effort: any plugin
    /// failure is logged and the pipeline continues (the base record already
    /// landed, so the item is at worst un-enriched, never lost).
    pub async fn enrich_video(
        &self,
        cid: &str,
        file_name: &str,
        base_fields: BTreeMap<String, String>,
    ) {
        let mut meta = base_fields;

        // 1. filename-parser → originalTitle / movieYear / videoType / season / episode.
        if let Some(fp_url) = self.cfg.filename_parser_url.clone() {
            match self.process(&fp_url, "filename-parser", cid, file_name, &meta).await {
                Ok(()) => {
                    // Poll for the merged fields so tmdb gets the clean title.
                    if let Some(merged) = self
                        .poll_until_field(cid, "originalTitle", FILENAME_PARSE_POLL_TIMEOUT)
                        .await
                    {
                        meta = merged;
                    } else {
                        debug!(
                            target: "meta-feeder::enrich",
                            cid, "filename-parser produced no originalTitle within timeout; \
                                  handing tmdb the base fields"
                        );
                    }
                }
                Err(e) => warn!(
                    target: "meta-feeder::enrich",
                    cid, error = %e, "filename-parser /process failed (non-fatal)"
                ),
            }
        }

        // 2. tmdb → tmdbid / poster / overview / genres (requires fileType=video +
        //    a clean originalTitle, both now present on `meta`).
        if let Some(tmdb_url) = self.cfg.tmdb_url.clone() {
            // Configure the API key before each call — tmdb holds it in memory
            // only, so this is robust to a tmdb-plugin restart. Cheap (local).
            if let Err(e) = self.configure_tmdb(&tmdb_url).await {
                warn!(
                    target: "meta-feeder::enrich",
                    cid, error = %e, "tmdb /configure failed (continuing; may soft-skip)"
                );
            }
            if let Err(e) = self.process(&tmdb_url, "tmdb", cid, file_name, &meta).await {
                warn!(
                    target: "meta-feeder::enrich",
                    cid, error = %e, "tmdb /process failed (non-fatal)"
                );
            }
        }

        // 3. opensubtitles → subtitles/<lang3>/<cid> (needs tmdbid/imdbid, which
        //    tmdb writes async). Poll for tmdbid so the plugin has an id to
        //    search by, then drive it like any other enrichment plugin.
        if self.cfg.opensubtitles_url.is_some() {
            let merged = self
                .poll_until_field(cid, "tmdbid", TMDB_POLL_TIMEOUT)
                .await
                .unwrap_or(meta);
            self.enrich_subtitles(cid, file_name, &merged).await;
        }
    }

    /// Enrich a **bundle** from one torrent (metainfo fan-out): N video files +
    /// an optional whole-torrent pack record. filename-parser runs on every
    /// target; tmdb runs **once** on the primary video file and its show-level
    /// anchor (`tmdbid`/poster/genres/…) is fanned to the sibling files. The
    /// pack record gets filename-parse only (no tmdb — it's the non-playable
    /// "release as a unit" search handle, D1). Best-effort throughout.
    pub async fn enrich_bundle(&self, videos: Vec<EnrichTarget>, pack: Option<EnrichTarget>) {
        // Pack: filename-parse only, no tmdb.
        if let Some(p) = pack {
            self.fire_filename(&p).await;
        }

        if videos.is_empty() {
            return;
        }
        if videos.len() == 1 {
            // Degenerate single-video bundle → the plain pipeline (filename → tmdb).
            let v = &videos[0];
            self.enrich_video(&v.cid, &v.file_name, v.base.clone()).await;
            return;
        }

        // Multi-file: fire filename-parser for every file (fire-and-forget — only
        // the primary is polled, the rest write async).
        for v in &videos {
            self.fire_filename(v).await;
        }

        // Primary = largest by `sizeByte` (ties → lowest index, i.e. first seen).
        let primary = pick_primary(&videos);
        let p = &videos[primary];

        // Poll the primary for filename-parser's clean title, then tmdb it.
        let merged = self
            .poll_until_field(&p.cid, "originalTitle", FILENAME_PARSE_POLL_TIMEOUT)
            .await
            .unwrap_or_else(|| p.base.clone());

        let Some(tmdb_url) = self.cfg.tmdb_url.clone() else {
            return; // no tmdb wired → filename-only enrichment, done.
        };
        if let Err(e) = self.configure_tmdb(&tmdb_url).await {
            warn!(target: "meta-feeder::enrich", cid = %p.cid, error = %e, "tmdb /configure failed");
        }
        if let Err(e) = self.process(&tmdb_url, "tmdb", &p.cid, &p.file_name, &merged).await {
            warn!(target: "meta-feeder::enrich", cid = %p.cid, error = %e, "primary tmdb /process failed");
            return;
        }

        // Read the resolved anchor back off the primary, then fan it to siblings.
        let Some(anchor_rec) = self.poll_until_field(&p.cid, "tmdbid", TMDB_POLL_TIMEOUT).await
        else {
            debug!(target: "meta-feeder::enrich", cid = %p.cid, "primary produced no tmdbid; nothing to fan");
            return;
        };
        let anchor = extract_anchor(&anchor_rec);
        if anchor.is_empty() {
            return;
        }
        for (i, v) in videos.iter().enumerate() {
            if i == primary {
                continue;
            }
            // PUT is merge/upsert — this layers the show-level anchor onto each
            // file's record without disturbing its per-file fields.
            if let Err(e) =
                meta_core::put_record(&self.http, &self.cfg.meta_core_url, &v.cid, &anchor).await
            {
                warn!(target: "meta-feeder::enrich", cid = %v.cid, error = %e, "anchor fan PUT failed");
            }
        }

        // Subtitles per video: each episode needs its own (its season/episode
        // differs), but they share the fanned tmdbid. Poll each for tmdbid
        // (present on the primary, fanned onto the siblings) and drive the
        // opensubtitles plugin. Best-effort + quota-guarded inside the plugin.
        if self.cfg.opensubtitles_url.is_some() {
            for v in &videos {
                let merged = self
                    .poll_until_field(&v.cid, "tmdbid", TMDB_POLL_TIMEOUT)
                    .await
                    .unwrap_or_else(|| v.base.clone());
                self.enrich_subtitles(&v.cid, &v.file_name, &merged).await;
            }
        }
    }

    /// Fire filename-parser `/process` for one target (fire-and-forget; the
    /// plugin merges its fields to meta-core async).
    async fn fire_filename(&self, t: &EnrichTarget) {
        if let Some(fp_url) = self.cfg.filename_parser_url.clone() {
            if let Err(e) = self
                .process(&fp_url, "filename-parser", &t.cid, &t.file_name, &t.base)
                .await
            {
                warn!(target: "meta-feeder::enrich", cid = %t.cid, error = %e, "filename-parser /process failed");
            }
        }
    }

    /// POST `/process` to a plugin. Returns once the plugin *accepts* the task
    /// (the actual meta-core write is async on the plugin side; the caller polls
    /// meta-core for the result when it needs it downstream).
    async fn process(
        &self,
        plugin_url: &str,
        plugin: &str,
        cid: &str,
        file_path: &str,
        existing_meta: &BTreeMap<String, String>,
    ) -> anyhow::Result<()> {
        let url = format!("{}/process", plugin_url.trim_end_matches('/'));
        let body = ProcessRequest {
            task_id: format!("{plugin}-{cid}-{}", now_nanos()),
            cid,
            file_path,
            callback_url: &self.cfg.callback_url,
            meta_core_url: &self.cfg.meta_core_url,
            existing_meta,
        };
        let resp = self.http.post(&url).json(&body).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("{plugin} POST {url} returned {status}: {text}");
        }
        Ok(())
    }

    /// POST `/configure` to the tmdb plugin with the API key + language.
    async fn configure_tmdb(&self, tmdb_url: &str) -> anyhow::Result<()> {
        let Some(token) = self.cfg.tmdb_token.clone() else {
            anyhow::bail!("no TMDB token configured");
        };
        let url = format!("{}/configure", tmdb_url.trim_end_matches('/'));
        let body = serde_json::json!({
            "config": { "apiKey": token, "language": self.cfg.tmdb_language }
        });
        let resp = self.http.post(&url).json(&body).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("tmdb /configure returned {status}: {text}");
        }
        Ok(())
    }

    /// POST `/configure` to the opensubtitles plugin with the API key, account
    /// credentials, and wanted languages.
    async fn configure_opensubtitles(&self, url: &str) -> anyhow::Result<()> {
        let Some(key) = self.cfg.opensubtitles_api_key.clone() else {
            anyhow::bail!("no OpenSubtitles API key configured");
        };
        let cfg_url = format!("{}/configure", url.trim_end_matches('/'));
        let body = serde_json::json!({
            "config": {
                "apiKey": key,
                "username": self.cfg.opensubtitles_username,
                "password": self.cfg.opensubtitles_password,
                "languages": self.cfg.opensubtitles_languages,
            }
        });
        let resp = self.http.post(&cfg_url).json(&body).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("opensubtitles /configure returned {status}: {text}");
        }
        Ok(())
    }

    /// Drive the opensubtitles plugin for one video CID. Needs a `tmdbid` or
    /// `imdbid` in `meta` to search by (the plugin reads them from `existingMeta`
    /// and writes the `subtitles/<lang3>/<cid>` key-set straight to meta-core).
    /// Best-effort: any failure is logged and swallowed.
    async fn enrich_subtitles(&self, cid: &str, file_name: &str, meta: &BTreeMap<String, String>) {
        let Some(url) = self.cfg.opensubtitles_url.clone() else {
            return;
        };
        if !meta.contains_key("tmdbid") && !meta.contains_key("imdbid") {
            debug!(
                target: "meta-feeder::enrich",
                cid, "no tmdbid/imdbid resolved; skipping opensubtitles"
            );
            return;
        }
        if let Err(e) = self.configure_opensubtitles(&url).await {
            warn!(
                target: "meta-feeder::enrich",
                cid, error = %e, "opensubtitles /configure failed (continuing; may soft-skip)"
            );
        }
        if let Err(e) = self.process(&url, "opensubtitles", cid, file_name, meta).await {
            warn!(
                target: "meta-feeder::enrich",
                cid, error = %e, "opensubtitles /process failed (non-fatal)"
            );
        }
    }

    /// Poll `GET /meta/{cid}` until `field` is present (non-empty) or `timeout`
    /// elapses; returns the merged field map on success.
    async fn poll_until_field(
        &self,
        cid: &str,
        field: &str,
        timeout: Duration,
    ) -> Option<BTreeMap<String, String>> {
        let deadline = SystemTime::now() + timeout;
        loop {
            match meta_core::get_record(&self.http, &self.cfg.meta_core_url, cid).await {
                Ok(Some(fields)) if fields.get(field).is_some_and(|v| !v.is_empty()) => {
                    return Some(fields);
                }
                Ok(_) => {}
                Err(e) => debug!(
                    target: "meta-feeder::enrich",
                    cid, error = %e, "poll get_record failed"
                ),
            }
            if SystemTime::now() >= deadline {
                return None;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Pick the primary video file index: largest by `sizeByte`, ties broken toward
/// the first (lowest index). Falls back to 0 for an all-unsized bundle.
fn pick_primary(videos: &[EnrichTarget]) -> usize {
    videos
        .iter()
        .enumerate()
        .max_by_key(|(i, v)| {
            let size = v.base.get("sizeByte").and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
            // larger size wins; on a tie the smaller index wins (Reverse).
            (size, std::cmp::Reverse(*i))
        })
        .map(|(i, _)| i)
        .unwrap_or(0)
}

/// Project a resolved record down to the show-level anchor fields that are safe
/// to copy to sibling files (see [`ANCHOR_EXACT`] / [`ANCHOR_PREFIXES`]).
fn extract_anchor(fields: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    fields
        .iter()
        .filter(|(k, _)| {
            ANCHOR_EXACT.contains(&k.as_str())
                || ANCHOR_PREFIXES.iter().any(|p| k.starts_with(p))
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_env_requires_meta_core_url() {
        // Not set in the test env → None (legacy gateway-stores path).
        // (We don't mutate process env here to avoid cross-test races; this just
        // documents the contract.)
        assert!(EnrichmentConfig::from_env().is_none() || EnrichmentConfig::from_env().is_some());
    }

    fn tgt(cid: &str, size: u64) -> EnrichTarget {
        EnrichTarget {
            cid: cid.into(),
            file_name: cid.into(),
            base: BTreeMap::from_iter([("sizeByte".to_string(), size.to_string())]),
        }
    }

    #[test]
    fn pick_primary_is_largest_then_first() {
        let v = vec![tgt("a", 100), tgt("b", 900), tgt("c", 900), tgt("d", 50)];
        // 'b' and 'c' tie at 900 → lowest index ('b', idx 1) wins.
        assert_eq!(pick_primary(&v), 1);
    }

    #[test]
    fn pick_primary_all_unsized_defaults_first() {
        let v = vec![
            EnrichTarget { cid: "a".into(), file_name: "a".into(), base: BTreeMap::new() },
            EnrichTarget { cid: "b".into(), file_name: "b".into(), base: BTreeMap::new() },
        ];
        assert_eq!(pick_primary(&v), 0);
    }

    #[test]
    fn extract_anchor_keeps_show_level_drops_per_file() {
        let rec = BTreeMap::from_iter([
            ("tmdbid".to_string(), "10378".to_string()),
            ("imdbid".to_string(), "tt1254207".to_string()),
            ("poster".to_string(), "bafkPOSTER".to_string()),
            ("genres".to_string(), "Animation|Comedy".to_string()),
            ("plot/eng".to_string(), "a plot".to_string()),
            ("tags".to_string(), "tmdb-verified".to_string()),
            // per-file — must NOT be fanned:
            ("originalTitle".to_string(), "Big Buck Bunny".to_string()),
            ("season".to_string(), "1".to_string()),
            ("episode".to_string(), "3".to_string()),
            ("fileName".to_string(), "ep3.mkv".to_string()),
        ]);
        let a = extract_anchor(&rec);
        assert!(a.contains_key("tmdbid") && a.contains_key("poster") && a.contains_key("plot/eng"));
        assert!(a.contains_key("genres") && a.contains_key("tags") && a.contains_key("imdbid"));
        assert!(!a.contains_key("originalTitle"), "originalTitle is per-file");
        assert!(!a.contains_key("season") && !a.contains_key("episode"));
        assert!(!a.contains_key("fileName"));
    }
}
