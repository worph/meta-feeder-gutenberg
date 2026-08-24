//! Lightweight meta-core write/read client for feeders that **self-publish**.
//!
//! Post-"smart feeder / dumb gateway" split: a feeder given a `META_CORE_URL`
//! writes its own base record into meta-core instead of returning it for the
//! gateway dispatcher to store. This keeps the gateway a pure aggregator +
//! request router; the feeder owns the full record lifecycle for its upstream.
//!
//! The write shape is **byte-for-byte the same** as the gateway's
//! `meta_core::build_metadata_body` (`crates/meta-gateway/src/meta_core.rs`):
//! `cid_*` fields collapse to the `cids/<cid> = "true"` key-set, and a
//! `provenance` JSON blob is stamped. Keeping them identical means a record
//! written by the feeder is indistinguishable from one the dispatcher wrote —
//! the meta-share ingester, meta-watch, and ranking all behave the same.
//!
//! `PUT /api/metadata/{cid}` is merge/upsert (confirmed: meta-core
//! `SetMetadataFlat` sets only the fields in the payload, never deletes), so the
//! feeder's base write and the enrichment plugins' later `PATCH /meta/{cid}`
//! merges accumulate on the same `/file/{cid}` hash.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use tracing::warn;

use crate::plugin::{HashKind, HashOutcome};
use crate::types::{DiscoveryRecord, Hash};

/// Provenance recorded on every gateway-acquired record. Mirrors the gateway
/// dispatcher's shape so feeder-written and dispatcher-written records are
/// interchangeable.
#[derive(Clone, Debug)]
pub struct Provenance {
    pub upstream: String,
    pub record_id: String,
    /// Stable per-deployment id (the gateway peer this feeder backs, from
    /// `META_GATEWAY_PEER_ID`, else the feeder hostname). Analytics group by it.
    pub gateway_peer: String,
    pub acquired_at: u64,
}

impl Provenance {
    /// Build a provenance stamp for `upstream`/`record_id`, stamping "now".
    pub fn now(upstream: &str, record_id: &str, gateway_peer: &str) -> Self {
        Provenance {
            upstream: upstream.to_string(),
            record_id: record_id.to_string(),
            gateway_peer: gateway_peer.to_string(),
            acquired_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        }
    }
}

/// JSON-stringified provenance value. Same key set + `"source":"gateway"` as the
/// gateway dispatcher (`crates/meta-gateway/src/meta_core.rs::provenance_to_json`).
pub fn provenance_to_json(p: &Provenance) -> String {
    serde_json::to_string(&serde_json::json!({
        "source":      "gateway",
        "upstream":    p.upstream,
        "recordId":    p.record_id,
        "gatewayPeer": p.gateway_peer,
        "acquiredAt":  p.acquired_at,
    }))
    .expect("static JSON shape — serialisation is infallible")
}

/// Build the flat meta-core PUT body from a [`DiscoveryRecord`] + provenance.
///
/// Identical transform to the gateway dispatcher: any `cid_*` field becomes a
/// `cids/<value> = "true"` key-set member (the CID is self-describing — the
/// algorithm is the multicodec; see METADATA_KEYS.md §2/§14.13), everything else
/// passes through, and `provenance` is stamped last (overriding any plugin-set
/// value).
pub fn build_metadata_body(
    record: &DiscoveryRecord,
    provenance: &Provenance,
) -> BTreeMap<String, String> {
    let mut body: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in &record.fields {
        if k.starts_with("cid_") && !v.is_empty() {
            body.insert(format!("cids/{v}"), "true".to_string());
        } else {
            body.insert(k.clone(), v.clone());
        }
    }
    body.insert("provenance".to_string(), provenance_to_json(provenance));
    body
}

/// `PUT /api/metadata/{cid}` — write (merge/upsert) the record body into
/// meta-core. `base_url` is the meta-core root (e.g. `http://…-core:9000`).
pub async fn put_record(
    http: &reqwest::Client,
    base_url: &str,
    cid: &str,
    body: &BTreeMap<String, String>,
) -> anyhow::Result<()> {
    let url = format!("{}/api/metadata/{}", base_url.trim_end_matches('/'), cid);
    let resp = http.put(&url).json(body).send().await?;
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("meta-core PUT {url} returned {status}: {text}");
    }
    Ok(())
}

/// Shape of meta-core's `GET /meta/{cid}` response (`{ hashId, metadata }`).
#[derive(Debug, Deserialize)]
struct MetaGetResponse {
    #[serde(default)]
    metadata: Option<BTreeMap<String, String>>,
}

/// `GET /meta/{cid}` — read the current flat field map for a record, or `None`
/// when meta-core has no record yet (404). Used to read back a plugin's merged
/// fields (e.g. filename-parser's `originalTitle`/`movieYear`) before handing
/// them to the next plugin in the pipeline.
pub async fn get_record(
    http: &reqwest::Client,
    base_url: &str,
    cid: &str,
) -> anyhow::Result<Option<BTreeMap<String, String>>> {
    let url = format!("{}/meta/{}", base_url.trim_end_matches('/'), cid);
    let resp = http.get(&url).send().await?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !resp.status().is_success() {
        anyhow::bail!("meta-core GET {url} returned {}", resp.status());
    }
    let parsed: MetaGetResponse = resp.json().await?;
    Ok(parsed.metadata)
}

/// Self-publishing store for feeders that own their record lifecycle (D4):
/// the feeder writes its own **metadata-only** records to meta-core and returns
/// sparse outcomes so the gateway dispatcher skips them; the meta-share ingester
/// Kamilata-propagates them.
///
/// **Byte outcomes are deliberately NOT self-published.** A feeder-side WebDAV
/// write to the gateway core would be seeded by nobody: the gateway core runs
/// with `ENABLE_FILE_WATCHER=false` (so meta-core emits no `/api/events/files`
/// for it, and the ingester's `ipfs_seed` — which seeds only watched files —
/// never sees it), and a sparse outcome makes the gateway skip its own
/// blockstore seed. So byte-bearing outcomes (torznab full-file / subtitles)
/// pass through unchanged and the **gateway** continues to WebDAV-PUT + bitswap-
/// seed them (it's the libp2p/bitswap host). Result: feeder owns the metadata
/// records (the Kamilata-propagated bulk), gateway owns byte delivery — a clean
/// split with no seeding regression. Tribler is metadata-only and uses
/// [`crate::enrich::Enricher`]; this is torznab's record-owning counterpart.
#[derive(Clone)]
pub struct FeederStore {
    http: reqwest::Client,
    meta_core_url: String,
    gateway_peer: String,
}

impl FeederStore {
    pub fn new(http: reqwest::Client, meta_core_url: String, gateway_peer: String) -> Self {
        FeederStore {
            http,
            meta_core_url: meta_core_url.trim_end_matches('/').to_string(),
            gateway_peer,
        }
    }

    /// Build from the feeder env. `None` when `META_CORE_URL` is unset (the
    /// feeder then falls back to the legacy "return outcomes, gateway stores"
    /// path). `gateway_peer` ← `META_GATEWAY_PEER_ID` / `HOSTNAME`.
    pub fn from_env() -> Option<Self> {
        let meta_core_url = env_nonempty("META_CORE_URL")?;
        let gateway_peer = env_nonempty("META_GATEWAY_PEER_ID")
            .or_else(|| env_nonempty("HOSTNAME"))
            .unwrap_or_else(|| "gateway-feeder".to_string());
        let http = crate::common::build_http_client(30, "meta-feeder-store", None);
        Some(Self::new(http, meta_core_url, gateway_peer))
    }

    pub fn meta_core_url(&self) -> &str {
        &self.meta_core_url
    }

    /// Raw keys containing `contains` (`GET /api/kv/search`). Returns
    /// `(keys, truncated)`; `truncated` means the server capped the result and
    /// there may be more.
    ///
    /// This is a keyspace scan, so it belongs on rare control-plane paths only —
    /// today, invalidating the gateway's search-coverage markers when a feeder's
    /// upstream source set changes. Never call it per search.
    pub async fn search_keys(&self, contains: &str, limit: usize) -> anyhow::Result<(Vec<String>, bool)> {
        let url = format!("{}/api/kv/search", self.meta_core_url);
        let resp = self
            .http
            .get(&url)
            .query(&[("contains", contains), ("limit", &limit.to_string())])
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("meta-core KV search `{contains}` returned {status}");
        }
        #[derive(Deserialize)]
        struct KvSearchResponse {
            #[serde(default)]
            keys: Vec<String>,
            #[serde(default)]
            truncated: bool,
        }
        let body: KvSearchResponse = resp.json().await?;
        Ok((body.keys, body.truncated))
    }

    /// Delete one raw KV key (`DELETE /api/kv/value?key=`). A missing key is not
    /// an error.
    pub async fn delete_kv(&self, key: &str) -> anyhow::Result<()> {
        let url = format!("{}/api/kv/value", self.meta_core_url);
        let resp = self.http.delete(&url).query(&[("key", key)]).send().await?;
        let status = resp.status();
        if status.is_success() || status == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        anyhow::bail!("meta-core KV DELETE {key} returned {status}");
    }

    /// Self-publish a **metadata-only** outcome to meta-core, returning the
    /// **sparse** outcome (`record: None`) so the gateway's `(_, None)` branch
    /// skips it — no double write. Byte outcomes and already-sparse outcomes are
    /// returned unchanged (the gateway stores + seeds the bytes; see the struct
    /// docs). On a write failure the original outcome is returned so the gateway
    /// stores it (graceful degradation, never lost).
    pub async fn publish(
        &self,
        upstream_id: &str,
        record_id: &str,
        outcome: HashOutcome,
    ) -> HashOutcome {
        // Only self-publish metadata-only outcomes (bytes None, record Some).
        if outcome.bytes.is_some() || outcome.record.is_none() {
            return outcome;
        }
        let cid = outcome.hash.0.clone();
        let record = outcome.record.as_ref().expect("record is Some by the guard");
        match self
            .store_metadata_only(upstream_id, record_id, &cid, record)
            .await
        {
            Ok(()) => sparse(outcome.hash, outcome.hash_kind),
            Err(e) => {
                warn!(target: "meta-feeder::store", cid = %cid, error = %e,
                      "metadata self-publish failed; falling back to gateway store");
                outcome
            }
        }
    }

    /// Metadata-only self-publish: `PUT /api/metadata/{cid}` with provenance.
    async fn store_metadata_only(
        &self,
        upstream_id: &str,
        record_id: &str,
        cid: &str,
        record: &DiscoveryRecord,
    ) -> anyhow::Result<()> {
        let prov = Provenance::now(upstream_id, record_id, &self.gateway_peer);
        let body = build_metadata_body(record, &prov);
        put_record(&self.http, &self.meta_core_url, cid, &body).await
    }
}

fn sparse(hash: Hash, hash_kind: HashKind) -> HashOutcome {
    HashOutcome {
        hash,
        hash_kind,
        bytes: None,
        record: None,
        file_extension: None,
    }
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec() -> DiscoveryRecord {
        DiscoveryRecord {
            upstream_id: "tribler".into(),
            record_id: "abc".into(),
            fields: BTreeMap::from_iter([
                ("title".into(), "Big Buck Bunny".into()),
                ("cid_btih_v1_file".into(), "bafkPERFILE".into()),
            ]),
        }
    }

    #[test]
    fn provenance_shape_matches_gateway() {
        let p = Provenance {
            upstream: "tribler".into(),
            record_id: "abc".into(),
            gateway_peer: "metagateway-tribler".into(),
            acquired_at: 1_747_504_000,
        };
        let v: serde_json::Value = serde_json::from_str(&provenance_to_json(&p)).unwrap();
        assert_eq!(v["source"], "gateway");
        assert_eq!(v["upstream"], "tribler");
        assert_eq!(v["recordId"], "abc");
        assert_eq!(v["gatewayPeer"], "metagateway-tribler");
        assert_eq!(v["acquiredAt"], 1_747_504_000_u64);
    }

    #[test]
    fn body_collapses_cid_fields_and_stamps_provenance() {
        let p = Provenance::now("tribler", "abc", "peer");
        let body = build_metadata_body(&rec(), &p);
        assert_eq!(body.get("title").map(String::as_str), Some("Big Buck Bunny"));
        // cid_* collapsed to cids/<value>=true; no cid_* survives.
        assert!(body.keys().all(|k| !k.starts_with("cid_")));
        assert_eq!(
            body.get("cids/bafkPERFILE").map(String::as_str),
            Some("true")
        );
        assert!(body.contains_key("provenance"));
    }
}
