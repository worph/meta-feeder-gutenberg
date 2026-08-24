//! The `FeederPlugin` trait + supporting types.
//!
//! A feeder plugin bridges one external upstream (`gutenberg`, `arxiv`,
//! `torznab`, …) into the MetaMesh network. It **finds** records and
//! **fetches** bytes; it does NOT hash-into-a-blockstore, store back into
//! meta-core, or speak libp2p — those stay in the gateway core. This is why
//! `FeederPlugin` has no `set_blockstore` / `set_tmdb_budget` hooks (the
//! gateway's `GatewayPlugin` carried them): preview-seeding moves to the core,
//! and a feeder that needs a TMDB budget owns it internally.
//!
//! The `serve_feeders` harness ([`crate::serve`]) wraps any set of
//! `FeederPlugin`s in the feeder HTTP contract; the gateway core's
//! `RemoteFeederPlugin` is the matching HTTP client.

use std::collections::HashMap;
use std::path::Path;

use async_trait::async_trait;
use futures::stream::BoxStream;

// Re-exported so plugins can `use meta_feeder_sdk::plugin::GatewayQuery` (the
// gateway's original `plugin.rs` re-exported it the same way).
pub use crate::query::GatewayQuery;
use crate::query::GatewaySearchEvent;
use crate::types::{ByteStream, DiscoveryRecord, GatewayError, Hash, PluginHealth};

/// One element of what [`FeederPlugin::compute_outcomes`] returns.
///
/// The `hash` is the value shipped to the gateway core. The other fields are
/// signals the core's auto-store uses to persist the content into meta-core.
/// The core's three-branch auto-store routes on `(bytes, record)`:
/// - `(Some, Some)` — full store: WebDAV PUT + metadata PUT with `filePath`.
/// - `(None, Some)` — metadata-only PUT (no local bytes; e.g. torznab samples
///   the middle 1 MiB of a multi-GiB video and surfaces only the record).
/// - `(_, None)` — skip. Cache hits land here.
///
/// `file_extension` MUST be `None` when `bytes` is `None`.
#[derive(Debug)]
pub struct HashOutcome {
    pub hash: Hash,
    /// Which hash family this CID belongs to. Drives the core's routing —
    /// the bitswap blockstore seed only fires for `Sha2_256`.
    pub hash_kind: HashKind,
    pub bytes: Option<bytes::Bytes>,
    pub record: Option<DiscoveryRecord>,
    /// File extension to append when naming the WebDAV-side blob (e.g.
    /// `"epub"`). No leading dot. MUST be `None` when `bytes` is `None`.
    pub file_extension: Option<String>,
}

/// Hash family discriminator on [`HashOutcome`].
///
/// - `Midhash256` — size-prefix-plus-middle-1MB-sample, custom multicodec
///   `0x1000`. Fast, MetaMesh-internal, not retrievable via public IPFS.
/// - `Sha2_256` — standard IPFS CIDv1 over full bytes. Retrievable via bitswap
///   once the bytes are in the core's blockstore. What every shipping plugin
///   today emits.
/// - `BtV1File` — a single file inside a BitTorrent v1 torrent, custom
///   multicodec `0x1001`. Locator, not content hash: opaque to bitswap.
/// - `NzbRelease` — a Usenet release (a Newznab listing), custom multicodec
///   `nzb-release` `0x1005`. Self-describing locator: the cid embeds `{host,
///   id}` in an identity multihash, opaque to bitswap and redeemed to bytes
///   only by a credentialed meta-share peer, which decodes the locator from the
///   cid and grabs the `.nzb` via `t=get` — no KV side-table.
/// - `CardLocator` — the identity of a *work* (a series, a film) as published by
///   a metadata bridge, custom multicodec `0x1007`. The only family with **no
///   bytes behind it at all**: the record IS the payload. Always paired with
///   `bytes: None`, so it lands in the `(None, Some)` metadata-only branch
///   above. Opaque to bitswap, and deliberately so — there is nothing to seed.
/// - `NzbPosting` — a Usenet posting we **scanned ourselves**, custom multicodec
///   `nzb-posting` `0x1003`. A content-derived digest over the article
///   Message-ID set (not a locator: no host is embedded), so any peer with a
///   plain NNTP provider can fetch it — no `IndexerCred`. Opaque to bitswap: the
///   digest is over the id set, not over any block, so the `.nzb` manifest
///   travels separately as an ordinary sha2-256 cid in the record's `manifest`
///   field. Produced by `hash::compute_nzb_posting_cid`.
/// - `YtVideo` — a **delegated-playback** reference, custom multicodec
///   `yt-video` `0x1008`. Like `CardLocator` it is always paired with
///   `bytes: None` and lands in the `(None, Some)` metadata-only branch, but it
///   means something stronger than "no bytes yet": the bytes **exist and are
///   permanently someone else's**. Nothing is fetched, seeded or
///   content-addressed, and meta-share's byte path REFUSES the cid with a `400`
///   rather than 404ing. Produced by `hash::compute_yt_video_cid`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashKind {
    /// Reserved: no feeder currently emits midhash256 outcomes, but the family
    /// is real across meta-core / meta-share, so the variant stays.
    #[allow(dead_code)]
    Midhash256,
    Sha2_256,
    BtV1File,
    NzbRelease,
    CardLocator,
    NzbPosting,
    YtVideo,
}

/// Startup-only configuration error returned by [`FeederPlugin::configure`].
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("plugin {plugin}: required config missing: {what}")]
    MissingConfig {
        plugin: &'static str,
        /// Human-readable description of the missing setting and where to
        /// supply it. Used by the harness's soft-skip warning.
        what: &'static str,
    },

    #[error("plugin {plugin}: cache dir setup failed: {source}")]
    CacheSetup {
        plugin: &'static str,
        #[source]
        source: std::io::Error,
    },

    #[error("plugin {plugin}: {source}")]
    Other {
        plugin: &'static str,
        #[source]
        source: anyhow::Error,
    },
}

/// Canonical field name for an upstream's stable per-record identifier.
/// Every plugin's `DiscoveryRecord.fields` MUST include this key
/// (`gutenbergid:11`, `tmdbid:31910`, `arxivid:2106.07447`). The rule is
/// `format!("{upstream_id}id")` — lowercase, no separator.
pub fn upstream_id_field(upstream_id: &str) -> String {
    format!("{upstream_id}id")
}

/// Key-set prefix for the provenance `source/<label>` field (METADATA_KEYS §5).
/// Each member is its own hash field (`source/gateway:nyaa.si = "true"`) so two
/// producers that reach the same content by different routes union without a
/// last-writer-wins clobber. The `<label>` uses `:` internally (not `/`, which
/// is the key-set path separator) — e.g. `gateway:nyaa.si`, `gateway:tribler`.
pub const SOURCE_KEYSET_PREFIX: &str = "source/";

/// Stamp the SDK's default provenance member — `source/gateway:<upstream_id>` —
/// onto a record's fields, but **only if it carries no `source/*` member yet**.
/// This gives every gateway hit a provenance label for free (so meta-watch never
/// falls back to "unknown" for a feeder record), while a plugin that knows a
/// finer origin (torznab → the specific Prowlarr indexer) stamps its own flat
/// `source/*` facets first (`source/gateway`, `source/<protocol>`,
/// `source/<indexer-slug>`) and this default then no-ops.
pub fn stamp_default_source(
    fields: &mut std::collections::BTreeMap<String, String>,
    upstream_id: &str,
) {
    if fields.keys().any(|k| k.starts_with(SOURCE_KEYSET_PREFIX)) {
        return;
    }
    fields.insert(
        format!("{SOURCE_KEYSET_PREFIX}gateway:{upstream_id}"),
        "true".to_string(),
    );
}

/// Static plugin contract. Each enabled `upstream_id` is implemented by
/// exactly one `Box<dyn FeederPlugin>`. Lives behind `&self` in steady state —
/// called concurrently, owns any interior mutability for caches.
///
/// **Field-naming convention.** Every record a plugin returns MUST include the
/// canonical `<upstream_id>id` field — see [`upstream_id_field`].
#[async_trait]
pub trait FeederPlugin: Send + Sync + 'static {
    fn upstream_id(&self) -> &'static str;

    fn configure(&mut self, cache_dir: &Path) -> Result<(), ConfigError>;

    async fn handle_query(
        &self,
        query: &GatewayQuery,
        max_results: usize,
    ) -> Result<Vec<DiscoveryRecord>, GatewayError>;

    /// Streaming variant of [`handle_query`]. The default impl collects
    /// [`handle_query`] and replays it as `Base*` + `Done` — correct for every
    /// plugin with no incremental enrichment (gutenberg, arxiv, …). torznab
    /// overrides it to decouple base records from rate-limited TMDB enrichment.
    async fn handle_query_stream(
        &self,
        query: &GatewayQuery,
        max_results: usize,
    ) -> Result<BoxStream<'static, GatewaySearchEvent>, GatewayError> {
        let records = self.handle_query(query, max_results).await?;
        let events = records
            .into_iter()
            .map(GatewaySearchEvent::Base)
            .chain(std::iter::once(GatewaySearchEvent::Done));
        Ok(Box::pin(futures::stream::iter(events)))
    }

    /// Resolve `record_id` into one or more content-addressed outcomes.
    ///
    /// - `Err(_)` — the upstream record itself is unresolvable.
    /// - `Ok(vec![])` — resolved but yielded zero outcomes.
    /// - `Ok(partial_vec)` — bundle-success: per-sibling failures are
    ///   best-effort (log + drop), surface whatever succeeded.
    async fn compute_outcomes(&self, record_id: &str) -> Result<Vec<HashOutcome>, GatewayError>;

    async fn handle_fetch(&self, _record_id: &str) -> Result<Option<ByteStream>, GatewayError> {
        Ok(None)
    }

    fn health(&self) -> PluginHealth {
        PluginHealth::Ok
    }

    fn served_file_types(&self) -> &'static [&'static str] {
        &[]
    }

    fn served_content_kinds(&self) -> &'static [&'static str] {
        &[]
    }

    /// Self-describing config schema. The feeder SDK serves this at
    /// `GET /config/schema` and renders a generic form from it — the gateway and
    /// UI carry no per-plugin field knowledge. Default: no configuration.
    fn config_schema(&self) -> crate::config::ConfigSchema {
        crate::config::ConfigSchema::default()
    }

    /// The plugin's current *effective* config as JSON, **unredacted** (secrets
    /// included). The SDK redacts it before serving `GET /config/values` and uses
    /// it as the merge base on the first save (before any `config.json` exists).
    /// Default: empty object. Implementors should mirror the keys in
    /// [`config_schema`](FeederPlugin::config_schema).
    fn config_values(&self) -> serde_json::Value {
        serde_json::json!({})
    }

    async fn get_blob(&self, _cid: &str) -> Option<Vec<u8>> {
        None
    }
}

/// Static plugin registry. Built once at startup; read-only thereafter.
/// Keyed by `upstream_id`.
pub type PluginRegistry = HashMap<&'static str, Box<dyn FeederPlugin>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_error_messages_include_plugin_name() {
        let e = ConfigError::MissingConfig {
            plugin: "scihub",
            what:
                "mirrors (set them in the dashboard or gateway-config.json plugins.scihub.mirrors)",
        };
        let msg = e.to_string();
        assert!(msg.contains("scihub"));
        assert!(msg.contains("mirrors"));
    }
}
