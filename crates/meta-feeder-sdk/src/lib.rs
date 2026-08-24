//! `meta-feeder-sdk` — the source-agnostic foundation shared by the gateway
//! core and every feeder sidecar binary.
//!
//! A *feeder* implements [`FeederPlugin`] (find records, fetch bytes) for one
//! or more upstreams, and [`serve_feeders`] exposes them over the feeder HTTP
//! contract. The gateway core consumes that contract via its
//! `RemoteFeederPlugin` and keeps the libp2p wire, the bitswap blockstore, the
//! hashing-into-the-blockstore, and the meta-core store-back to itself.
//!
//! Deliberately libp2p-free and blockstore-free — see this crate's `Cargo.toml`.

// `redb::Error` is a large enum; the cache wrappers (`cache.rs`) return it
// directly rather than boxing on every embedded-DB call. Boxing each would be
// pure noise for a single-process store, so silence the lint crate-wide.
#![allow(clippy::result_large_err)]

/// pcs-convention default meta-core root a self-publishing feeder targets when
/// neither its dashboard config nor the `META_CORE_URL` env seed sets one. Keeps
/// the modular storage seam out of compose: a standard install just works; a
/// non-standard meta-core is set in the feeder's config UI. Matches the
/// `metacore-app` service the AppStore feeder apps ship on the shared network.
pub const DEFAULT_META_CORE_URL: &str = "http://metacore-app:9000";

pub mod cache;
pub mod common;
pub mod config;
pub mod enrich;
pub mod filename_meta;
pub mod hash;
pub mod lang;
pub mod meta_core;
pub mod plugin;
pub mod query;
pub mod query_eval;
pub mod serve;
pub mod types;

pub use config::{ConfigField, ConfigSchema, FieldKind};
pub use enrich::{EnrichTarget, EnrichmentConfig, Enricher};
pub use meta_core::FeederStore;
pub use plugin::{
    upstream_id_field, ConfigError, FeederPlugin, HashKind, HashOutcome, PluginRegistry,
};
pub use query::{GatewayQuery, GatewaySearchEvent, GatewayWireError, Negation, RangeFilter};
pub use serve::{
    configure_plugins, router, serve_feeders, ComputeRequest, ComputeResponse, HashKindDto,
    HealthResponse, ManifestResponse, OutcomeDto, PluginManifest, QueryRequest, QueryResponse,
};
pub use types::{ByteStream, DiscoveryId, DiscoveryRecord, GatewayError, Hash, PluginHealth};
