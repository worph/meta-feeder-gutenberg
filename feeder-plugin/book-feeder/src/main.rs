//! `book-feeder` — the book tier's feeder sidecar.
//!
//! Hosts two plugins over the feeder HTTP contract via
//! [`meta_feeder_sdk::serve_feeders`]:
//! - [`GutenbergPlugin`] — Project Gutenberg editions (`document` × `book`);
//! - [`OpenLibraryPlugin`] — Open Library work cards (`card` × `book`).
//!
//! The gateway core reaches it server-to-server on the internal docker network.
//!
//! Env:
//! - `META_FEEDER_HTTP_LISTEN` — listen addr (default `0.0.0.0:8080`)
//! - `META_FEEDER_STATE_DIR`   — per-plugin cache root (default `/data/meta-feeder`)
//! - `OPENLIBRARY_CONTACT`     — e-mail/URL sent in the Open Library User-Agent;
//!   first-boot seed for the `contact` config field (the saved field wins)
//! - `OPENLIBRARY_TOP_N`, `OPENLIBRARY_DISCOVERY_N` — optional card result sizing
//! - `RUST_LOG`                — tracing filter (default `info`)

use std::net::SocketAddr;
use std::path::Path;

use book_feeder::gutenberg::GutenbergPlugin;
use book_feeder::openlibrary::OpenLibraryPlugin;
use book_feeder::openlibrary_client;
use meta_feeder_sdk::plugin::FeederPlugin;
use meta_feeder_sdk::serve_feeders;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let listen: SocketAddr = std::env::var("META_FEEDER_HTTP_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()?;
    let state_dir =
        std::env::var("META_FEEDER_STATE_DIR").unwrap_or_else(|_| "/data/meta-feeder".to_string());

    // ONE budget for both plugins: the Gutenberg → Open Library binding and the
    // card plugin call the same upstream, which rate-limits per client, not per
    // plugin. Two buckets would each think they had the whole allowance.
    let contact =
        openlibrary_client::load_contact(&Path::new(&state_dir).join("gateway").join("openlibrary"));
    let budget = openlibrary_client::budget(contact.is_some());

    let plugins: Vec<Box<dyn FeederPlugin>> = vec![
        Box::new(GutenbergPlugin::new().with_openlibrary_budget(budget.clone())),
        Box::new(OpenLibraryPlugin::new().with_budget(budget)),
    ];
    serve_feeders(plugins, state_dir, listen).await
}
