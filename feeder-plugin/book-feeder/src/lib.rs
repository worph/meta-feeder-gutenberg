//! `book-feeder` library surface — exposes the plugins so integration tests
//! (and, later, other binaries) can construct them. The binary (`main.rs`) is a
//! thin wrapper over [`meta_feeder_sdk::serve_feeders`].
//!
//! Two upstreams:
//! - [`gutenberg`] — Project Gutenberg editions, the byte tier (`document` × `book`);
//! - [`openlibrary`] — Open Library work cards, the identity tier (`card` × `book`),
//!   over the client in [`openlibrary_client`], which `gutenberg` also uses to
//!   bind an edition to its work.

pub mod gutenberg;
pub mod openlibrary;
pub mod openlibrary_client;
