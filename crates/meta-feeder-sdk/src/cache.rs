//! Per-plugin midhash cache backed by redb.
//!
//! Each plugin gets its own redb file at
//! `<state_dir>/gateway/<upstream_id>/cache.redb`. Keys are upstream
//! `record_id`s (DOI for sci-hub, book id for gutenberg, …); values are the
//! `Midhash` strings the plugin computed by fetching + hashing the upstream
//! file.
//!
//! The schema is identical to meta-share v1's so old `.redb` files copy
//! across. redb is synchronous and sub-millisecond for small ops, so calls
//! run inline from async handlers without `spawn_blocking`.

use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableTableMetadata, TableDefinition};

pub const CACHE_FILENAME: &str = "cache.redb";

const MIDHASH_TABLE: TableDefinition<'_, &str, &str> = TableDefinition::new("midhash");
const BLOBS_TABLE: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("blobs");
const COVER_CID_TABLE: TableDefinition<'_, &str, &str> = TableDefinition::new("cover_cid");
const BIBREC_TABLE: TableDefinition<'_, &str, &str> = TableDefinition::new("bibrec");
const PREVIEW_CID_TABLE: TableDefinition<'_, &str, &str> = TableDefinition::new("preview_cid");
/// Full-file sha2-256 IPFS cid for a previously-fetched torrent payload
/// (torznab's bt-fetch path). Distinct from `MIDHASH_TABLE` because the
/// hash families differ: `midhash` is the synthetic midhash256-from-infohash
/// fallback; `fullhash` records "we actually downloaded the bytes and this is
/// the IPFS CIDv1 over them."
const FULLHASH_TABLE: TableDefinition<'_, &str, &str> = TableDefinition::new("fullhash");
/// Enumerated torrent file list (JSON `Vec<TorrentFile>`) keyed by the
/// torrent's `record_id` (infohash).
const FILELIST_TABLE: TableDefinition<'_, &str, &str> = TableDefinition::new("filelist");
/// Cached TMDB *search* results keyed by a stable lookup key. The value is the
/// JSON encoding of the top `TmdbHit`, or the literal `"null"` negative-cache
/// sentinel.
const TMDB_SEARCH_TABLE: TableDefinition<'_, &str, &str> = TableDefinition::new("tmdb_search");
/// Cached TMDB `GET /3/tv/{id}` structural details keyed by `tmdbid`.
const TMDB_TVDETAILS_TABLE: TableDefinition<'_, &str, &str> =
    TableDefinition::new("tmdb_tvdetails");
/// Cached TMDB `GET /3/{tv,movie}/{id}/external_ids` keyed by `tmdbid`.
const TMDB_EXTIDS_TABLE: TableDefinition<'_, &str, &str> = TableDefinition::new("tmdb_extids");
/// Cached TMDB `GET /3/movie/{id}` details keyed by `tmdbid`.
const TMDB_MOVIEDETAILS_TABLE: TableDefinition<'_, &str, &str> =
    TableDefinition::new("tmdb_moviedetails");
/// Cached **ranked** TMDB `search/multi` anchors keyed by the normalized
/// free-text query.
const TMDB_PRINCIPAL_TOPN_TABLE: TableDefinition<'_, &str, &str> =
    TableDefinition::new("tmdb_principal_topn");
/// Subtitle linkage discovered at search-enrich time, keyed by the torrent's
/// `record_id` (infohash). Value is the JSON encoding of a `Vec<SubtitleLink>`.
const SUBTITLES_TABLE: TableDefinition<'_, &str, &str> = TableDefinition::new("subtitles");
/// OpenSubtitles search results keyed by `"<tmdb_id>\x01<lang3>"`. Value is the
/// resolved subtitle's cid, or the literal `"null"` negative-cache sentinel.
const OPENSUBTITLES_TABLE: TableDefinition<'_, &str, &str> = TableDefinition::new("opensubtitles");
/// Small durable plugin scratch values that must survive a restart but aren't
/// content-derived — today, the torznab plugin's Prowlarr indexer-set
/// fingerprint, so a set that changed *while the feeder was down* is still
/// detected as a change on the next boot. Keys are plugin-namespaced
/// (`prowlarr:indexer-set`). redb creates the table on open, so an older
/// `.redb` file gains it transparently.
const MISC_TABLE: TableDefinition<'_, &str, &str> = TableDefinition::new("misc");

/// Per-plugin midhash cache. Cheap to clone (the inner `Database` is shared
/// via `Arc`).
#[derive(Clone)]
pub struct MidhashCache {
    db: Arc<Database>,
}

/// Generate the standard `get`/`put` accessor pair for a `&str → &str` redb
/// table.
macro_rules! str_table_accessors {
    ($(#[$gmeta:meta])* $get:ident, $(#[$pmeta:meta])* $put:ident, $table:ident $(,)?) => {
        $(#[$gmeta])*
        pub fn $get(&self, key: &str) -> Result<Option<String>, redb::Error> {
            let tx = self.db.begin_read()?;
            let table = tx.open_table($table)?;
            Ok(table.get(key)?.map(|v| v.value().to_string()))
        }

        $(#[$pmeta])*
        pub fn $put(&self, key: &str, value: &str) -> Result<(), redb::Error> {
            let tx = self.db.begin_write()?;
            {
                let mut table = tx.open_table($table)?;
                table.insert(key, value)?;
            }
            tx.commit()?;
            Ok(())
        }
    };
}

impl MidhashCache {
    pub fn open(cache_dir: &Path) -> Result<Self, redb::Error> {
        let path = cache_dir.join(CACHE_FILENAME);
        let db = Database::create(&path)?;
        {
            let tx = db.begin_write()?;
            tx.open_table(MIDHASH_TABLE)?;
            tx.open_table(BLOBS_TABLE)?;
            tx.open_table(COVER_CID_TABLE)?;
            tx.open_table(BIBREC_TABLE)?;
            tx.open_table(PREVIEW_CID_TABLE)?;
            tx.open_table(FULLHASH_TABLE)?;
            tx.open_table(FILELIST_TABLE)?;
            tx.open_table(TMDB_SEARCH_TABLE)?;
            tx.open_table(TMDB_TVDETAILS_TABLE)?;
            tx.open_table(TMDB_EXTIDS_TABLE)?;
            tx.open_table(TMDB_MOVIEDETAILS_TABLE)?;
            tx.open_table(TMDB_PRINCIPAL_TOPN_TABLE)?;
            tx.open_table(SUBTITLES_TABLE)?;
            tx.open_table(OPENSUBTITLES_TABLE)?;
            tx.open_table(MISC_TABLE)?;
            tx.commit()?;
        }
        Ok(Self { db: Arc::new(db) })
    }

    str_table_accessors!(get_midhash, put_midhash, MIDHASH_TABLE);

    str_table_accessors!(
        /// Read a durable plugin scratch value (see [`MISC_TABLE`]).
        get_misc,
        /// Write a durable plugin scratch value (see [`MISC_TABLE`]).
        put_misc,
        MISC_TABLE,
    );

    pub fn entry_count(&self) -> Result<u64, redb::Error> {
        let tx = self.db.begin_read()?;
        let table = tx.open_table(MIDHASH_TABLE)?;
        Ok(table.len()?)
    }

    pub fn get_blob(&self, cid: &str) -> Result<Option<Vec<u8>>, redb::Error> {
        let tx = self.db.begin_read()?;
        let table = tx.open_table(BLOBS_TABLE)?;
        Ok(table.get(cid)?.map(|v| v.value().to_vec()))
    }

    pub fn put_blob(&self, cid: &str, bytes: &[u8]) -> Result<(), redb::Error> {
        let tx = self.db.begin_write()?;
        {
            let mut table = tx.open_table(BLOBS_TABLE)?;
            table.insert(cid, bytes)?;
        }
        tx.commit()?;
        Ok(())
    }

    str_table_accessors!(get_cover_cid, put_cover_cid, COVER_CID_TABLE);

    str_table_accessors!(
        /// Read a previously-stored full-file IPFS cid for `record_id`.
        get_fullhash,
        /// Record the IPFS cid produced by a successful full fetch.
        put_fullhash,
        FULLHASH_TABLE
    );

    str_table_accessors!(
        /// Read the cached torrent file list (JSON) for `record_id`.
        get_filelist,
        /// Cache the enumerated torrent file list (JSON `Vec<TorrentFile>`).
        put_filelist,
        FILELIST_TABLE
    );

    str_table_accessors!(
        /// Read the cached subtitle linkage (JSON `Vec<SubtitleLink>`).
        get_subtitles,
        /// Cache the discovered subtitle linkage (JSON `Vec<SubtitleLink>`).
        put_subtitles,
        SUBTITLES_TABLE
    );

    str_table_accessors!(
        /// Read a cached OpenSubtitles lookup by `"<tmdb_id>\x01<lang3>"`.
        get_opensubtitles,
        /// Cache an OpenSubtitles lookup result (cid or `"null"` sentinel).
        put_opensubtitles,
        OPENSUBTITLES_TABLE
    );

    str_table_accessors!(get_preview_cid, put_preview_cid, PREVIEW_CID_TABLE);

    str_table_accessors!(
        /// Read a cached TMDB search result by lookup key.
        get_tmdb_search,
        /// Cache a TMDB search result under `key`.
        put_tmdb_search,
        TMDB_SEARCH_TABLE
    );

    str_table_accessors!(
        /// Read cached TMDB TV-details JSON by `tmdbid`.
        get_tmdb_tvdetails,
        /// Cache TMDB TV-details JSON under `tmdbid`.
        put_tmdb_tvdetails,
        TMDB_TVDETAILS_TABLE
    );

    str_table_accessors!(
        /// Read cached TMDB external-ids JSON by `tmdbid`.
        get_tmdb_extids,
        /// Cache TMDB external-ids JSON under `tmdbid`.
        put_tmdb_extids,
        TMDB_EXTIDS_TABLE
    );

    str_table_accessors!(
        /// Read cached TMDB movie-details JSON by `tmdbid`.
        get_tmdb_moviedetails,
        /// Cache TMDB movie-details JSON under `tmdbid`.
        put_tmdb_moviedetails,
        TMDB_MOVIEDETAILS_TABLE
    );

    str_table_accessors!(
        /// Read the cached ranked anchor list by normalized query key.
        get_tmdb_principal_topn,
        /// Cache the ranked anchor list under the normalized query key.
        put_tmdb_principal_topn,
        TMDB_PRINCIPAL_TOPN_TABLE
    );

    pub fn get_bibrec(
        &self,
        record_id: &str,
    ) -> Result<Option<std::collections::BTreeMap<String, String>>, redb::Error> {
        let tx = self.db.begin_read()?;
        let table = tx.open_table(BIBREC_TABLE)?;
        let Some(v) = table.get(record_id)? else {
            return Ok(None);
        };
        let s = v.value();
        Ok(serde_json::from_str(s).ok())
    }

    pub fn put_bibrec(
        &self,
        record_id: &str,
        fields: &std::collections::BTreeMap<String, String>,
    ) -> Result<(), redb::Error> {
        let json = serde_json::to_string(fields).unwrap_or_else(|_| "{}".to_string());
        let tx = self.db.begin_write()?;
        {
            let mut table = tx.open_table(BIBREC_TABLE)?;
            table.insert(record_id, json.as_str())?;
        }
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_cache_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    #[test]
    fn put_then_get_returns_value() {
        let dir = tmp_cache_dir();
        let cache = MidhashCache::open(dir.path()).expect("open");
        assert_eq!(cache.get_midhash("10.1038/x").unwrap(), None);
        cache.put_midhash("10.1038/x", "bafyXYZ").expect("put");
        assert_eq!(
            cache.get_midhash("10.1038/x").unwrap().as_deref(),
            Some("bafyXYZ")
        );
    }

    #[test]
    fn put_overwrites_existing_value() {
        let dir = tmp_cache_dir();
        let cache = MidhashCache::open(dir.path()).expect("open");
        cache.put_midhash("k", "v1").unwrap();
        cache.put_midhash("k", "v2").unwrap();
        assert_eq!(cache.get_midhash("k").unwrap().as_deref(), Some("v2"));
    }

    #[test]
    fn values_persist_across_reopens() {
        let dir = tmp_cache_dir();
        {
            let cache = MidhashCache::open(dir.path()).expect("open 1");
            cache.put_midhash("k", "persistent").unwrap();
        }
        let cache = MidhashCache::open(dir.path()).expect("open 2");
        assert_eq!(
            cache.get_midhash("k").unwrap().as_deref(),
            Some("persistent")
        );
    }

    #[test]
    fn tmdb_search_cache_distinguishes_miss_from_absent() {
        let dir = tmp_cache_dir();
        let cache = MidhashCache::open(dir.path()).expect("open");
        assert_eq!(cache.get_tmdb_search("m\u{1}naruto\u{1}").unwrap(), None);
        cache.put_tmdb_search("m\u{1}naruto\u{1}", "null").unwrap();
        assert_eq!(
            cache
                .get_tmdb_search("m\u{1}naruto\u{1}")
                .unwrap()
                .as_deref(),
            Some("null")
        );
        cache
            .put_tmdb_search("t\u{1}spy x family\u{1}2019", "{\"tmdbid\":120089}")
            .unwrap();
        drop(cache);
        let cache = MidhashCache::open(dir.path()).expect("reopen");
        assert_eq!(
            cache
                .get_tmdb_search("t\u{1}spy x family\u{1}2019")
                .unwrap()
                .as_deref(),
            Some("{\"tmdbid\":120089}")
        );
    }

    #[test]
    fn tmdb_tvdetails_cache_roundtrips() {
        let dir = tmp_cache_dir();
        let cache = MidhashCache::open(dir.path()).expect("open");
        assert_eq!(cache.get_tmdb_tvdetails("120089").unwrap(), None);
        cache
            .put_tmdb_tvdetails("120089", "{\"number_of_seasons\":2}")
            .unwrap();
        assert_eq!(
            cache.get_tmdb_tvdetails("120089").unwrap().as_deref(),
            Some("{\"number_of_seasons\":2}")
        );
    }

    #[test]
    fn entry_count_reflects_inserts() {
        let dir = tmp_cache_dir();
        let cache = MidhashCache::open(dir.path()).expect("open");
        assert_eq!(cache.entry_count().unwrap(), 0);
        cache.put_midhash("a", "x").unwrap();
        cache.put_midhash("b", "y").unwrap();
        assert_eq!(cache.entry_count().unwrap(), 2);
        cache.put_midhash("a", "x2").unwrap();
        assert_eq!(cache.entry_count().unwrap(), 2);
    }
}
