//! Content-addressed identifiers used by feeder plugins' `compute_outcomes`
//! to hash upstream-fetched bytes.
//!
//! Two function families live here:
//!
//! - [`compute_midhash256`] / [`compute_midhash256_from_sample`] —
//!   the size-prefix-plus-middle-1MB-sample identification primitive shared
//!   with `meta-sort` / `meta-share` / meta-core. Custom multicodec `0x1000`.
//!   Fast but not IPFS-interop. The `_from_sample` entrypoint takes the
//!   middle slice directly so plugins that can only fetch a partial range
//!   (BitTorrent BEP 9, sliced HTTP) don't have to buffer the whole file
//!   just to throw most of it away. **Rust-only by construction.**
//! - [`compute_ipfs_cid`] is the standard IPFS CIDv1 produced over the full
//!   bytes — raw codec `0x55` for a single-chunk file, UnixFS dag-pb codec
//!   `0x70` rooted on chunked raw-codec leaves for larger files. Output matches
//!   `ipfs add --cid-version=1 --raw-leaves=true --chunker=size-262144
//!   --hash=sha2-256 <file>`.
//!
//! Cross-implementation parity for `compute_midhash256` with the TypeScript
//! `FastHash.ts` is load-bearing — keep the fixture tests pinned, a regression
//! silently breaks dedupe across the platform.

use bytes::Bytes;
use sha2::{Digest, Sha256};

/// Sample window in bytes (1 MiB). Files this size or smaller are hashed
/// in full; larger files are hashed over the middle 1 MiB slice.
const SAMPLE_SIZE: usize = 1024 * 1024;

/// Per-block IPFS chunk size (256 KiB), matches kubo's
/// `--chunker=size-262144` default.
pub const IPFS_CHUNK_SIZE: usize = 256 * 1024;

/// dag-pb fanout — children per internal node. Matches kubo's balanced
/// builder default.
pub const IPFS_FANOUT: usize = 174;

/// Output of [`compute_ipfs_blocks`]. Carries the root CID **plus every
/// intermediate block** (leaf chunks AND internal dag-pb nodes) keyed by
/// their own CID. Pass `.blocks` to the gateway's blockstore so peers
/// can fetch the file by CID via bitswap.
#[derive(Debug, Clone)]
pub struct IpfsBlocks {
    /// Canonical "bafy…" / "bafk…" root cid string. Identical to what
    /// [`compute_ipfs_cid`] returns for the same input.
    pub root: String,
    /// Every block — root included — keyed by its own cid. For a
    /// single-leaf file, this is a one-entry vec `[(root, payload)]`
    /// (raw codec, payload is the file bytes). For multi-leaf files,
    /// it is `leaves ++ internal_nodes` in build order; the **last**
    /// entry is the root.
    pub blocks: Vec<(String, Bytes)>,
}

/// Compute a midhash256 CID matching the TypeScript implementation in
/// `meta-hash/src/lib/file-id/FastHash.ts`:
///
/// 1. Build the hash input as `[size:u64-be][middle 1 MiB sample]` (or the
///    whole file if it fits in 1 MiB).
/// 2. SHA-256 it.
/// 3. Wrap in CIDv1 with custom multicodec `0x1000` for both the codec field
///    and the multihash hash-code field, length 32, then multibase-base32-lower
///    with the `b` prefix.
///
/// **Endianness for the size prefix is big-endian** to match TS's
/// `writeBigUInt64BE`. Cross-impl mismatch here would silently mangle every
/// import — keep the test pinned.
pub fn compute_midhash256(bytes: &[u8]) -> String {
    let size = bytes.len();
    let sample: &[u8] = if size <= SAMPLE_SIZE {
        bytes
    } else {
        let start = (size - SAMPLE_SIZE) / 2;
        &bytes[start..start + SAMPLE_SIZE]
    };
    compute_midhash256_from_sample(size as u64, sample)
}

/// Same midhash256 primitive as [`compute_midhash256`], but takes the
/// sample bytes directly rather than slicing them out of a full file.
/// For upstreams that can only deliver a middle range (BitTorrent BEP 9,
/// sliced HTTP), this avoids forcing the caller to buffer the whole file
/// in memory just to throw most of it away.
///
/// `total_size` is the **full file size in bytes** — used as the
/// size prefix, NOT `middle_sample.len()`.
pub fn compute_midhash256_from_sample(total_size: u64, middle_sample: &[u8]) -> String {
    let size_be = total_size.to_be_bytes();
    let mut hasher = Sha256::new();
    hasher.update(size_be);
    hasher.update(middle_sample);
    let digest: [u8; 32] = hasher.finalize().into();

    // Multicodec 0x1000 encoded as unsigned varint = [0x80, 0x20].
    // CIDv1 layout: [version=0x01][codec varint][multihash code varint][len][digest]
    let mut wire = Vec::with_capacity(38);
    wire.push(0x01);
    wire.extend_from_slice(&[0x80, 0x20]);
    wire.extend_from_slice(&[0x80, 0x20]);
    wire.push(0x20);
    wire.extend_from_slice(&digest);

    format!("b{}", base32_lower_no_padding(&wire))
}

/// Wrap a 20-byte BitTorrent v1 info-hash as a multibase CIDv1.
///
/// BT v1 info-hashes ARE SHA-1 digests of the bencoded info dict — so
/// the natural CID form is `codec=raw (0x55)` + `multihash=sha1 (0x11),
/// len=20, digest=<infohash bytes>`.
pub fn compute_bt_info_cid(infohash_20: &[u8; 20]) -> String {
    // CIDv1 layout: [version=0x01][codec varint][multihash code varint][len][digest]
    let mut wire = Vec::with_capacity(4 + 20);
    wire.push(0x01); // CIDv1 version
    wire.push(0x55); // codec: raw
    wire.push(0x11); // multihash: sha1
    wire.push(0x14); // multihash length: 20
    wire.extend_from_slice(infohash_20);
    format!("b{}", base32_lower_no_padding(&wire))
}

/// Custom multicodec for a single file inside a **BitTorrent v1** torrent
/// (`btih-v1-file`). MetaMesh-private, adjacent to midhash256's `0x1000`.
pub const BTIH_V1_FILE_CODEC: u64 = 0x1001;

/// Custom multicodec for a single file inside a **BitTorrent v2** torrent
/// (`btih-v2-file`).
pub const BTIH_V2_FILE_CODEC: u64 = 0x1002;

/// Encode a `btih-v1-file` CID — a single file inside a BT v1 torrent,
/// addressed by the 20-byte v1 infohash (SHA-1 of the bencoded `info`
/// dict) plus the zero-based file index in `info.files`. Single-file
/// torrents use index `0`.
pub fn compute_bt_v1_file_cid(infohash_20: &[u8; 20], file_index: u64) -> String {
    bt_file_cid(BTIH_V1_FILE_CODEC, infohash_20, file_index)
}

/// Encode a `btih-v2-file` CID — a single file inside a BT v2 torrent
/// (BEP 52), addressed by the 32-byte v2 infohash plus the file's
/// canonical traversal index in `file tree`.
pub fn compute_bt_v2_file_cid(infohash_32: &[u8; 32], file_index: u64) -> String {
    bt_file_cid(BTIH_V2_FILE_CODEC, infohash_32, file_index)
}

/// Shared encoder for the two torrent-file CID families. `infohash` is 20
/// bytes for v1 / 32 for v2; `codec` selects the family.
fn bt_file_cid(codec: u64, infohash: &[u8], file_index: u64) -> String {
    // digest = infohash ‖ varint(file_index)
    let mut digest = Vec::with_capacity(infohash.len() + 2);
    digest.extend_from_slice(infohash);
    write_pb_varint(file_index, &mut digest);

    // CIDv1: [version][codec varint][multihash code varint][len varint][digest]
    let mut wire = Vec::with_capacity(1 + 4 + digest.len());
    wire.push(0x01);
    write_pb_varint(codec, &mut wire);
    write_pb_varint(codec, &mut wire);
    write_pb_varint(digest.len() as u64, &mut wire);
    wire.extend_from_slice(&digest);
    format!("b{}", base32_lower_no_padding(&wire))
}

/// Custom multicodec for a self-describing **Newznab release** locator
/// (`nzb-release`). MetaMesh-private, adjacent to the torrent-file codecs in the
/// `0x10xx` range. Unlike a content hash, the cid *embeds* the indexer host +
/// release id in an identity multihash, so the credentialed meta-share peer
/// decodes `{host,id}` straight from the cid and grabs the `.nzb` via `t=get`
/// only at playback — no side-table, no `.nzb` download at search time. (The
/// `0x1004` `sha256(host‖id)` opaque hash and the `0x1003` `nzb-posting`
/// variant were both removed; only this self-describing form is emitted.)
pub const NZB_RELEASE_CODEC: u64 = 0x1005;

/// Digest byte ceiling for an `nzb-release` cid, matching meta-share's
/// `MAX_MULTIHASH_SIZE` — its `CidGeneric<64>` rejects a longer multihash.
const NZB_RELEASE_MAX_DIGEST: usize = 64;

/// Encode a self-describing `nzb-release` CID from a Newznab indexer's host +
/// bare release id (the hex `<guid>` id). The multihash is *identity* (`0x00`)
/// and its digest embeds the locator as `varint(host_len) ‖ host ‖ id_bytes`,
/// where `id_bytes` is the hex-decoded id. meta-share's `decode_nzb_release_cid`
/// is the exact inverse. Host-namespaced: the same release on two indexers
/// yields different cids.
///
/// Returns `None` when the id isn't hex or the digest would overflow the
/// multihash budget (an oversized host) — the caller drops that row.
pub fn compute_nzb_release_cid(api_base: &str, release_id: &str) -> Option<String> {
    let id_bytes = decode_hex_id(release_id)?;
    let host_b = api_base.as_bytes();

    // identity-multihash digest = varint(host_len) ‖ host ‖ id_bytes
    let mut digest = Vec::with_capacity(2 + host_b.len() + id_bytes.len());
    write_pb_varint(host_b.len() as u64, &mut digest);
    digest.extend_from_slice(host_b);
    digest.extend_from_slice(&id_bytes);
    if digest.len() > NZB_RELEASE_MAX_DIGEST {
        return None;
    }

    // CIDv1: [version=0x01][codec varint][mh code=0x00 identity][len varint][digest]
    let mut wire = Vec::with_capacity(1 + 3 + 2 + digest.len());
    wire.push(0x01);
    write_pb_varint(NZB_RELEASE_CODEC, &mut wire);
    wire.push(0x00); // multihash code: identity
    write_pb_varint(digest.len() as u64, &mut wire);
    wire.extend_from_slice(&digest);
    Some(format!("b{}", base32_lower_no_padding(&wire)))
}

/// Hex-decode a Newznab release id. The guid parser only keeps hex runs, so the
/// id is all-hex by construction; odd-length ids are left-padded with a `0`
/// nibble. Returns `None` on any non-hex byte (defensive).
fn decode_hex_id(id: &str) -> Option<Vec<u8>> {
    let padded;
    let s: &str = if id.len() % 2 == 1 {
        padded = format!("0{id}");
        &padded
    } else {
        id
    };
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks(2) {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out.push(((hi << 4) | lo) as u8);
    }
    Some(out)
}

/// Custom multicodec for a **card locator** — the identity of a *work* (a
/// series, a film) as published by a metadata bridge, rather than of any
/// bytes. MetaMesh-private, the next free slot after `0x1006` url. (`0x1004`
/// nzb-release-hash stays **retired — do not reuse**. `0x1003` was retired too,
/// but has since been **revived** for the self-scanned Usenet posting identity —
/// see [`NZB_POSTING_CODEC`] and `METADATA_KEYS.md` §2.)
///
/// This is the first CID family in the platform where **no bytes exist
/// anywhere, ever** — `0x1005`/`0x1006` are locators too, but both eventually
/// resolve to bytes (NNTP, HTTP), and `0x1000` midhash256 is a digest over
/// real file bytes. A card is pure identity: the record *is* the payload, and
/// it lives in meta-core. Consequences, all already handled:
///
/// * Bitswap seeding is gated on `HashKind::Sha2_256` (gateway invariant 6),
///   so a card is never offered to the swarm — nothing to seed.
/// * meta-share's byte path 404s for it, which is correct.
/// * It ranks `RANK_LOCATOR` (5), so it can never outrank a real digest for
///   `canonical_cid` — which is what makes the future "one card, two CIDs"
///   merge (a TMDB and a MyAnimeList locator on one meta-core record) safe.
/// The `url` locator codec (`0x1006`).
///
/// ⚠ Back-ported into this vendored SDK copy because the shared crate has it and
/// this one predates it. Keep it byte-identical to
/// `meta-feeder-sdk::hash::compute_url_cid`: it is how a record gets an address
/// without the feeder ever fetching bytes, and two encoders disagreeing would
/// mint two cids for one URL.
pub const URL_LOCATOR_CODEC: u64 = 0x1006;

/// Encode an `http(s)` URL as a `0x1006` identity-multihash CIDv1.
///
/// The digest IS the URL bytes, so this costs no I/O and no lookup — which is
/// what makes it usable at search time, on every record, before anything has
/// been fetched. Resolution happens later, on whichever peer wants the bytes.
///
/// `None` for anything that is not an absolute http(s) URL: a relative or
/// malformed value would encode fine and then fail unresolvably at read time,
/// which is a far more expensive way to find out.
pub fn compute_url_cid(url: &str) -> Option<String> {
    let u = url.trim();
    if !(u.starts_with("http://") || u.starts_with("https://")) {
        return None;
    }
    let digest = u.as_bytes();
    let mut wire = Vec::with_capacity(1 + 3 + 2 + digest.len());
    wire.push(0x01); // CIDv1
    write_pb_varint(URL_LOCATOR_CODEC, &mut wire);
    wire.push(0x00); // multihash code: identity
    write_pb_varint(digest.len() as u64, &mut wire);
    wire.extend_from_slice(digest);
    Some(format!("b{}", base32_lower_no_padding(&wire)))
}

pub const CARD_LOCATOR_CODEC: u64 = 0x1007;

/// Digest byte ceiling for a card locator, matching meta-share's
/// `MAX_MULTIHASH_SIZE` — its `CidGeneric<64>` rejects a longer multihash.
const CARD_LOCATOR_MAX_DIGEST: usize = 64;

/// Encode a self-describing **card locator** CID from a metadata source and
/// that source's own id for the work (`("tmdb", "tv:95479")`,
/// `("mal", "52991")`). The multihash is *identity* (`0x00`) and its digest
/// embeds the locator as `varint(source_len) ‖ source ‖ id`, both UTF-8 —
/// exactly the shape [`compute_nzb_release_cid`] uses, minus the hex decode
/// (a card id is arbitrary text, not a hex release id).
///
/// **The CID is a pure function of `(source, id)`**, which is the whole point:
/// any peer that knows a tmdb id can derive the card's address offline and
/// look it up locally, with no discovery round-trip. It also means the same
/// card discovered by two different peers converges on one meta-core record
/// for free, via the `cids/<bareCid>` reverse index.
///
/// Source-namespaced: the same work on TMDB and on MyAnimeList yields
/// different cids. Merging those two into one record is a meta-core alias
/// operation driven by an explicit cross-source id mapping — never inferred
/// here (see `meta-gateway/docs/others/card-tier-search.md` §9).
///
/// Returns `None` when `source` is empty or the digest would overflow the
/// multihash budget — the caller drops that card.
pub fn compute_card_cid(source: &str, id: &str) -> Option<String> {
    if source.is_empty() || id.is_empty() {
        return None;
    }
    let source_b = source.as_bytes();
    let id_b = id.as_bytes();

    // identity-multihash digest = varint(source_len) ‖ source ‖ id
    let mut digest = Vec::with_capacity(2 + source_b.len() + id_b.len());
    write_pb_varint(source_b.len() as u64, &mut digest);
    digest.extend_from_slice(source_b);
    digest.extend_from_slice(id_b);
    if digest.len() > CARD_LOCATOR_MAX_DIGEST {
        return None;
    }

    // CIDv1: [version=0x01][codec varint][mh code=0x00 identity][len varint][digest]
    let mut wire = Vec::with_capacity(1 + 3 + 2 + digest.len());
    wire.push(0x01);
    write_pb_varint(CARD_LOCATOR_CODEC, &mut wire);
    wire.push(0x00); // multihash code: identity
    write_pb_varint(digest.len() as u64, &mut wire);
    wire.extend_from_slice(&digest);
    Some(format!("b{}", base32_lower_no_padding(&wire)))
}

// ---------------------------------------------------------------------------
// Delegated-playback locators — `yt-video` (0x1008) and `ext-play` (0x1009)
// ---------------------------------------------------------------------------
//
// These two are the limit case *beyond* `card` (0x1007). `card` addresses a
// work with no bytes anywhere; these address a **rendition whose bytes exist
// but are permanently someone else's**. An external player renders the media,
// nothing is transported, nothing is content-addressed, nothing is re-seedable.
//
// See `docs/cid-formats.md` §7 and
// `docs/study/listenbrainz-youtube-playback-tier-2026-08-21.md`.

/// Custom multicodec for a **YouTube rendition** — delegated playback.
///
/// Its own codec rather than a `url` (`0x1006`) for three reasons, in
/// descending order of how much trouble the alternative causes:
///
/// * **`0x1006` means "fetch these bytes once and seed them".** Point a `url`
///   locator at a `watch` page and meta-share does exactly that: fetches the
///   HTML, chunks it, seeds it, and links the resulting content CID back onto
///   the record *as the file*. That is the MSR1 poisoning shape, except
///   permanent and replicated across peers. A resolved `googlevideo` URL is no
///   better — time-limited and IP-bound, so it is not a locator at all.
/// * **The id is canonical, a URL is not.** `youtube.com` / `music.youtube.com`
///   / `youtu.be` all address the same video, and query parameters are tracking
///   noise, so the same rendition would mint several different CIDs. Being a
///   pure function of `(kind, id)`, this codec makes two peers that resolve the
///   same track converge on one meta-core record for free, via the
///   `cids/<bareCid>` reverse index — the property that makes [`compute_card_cid`]
///   useful, for the same reason.
/// * **It is tiny** — 11 bytes of id plus a short kind prefix.
///
/// ⚠ **An immutable id is not durable playback.** Takedowns, geo-blocks and
/// embed-disabled uploads all kill a reference whose CID stays valid forever —
/// the same rot class as [`NZB_RELEASE_CODEC`]. The mitigation is at the
/// *record* level (carry several ranked references per recording so a client
/// can fall through), never at the CID level.
pub const YT_VIDEO_CODEC: u64 = 0x1008;

/// Custom multicodec for an **external page to open, never fetch**.
///
/// Wire-identical to `url` (`0x1006`) apart from the codec, and that difference
/// is the entire contract: `0x1006` resolves cache-through and lands in the
/// blockstore, `0x1009` **must be refused by the byte path** and only ever
/// surfaces as an "Open on …↗" affordance. Use it for anything worth linking
/// but not worth a per-provider codec (Bandcamp, Jamendo, an arbitrary stream
/// page).
pub const EXT_PLAY_CODEC: u64 = 0x1009;

/// Encode a [`YT_VIDEO_CODEC`] locator from a `kind` and a bare YouTube id.
///
/// `kind` is `"video"` | `"playlist"` | `"channel"`; `id` is the bare
/// identifier (`4D7u5KF7SP8`, `OLAK5uy_…`, `UCRr1xG_2WIDs18a6cIiCxeA`) — **not**
/// a URL, and not a `watch?v=` fragment. The framing is byte-identical to
/// [`compute_card_cid`] on purpose, so each of the seven rank/decode
/// implementations extends a parser it already wrote rather than learning a
/// novel shape.
///
/// Returns `None` on an empty field or a digest past the multihash budget —
/// the caller drops that reference.
pub fn compute_yt_video_cid(kind: &str, id: &str) -> Option<String> {
    kinded_locator_cid(YT_VIDEO_CODEC, kind, id)
}

/// Encode an [`EXT_PLAY_CODEC`] locator from an `http(s)` URL.
///
/// The scheme check is not cosmetic: it is what keeps a `javascript:` or
/// `data:` payload from ever reaching a client that will hand this string to
/// an anchor or a new tab.
pub fn compute_ext_play_cid(url: &str) -> Option<String> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return None;
    }
    let digest = url.as_bytes();
    if digest.is_empty() || digest.len() > CARD_LOCATOR_MAX_DIGEST {
        return None;
    }
    Some(identity_locator_cid(EXT_PLAY_CODEC, digest))
}

/// Shared encoder for the `varint(len(kind)) ‖ kind ‖ id` locator framing used
/// by [`compute_card_cid`] and [`compute_yt_video_cid`].
fn kinded_locator_cid(codec: u64, kind: &str, id: &str) -> Option<String> {
    if kind.is_empty() || id.is_empty() {
        return None;
    }
    let kind_b = kind.as_bytes();
    let id_b = id.as_bytes();

    let mut digest = Vec::with_capacity(2 + kind_b.len() + id_b.len());
    write_pb_varint(kind_b.len() as u64, &mut digest);
    digest.extend_from_slice(kind_b);
    digest.extend_from_slice(id_b);
    if digest.len() > CARD_LOCATOR_MAX_DIGEST {
        return None;
    }
    Some(identity_locator_cid(codec, &digest))
}

/// CIDv1 with an *identity* multihash:
/// `[version=0x01][codec varint][mh=0x00][len varint][digest]`, base32lower.
fn identity_locator_cid(codec: u64, digest: &[u8]) -> String {
    let mut wire = Vec::with_capacity(1 + 3 + 2 + digest.len());
    wire.push(0x01);
    write_pb_varint(codec, &mut wire);
    wire.push(0x00); // multihash code: identity
    write_pb_varint(digest.len() as u64, &mut wire);
    wire.extend_from_slice(digest);
    format!("b{}", base32_lower_no_padding(&wire))
}

/// Custom multicodec for a **Usenet posting** identity (`nzb-posting`),
/// minted from the article Message-IDs of a release we scanned ourselves.
///
/// Unlike [`NZB_RELEASE_CODEC`] (`0x1005`), this is **not** a locator: it
/// embeds no indexer host, so any peer with a plain NNTP provider can fetch the
/// articles — no `IndexerCred`, no `t=get` grab. That portability is the whole
/// point of running our own header scanner.
///
/// It is also **not** fetchable by cid: the digest is over the Message-ID set,
/// not over any block's bytes, so bitswap can never satisfy a want for it (the
/// receiver derives a block's cid by hashing the block — see
/// `beetswap::incoming_stream`). The `.nzb` manifest travels separately, as an
/// ordinary sha2-256 IPFS cid in the record's `manifest` field. See
/// `meta-gateway/docs/others/self-hosted-usenet-indexer-study.md` §5.
pub const NZB_POSTING_CODEC: u64 = 0x1003;

/// Mint an `nzb-posting` cid (`0x1003`) from a release's article Message-IDs.
///
/// # The normalisation rule IS the dedup contract
///
/// Mirrored verbatim in meta-share (`nzb/manifest.rs`), which re-derives this
/// digest from the fetched manifest and rejects a mismatch. **Any change here
/// must land on both sides together** — a drift silently makes every posting
/// unplayable. The rule:
///
/// 1. Trim each id, and strip a surrounding `<…>` pair if present (NZB
///    `<segment>` bodies carry the bare form, but be defensive — NNTP `BODY`
///    re-adds the brackets).
/// 2. Drop empties.
/// 3. Deduplicate, then **sort** lexicographically by bytes.
/// 4. Join with `\n` and SHA-256 the result.
///
/// Case is **preserved**: a Message-ID's local part is case-sensitive per RFC
/// 5322, so lowercasing could collide two genuinely distinct articles.
///
/// Sorting is what makes the cid survive segment/file reordering between two
/// NZBs of the same posting — the reason we hash the *id set* and never the raw
/// `.nzb` bytes (the retired `0x1004` did the latter and broke dedup; see
/// meta-share `nzb/mod.rs`).
///
/// **v1 scope:** every segment counts, par2 volumes included. Two NZBs for one
/// posting that disagree about whether to bundle par2 therefore mint different
/// cids. Acceptable while we are the only producer of these manifests; if a
/// second producer ever appears, the fix is to filter par2 subjects here *and*
/// in meta-share's mirror — do not do it on one side only.
pub fn compute_nzb_posting_cid<S: AsRef<str>>(message_ids: &[S]) -> String {
    let mut ids: Vec<&str> = message_ids
        .iter()
        .map(|s| {
            let t = s.as_ref().trim();
            t.strip_prefix('<')
                .and_then(|r| r.strip_suffix('>'))
                .unwrap_or(t)
        })
        .filter(|s| !s.is_empty())
        .collect();
    ids.sort_unstable();
    ids.dedup();

    let mut hasher = Sha256::new();
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            hasher.update(b"\n");
        }
        hasher.update(id.as_bytes());
    }
    let digest: [u8; 32] = hasher.finalize().into();

    // CIDv1: [version=0x01][codec varint][mh code=0x12 sha2-256][len=32][digest]
    let mut wire = Vec::with_capacity(1 + 3 + 2 + digest.len());
    wire.push(0x01);
    write_pb_varint(NZB_POSTING_CODEC, &mut wire);
    wire.push(0x12); // multihash code: sha2-256
    write_pb_varint(digest.len() as u64, &mut wire);
    wire.extend_from_slice(&digest);
    format!("b{}", base32_lower_no_padding(&wire))
}

/// Compute a standard IPFS CIDv1 over `bytes`. Output matches kubo's
/// `ipfs add --cid-version=1 --raw-leaves=true --chunker=size-262144
/// --hash=sha2-256 <file>`.
pub fn compute_ipfs_cid(bytes: &[u8]) -> String {
    compute_ipfs_blocks(bytes).root
}

/// Same wire-format guarantee as [`compute_ipfs_cid`], but exposes every
/// block produced along the way (leaf chunks + internal dag-pb nodes)
/// so callers can populate a bitswap blockstore in one pass.
pub fn compute_ipfs_blocks(bytes: &[u8]) -> IpfsBlocks {
    // Leaves: chunk bytes into raw-codec blocks. Empty input still gets
    // one (empty) leaf so the cid is well-defined.
    let mut blocks: Vec<(String, Bytes)> = Vec::new();
    let leaves: Vec<(Vec<u8>, u64)> = if bytes.is_empty() {
        let cid_wire = ipfs_cid_wire(0x55, &sha2_256(b""));
        blocks.push((cid_string(&cid_wire), Bytes::new()));
        vec![(cid_wire, 0)]
    } else {
        bytes
            .chunks(IPFS_CHUNK_SIZE)
            .map(|chunk| {
                let cid_wire = ipfs_cid_wire(0x55, &sha2_256(chunk));
                blocks.push((cid_string(&cid_wire), Bytes::copy_from_slice(chunk)));
                (cid_wire, chunk.len() as u64)
            })
            .collect()
    };

    // Single-leaf file: return the leaf cid as the file cid.
    if leaves.len() == 1 {
        let root = cid_string(&leaves[0].0);
        return IpfsBlocks { root, blocks };
    }

    // Multi-leaf: build a balanced tree of UnixFS dag-pb internal nodes.
    let mut level: Vec<(Vec<u8>, u64, u64)> = leaves
        .into_iter()
        .map(|(cid, size)| (cid, size, size))
        .collect();

    while level.len() > 1 {
        let mut next: Vec<(Vec<u8>, u64, u64)> =
            Vec::with_capacity(level.len().div_ceil(IPFS_FANOUT));
        for batch in level.chunks(IPFS_FANOUT) {
            let blocksizes: Vec<u64> = batch.iter().map(|(_, sz, _)| *sz).collect();
            let filesize: u64 = blocksizes.iter().sum();
            let unixfs_data = encode_unixfs_file(filesize, &blocksizes);
            let node_bytes = encode_dagpb_node(batch, &unixfs_data);
            let node_cid = ipfs_cid_wire(0x70, &sha2_256(&node_bytes));
            blocks.push((cid_string(&node_cid), Bytes::from(node_bytes.clone())));
            let own_tsize: u64 =
                batch.iter().map(|(_, _, t)| *t).sum::<u64>() + node_bytes.len() as u64;
            next.push((node_cid, filesize, own_tsize));
        }
        level = next;
    }
    let root = cid_string(&level[0].0);
    IpfsBlocks { root, blocks }
}

fn cid_string(wire: &[u8]) -> String {
    format!("b{}", base32_lower_no_padding(wire))
}

/// CIDv1 wire-form bytes: `[version=0x01][codec varint][multihash code
/// varint=0x12 sha2-256][len=0x20][digest...]`.
fn ipfs_cid_wire(codec: u8, digest: &[u8; 32]) -> Vec<u8> {
    debug_assert!(
        codec < 0x80,
        "codec varint must fit in one byte for this helper"
    );
    let mut wire = Vec::with_capacity(36);
    wire.push(0x01); // CIDv1
    wire.push(codec);
    wire.push(0x12); // multihash code: sha2-256
    wire.push(0x20); // digest length: 32
    wire.extend_from_slice(digest);
    wire
}

fn sha2_256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// LEB128 unsigned varint encoding into `out`.
fn write_pb_varint(value: u64, out: &mut Vec<u8>) {
    let mut v = value;
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn write_pb_varint_field(field: u32, value: u64, out: &mut Vec<u8>) {
    write_pb_varint((field as u64) << 3, out); // tag: (field << 3) | wire-type 0 (varint)
    write_pb_varint(value, out);
}

fn write_pb_bytes_field(field: u32, value: &[u8], out: &mut Vec<u8>) {
    write_pb_varint(((field as u64) << 3) | 2, out); // wire type 2 = length-delimited
    write_pb_varint(value.len() as u64, out);
    out.extend_from_slice(value);
}

/// UnixFS protobuf payload for a File node: `Type=File, filesize,
/// blocksizes[]`.
fn encode_unixfs_file(filesize: u64, blocksizes: &[u64]) -> Vec<u8> {
    let mut out = Vec::new();
    write_pb_varint_field(1, 2, &mut out); // Type = File
    write_pb_varint_field(3, filesize, &mut out); // filesize
    for &bs in blocksizes {
        write_pb_varint_field(4, bs, &mut out); // blocksizes (repeated)
    }
    out
}

/// dag-pb PBNode for an internal UnixFS file node. Canonical wire order
/// pinned by the dag-pb spec: `Links` (tag 2) first, `Data` (tag 1) second.
fn encode_dagpb_node(children: &[(Vec<u8>, u64, u64)], data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for (cid_wire, _filesize, tsize) in children {
        let mut link_bytes = Vec::new();
        write_pb_bytes_field(1, cid_wire, &mut link_bytes); // Hash
        write_pb_varint_field(3, *tsize, &mut link_bytes); // Tsize
        write_pb_bytes_field(2, &link_bytes, &mut out); // Links (tag 2) first
    }
    write_pb_bytes_field(1, data, &mut out); // Data (tag 1) last
    out
}

/// RFC 4648 base32 with the lowercase alphabet and no padding. Used for the
/// multibase `b` prefix.
pub(crate) fn base32_lower_no_padding(input: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut out = String::with_capacity(input.len().div_ceil(5) * 8);
    let mut buffer: u64 = 0;
    let mut bits: u32 = 0;
    for &b in input {
        buffer = (buffer << 8) | b as u64;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            let idx = ((buffer >> bits) & 0x1F) as usize;
            out.push(ALPHABET[idx] as char);
        }
    }
    if bits > 0 {
        let idx = ((buffer << (5 - bits)) & 0x1F) as usize;
        out.push(ALPHABET[idx] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn midhash256_matches_ts_fixtures() {
        assert_eq!(
            compute_midhash256(b"hello world"),
            "bagacbabaec7v3fu2ygzh3e2sybg3fbzmisry2hbtpmck6vx3yftea6vzq35r4"
        );
        assert_eq!(
            compute_midhash256(b""),
            "bagacbabaecxvk4hvugaqw6xxrsxuxrykmyhq35i6ik5pshkn4wzdfdpa5a67y"
        );
        let mb_zeros = vec![0u8; 1024 * 1024];
        assert_eq!(
            compute_midhash256(&mb_zeros),
            "bagacbabaeawlkt4hn34sxuieoosrnnag3g3wv7gc6alzwfms2nrph2hochrx4"
        );
    }

    #[test]
    fn midhash256_from_sample_matches_full_bytes() {
        let small: &[u8] = b"hello world";
        assert_eq!(
            compute_midhash256_from_sample(small.len() as u64, small),
            compute_midhash256(small),
        );

        assert_eq!(
            compute_midhash256_from_sample(0, b""),
            compute_midhash256(b""),
        );

        let one_mb = vec![0u8; SAMPLE_SIZE];
        assert_eq!(
            compute_midhash256_from_sample(one_mb.len() as u64, &one_mb),
            compute_midhash256(&one_mb),
        );

        let two_mb = vec![0u8; 2 * SAMPLE_SIZE];
        let start = (two_mb.len() - SAMPLE_SIZE) / 2;
        let middle = &two_mb[start..start + SAMPLE_SIZE];
        assert_eq!(
            compute_midhash256_from_sample(two_mb.len() as u64, middle),
            compute_midhash256(&two_mb),
        );
    }

    #[test]
    fn midhash256_from_sample_partial_sample_stable() {
        let sample = vec![0u8; SAMPLE_SIZE];
        let total_5gb: u64 = 5 * 1024 * 1024 * 1024;
        let total_10gb: u64 = 10 * 1024 * 1024 * 1024;

        let h1 = compute_midhash256_from_sample(total_5gb, &sample);
        let h2 = compute_midhash256_from_sample(total_5gb, &sample);
        assert_eq!(h1, h2, "same (size, sample) must hash identically");

        let h3 = compute_midhash256_from_sample(total_10gb, &sample);
        assert_ne!(
            h1, h3,
            "size prefix must contribute to cid — same sample, different size"
        );
    }

    #[test]
    fn bt_v1_file_cid_wire_layout() {
        let infohash: [u8; 20] = [
            0xf9, 0xc8, 0xa7, 0xb6, 0xe5, 0xd4, 0xc3, 0xb2, 0xa1, 0x90, 0x8f, 0x7e, 0x6d, 0x5c,
            0x4b, 0x3a, 0x29, 0x18, 0x07, 0x06,
        ];

        let mut want = vec![0x01, 0x81, 0x20, 0x81, 0x20, 0x15];
        want.extend_from_slice(&infohash);
        want.push(0x04);
        assert_eq!(
            compute_bt_v1_file_cid(&infohash, 4),
            format!("b{}", base32_lower_no_padding(&want))
        );

        let mut want0 = vec![0x01, 0x81, 0x20, 0x81, 0x20, 0x15];
        want0.extend_from_slice(&infohash);
        want0.push(0x00);
        assert_eq!(
            compute_bt_v1_file_cid(&infohash, 0),
            format!("b{}", base32_lower_no_padding(&want0))
        );

        let mut want200 = vec![0x01, 0x81, 0x20, 0x81, 0x20, 0x16];
        want200.extend_from_slice(&infohash);
        want200.extend_from_slice(&[0xC8, 0x01]);
        assert_eq!(
            compute_bt_v1_file_cid(&infohash, 200),
            format!("b{}", base32_lower_no_padding(&want200))
        );
    }

    #[test]
    fn bt_v2_file_cid_wire_layout() {
        let infohash: [u8; 32] = [0xAB; 32];
        let mut want = vec![0x01, 0x82, 0x20, 0x82, 0x20, 0x21];
        want.extend_from_slice(&infohash);
        want.push(0x00);
        assert_eq!(
            compute_bt_v2_file_cid(&infohash, 0),
            format!("b{}", base32_lower_no_padding(&want))
        );
    }

    #[test]
    fn bt_file_cid_properties() {
        let ih1 = [0x11u8; 20];
        let ih2 = [0x22u8; 32];

        assert_eq!(
            compute_bt_v1_file_cid(&ih1, 3),
            compute_bt_v1_file_cid(&ih1, 3)
        );
        assert_ne!(
            compute_bt_v1_file_cid(&ih1, 0),
            compute_bt_v1_file_cid(&ih1, 1)
        );
        assert!(compute_bt_v1_file_cid(&ih1, 0).starts_with('b'));
        assert!(compute_bt_v2_file_cid(&ih2, 0).starts_with('b'));
        assert_ne!(
            compute_bt_v1_file_cid(&[0xAB; 20], 0),
            compute_bt_v2_file_cid(&[0xAB; 32], 0)
        );
    }

    #[test]
    fn base32_known_vectors() {
        assert_eq!(base32_lower_no_padding(b""), "");
        assert_eq!(base32_lower_no_padding(b"f"), "my");
        assert_eq!(base32_lower_no_padding(b"fo"), "mzxq");
        assert_eq!(base32_lower_no_padding(b"foo"), "mzxw6");
        assert_eq!(base32_lower_no_padding(b"foob"), "mzxw6yq");
        assert_eq!(base32_lower_no_padding(b"foobar"), "mzxw6ytboi");
    }

    #[test]
    fn yt_video_cid_matches_golden_vector() {
        // Pinned by `/cid-rank-vectors.json`'s `yt-video-locator` entry. The id
        // is the real resolver output for Daft Punk / "Get Lucky" measured in
        // the delegated-playback study §4.3.
        assert_eq!(
            compute_yt_video_cid("video", "4D7u5KF7SP8").unwrap(),
            "bagecaaarav3gszdfn42ein3vgvfumn2tka4a"
        );
    }

    #[test]
    fn yt_video_cid_framing_matches_card_locator() {
        // The two codecs share the `varint(len(kind)) || kind || id` framing on
        // purpose, so every decoder extends a parser it already wrote. If this
        // ever fails, the two shapes have drifted and §7.2 of docs/cid-formats.md
        // is no longer true.
        let yt = compute_yt_video_cid("tmdb", "tv:95479").unwrap();
        let card = compute_card_cid("tmdb", "tv:95479").unwrap();
        // Same digest, different codec => same length, differing only in the
        // codec varint region.
        assert_ne!(yt, card);
        assert_eq!(yt.len(), card.len());
    }

    #[test]
    fn yt_video_cid_is_a_pure_function_of_kind_and_id() {
        // Convergence is the reason this is a codec and not a URL: two peers
        // that resolve the same video must derive the same address offline.
        assert_eq!(
            compute_yt_video_cid("video", "4D7u5KF7SP8"),
            compute_yt_video_cid("video", "4D7u5KF7SP8")
        );
        // ...and the kind is part of the identity, not decoration.
        assert_ne!(
            compute_yt_video_cid("video", "OLAK5uy_kZ8Xq"),
            compute_yt_video_cid("playlist", "OLAK5uy_kZ8Xq")
        );
    }

    #[test]
    fn yt_video_cid_rejects_empty_fields() {
        assert!(compute_yt_video_cid("", "4D7u5KF7SP8").is_none());
        assert!(compute_yt_video_cid("video", "").is_none());
    }

    #[test]
    fn ext_play_cid_matches_golden_vector() {
        assert_eq!(
            compute_ext_play_cid("https://example.bandcamp.com/album/x").unwrap(),
            "bagesaabenb2hi4dthixs6zlymfwxa3dffzrgc3temnqw24bomnxw2l3bnrrhk3jppa"
        );
    }

    #[test]
    fn ext_play_cid_rejects_non_http_schemes() {
        // Load-bearing: a client hands this string to an anchor or a new tab,
        // so a `javascript:` or `data:` payload must never mint a locator.
        assert!(compute_ext_play_cid("javascript:alert(1)").is_none());
        assert!(compute_ext_play_cid("data:text/html,<script>").is_none());
        assert!(compute_ext_play_cid("ftp://example.com/x").is_none());
        assert!(compute_ext_play_cid("").is_none());
    }

    #[test]
    fn ext_play_cid_differs_from_the_url_locator_for_the_same_url() {
        // The whole contract lives in the codec slot: `0x1006` means "fetch and
        // seed these bytes", `0x1009` means "open this, there are no bytes for
        // us". Wire-identical otherwise — which is exactly why a decoder that
        // matched on *shape* would seed a Bandcamp page as the file.
        let url = "https://example.bandcamp.com/album/x";
        let ext = compute_ext_play_cid(url).unwrap();
        let card = compute_card_cid("x", url).unwrap();
        assert_ne!(ext, card);
    }

    #[test]
    fn ipfs_cid_single_leaf_matches_kubo() {
        assert_eq!(
            compute_ipfs_cid(b"hello world"),
            "bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e"
        );
    }

    #[test]
    fn ipfs_cid_empty_matches_kubo() {
        assert_eq!(
            compute_ipfs_cid(b""),
            "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
        );
    }

    #[test]
    fn ipfs_cid_multi_leaf_zeros_stable() {
        let mb_zeros = vec![0u8; 1024 * 1024];
        assert_eq!(
            compute_ipfs_cid(&mb_zeros),
            "bafybeiadh3bekpwtewjvauqeucf7yzqrb3ixsxzltnuwed4pxangtpou6m"
        );
    }

    #[test]
    fn ipfs_blocks_single_leaf() {
        let payload = b"hello world";
        let out = compute_ipfs_blocks(payload);
        assert_eq!(out.root, compute_ipfs_cid(payload));
        assert_eq!(out.blocks.len(), 1);
        assert_eq!(out.blocks[0].0, out.root);
        assert_eq!(out.blocks[0].1.as_ref(), payload);
    }

    #[test]
    fn ipfs_blocks_multi_leaf() {
        let mb_zeros = vec![0u8; 1024 * 1024];
        let out = compute_ipfs_blocks(&mb_zeros);
        assert_eq!(
            out.root,
            "bafybeiadh3bekpwtewjvauqeucf7yzqrb3ixsxzltnuwed4pxangtpou6m"
        );
        assert_eq!(out.blocks.len(), 5);
        assert_eq!(out.blocks.last().unwrap().0, out.root);
        for (_cid, block) in &out.blocks[..4] {
            assert_eq!(block.len(), 256 * 1024);
            assert!(block.iter().all(|&b| b == 0));
        }
    }

    #[test]
    fn ipfs_blocks_empty() {
        let out = compute_ipfs_blocks(b"");
        assert_eq!(
            out.root,
            "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
        );
        assert_eq!(out.blocks.len(), 1);
        assert_eq!(out.blocks[0].1.len(), 0);
        assert_eq!(out.blocks[0].0, out.root);
    }

    // ---- card locator (0x1007) --------------------------------------------

    /// Inverse of [`base32_lower_no_padding`], test-only. Lets the card tests
    /// assert the actual CIDv1 bytes (codec, multihash code, digest) rather
    /// than just round-tripping the encoder against itself.
    fn base32_lower_decode(s: &str) -> Vec<u8> {
        const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
        let mut out = Vec::new();
        let (mut acc, mut bits) = (0u32, 0u32);
        for c in s.bytes() {
            let v = ALPHABET.iter().position(|&a| a == c).expect("base32 char") as u32;
            acc = (acc << 5) | v;
            bits += 5;
            if bits >= 8 {
                bits -= 8;
                out.push((acc >> bits) as u8);
                acc &= (1 << bits) - 1;
            }
        }
        out
    }

    /// The property the whole card design leans on: the CID is a pure function
    /// of `(source, id)`, so any peer can derive a card's address offline. If
    /// this literal ever changes, every already-published card silently forks
    /// into a second identity — treat a failure here as a wire break.
    #[test]
    fn card_cid_is_deterministic() {
        let cid = compute_card_cid("tmdb", "tv:95479").expect("card cid");
        assert_eq!(cid, compute_card_cid("tmdb", "tv:95479").unwrap());
        assert_eq!(cid, "bagdsaaanar2g2zdcor3duojvgq3ts");
    }

    /// Source- and kind-namespaced: the same numeric id on a different bridge,
    /// or the same id under a different media kind, must not collide.
    #[test]
    fn card_cid_is_source_and_kind_namespaced() {
        let tmdb = compute_card_cid("tmdb", "tv:95479").unwrap();
        let mal = compute_card_cid("mal", "95479").unwrap();
        let movie = compute_card_cid("tmdb", "movie:95479").unwrap();
        assert_ne!(tmdb, mal);
        assert_ne!(tmdb, movie);
    }

    /// Pins the on-the-wire CIDv1 layout: v1, codec `0x1007` (varint
    /// `[0x87,0x20]`), **identity** multihash `0x00` (this family has no
    /// digest — the locator itself is the payload), then
    /// `varint(source_len) ‖ source ‖ id`.
    #[test]
    fn card_cid_wire_shape() {
        let cid = compute_card_cid("tmdb", "tv:95479").unwrap();
        assert!(cid.starts_with('b'), "multibase base32-lower prefix");
        let wire = base32_lower_decode(&cid[1..]);

        assert_eq!(wire[0], 0x01, "CIDv1");
        assert_eq!(&wire[1..3], &[0x87, 0x20], "codec varint for 0x1007");
        assert_eq!(wire[3], 0x00, "identity multihash — no digest");

        let digest_len = wire[4] as usize;
        let digest = &wire[5..5 + digest_len];
        assert_eq!(digest[0], 4, "varint(len(\"tmdb\"))");
        assert_eq!(&digest[1..5], b"tmdb");
        assert_eq!(&digest[5..], b"tv:95479");
    }

    /// Empty inputs are rejected rather than encoded as an ambiguous locator,
    /// and an id past the 64-byte multihash budget (meta-share's
    /// `CidGeneric<64>`) returns `None` so the caller drops the card instead of
    /// emitting a cid that peer would refuse to parse.
    #[test]
    fn card_cid_rejects_empty_and_oversized() {
        assert!(compute_card_cid("", "tv:1").is_none());
        assert!(compute_card_cid("tmdb", "").is_none());
        let huge = "x".repeat(CARD_LOCATOR_MAX_DIGEST);
        assert!(compute_card_cid("tmdb", &huge).is_none());
        // Exactly at the ceiling still encodes.
        let fits = "x".repeat(CARD_LOCATOR_MAX_DIGEST - 5); // varint(4) + "tmdb"
        assert!(compute_card_cid("tmdb", &fits).is_some());
    }

    // ---- nzb-posting (0x1003) — the dedup contract -----------------------
    //
    // These pin the normalisation rule documented on `compute_nzb_posting_cid`.
    // meta-share re-derives the same digest from the fetched manifest and
    // rejects a mismatch, so a change that breaks one of these silently makes
    // every self-scanned posting unplayable. Mirror any edit on both sides.

    /// Segment/file **order must not matter** — that is the whole reason we
    /// hash a sorted id set instead of the raw `.nzb` bytes (the retired
    /// `0x1004` did the latter and broke dedup across reposts).
    #[test]
    fn nzb_posting_cid_is_order_independent() {
        let a = ["c@x.com", "a@x.com", "b@x.com"];
        let b = ["a@x.com", "b@x.com", "c@x.com"];
        let c = ["b@x.com", "c@x.com", "a@x.com"];
        assert_eq!(compute_nzb_posting_cid(&a), compute_nzb_posting_cid(&b));
        assert_eq!(compute_nzb_posting_cid(&a), compute_nzb_posting_cid(&c));
    }

    /// Angle brackets, surrounding whitespace, empty entries and duplicates are
    /// all normalised away. NZB `<segment>` bodies carry the bare form but NNTP
    /// `BODY` re-adds the brackets, so both spellings must converge.
    #[test]
    fn nzb_posting_cid_normalises_brackets_blanks_and_dupes() {
        let bare = ["a@x.com", "b@x.com"];
        let noisy = [" <a@x.com> ", "b@x.com", "", "  ", "a@x.com"];
        assert_eq!(
            compute_nzb_posting_cid(&bare),
            compute_nzb_posting_cid(&noisy)
        );
    }

    /// Case is preserved: a Message-ID's local part is case-sensitive per RFC
    /// 5322, so lowercasing could collide two genuinely distinct articles.
    #[test]
    fn nzb_posting_cid_is_case_sensitive() {
        assert_ne!(
            compute_nzb_posting_cid(&["Abc@x.com"]),
            compute_nzb_posting_cid(&["abc@x.com"])
        );
    }

    /// A different article set is a different posting.
    #[test]
    fn nzb_posting_cid_differs_on_different_ids() {
        assert_ne!(
            compute_nzb_posting_cid(&["a@x.com", "b@x.com"]),
            compute_nzb_posting_cid(&["a@x.com", "c@x.com"])
        );
    }

    /// Wire shape: CIDv1, codec `0x1003` (varint `[0x83, 0x20]`), multihash
    /// sha2-256 (`0x12`), 32-byte digest. Pinned because meta-share matches on
    /// the **codec** slot — a drift here makes every posting undecodable there.
    #[test]
    fn nzb_posting_cid_wire_shape() {
        let cid = compute_nzb_posting_cid(&["a@x.com"]);
        assert!(cid.starts_with('b'), "multibase base32-lower prefix");

        // Round-trip the base32 body back to bytes to assert the header.
        let decoded = base32_lower_decode(&cid[1..]);
        assert_eq!(decoded[0], 0x01, "CIDv1");
        assert_eq!(&decoded[1..3], &[0x83, 0x20], "varint(0x1003)");
        assert_eq!(decoded[3], 0x12, "multihash sha2-256");
        assert_eq!(decoded[4], 0x20, "digest length 32");
        assert_eq!(decoded.len(), 5 + 32);

        // The digest is sha256 over the newline-joined sorted set.
        let expect: [u8; 32] = Sha256::digest(b"a@x.com").into();
        assert_eq!(&decoded[5..], &expect);
    }
}
