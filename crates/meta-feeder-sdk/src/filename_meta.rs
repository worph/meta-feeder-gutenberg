//! Season / episode extraction from a torrent release title.
//!
//! This is a Rust port of `@metazla/filename-tools`
//! (`packages/filename-tool/src/lib/...`) — specifically the
//! `FileNameVideoMetaExtractor.computeSeasonAndEpisodeFromFilename` →
//! `SerieFilesNameAnalysisTool.findSeasonAndEpisode` pipeline plus the
//! `FileNameCleaner` pre-clean steps it depends on. filename-tools is the
//! best season/episode heuristic we have (the meta-sort `torrent` plugin
//! already uses it), so rather than invent a second, weaker heuristic for
//! the gateway we mirror its pattern set and ordering verbatim.
//!
//! ## Differences from the TS original (deliberate, documented)
//!
//! 1. **No parent-folder fallback.** The TS extractor also reads
//!    `Season N` out of the *parent directory* name. A torznab hit is a
//!    single release title with no directory context, so that branch is
//!    dropped — `computeSeasonAndEpisodeFromFilePath`'s folder lookup
//!    simply has nothing to read.
//! 2. **Rightmost-number fallback uses the captured digits, not the raw
//!    regex match.** The TS `extractRightmostNumber` does
//!    `parseFloat(fullMatch)` where the match can include a leading `.`
//!    (the `soloEp` boundary fragment is `(?:\.|\b)`), so `parseFloat`
//!    can yield e.g. `0.117` for `.117.`. We read capture group 1 (the
//!    bare number) instead, which is what the function *means* to return.
//!
//! The caller (`torznab::into_discovery_record`) gates this on TV-ness so
//! a movie's release year can't be misread as an episode number by the
//! rightmost-number fallback.

use std::collections::BTreeSet;
use std::sync::LazyLock;

use regex::Regex;

/// Structured season/episode extracted from a release title. Each field
/// is the stringified number exactly as the TS extractor would emit it
/// (`"" + n`): whole numbers render without a decimal, half-episode
/// specials keep the `.5` (e.g. `"13"`, `"13.5"`).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SeasonEpisode {
    pub season: Option<String>,
    pub episode: Option<String>,
    /// First episode of a multi-episode **pack/batch** (`(01-10)` → `"1"`).
    /// Set together with [`episode_end`]; mutually exclusive with the
    /// single-episode [`episode`] field.
    pub episode_start: Option<String>,
    /// Last episode of a multi-episode pack/batch (`(01-10)` → `"10"`).
    pub episode_end: Option<String>,
    /// `season * 10000 + episode` disambiguator (matches the TS
    /// `increment` field). Computed for completeness; the torznab caller
    /// currently only persists `season`/`episode`.
    pub increment: Option<String>,
    /// True when this release is a **pack/batch** rather than a single
    /// episode: an explicit episode range (`(01-10)`, `第29-38話`), a season
    /// range (`S01-S03`), or a whole-season release (`Season 3`, `S03` with
    /// no episode). Drives `contentKind=pack` (see the torznab caller); a pack
    /// is deliberately **not** given a single `episode`.
    pub is_pack: bool,
    /// True when [`season`] came from an explicit season-bearing token
    /// (`S01E05`, `Season 2`, `S03`). An episode with no such token is left
    /// season-less (no "assume season 1" default), so a per-file reparse uses
    /// this to know it should borrow the season from the pack/parent title (see
    /// [`extract_season_episode_with_context`]).
    pub season_explicit: bool,
}

impl SeasonEpisode {
    /// A pack carrying a concrete inclusive episode range.
    pub fn has_episode_range(&self) -> bool {
        self.episode_start.is_some() && self.episode_end.is_some()
    }
}

// ---------------------------------------------------------------------------
// Pattern fragments — copied 1:1 from
// `filename-tool/src/lib/config/SeasonAndEpisodePatterns.ts`.
// ---------------------------------------------------------------------------

const SW: &str = r"(?:\.|\b)"; // a dot or word boundary
const S: &str = r"(?:\.|\b|\s)";
const END: &str = r"(?:(?:\.|\b).*)"; // trailing remainder, matched so it's anchored
const EP4: &str = r"((?:\d{1,4})(?:\.5)?)"; // 1..9999(.5)
const EP3: &str = r"((?:\d{1,3})(?:\.5)?)"; // 1..999(.5)
const EP2: &str = r"((?:\d{1,2})(?:\.5)?)"; // 1..99(.5)
const SEA: &str = r"((?:\d{1,2})(?:\.5)?)"; // season 1..99
const EP4MY: &str = r"((?:\d{1,3}|1[0-7]\d{2}|1800)(?:\.5)?)"; // 1..1800(.5)

/// Season-AND-episode patterns, longest/most-specific first. Each has
/// exactly two capture groups: group 1 = season, group 2 = episode.
/// Order matters — the first to match wins (mirrors the TS array order).
static SEASON_EPISODE_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        format!(r"(?i){SW}Season{S}*{SEA}{S}*Episode{S}*{EP4}{END}"),
        format!(r"(?i){SW}Saison{S}*{SEA}{S}*Episode{S}*{EP4}{END}"),
        format!(r"(?i){SW}\[{SEA}{S}*x{S}*{EP4}\]{END}"),
        format!(r"(?i){SW}\({SEA}{S}*x{S}*{EP4}\){END}"),
        format!(r"(?i){SW}{SEA}{S}*x{S}*{EP4}{END}"),
        format!(r"(?i){SW}S{SEA}{S}*x{S}*{EP4}{END}"),
        format!(r"(?i){SW}S{SEA}{S}*E{EP4}{END}"),
        format!(r"(?i){SW}{SEA}{S}*E{EP4}{END}"),
        format!(r"(?i){SW}S{S}*{SEA}{S}*E{S}*{EP2}{END}"),
        format!(r"(?i){SW}S{S}*{SEA}{S}+{EP2}{END}"),
        format!(r"(?i){SW}S{SEA}{S}*-{S}*{EP2}{END}"),
        format!(r"(?i){SW}{SEA}{S}*-{S}*{EP2}{END}"),
    ]
    .iter()
    .map(|p| Regex::new(p).expect("season+episode pattern compiles"))
    .collect()
});

/// Episode-only patterns. Each has exactly one capture group: the episode.
static EPISODE_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        format!(r"(?i){SW}Episode{S}*{EP4}{END}"),
        format!(r"(?i){SW}Epizoda{S}*{EP4}{END}"),
        format!(r"(?i){SW}E{S}*{EP4MY}{END}"),
        format!(r"(?i){SW}Ep{EP4MY}{END}"),
    ]
    .iter()
    .map(|p| Regex::new(p).expect("episode-only pattern compiles"))
    .collect()
});

/// `soloEp` — the rightmost-number fallback. Capture group 1 is the number.
static SOLO_EP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&format!(r"{SW}{EP3}{SW}")).expect("soloEp compiles"));

// ---------------------------------------------------------------------------
// Episode-range / pack detection. Anime + TV batches encode a *range* of
// episodes (`(01-10)`, `01-28`, `第29-38話`, `26~37`) or a whole season
// (`Season 3`, `S03`). The TS `findSeasonAndEpisode` had no range concept, so
// its `{SEA}-{EP2}` season+episode pattern mis-reads `01-10` as "S1 E10" and a
// batch collapses to one phantom episode. We detect ranges FIRST (before
// `find_season_and_episode`) and emit `episode_start`/`episode_end` +
// `is_pack`, so the caller can stamp `contentKind=pack` instead.
//
// Disambiguation from a real `S1E2`/`1x05`/`S2 - 07`:
//   - ranges use a `-`/`~` separator with TWO numbers and `end > start`;
//   - the bare form requires the start digit to be preceded by a space/`_`/`-`
//     (or string start), so the `2` in `S2 - 07` (preceded by `S`) is NOT read
//     as a range start — that release falls through to the single-episode path;
//   - 4-digit year-like / out-of-range operands are rejected.
// ---------------------------------------------------------------------------

/// Bracketed/parenthesised episode range: `(01-10)`, `[01-12]`, `(1~12)`.
static RANGE_BRACKET: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"[\[(]\s*0*(\d{1,4})\s*[-~]\s*0*(\d{1,4})\s*[\])]").expect("bracket range compiles")
});

/// CJK episode range: `第29-38話` / `第29~38話`.
static RANGE_CJK: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"第\s*0*(\d{1,4})\s*[-~]\s*0*(\d{1,4})\s*話").expect("cjk range compiles")
});

/// Bare episode range with a **tight** separator: `01-28`, `26~37` (no spaces
/// around the `-`/`~`). The start digit must follow `^`/space/`_` so a
/// season-prefixed `S2-07` (the `2` follows `S`) doesn't match. The tight
/// separator is the disambiguator from the very common `Show 2 - 11` /
/// `Show - 11` episode-separator convention (spaced dash → single episode, not
/// a range); spaced ranges only come through the unambiguous bracketed form.
static RANGE_BARE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:^|[\s_])0*(\d{1,4})[-~]0*(\d{1,4})(?:[\s_)\]]|$)")
        .expect("bare range compiles")
});

/// Season range (`S01-S03`, `S1~S3`) — a whole-series pack with no single
/// season. Matched only as a fallback when no episode range is present.
static SEASON_RANGE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bS\s*0*(\d{1,2})\s*[-~]\s*S\s*0*(\d{1,2})\b").expect("season range compiles")
});

/// `(start, end)` is a plausible inclusive episode range: ascending, and
/// neither operand is a 4-digit year / resolution-like number.
fn plausible_range(start: u32, end: u32) -> bool {
    end > start && (1..1900).contains(&start) && end < 1900
}

/// Detect an episode range anywhere in the (precleaned) title. Returns the
/// first plausible `(start, end)` found, trying the unambiguous bracketed and
/// CJK forms before the bare form.
fn detect_episode_range(s: &str) -> Option<(u32, u32)> {
    for re in [&*RANGE_BRACKET, &*RANGE_CJK, &*RANGE_BARE] {
        for caps in re.captures_iter(s) {
            let start = caps.get(1).and_then(|m| m.as_str().parse::<u32>().ok());
            let end = caps.get(2).and_then(|m| m.as_str().parse::<u32>().ok());
            if let (Some(a), Some(b)) = (start, end) {
                if plausible_range(a, b) {
                    return Some((a, b));
                }
            }
        }
    }
    None
}

/// 8-char CRC32 (SFV) marker, e.g. the `[FC412C51]` Nyaa suffix.
static SFV: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b[A-F0-9]{8}\b").expect("sfv compiles"));

// ---------------------------------------------------------------------------
// Keyword pre-clean — copied from
// `filename-tool/src/lib/config/KeywordsArray.ts`.
// Removed before season/episode detection so release tags like `x264`,
// `1080p`, `720` can't be mistaken for episode numbers by the fallback.
// ---------------------------------------------------------------------------

/// Word-bounded keywords (resolution/codec/source/group tags). Order is
/// irrelevant once compiled individually, but we sort longest-first to
/// match the TS constructor (`keyword.length` desc) for identical
/// overlap behaviour.
const KEYWORDS: &[&str] = &[
    "VF",
    "aXXo",
    "STFR",
    "SUBFORCED",
    "BrRipx264",
    "YIFY",
    "TRUEFRENCH",
    "VFF",
    "ATVP",
    "RenewalEX",
    "BD",
    "DD",
    "8bits",
    "8 bits",
    "10bits",
    "10 bits",
    "part 1",
    "part 2",
    "part 3",
    "part 4",
    "part A",
    "part B",
    "part C",
    "part D",
    "AAC5",
    "AAC5.1",
    "1080i",
    "1080p",
    "2160p",
    "480i",
    "480p",
    "4k",
    "576i",
    "576p",
    "720",
    "720i",
    "720p",
    "aac",
    "aac4",
    "ac3",
    "amzn",
    "apple tv+",
    "avi",
    "bbc",
    "bd5",
    "bdrip",
    "blu-ray",
    "bluray",
    "brrip",
    "cam",
    "cw",
    "dc",
    "dcu",
    "ddp5.1",
    "director's cut",
    "disney+",
    "divx",
    "divx5",
    "dl",
    "dsr",
    "dsrip",
    "dts",
    "dual audio",
    "dubbed",
    "dvd",
    "dvdivx",
    "dvdr",
    "dvdrip",
    "dvdscr",
    "dvdscreener",
    "eng",
    "eng sub",
    "esp",
    "fan edit",
    "fhd",
    "flv",
    "fs",
    "ger",
    "german",
    "h.264",
    "h.265",
    "h264",
    "h265",
    "hardcoded",
    "hbo",
    "hd",
    "hddvd",
    "hdr",
    "hdrip",
    "hdtv",
    "hdtvrip",
    "hevc",
    "hq",
    "hrhd",
    "hrhdtv",
    "hulu",
    "imax",
    "ita",
    "jpn",
    "ld",
    "md",
    "mkv",
    "mp3",
    "mp4",
    "mpeg",
    "mpg",
    "mq",
    "multi",
    "multisubs",
    "netflix",
    "nf",
    "nfofix",
    "ntsc",
    "ogg",
    "ogm",
    "ova",
    "pal",
    "pdtv",
    "r3",
    "r5",
    "rerip",
    "rsvcd",
    "screener",
    "sd",
    "se",
    "subbed",
    "svcd",
    "tc",
    "telecine",
    "telesync",
    "ts",
    "tv series",
    "uhd",
    "uhdtv",
    "uhdv",
    "v2",
    "vcd",
    "vostfr",
    "web",
    "web-dl",
    "webcast",
    "webrip",
    "wmv",
    "ws",
    "www",
    "x264",
    "x265",
    "xsvcd",
    "xvid",
    "xvidvd",
    "xxx",
    "bits",
    "AnimeServ",
];

/// Substrings removed without word boundaries (the TS `substringArray`).
const SUBSTRINGS: &[&str] = &["v1", "v2", "v3", "v4", "v5"];

/// Compiled keyword regexes (`\bKEYWORD\b`, case-insensitive), longest-first.
static KEYWORD_RES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    let mut kws: Vec<&str> = KEYWORDS.to_vec();
    kws.sort_by_key(|k| std::cmp::Reverse(k.len()));
    kws.into_iter()
        .map(|k| Regex::new(&format!(r"(?i)\b{}\b", regex::escape(k))).expect("keyword compiles"))
        .collect()
});

/// Compiled substring regexes (no boundaries, case-insensitive), longest-first.
static SUBSTRING_RES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    let mut subs: Vec<&str> = SUBSTRINGS.to_vec();
    subs.sort_by_key(|s| std::cmp::Reverse(s.len()));
    subs.into_iter()
        .map(|s| Regex::new(&format!(r"(?i){}", regex::escape(s))).expect("substring compiles"))
        .collect()
});

// ---------------------------------------------------------------------------
// Language tags — anime/torznab upstreams encode language as release-name
// tags (`VOSTFR`, `ENG SUB`, `JPN`, `GER`, `MULTi`, …) rather than a
// structured field. `extract_languages` reads *every* tag off the raw title
// and returns the full set, so a `MULTi VOSTFR` release yields `{mul, fre}`.
// The gateway writes each as a `languages/<lang3> = "true"` key-set member
// (METADATA_KEYS §9) that the preferred-language filter matches against.
//
// Patterns are ported from the battle-tested torrent-name parsers
// (dreulavelle/PTT, Radarr `LanguageParser.cs`); the lookbehind/lookahead
// guards those use to dodge false positives (`DTS ES`, `WEB-DL`, `Shang-Chi`,
// `Tel Aviv`, `www.x.tld`) are reproduced here in code, since Rust's `regex`
// crate has no lookaround. Two defensive choices do most of the work:
//   1. **No bare 2-letter codes** (`en`/`de`/`es`/`it`/`fi`/`nl`/`pl`/…). They
//      collide with ordinary words, names, and domain TLDs. We rely on the
//      3-letter scene codes (`eng`/`ger`/`ita`) and full names instead.
//   2. **Word boundaries on every tag**, so `\bita\b` can't fire inside
//      `digital`, `\bpor\b` inside `portal`, `\bchi\b` inside `Chicago`.
// ---------------------------------------------------------------------------

/// Website/domain noise (`www.foo.bar/baz`) stripped before matching so a TLD
/// or path token can't be read as a language. Only `www.`-prefixed runs are
/// removed — a generic `host.tld` matcher would eat dotted release names like
/// `Spy.x.Family`. Runs on the raw (still-dotted) title.
static URL_STRIP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bwww\.\S+").expect("url strip re compiles"));

/// Guard phrases removed from the separator-normalized title before language
/// matching, because each contains a substring that would otherwise
/// false-positive a tag. None of these phrases is itself a language tag, so
/// removal is loss-free for detection — this is how we reproduce the upstream
/// parsers' lookbehind guards without lookaround:
///   - `shang chi` → guards `\bchi\b` (the film "Shang-Chi", Chinese)
///   - `tel aviv`  → guards `\btel\b` (Telugu)
///   - `web dl` / `webdl` → guards `\bdl\b` (German dual-language marker)
static LANG_GUARD_STRIP: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:shang\s+chi|tel\s+aviv|web\s+dl|webdl)\b")
        .expect("lang guard re compiles")
});

/// Release-title language tags → ISO 639-2/B (alpha-3). **The codes match
/// meta-share's UI language vocabulary** (`ui/js/language-prefs.js`
/// `ISO_639_1_TO_639_2`: `fre`/`ger`/`chi`, not the 639-3 `fra`/`deu`/`zho`),
/// because meta-share's "languages I read" preference is the source of truth
/// the meta-watch filter compares against — the enriched value must be in the
/// same space the chips use. (This deliberately diverges from `gutenberg.rs` /
/// METADATA_KEYS §1's 639-3 convention; aligning those is a separate cleanup.)
///
/// `mul` (ISO 639-2 "Multiple languages") is the honest marker for
/// `MULTi`/`dual-audio` — we can't enumerate which languages a multi release
/// carries, so we don't guess. A `MULTi VOSTFR` release matches *both* `mul`
/// and `fre`: every pattern that matches contributes its code, so the result
/// is a set, not a first-wins pick.
static LANGUAGE_TAGS: LazyLock<Vec<(Regex, &'static str)>> = LazyLock::new(|| {
    [
        // Multi / dual-audio → "mul". `dl` matches only after "web dl" has
        // been stripped (see LANG_GUARD_STRIP).
        (
            r"\bmulti(?:lang)?\b|\bmultisubs?\b|\bdual\s?audio\b|\bdual\b|\bdl\b",
            "mul",
        ),
        // French — the richest scene vocabulary (truefrench/vff/vfq/vostfr…).
        (
            r"\btruefrench\b|\bvostfr\b|\bsubfrench\b|\bfrench\b|\bvf[fqi2]?\b|\bstfr\b|\bfra\b|\bfre\b",
            "fre",
        ),
        (r"\benglish\b|\beng\s?sub\b|\beng\b", "eng"),
        (r"\bjapanese\b|\bjpn\b|\bjap\b", "jpn"),
        (r"\bitalian\b|\bita\b", "ita"),
        (r"\bgerman\b|\bger\b|\bdeu\b", "ger"),
        (
            r"\bspanish\b|\bcastellano\b|\bespa[nñ]ol\b|\blatino\b|\besp\b|\bspa\b",
            "spa",
        ),
        (r"\bportuguese\b|\bdublado\b|\blegendado\b|\bpor\b", "por"),
        (r"\bkorean\b|\bkor\b", "kor"),
        (
            r"\bchinese\b|\bmandarin\b|\bcantonese\b|\bchs\b|\bcht\b|\bzho\b|\bchi\b",
            "chi",
        ),
        (r"\brussian\b|\brus\b", "rus"),
        (r"\bdutch\b|\bflemish\b|\bnld\b|\bdut\b", "dut"),
        (r"\bpolish\b|\bpl\s?dub\b|\bdub\s?pl\b|\blektor\b|\bpol\b", "pol"),
        (r"\bczech\b|\bcze\b|\bces\b", "cze"),
        (r"\bslovak\b|\bslo\b|\bslk\b", "slo"),
        (r"\bhungarian\b|\bhundub\b|\bhun\b", "hun"),
        (
            r"\bromanian\b|\brosub(?:bed)?\b|\brodubbed\b|\brum\b|\bron\b",
            "rum",
        ),
        (r"\bgreek\b|\bgre\b|\bell\b", "gre"),
        (r"\bukrainian\b|\bukr\b", "ukr"),
        (r"\bbulgarian\b|\bbgaudio\b|\bbul\b", "bul"),
        (r"\bswedish\b|\bswesub\b|\bswe\b", "swe"),
        (r"\bnorwegian\b|\bnorsk\b", "nor"),
        (r"\bdanish\b|\bdansk\b", "dan"),
        (r"\bfinnish\b|\bsuomi\b", "fin"),
        (r"\bturkish\b|\btur\b", "tur"),
        (r"\barabic\b|\bara\b", "ara"),
        (r"\bhindi\b|\bhin\b", "hin"),
        (r"\btamil\b|\btam\b", "tam"),
        (r"\btelugu\b|\btel\b", "tel"),
        (r"\bmalayalam\b|\bmal\b", "mal"),
        (r"\bvietnamese\b|\bvie\b", "vie"),
        (r"\bthai\b|\btha\b", "tha"),
    ]
    .iter()
    .map(|(p, code)| {
        (
            Regex::new(&format!("(?i){p}")).expect("language tag re compiles"),
            *code,
        )
    })
    .collect()
});

/// Characters `cleanSpecialChars` replaces with a space (note: `.` and
/// `-` are deliberately preserved — they carry `S01.E02` / `Show - 07`
/// structure).
const SPECIAL_CHARS: &[char] = &[
    '\'', ',', '_', '@', '#', '$', '%', '^', '&', '*', '+', '=', '<', '>', '?', '|', ':', ';', '"',
    '`', '~',
];

/// Extract season/episode from a release title.
///
/// Faithful to `computeSeasonAndEpisodeFromFilename` →
/// `findSeasonAndEpisode` → `computeSeasonAndEpisodeFromFilePath`
/// (minus the parent-folder branch — see the module doc).
pub fn extract_season_episode(title: &str) -> SeasonEpisode {
    let cleaned = preclean(title);
    // Range detection runs on the *raw* title (with `~` normalized to `-`),
    // not the precleaned one: `preclean` replaces `~` with a space (it's a
    // "special char"), which would erase a `26~37` range, and it never touches
    // the `(`/`[` brackets the bracketed form anchors on.
    let range_src = title.replace('~', "-");

    // Pack detection runs first — an episode range (`(01-10)`) would otherwise
    // be mis-read as `S1E10` by `find_season_and_episode`'s `{SEA}-{EP2}`
    // pattern. A pack is never given a single `episode`.
    if let Some((start, end)) = detect_episode_range(&range_src) {
        let season = find_pack_season(&cleaned);
        return SeasonEpisode {
            is_pack: true,
            episode_start: Some(start.to_string()),
            episode_end: Some(end.to_string()),
            // An explicit season token (`S2 (01-10)`) buckets the pack; a bare
            // range (`Frieren (01-28)`) leaves the season for the caller.
            season: season.map(|s| s.to_string()),
            season_explicit: season.is_some(),
            ..Default::default()
        };
    }
    // Season range (`S01-S03`) — a whole-series pack, no single season/episode.
    if SEASON_RANGE.is_match(&range_src) {
        return SeasonEpisode {
            is_pack: true,
            ..Default::default()
        };
    }

    let (sea, ep) = find_season_and_episode(&cleaned);

    let mut out = SeasonEpisode::default();
    // season/episode 0 are valid; only < 0 means "not found".
    if sea >= 0.0 {
        out.season = Some(fmt_num(sea));
        out.season_explicit = true;
    }
    if ep >= 0.0 {
        out.episode = Some(fmt_num(ep));
        // An episode with no season is left season-LESS on purpose: a season is
        // only asserted when explicitly hinted — an S-token in the title here,
        // or (via extract_season_episode_with_context) the parent/pack title.
        // Absolute-numbered releases ("Show - 28") thus carry an episode and no
        // season, so the UI buckets them under "Episodes" rather than a
        // fabricated "Season 1". `increment` encodes a season, so it is only
        // emitted when the season is known.
        if sea >= 0.0 {
            out.increment = Some(fmt_num(sea * 10000.0 + ep));
        }
    } else if sea >= 0.0 {
        // Season present, no episode → a whole-season pack (`Season 3`, `S03`).
        out.is_pack = true;
    }
    out
}

/// Extract season/episode using a `<parent>/<filename>` style context — the
/// per-file path of an opened multi-file (pack) torrent. Parses the file's own
/// `filename` first; when the filename yields an episode but **no** season, the
/// season is recovered from the `parent` (the pack/torrent title). This
/// restores the parent-folder season fallback the TS
/// `computeSeasonAndEpisodeFromFilePath` had and this port originally dropped
/// (see the module doc) — so `Frieren S2 [Batch] / Frieren - 03.mkv` resolves
/// to S2E3 rather than the inherited pack-level number.
pub fn extract_season_episode_with_context(parent: &str, filename: &str) -> SeasonEpisode {
    let mut file_se = extract_season_episode(filename);
    // Only borrow the parent's season for a concrete single episode whose own
    // filename carries no *explicit* season token (so it is otherwise
    // season-less). A per-file record is an episode, not a pack, so we don't
    // propagate the parent's pack/range shape here.
    if file_se.episode.is_some() && !file_se.season_explicit {
        if let Some(s) = find_pack_season(&preclean(parent)) {
            file_se.season = Some(s.to_string());
            file_se.season_explicit = true;
            if let Some(ep) = file_se.episode.as_deref().and_then(|e| e.parse::<f64>().ok()) {
                file_se.increment = Some(fmt_num(s as f64 * 10000.0 + ep));
            }
        }
    }
    file_se
}

/// Season number parsed from a keyword'd season token (`Season N`, `Saison N`,
/// `S NN`) anywhere in the (precleaned) title — used to bucket a pack and to
/// recover a per-file season from the parent. Excludes the bare-number season
/// form so an episode-only title isn't mis-bucketed.
fn find_pack_season(s: &str) -> Option<i64> {
    let padded = format!(" {s} ");
    for re in SEASON_WORD_ONLY_PATTERNS.iter() {
        if let Some(caps) = re.captures(&padded) {
            let v = parse_group(&caps, 1);
            if v >= 0.0 {
                return Some(v as i64);
            }
        }
    }
    None
}

/// Best-effort set of languages a release supports (audio **and** subtitles,
/// mixed), inferred from its title tags.
///
/// Anime/torznab upstreams encode language as release-name tags (`VOSTFR`,
/// `ENG SUB`, `JPN`, `GER`, `MULTi`, …) rather than a structured field, so we
/// read them off the raw title here. Returns the **full set** of ISO 639-2/B
/// alpha-3 codes found (matching meta-share's UI vocabulary — see
/// [`LANGUAGE_TAGS`]); a `MULTi VOSTFR` release yields `{mul, fre}`. The set is
/// empty when the title carries no recognizable tag — the caller turns that
/// into the `languages/und` sentinel.
pub fn extract_languages(title: &str) -> BTreeSet<String> {
    // 1. Strip `www.…` domain noise while the dots are still intact.
    let stripped = URL_STRIP.replace_all(title, " ");
    // 2. Normalize `.`/`_`/`-` separators to spaces so word-boundary tags
    //    anchor inside dotted/underscored/hyphenated release names — and so
    //    the guard phrases ("web dl", "shang chi") match regardless of which
    //    separator the original used.
    let norm: String = stripped
        .chars()
        .map(|c| if matches!(c, '.' | '_' | '-') { ' ' } else { c })
        .collect();
    // 3. Remove guard phrases that would otherwise false-positive a tag.
    let guarded = LANG_GUARD_STRIP.replace_all(&norm, " ");
    // 4. Collect every matching code (a set — dedupes, deterministic order).
    let mut out = BTreeSet::new();
    for (re, code) in LANGUAGE_TAGS.iter() {
        if re.is_match(&guarded) {
            out.insert((*code).to_string());
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Quality — the resolution *tier* of a release, normalized to a single label
// (`quality` field, METADATA_KEYS §3). Torznab `<item>`s carry no structured
// resolution, so — like season/episode and language — the release title is the
// only source. Mirrors the client-side `parseVersion()` heuristic in
// meta-watch's `detail.js` so a gateway-filled `quality` and the UI's
// title-parse fallback agree. Interlaced (`1080i`/`720i`) and `4k` collapse to
// the progressive/`2160p` tier; an explicit `WxH` token is mapped by height.
// ---------------------------------------------------------------------------

/// `2160p`/`4k`, `1080p`/`1080i`, `720p`/`720i`, `576p`, `480p`, or a `WxH`
/// token (`1920x1080`) → a normalized `"<n>p"` tier. `None` when the title
/// carries no recognizable resolution.
pub fn extract_quality(title: &str) -> Option<String> {
    static TIER: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)\b(2160p|4k|1080p|1080i|720p|720i|576p|480p)\b").expect("tier re compiles")
    });
    static WXH: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)\b\d{3,4}\s*x\s*(\d{3,4})\b").expect("wxh re compiles"));
    // Normalize `.`/`_` separators so `\b`-anchored tokens match inside
    // dot/underscore releases (`BDRip_1920x1080_x264`, `Show.1080p.x264`).
    let t = title.replace(['.', '_'], " ");
    if let Some(m) = TIER.captures(&t).and_then(|c| c.get(1)) {
        let tok = m.as_str().to_lowercase();
        return Some(
            match tok.as_str() {
                "4k" => "2160p",
                "1080i" => "1080p",
                "720i" => "720p",
                other => other,
            }
            .to_string(),
        );
    }
    if let Some(h) = WXH
        .captures(&t)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<u32>().ok())
    {
        let tier = if h >= 2000 {
            "2160p"
        } else if h >= 1000 {
            "1080p"
        } else if h >= 700 {
            "720p"
        } else if h >= 500 {
            "576p"
        } else {
            "480p"
        };
        return Some(tier.to_string());
    }
    None
}

/// Extract the video codec from a release title (METADATA_KEYS.md `codec`, a §12
/// structured search facet). Title-derived / best-effort — same provenance and
/// caveats as [`extract_quality`] (the authoritative per-stream codec lives in
/// `stream/{n}` for locally-ingested files). Normalized to ffmpeg-style tokens
/// so a `codec:hevc` query matches both encoder spellings (`x265`/`H.265`) and
/// any stream-derived value.
pub fn extract_codec(title: &str) -> Option<String> {
    static CODEC: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)\b(x265|h\s?265|hevc|x264|h\s?264|avc|av1|xvid|divx|vp9)\b")
            .expect("codec re compiles")
    });
    // Normalize `.`/`_` separators (and the resulting `H 264`) so `\b`-anchored
    // tokens match inside dot/underscore releases (`Show.x265.WEB`, `M_H.264`).
    let t = title.replace(['.', '_'], " ");
    let tok = CODEC.captures(&t)?.get(1)?.as_str().to_lowercase();
    let cleaned = tok.replace(' ', "");
    let norm = match cleaned.as_str() {
        "x265" | "h265" | "hevc" => "hevc",
        "x264" | "h264" | "avc" => "h264",
        other => other, // av1, xvid, divx, vp9
    };
    Some(norm.to_string())
}

/// `removeKeywordsFromFilename` + `removeSFV` + `cleanSpecialChars` +
/// `cleanSpace`, in that order.
fn preclean(filename: &str) -> String {
    let mut s = filename.to_string();
    for re in KEYWORD_RES.iter() {
        s = re.replace_all(&s, "").into_owned();
    }
    for re in SUBSTRING_RES.iter() {
        s = re.replace_all(&s, "").into_owned();
    }
    // removeSFV — first occurrence only (TS uses a non-global replace).
    s = SFV.replace(&s, "").into_owned();
    // cleanSpecialChars — replace each special char with a space.
    s = s
        .chars()
        .map(|c| if SPECIAL_CHARS.contains(&c) { ' ' } else { c })
        .collect();
    // cleanSpace — collapse runs of whitespace and trim.
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// `findSeasonAndEpisode`: returns `(season, episode)` as floats, `-1.0`
/// for "not found".
fn find_season_and_episode(filename: &str) -> (f64, f64) {
    // Pad with spaces so word-boundary fragments anchor at the ends
    // exactly as the TS code does (`" " + filename + " "`).
    let padded = format!(" {filename} ");

    for re in SEASON_EPISODE_PATTERNS.iter() {
        if let Some(caps) = re.captures(&padded) {
            let season = parse_group(&caps, 1);
            let episode = parse_group(&caps, 2);
            return (season, episode);
        }
    }
    for re in EPISODE_PATTERNS.iter() {
        if let Some(caps) = re.captures(&padded) {
            let episode = parse_group(&caps, 1);
            return (-1.0, episode);
        }
    }
    // Season-only pack (`Season 3`, `Saison 3`, `S03`) with no episode. Caught
    // here — *before* the bare rightmost-number fallback — so a whole-season
    // pack reports `(season, no-episode)` instead of the fallback mis-reading
    // its season digit as an episode and `extract_season_episode` then
    // defaulting season to 1 (the `Spy x Family Season 3 (S03)` -> `S01E03`
    // bug). The TS pipeline relied on a parent-folder lookup for this case,
    // which the gateway dropped (see module doc).
    for re in SEASON_WORD_ONLY_PATTERNS.iter() {
        if let Some(caps) = re.captures(&padded) {
            let season = parse_group(&caps, 1);
            if season >= 0.0 {
                return (season, -1.0);
            }
        }
    }
    // Last hope: the rightmost bare number. `if (number)` in the TS is
    // falsy for 0, so a lone 0 doesn't count as an episode.
    if let Some(n) = rightmost_number(&padded) {
        if n != 0.0 {
            return (-1.0, n);
        }
    }
    (-1.0, -1.0)
}

fn parse_group(caps: &regex::Captures, idx: usize) -> f64 {
    caps.get(idx)
        .and_then(|m| m.as_str().parse::<f64>().ok())
        .unwrap_or(-1.0)
}

/// `extractRightmostNumber` — the last `soloEp` match's number.
fn rightmost_number(s: &str) -> Option<f64> {
    SOLO_EP
        .captures_iter(s)
        .filter_map(|c| c.get(1))
        .last()
        .and_then(|m| m.as_str().parse::<f64>().ok())
}

/// Stringify like JS `"" + n`: `2.0 -> "2"`, `1.5 -> "1.5"`. Rust's f64
/// `Display` already drops the trailing `.0`, so this is a thin wrapper
/// kept for intent.
fn fmt_num(n: f64) -> String {
    format!("{n}")
}

// ---------------------------------------------------------------------------
// Title cleaning — port of `FileNameVideoMetaExtractor.cleanTitle` +
// `SerieFilesNameAnalysisTool.{removeBeyondHyphen, removeSeasonAndEpisode}`.
// The gateway previously used a separate hand-rolled `clean_torrent_title`
// that missed underscores/dots, the keyword list, `removeBeyondHyphen`, and
// season-word/episode-range stripping — so TMDB enrichment failed on many
// release-name shapes. Reuse the already-ported building blocks instead.
// ---------------------------------------------------------------------------

/// Season-only patterns (TS `seasonPatterns`). Each removes a `Season N` /
/// `S N` / trailing bare number and everything after it.
static SEASON_ONLY_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        format!(r"(?i){SW}Season{S}*{SEA}{END}"),
        format!(r"(?i){SW}Saison{S}*{SEA}{END}"),
        format!(r"(?i){SW}S{S}*{SEA}{END}"),
        format!(r"(?i){SW}{SEA}{END}"),
    ]
    .iter()
    .map(|p| Regex::new(p).expect("season-only pattern compiles"))
    .collect()
});

/// Season-only patterns restricted to the **keyword'd** forms (`Season N`,
/// `Saison N`, `S NN`) — used by [`find_season_and_episode`] to recognise a
/// whole-season *pack* (no episode) before the bare rightmost-number fallback
/// fires. Deliberately excludes [`SEASON_ONLY_PATTERNS`]'s 4th, bare-`{SEA}`
/// entry: that one matches any lone number and would turn an episode-only
/// title (`Show - 37`) into a bogus season. `SxxEyy` never reaches here because
/// [`SEASON_EPISODE_PATTERNS`] is tried first in `find_season_and_episode`.
static SEASON_WORD_ONLY_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        format!(r"(?i){SW}Season{S}*{SEA}{END}"),
        format!(r"(?i){SW}Saison{S}*{SEA}{END}"),
        format!(r"(?i){SW}S{S}*{SEA}{END}"),
    ]
    .iter()
    .map(|p| Regex::new(p).expect("season-word-only pattern compiles"))
    .collect()
});

/// `cleanTags` — bracketed/parenthesised/braced groups.
static TAG_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[[^\]]*\]|\([^)]*\)|\{[^}]*\}").expect("tag re compiles"));
/// `removeTagSeparator` — stray bracket chars.
static TAG_SEP_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[\[\]\(\)\{\}]").expect("tag-sep re compiles"));
/// `removeYear` — a standalone 19xx/20xx.
static YEAR_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(?:19|20)\d{2}\b").expect("year re compiles"));
/// `removeBeyondHyphen` — capture everything before the first ` - `
/// (space-delimited) hyphen.
static BEYOND_HYPHEN_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(.*?)(?:\s+-\s+|\s+-)").expect("beyond-hyphen re compiles"));
static TRAILING_HYPHEN_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\s*-\s*$").expect("trailing-hyphen re compiles"));

fn clean_special_chars(s: &str) -> String {
    s.chars()
        .map(|c| if SPECIAL_CHARS.contains(&c) { ' ' } else { c })
        .collect()
}

fn remove_keywords(s: &str) -> String {
    let mut out = s.to_string();
    for re in KEYWORD_RES.iter() {
        out = re.replace_all(&out, "").into_owned();
    }
    for re in SUBSTRING_RES.iter() {
        out = re.replace_all(&out, "").into_owned();
    }
    out
}

fn remove_beyond_hyphen(s: &str) -> String {
    if let Some(c) = BEYOND_HYPHEN_RE.captures(s) {
        return c
            .get(1)
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_default();
    }
    TRAILING_HYPHEN_RE.replace(s, "").trim().to_string()
}

fn remove_season_and_episode(s: &str) -> String {
    for re in SEASON_EPISODE_PATTERNS
        .iter()
        .chain(EPISODE_PATTERNS.iter())
        .chain(SEASON_ONLY_PATTERNS.iter())
    {
        if re.is_match(s) {
            return re.replace(s, "").trim().to_string();
        }
    }
    // removeRightmostNumber fallback.
    SOLO_EP.replace(s, "").trim().to_string()
}

/// Produce a clean show/movie title from a raw release name, faithful to
/// filename-tools' `cleanTitle` pipeline. This is what should feed a TMDB
/// search — NOT a hand-rolled cleaner.
pub fn clean_title(raw: &str) -> String {
    let mut s = clean_special_chars(raw); // `_ ~ : ; , ' "` … → space (keeps `.` `-`)
    s = remove_keywords(&s); // 172 release/quality keywords + v1..v5
    s = TAG_RE.replace_all(&s, "").into_owned(); // cleanTags
    s = remove_beyond_hyphen(&s); // drop everything after the first " - "
    s = TAG_SEP_RE.replace_all(&s, "").into_owned(); // stray brackets
    s = YEAR_RE.replace_all(&s, "").into_owned(); // removeYear
    s = SFV.replace(&s, "").into_owned(); // removeSFV
    s = remove_season_and_episode(&s); // S/E, season-word, episode-range, rightmost number
    s = s.replace('.', " "); // cleanDot
    let s = s.trim().trim_start_matches('-').trim_end_matches('-');
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn se(title: &str) -> (Option<String>, Option<String>) {
        let r = extract_season_episode(title);
        (r.season, r.episode)
    }

    #[test]
    fn strict_sxxeyy() {
        assert_eq!(
            se("Game of Thrones S01E02 1080p WEB-DL x264-GROUP"),
            (Some("1".into()), Some("2".into()))
        );
    }

    #[test]
    fn dotted_release_name() {
        assert_eq!(
            se("The.Wire.S03E07.720p.BluRay.x264"),
            (Some("3".into()), Some("7".into()))
        );
    }

    #[test]
    fn clean_title_collapses_release_variants() {
        let ct = |s: &str| clean_title(s).to_lowercase();
        // Season-word, underscore/range, dotted+EnN, and the trailing-num
        // Nyaa shape all collapse to the bare show title (the cases that
        // were failing TMDB enrichment).
        assert_eq!(
            ct("[yolerejiju] Spy x Family Season 1 (S01) (WEBRip 1080p x265 10bit)"),
            "spy x family"
        );
        assert_eq!(
            ct("[Kashikoi]_Spy_X_Family_-_26~37_(S2 01~12)_[BDRip 1080p]"),
            "spy x family"
        );
        assert_eq!(
            ct("Spy.x.Family.E31.El.Temible.Creuer.WEBRip.x264-H3AsO"),
            "spy x family"
        );
        assert_eq!(
            ct("[Naruto-Kun.Hu] Spy x Family 2 - 11 [1080p].mkv"),
            "spy x family"
        );
    }

    #[test]
    fn concise_nxnn() {
        assert_eq!(
            se("Some Show 1x05 720p x264"),
            (Some("1".into()), Some("5".into()))
        );
    }

    #[test]
    fn bracketed_concise() {
        assert_eq!(
            se("Some Show [2x10] HDTV"),
            (Some("2".into()), Some("10".into()))
        );
    }

    #[test]
    fn verbose_season_episode() {
        assert_eq!(
            se("My Show Season 2 Episode 13"),
            (Some("2".into()), Some("13".into()))
        );
    }

    #[test]
    fn anime_trailing_number_is_season_less() {
        // No season marker → episode via rightmost-number fallback, and NO
        // season is fabricated (absolute numbering). SFV id stripped, brackets
        // ignored. The UI buckets a season-less episode under "Episodes".
        assert_eq!(
            se("[SubsPlease] Some Anime - 117 [FC412C51]"),
            (None, Some("117".into()))
        );
    }

    #[test]
    fn half_episode_special_is_preserved() {
        assert_eq!(
            se("Some Anime S01E13.5"),
            (Some("1".into()), Some("13.5".into()))
        );
    }

    #[test]
    fn increment_is_season_times_10000_plus_episode() {
        let r = extract_season_episode("Show S02E05");
        assert_eq!(r.increment.as_deref(), Some("20005"));
    }

    #[test]
    fn four_digit_year_is_not_misread_as_episode() {
        // The `soloEp` fallback caps at 3 digits, so a 4-digit release
        // year can't be picked up — no episode here.
        let r = extract_season_episode("The Matrix 1999 1080p BluRay x264");
        assert_eq!(r.episode, None);
    }

    #[test]
    fn short_number_in_movie_title_is_grabbed_by_fallback() {
        // A bare 1-3 digit number in a movie title DOES trip the
        // rightmost-number fallback (here: "13"). This is exactly why the
        // torznab caller gates season/episode on TV-ness — a movie record
        // must not inherit this bogus episode.
        let r = extract_season_episode("Apollo 13 1080p BluRay x264");
        assert_eq!(r.episode.as_deref(), Some("13"));
    }

    #[test]
    fn keyword_strip_prevents_codec_false_positive() {
        // "x264" must be removed so "264" isn't read as the episode.
        let r = extract_season_episode("Plain Movie Title x264");
        assert_eq!(r.episode, None);
    }

    #[test]
    fn no_numbers_yields_nothing() {
        let r = extract_season_episode("Just A Plain Title");
        assert_eq!(r.season, None);
        assert_eq!(r.episode, None);
    }

    #[test]
    fn season_only_pack_is_not_misread_as_episode() {
        // Whole-season packs (no episode) must report the season with NO
        // episode — not `S01E0N` from the rightmost-number fallback. This is
        // the original bug: `Spy x Family Season 3 (S03)` was parsed as
        // season 1, episode 3, dumping the S3 pack into the Season-1 bucket.
        assert_eq!(
            se("[yolerejiju] Spy x Family Season 3 (S03) (WEB-DL 1080p H.264 DDP)"),
            (Some("3".into()), None)
        );
        assert_eq!(
            se("[yolerejiju] Spy x Family Season 2 (S02) (WEBRip 1080p x265)"),
            (Some("2".into()), None)
        );
        // Bare `S03` pack (no `Season` word) via the S-prefixed pattern.
        assert_eq!(
            se("[Trix] SPY FAMILY S03 v2 (Batch) [WEBRip 1080p AV1 Opus]"),
            (Some("3".into()), None)
        );
        assert_eq!(
            se("Spy.x.Family.S03.MULTi.1080p.WEBRiP.AV1-KAF"),
            (Some("3".into()), None)
        );
        // French season word.
        assert_eq!(se("Une Serie Saison 4 1080p"), (Some("4".into()), None));
    }

    #[test]
    fn explicit_sxxeyy_still_wins_over_season_only() {
        // `SEASON_EPISODE_PATTERNS` runs first, so a real SxxEyy is unaffected
        // by the new season-only branch.
        assert_eq!(
            se("Spy x Family Season 3 S03E13 1080p"),
            (Some("3".into()), Some("13".into()))
        );
    }

    #[test]
    fn episode_range_is_a_pack_not_sxxeyy() {
        // The headline bug: `(01-10)` was mis-read as S1 E10 by the
        // `{SEA}-{EP2}` pattern. Now it's a pack with an episode range and no
        // single `episode`.
        let r = extract_season_episode("Frieren (01-28) [Batch] 1080p");
        assert!(r.is_pack);
        assert_eq!(r.episode, None);
        assert_eq!(r.episode_start.as_deref(), Some("1"));
        assert_eq!(r.episode_end.as_deref(), Some("28"));

        // Bare, dash-delimited, leading-zero range.
        let r = extract_season_episode("Some Anime 01-12 1080p");
        assert!(r.is_pack);
        assert_eq!(r.episode_start.as_deref(), Some("1"));
        assert_eq!(r.episode_end.as_deref(), Some("12"));

        // Tilde range, no leading zero.
        let r = extract_season_episode("[Kashikoi] Spy x Family - 26~37 [BDRip]");
        assert!(r.is_pack);
        assert_eq!(r.episode_start.as_deref(), Some("26"));
        assert_eq!(r.episode_end.as_deref(), Some("37"));
    }

    #[test]
    fn explicit_season_buckets_a_pack() {
        // `S2 (01-10)` → pack in season 2, episodes 1–10.
        let r = extract_season_episode("[grp] Frieren S2 (01-10) (WEBRip 1080p)");
        assert!(r.is_pack);
        assert_eq!(r.season.as_deref(), Some("2"));
        assert_eq!(r.episode_start.as_deref(), Some("1"));
        assert_eq!(r.episode_end.as_deref(), Some("10"));
    }

    #[test]
    fn cjk_episode_range_is_a_pack() {
        let r = extract_season_episode("[北宇治字幕组] Frieren [第29-38話][合集][1080p]");
        assert!(r.is_pack);
        assert_eq!(r.episode_start.as_deref(), Some("29"));
        assert_eq!(r.episode_end.as_deref(), Some("38"));
    }

    #[test]
    fn season_range_is_a_pack_with_no_single_season() {
        let r = extract_season_episode("Frieren S01-S03 Complete 1080p");
        assert!(r.is_pack);
        assert_eq!(r.season, None);
        assert_eq!(r.episode, None);
        assert_eq!(r.episode_start, None);
    }

    #[test]
    fn whole_season_release_is_a_pack() {
        // A season-only release (`Season 3`, `S03`, no episode) is a pack too —
        // the original "season-level release shows as one media card" bug.
        let r = extract_season_episode("[grp] Spy x Family Season 3 (S03) (WEB-DL 1080p)");
        assert!(r.is_pack);
        assert_eq!(r.season.as_deref(), Some("3"));
        assert_eq!(r.episode, None);
    }

    #[test]
    fn single_episode_is_not_a_pack() {
        // The disambiguation guards: `S01E02`, `1x05`, and `S2 - 07` are all
        // single episodes, never packs.
        for t in ["Game of Thrones S01E02 1080p", "Some Show 1x05 720p"] {
            assert!(!extract_season_episode(t).is_pack, "{t} mis-detected as pack");
        }
        let r = extract_season_episode("[Greek-Nakama] Frieren S2 - 07 [1080p]");
        assert!(!r.is_pack);
        assert_eq!(r.season.as_deref(), Some("2"));
        assert_eq!(r.episode.as_deref(), Some("7"));

        // The `Show <season> - <episode>` anime convention (spaced dash) is a
        // single episode, NOT a `2..11` range — the tight-separator rule keeps
        // this out of pack detection.
        let r = extract_season_episode("[Naruto-Kun.Hu] Spy x Family 2 - 11 [1080p]");
        assert!(!r.is_pack);
        assert_eq!(r.season.as_deref(), Some("2"));
        assert_eq!(r.episode.as_deref(), Some("11"));

        // A year range in a movie title is not an episode range.
        assert!(!extract_season_episode("Some Doc 2010-2011 1080p").is_pack);
    }

    #[test]
    fn per_file_context_recovers_season_from_parent() {
        // Opened pack: the inner file carries the episode, the pack title the
        // season. `Frieren - 03.mkv` alone is season-less; with the S2 pack
        // parent it resolves to S2E3 (the parent is the "season hint").
        let r = extract_season_episode_with_context("Frieren S2 (01-10) [Batch]", "Frieren - 03.mkv");
        assert_eq!(r.season.as_deref(), Some("2"));
        assert_eq!(r.episode.as_deref(), Some("3"));
        assert!(!r.is_pack);

        // The file's own explicit season wins over the parent.
        let r = extract_season_episode_with_context("Frieren S2 [Batch]", "Frieren S01E05.mkv");
        assert_eq!(r.season.as_deref(), Some("1"));
        assert_eq!(r.episode.as_deref(), Some("5"));
    }

    #[test]
    fn absolute_numbered_anime_is_season_less() {
        // No season keyword → the rightmost-number fallback still finds the
        // episode, but NO season is fabricated. Season is only ever asserted
        // from an explicit token (title or parent), never inferred — so an
        // absolute-numbered release lands in the UI's "Episodes" bucket.
        assert_eq!(
            se("[Naruto-Kun.Hu] Spy x Family - 37 [1080p].mkv"),
            (None, Some("37".into()))
        );
    }

    fn langs(s: &str) -> BTreeSet<String> {
        extract_languages(s)
    }

    #[test]
    fn language_from_release_tags() {
        // ISO 639-2/B codes — match meta-share's UI vocabulary (fre/ger/chi).
        assert!(langs("[SubsPlease] Some Anime - 117 VOSTFR [FC412C51]").contains("fre"));
        assert!(langs("Some.Movie.GERMAN.1080p.BluRay.x264").contains("ger"));
        assert!(langs("Show ENG SUB 720p").contains("eng"));
        assert!(langs("Spy.x.Family.JPN.1080p").contains("jpn"));
        assert!(langs("Film TRUEFRENCH 1080p").contains("fre"));
    }

    #[test]
    fn multi_is_mul_and_collects_every_language() {
        // `languages` is the union of all supported audio+sub languages.
        // MULTi → "mul" (ISO 639-2 multiple); a co-present specific tag is
        // ALSO collected (the set, not a first-wins pick).
        assert_eq!(
            langs("Show MULTI VOSTFR 1080p"),
            BTreeSet::from(["mul".to_string(), "fre".to_string()])
        );
        assert!(langs("Spy x Family S01E05 MULTi 1080p").contains("mul"));
        assert!(langs("Some Anime - 12 [Dual Audio] 1080p").contains("mul"));
        // A bilingual release collects both specific codes.
        assert_eq!(
            langs("Movie.2021.GERMAN.ENGLISH.1080p"),
            BTreeSet::from(["ger".to_string(), "eng".to_string()])
        );
    }

    #[test]
    fn no_language_tag_yields_empty_set() {
        // A plain quality/codec release name carries no language tag — the
        // caller maps the empty set to the `languages/und` sentinel.
        assert!(langs("Game of Thrones S01E02 1080p WEB-DL x264-GROUP").is_empty());
        // Short codes embedded in words/names must not false-positive: "ita"
        // inside "digital", "por" inside "portal", etc.
        assert!(langs("VFX Reel 2024 1080p").is_empty());
        assert!(langs("Digital Capital Portal 1080p").is_empty());
    }

    #[test]
    fn lookaround_guards_reproduced_in_code() {
        // WEB-DL must not read as `dl`→mul (German dual-language marker).
        assert!(!langs("Movie 2021 WEB-DL x264").contains("mul"));
        // "Shang-Chi" must not read as `chi` (Chinese)…
        assert!(!langs("Shang-Chi.2021.1080p.BluRay").contains("chi"));
        // …but a real CHI tag still does.
        assert!(langs("Movie.2021.CHI.1080p").contains("chi"));
        // "Tel Aviv" must not read as `tel` (Telugu).
        assert!(!langs("Tel Aviv On Fire 2019 1080p").contains("tel"));
        // www. domain noise (incl. its TLD) must not contribute a language.
        assert!(langs("www.Torrenting.it Plain Movie 1080p").is_empty());
    }

    #[test]
    fn quality_tiers_and_normalization() {
        let q = |s: &str| extract_quality(s);
        assert_eq!(
            q("Game of Thrones S01E02 1080p WEB-DL x264").as_deref(),
            Some("1080p")
        );
        assert_eq!(
            q("The.Wire.S03E07.720p.BluRay.x264").as_deref(),
            Some("720p")
        );
        // 4k / interlaced collapse to the canonical progressive tier.
        assert_eq!(q("Some Movie 2160p").as_deref(), Some("2160p"));
        assert_eq!(q("Some Movie 4K HDR").as_deref(), Some("2160p"));
        assert_eq!(q("Old Broadcast 1080i HDTV").as_deref(), Some("1080p"));
        // WxH fallback, mapped by height; dot/underscore separators handled.
        assert_eq!(q("BDRip_1920x1080_x264").as_deref(), Some("1080p"));
        assert_eq!(q("Show 1280x720").as_deref(), Some("720p"));
        // No resolution token → None (never guess).
        assert_eq!(q("Just A Plain Title"), None);
    }

    #[test]
    fn codec_normalization() {
        let c = |s: &str| extract_codec(s);
        // HEVC encoder spellings collapse to the codec name.
        assert_eq!(c("Movie.2020.1080p.x265-GROUP").as_deref(), Some("hevc"));
        assert_eq!(c("Movie 2020 HEVC 10bit").as_deref(), Some("hevc"));
        assert_eq!(c("Movie.2020.H.265.WEB").as_deref(), Some("hevc"));
        // AVC encoder spellings collapse to h264.
        assert_eq!(c("Show.S01E01.720p.x264").as_deref(), Some("h264"));
        assert_eq!(c("Show S01E01 H 264 AAC").as_deref(), Some("h264"));
        // Others kept as-is.
        assert_eq!(c("Clip 2023 AV1 Opus").as_deref(), Some("av1"));
        assert_eq!(c("Old Release XviD").as_deref(), Some("xvid"));
        // No codec token → None (never guess).
        assert_eq!(c("Just A Plain Title 1080p"), None);
    }
}
