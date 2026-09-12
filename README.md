# meta-feeder-book

Book **feeder sidecar** for [MetaMesh](https://github.com/worph/meta-gateway).
One binary, `book-feeder`, hosts two upstreams:

| Upstream | Serves | What it is |
|----------|--------|------------|
| `gutenberg` | `document` × `book` | Public-domain **editions** from Project Gutenberg (via the [Gutendex](https://gutendex.com) API), with an epub url-locator per record |
| `openlibrary` | `card` × `book` | **Work cards** from [Open Library](https://openlibrary.org) — the book identity tier (`TMDB : video :: AniList : comics :: Open Library : books`) |

The two meet through `openlibraryid`. A Gutenberg edition carries it only when
Open Library itself records the link: exactly one Open Library work lists the
etext's Project Gutenberg id (`search.json?q=id_project_gutenberg:<id>`). The
edition then also carries the transient trust claim `anchored=true`,
`anchorSource=openlibrary`, which the gateway stores as
`anchoredBy/openlibrary:OL…W`. There is no title or author matching.

An Open Library card is addressed `compute_card_cid("openlibrary", "OL…W")`,
the bare work id, byte-identical to meta-read's `card_cid::openlibrary`. A work
with no cover or no description gets no card.

## Role in MetaMesh

A feeder is a stateless HTTP sidecar. It **finds records and fetches bytes**; it
does *not* talk to meta-core or the libp2p blockstore. The gateway core that
calls it owns the meta-core store-back and the blockstore seeding. A gateway
registers this feeder as a `RemoteFeederPlugin` pointing at its `/` and then
drives the contract:

| Endpoint | Purpose |
|----------|---------|
| `GET /manifest` | feeder identity + capabilities |
| `GET /health` | liveness |
| `POST /query`, `POST /query_stream` | structured search against the upstream |
| `POST /compute` | enrichment / outcome compute |
| `GET /fetch/:upstream_id/:record_id` | fetch a record's bytes |
| `GET /blob/:upstream_id/:cid` | fetch a content-addressed blob |
| `GET /config`, `GET /config/schema`, `GET\|PUT /config/values` | runtime config UI + API |

## Configuration

| Env var | Default | Notes |
|---------|---------|-------|
| `META_FEEDER_HTTP_LISTEN` | `0.0.0.0:8080` | HTTP listen address |
| `META_FEEDER_STATE_DIR` | `/data/meta-feeder` | redb cache + state |
| `OPENLIBRARY_CONTACT` | — | e-mail or URL sent in the Open Library User-Agent. First-boot seed for the `contact` config field; a value saved in the config UI wins |
| `OPENLIBRARY_TOP_N` | `12` | cards per search |
| `OPENLIBRARY_DISCOVERY_N` | `20` | cards per browse row (`popular:true` → Open Library trending, `fresh:true` → recently added) |
| `RUST_LOG` | `info` | tracing filter |

No upstream API key is required. Open Library asks API clients to identify
themselves, so set a contact in production: an identified client gets a larger
request budget. Both upstreams share one Open Library rate budget. The config
page edits the `gutenberg` upstream, and the contact saved there is read by
both.

## Image

```
ghcr.io/worph/meta-feeder-book
```

Exposes `8080`. Built and pushed by CI on every push to `main` (the `main`
tag) and on `v*` tags (semver tags).

## Build locally

The build context is the **repo root** (the Cargo workspace).
`meta-feeder-sdk` is a git dependency pinned by tag, so the builder needs
network access to github.com:

```bash
docker build -f feeder-plugin/book-feeder/Dockerfile -t ghcr.io/worph/meta-feeder-book:dev .
```

Tests (`cargo` is not needed on the host; wiremock stands in for every upstream):

```bash
docker run --rm -v "$PWD":/src -w /src -e CARGO_TARGET_DIR=/target rust:1.89 \
  cargo test -p book-feeder
```

## Repo layout

```
Cargo.toml                      # workspace: members = feeder-plugin/*
feeder-plugin/book-feeder/      # this feeder's crate + Dockerfile
  src/gutenberg.rs              #   `gutenberg` upstream
  src/openlibrary.rs            #   `openlibrary` upstream (cards)
  src/openlibrary_client.rs     #   Open Library client (budget, cache, binding lookup)
crates/meta-feeder-sdk/         # old vendored SDK copy — no longer built (excluded)
```

`meta-feeder-sdk` comes from [`worph/meta-feeder-sdk`](https://github.com/worph/meta-feeder-sdk)
at the tag in `feeder-plugin/book-feeder/Cargo.toml`. An SDK change reaches
this feeder only when it is tagged and the pin is bumped.
