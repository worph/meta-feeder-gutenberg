//! End-to-end contract test for the feeder HTTP surface: boots the real
//! `serve_feeders` router (book-feeder hosting GutenbergPlugin) on an ephemeral
//! port, pointed at a wiremock Gutendex, and drives `/manifest`, `/query`, and
//! `/compute` over HTTP. Deterministic — no live Gutendex.

use book_feeder::gutenberg::GutenbergPlugin;
use meta_feeder_sdk::{
    configure_plugins, hash, router, ComputeRequest, ComputeResponse, GatewayQuery, HashKindDto,
    ManifestResponse, QueryRequest, QueryResponse,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Spawn the feeder router (Gutenberg pointed at `upstream`) on an ephemeral
/// port; return its base URL. The TempDir is returned so the per-plugin cache
/// outlives the test.
async fn spawn_feeder(upstream: &MockServer) -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let plugin = GutenbergPlugin::with_base_url(upstream.uri());
    let plugins =
        configure_plugins(vec![Box::new(plugin)], dir.path()).expect("configure plugins");
    let app = router(plugins, "book-feeder-test".to_string(), dir.path());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (format!("http://{addr}"), dir)
}

fn books_json() -> serde_json::Value {
    serde_json::json!({
        "results": [{
            "id": 1342,
            "title": "Pride and Prejudice",
            "authors": [{"name": "Austen, Jane"}],
            "languages": ["en"],
            "formats": { "application/epub+zip": "EPUB_PLACEHOLDER" }
        }]
    })
}

#[tokio::test]
async fn manifest_lists_book_capabilities() {
    let upstream = MockServer::start().await;
    let (base, _dir) = spawn_feeder(&upstream).await;

    let manifest: ManifestResponse = reqwest::Client::new()
        .get(format!("{base}/manifest"))
        .send()
        .await
        .expect("GET /manifest")
        .json()
        .await
        .expect("decode manifest");

    assert_eq!(manifest.plugins.len(), 1);
    let p = &manifest.plugins[0];
    assert_eq!(p.id, "gutenberg");
    assert_eq!(p.served_file_types, vec!["document".to_string()]);
    assert_eq!(p.served_content_kinds, vec!["book".to_string()]);
}

#[tokio::test]
async fn query_returns_gutenberg_records() {
    let upstream = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/books/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(books_json()))
        .mount(&upstream)
        .await;
    let (base, _dir) = spawn_feeder(&upstream).await;

    let resp: QueryResponse = reqwest::Client::new()
        .post(format!("{base}/query"))
        .json(&QueryRequest {
            upstream_id: "gutenberg".to_string(),
            query: GatewayQuery::from_free_text("alice"),
            max_results: 50,
        })
        .send()
        .await
        .expect("POST /query")
        .json()
        .await
        .expect("decode query response");

    assert_eq!(resp.records.len(), 1);
    let r = &resp.records[0];
    assert_eq!(r.upstream_id, "gutenberg");
    assert_eq!(r.fields.get("gutenbergid").map(String::as_str), Some("1342"));
    assert_eq!(r.fields.get("contentKind").map(String::as_str), Some("book"));
}

#[tokio::test]
async fn compute_returns_outcome_with_inline_epub_bytes() {
    let upstream = MockServer::start().await;
    let epub_bytes = b"<<fake epub bytes for contract test>>".to_vec();
    let expected_cid = hash::compute_ipfs_cid(&epub_bytes);
    let epub_path = "/ebooks/1342.epub";

    let book = serde_json::json!({
        "id": 1342,
        "title": "Pride and Prejudice",
        "authors": [{"name": "Austen, Jane"}],
        "languages": ["en"],
        "formats": { "application/epub+zip": format!("{}{epub_path}", upstream.uri()) }
    });
    Mock::given(method("GET"))
        .and(path("/books/1342"))
        .respond_with(ResponseTemplate::new(200).set_body_json(book))
        .mount(&upstream)
        .await;
    Mock::given(method("GET"))
        .and(path(epub_path))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(epub_bytes.clone()))
        .mount(&upstream)
        .await;

    let (base, _dir) = spawn_feeder(&upstream).await;

    let resp: ComputeResponse = reqwest::Client::new()
        .post(format!("{base}/compute"))
        .json(&ComputeRequest {
            upstream_id: "gutenberg".to_string(),
            record_id: "1342".to_string(),
        })
        .send()
        .await
        .expect("POST /compute")
        .json()
        .await
        .expect("decode compute response");

    assert_eq!(resp.outcomes.len(), 1);
    let o = &resp.outcomes[0];
    assert_eq!(o.hash, expected_cid);
    assert_eq!(o.hash_kind, HashKindDto::Sha2_256);
    assert_eq!(o.file_extension.as_deref(), Some("epub"));
    // Bytes ride inline base64 on the v1 contract — decode + verify they hash
    // back to the advertised CID (the core's auto-store will do exactly this).
    let b64 = o.bytes_b64.as_ref().expect("compute returned inline bytes");
    use base64::Engine as _;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .expect("valid base64");
    assert_eq!(decoded, epub_bytes);
    assert_eq!(hash::compute_ipfs_cid(&decoded), expected_cid);
}

#[tokio::test]
async fn query_unknown_upstream_is_404() {
    let upstream = MockServer::start().await;
    let (base, _dir) = spawn_feeder(&upstream).await;

    let status = reqwest::Client::new()
        .post(format!("{base}/query"))
        .json(&QueryRequest {
            upstream_id: "torznab".to_string(),
            query: GatewayQuery::from_free_text("x"),
            max_results: 10,
        })
        .send()
        .await
        .expect("POST /query")
        .status();

    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
}
