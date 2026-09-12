//! End-to-end contract test for the feeder HTTP surface: boots the real
//! `serve_feeders` router (book-feeder hosting GutenbergPlugin AND
//! OpenLibraryPlugin, as `main.rs` does) on an ephemeral port, pointed at one
//! wiremock server that plays Gutendex, the Gutenberg RDF host and Open Library,
//! and drives `/manifest`, `/query`, and `/compute` over HTTP. Deterministic —
//! no live upstream.

use book_feeder::gutenberg::GutenbergPlugin;
use book_feeder::openlibrary::OpenLibraryPlugin;
use book_feeder::openlibrary_client;
use meta_feeder_sdk::{
    configure_plugins, hash, router, ComputeRequest, ComputeResponse, FeederPlugin, GatewayQuery,
    HashKindDto, ManifestResponse, QueryRequest, QueryResponse,
};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Spawn the feeder router (every upstream pointed at `upstream`) on an
/// ephemeral port; return its base URL. The TempDir is returned so the
/// per-plugin cache outlives the test.
async fn spawn_feeder(upstream: &MockServer) -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let budget = openlibrary_client::budget(false);
    let plugins: Vec<Box<dyn FeederPlugin>> = vec![
        Box::new(
            GutenbergPlugin::with_base_url(upstream.uri())
                .with_rdf_base_url(upstream.uri())
                .with_openlibrary_base_url(upstream.uri())
                .with_openlibrary_budget(budget.clone()),
        ),
        Box::new(OpenLibraryPlugin::with_api_base(upstream.uri()).with_budget(budget)),
    ];
    let plugins = configure_plugins(plugins, dir.path()).expect("configure plugins");
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
async fn manifest_lists_both_upstreams() {
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

    assert_eq!(manifest.plugins.len(), 2);
    let gutenberg = manifest.plugins.iter().find(|p| p.id == "gutenberg").expect("gutenberg");
    assert_eq!(gutenberg.served_file_types, vec!["document".to_string()]);
    assert_eq!(gutenberg.served_content_kinds, vec!["book".to_string()]);
    let openlibrary = manifest
        .plugins
        .iter()
        .find(|p| p.id == "openlibrary")
        .expect("openlibrary");
    assert_eq!(openlibrary.served_file_types, vec!["card".to_string()]);
    assert_eq!(openlibrary.served_content_kinds, vec!["book".to_string()]);
}

#[tokio::test]
async fn query_returns_gutenberg_records_with_their_open_library_claim() {
    let upstream = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/books/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(books_json()))
        .mount(&upstream)
        .await;
    Mock::given(method("GET"))
        .and(path("/search.json"))
        .and(query_param("q", "id_project_gutenberg:1342"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "numFound": 1,
            "docs": [{ "key": "/works/OL66554W", "id_project_gutenberg": ["1342", "42671"] }]
        })))
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
    let f = |k: &str| r.fields.get(k).map(String::as_str);
    assert_eq!(r.upstream_id, "gutenberg");
    assert_eq!(f("gutenbergid"), Some("1342"));
    assert_eq!(f("contentKind"), Some("book"));
    assert_eq!(f("authors/Jane Austen"), Some("true"));
    assert_eq!(f("languages/eng"), Some("true"));
    // The transient claim the gateway transcribes into
    // `anchoredBy/openlibrary:OL66554W`.
    assert_eq!(f("openlibraryid"), Some("OL66554W"));
    assert_eq!(f("anchored"), Some("true"));
    assert_eq!(f("anchorSource"), Some("openlibrary"));
    assert_eq!(f("anchorMethod"), None, "an identifier link is not a title match");
}

#[tokio::test]
async fn query_returns_an_open_library_card() {
    let upstream = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/search.json"))
        .and(query_param("q", "pride"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "numFound": 1,
            "docs": [{
                "key": "/works/OL66554W",
                "title": "Pride and Prejudice",
                "author_name": ["Jane Austen"],
                "cover_i": 14348537,
                "first_publish_year": 1813
            }]
        })))
        .mount(&upstream)
        .await;
    Mock::given(method("GET"))
        .and(path("/works/OL66554W.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "key": "/works/OL66554W",
            "description": "A novel of manners."
        })))
        .mount(&upstream)
        .await;
    let (base, _dir) = spawn_feeder(&upstream).await;

    let mut query = GatewayQuery::from_free_text("pride");
    query.filters.insert("fileType".into(), vec!["card".into()]);
    let resp: QueryResponse = reqwest::Client::new()
        .post(format!("{base}/query"))
        .json(&QueryRequest {
            upstream_id: "openlibrary".to_string(),
            query,
            max_results: 10,
        })
        .send()
        .await
        .expect("POST /query")
        .json()
        .await
        .expect("decode query response");

    assert_eq!(resp.records.len(), 1);
    let r = &resp.records[0];
    assert_eq!(r.upstream_id, "openlibrary");
    assert_eq!(r.record_id, "OL66554W");
    assert_eq!(r.fields.get("fileType").map(String::as_str), Some("card"));
    assert_eq!(
        r.fields
            .get("cids/bagdsaaaubnxxazlonruwe4tboj4u6tbwgy2tkncx")
            .map(String::as_str),
        Some("true")
    );
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
async fn compute_for_an_open_library_card_is_a_byteless_card_locator() {
    let upstream = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/search.json"))
        .and(query_param("q", "key:/works/OL45804W"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "numFound": 1,
            "docs": [{
                "key": "/works/OL45804W",
                "title": "Fantastic Mr Fox",
                "author_name": ["Roald Dahl"],
                "cover_i": 6498519,
                "first_publish_year": 1970
            }]
        })))
        .mount(&upstream)
        .await;
    Mock::given(method("GET"))
        .and(path("/works/OL45804W.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "key": "/works/OL45804W",
            "description": "A clever fox outwits three farmers."
        })))
        .mount(&upstream)
        .await;
    let (base, _dir) = spawn_feeder(&upstream).await;

    let resp: ComputeResponse = reqwest::Client::new()
        .post(format!("{base}/compute"))
        .json(&ComputeRequest {
            upstream_id: "openlibrary".to_string(),
            record_id: "OL45804W".to_string(),
        })
        .send()
        .await
        .expect("POST /compute")
        .json()
        .await
        .expect("decode compute response");

    assert_eq!(resp.outcomes.len(), 1);
    let o = &resp.outcomes[0];
    assert_eq!(o.hash, "bagdsaaaubnxxazlonruwe4tboj4u6tbugu4dancx");
    assert_eq!(o.hash_kind, HashKindDto::CardLocator);
    assert!(o.bytes_b64.is_none());
    assert!(o.file_extension.is_none());
    let record = o.record.as_ref().expect("the card record rides the outcome");
    assert_eq!(record.fields.get("title").map(String::as_str), Some("Fantastic Mr Fox"));
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
