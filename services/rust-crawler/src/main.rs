use anyhow::{anyhow, bail, Context, Result};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use flate2::read::GzDecoder;
use reqwest::{header::RANGE, Client, Url};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    env,
    io::{Cursor, Read},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;
use tokio_postgres::{Client as PgClient, NoTls};

const COLLINFO_URL: &str = "https://index.commoncrawl.org/collinfo.json";
const TOP_K: i64 = 5;
const DEFAULT_RECORD_LIMIT: usize = 100;
const DEFAULT_PAGE_LIMIT: usize = 30;
const MAX_PAGE_LIMIT: usize = 30;
const MAX_WARC_RECORD_BYTES: usize = 8 * 1024 * 1024;
const MAX_TEXT_CHARS: usize = 50_000;

#[derive(Clone)]
struct AppState {
    db: Arc<PgClient>,
    http: Client,
    configured_index: Option<String>,
    record_limit: usize,
    page_limit: usize,
    index_semaphore: Arc<Semaphore>,
    domain_similarity_threshold: f32,
}

#[derive(Debug, Deserialize)]
struct SearchRequest {
    #[serde(default)]
    original_query: String,
    #[serde(default)]
    normalized_query: String,
    #[serde(default)]
    requested_domain: Option<String>,
    #[serde(default)]
    candidate_domains: Vec<CandidateDomainInput>,
    #[serde(default)]
    index_collection: Option<String>,
    #[serde(default)]
    sector: Option<String>,
    #[serde(default)]
    keywords: Vec<String>,
    /// The normal search path stores only Common Crawl capture metadata. Page
    /// bodies are intentionally fetched only through an explicit detail call.
    #[serde(default = "default_true")]
    metadata_only: bool,
    #[serde(default)]
    query_embedding: Vec<f32>,
    #[serde(default)]
    url_embeddings: Vec<UrlEmbeddingInput>,
}

#[derive(Debug, Deserialize)]
struct UrlEmbeddingInput {
    url: String,
    embedding: Vec<f32>,
}

#[derive(Debug, Deserialize)]
struct ExtractRequest {
    url: String,
    #[serde(default)]
    sector: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum CandidateDomainInput {
    Name(String),
    Details(CandidateDomain),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct CandidateDomain {
    domain: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    captures: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    matched_pages: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    example_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    discovery_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    similarity: Option<f32>,
}

#[derive(Debug, Deserialize)]
struct CrawlInfo {
    id: String,
    #[serde(rename = "cdx-api")]
    cdx_api: String,
}

#[derive(Debug, Deserialize)]
struct CdxRecord {
    timestamp: String,
    url: String,
    digest: Option<String>,
    length: Option<String>,
    offset: Option<String>,
    filename: Option<String>,
    status: Option<String>,
    mime: Option<String>,
}

#[derive(Debug, Serialize)]
struct Website {
    domain: String,
    category: Option<String>,
    url: String,
    title: Option<String>,
    description: Option<String>,
    snippet: String,
    crawl_timestamp: Option<String>,
    warc_filename: Option<String>,
    warc_offset: Option<i64>,
}

#[derive(Debug, Serialize)]
struct SearchResponse {
    status: String,
    source: String,
    index_collection: Option<String>,
    requested_domain: Option<String>,
    sector: Option<String>,
    keywords: Vec<String>,
    original_query: String,
    domains: Vec<String>,
    registered_domains: Vec<String>,
    candidate_domains: Vec<CandidateDomain>,
    websites: Vec<Website>,
    matched_count: usize,
    crawled_pages: usize,
    crawl_failures: usize,
    needs_candidates: bool,
    needs_url_embeddings: bool,
    url_embedding_candidates: Vec<String>,
    message: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct IndexedURL {
    url: String,
    status: Option<i32>,
    mime_type: Option<String>,
    language: Option<String>,
    warc_filename: Option<String>,
    warc_offset: Option<i64>,
    warc_length: Option<i64>,
    fetch_time: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IndexedDomain {
    domain: String,
    sector_score: f32,
    embedding: Vec<f32>,
    #[serde(default)]
    sample_urls: Vec<IndexedURL>,
}

#[derive(Debug, Deserialize)]
struct IndexBatchRequest {
    collection: String,
    shard_path: String,
    sector: String,
    domain_count: i64,
    domains: Vec<IndexedDomain>,
}

#[derive(Debug, Deserialize)]
struct SectorEmbeddingInput {
    sector: String,
    description: String,
    embedding: Vec<f32>,
}

#[derive(Debug, Deserialize)]
struct SectorEmbeddingsRequest {
    sectors: Vec<SectorEmbeddingInput>,
}

#[derive(Debug, Deserialize)]
struct IndexCompleteRequest {
    collection: String,
    shard_path: String,
    sector: String,
    status: String,
    domain_count: i64,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IndexProgressQuery {
    collection: String,
    sector: String,
}

#[derive(Debug)]
struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({"error": self.1}))).into_response()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let db = connect_database().await?;
    ensure_metadata_schema(&db).await?;
    let domain_similarity_threshold = env::var("CC_DOMAIN_SIMILARITY_THRESHOLD")
        .unwrap_or_else(|_| "0.35".to_string())
        .parse::<f32>()
        .context("CC_DOMAIN_SIMILARITY_THRESHOLD must be a number from 0 to 1")?;
    if !(0.0..=1.0).contains(&domain_similarity_threshold) {
        bail!("CC_DOMAIN_SIMILARITY_THRESHOLD must be between 0 and 1");
    }
    let state = AppState {
        db: Arc::new(db),
        http: Client::builder()
            .timeout(Duration::from_secs(180))
            .user_agent("SearchEngineWork-rust-crawler/0.2 (Common Crawl index client)")
            .build()?,
        configured_index: env::var("CC_INDEX_URL")
            .ok()
            .filter(|v| !v.trim().is_empty()),
        record_limit: parse_limit("CC_RECORD_LIMIT", DEFAULT_RECORD_LIMIT, 100)?,
        page_limit: parse_limit("CC_PAGE_LIMIT", DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT)?,
        index_semaphore: Arc::new(Semaphore::new(parse_limit(
            "CC_MAX_PARALLEL_DOMAINS",
            2,
            5,
        )?)),
        domain_similarity_threshold,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/search", post(search))
        .route("/api/v1/pages/extract", post(extract_page))
        .route("/internal/v1/index/sectors", post(index_sectors))
        .route("/internal/v1/index/batch", post(index_batch))
        .route("/internal/v1/index/complete", post(complete_index_shard))
        .route("/internal/v1/index/progress", get(index_progress))
        .with_state(state);
    // Render sets PORT for each service. Keep the current localhost default for
    // local Docker development, while allowing an explicit address to override it.
    let address = env::var("CRAWLER_LISTEN_ADDR").unwrap_or_else(|_| {
        match env::var("PORT") {
            Ok(port) if !port.trim().is_empty() => format!("0.0.0.0:{port}"),
            _ => "127.0.0.1:8081".to_string(),
        }
    });
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .with_context(|| format!("could not bind crawler service to {address}"))?;
    println!("Rust crawler listening on http://{address}");
    axum::serve(listener, app)
        .await
        .context("crawler HTTP server failed")?;
    Ok(())
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status":"healthy","service":"rust-crawler"}))
}

async fn index_sectors(
    State(state): State<AppState>,
    Json(input): Json<SectorEmbeddingsRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mut stored = 0usize;
    for sector in input.sectors {
        validate_index_sector(&sector.sector)?;
        let vector = embedding_literal(&sector.embedding)?;
        state.db.execute(
            r#"INSERT INTO public.cc_index_sectors (sector, description, embedding, model_name, updated_at)
               VALUES ($1,$2,$3::text::vector,'BAAI/bge-small-en-v1.5',NOW())
               ON CONFLICT (sector) DO UPDATE SET description=EXCLUDED.description,
                 embedding=EXCLUDED.embedding, model_name=EXCLUDED.model_name, updated_at=NOW()"#,
            &[&sector.sector, &sector.description, &vector],
        ).await.map_err(|error| internal_error(error.into()))?;
        stored += 1;
    }
    Ok(Json(serde_json::json!({"stored_sector_vectors": stored})))
}

async fn index_batch(
    State(state): State<AppState>,
    Json(input): Json<IndexBatchRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    validate_index_sector(&input.sector)?;
    if input.collection.trim().is_empty() || input.shard_path.trim().is_empty() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "collection and shard_path are required".to_string(),
        ));
    }
    state
        .db
        .execute(
            r#"INSERT INTO public.cc_index_shards
           (collection_name, shard_path, sector, status, started_at, updated_at)
           VALUES ($1,$2,$3,'processing',NOW(),NOW())
           ON CONFLICT (collection_name, shard_path, sector) DO UPDATE SET
             status='processing', last_error=NULL, updated_at=NOW()"#,
            &[&input.collection, &input.shard_path, &input.sector],
        )
        .await
        .map_err(|error| internal_error(error.into()))?;

    let mut wire_domains = Vec::with_capacity(input.domains.len());
    let mut domain_count = 0usize;
    let mut url_count = 0usize;
    for domain in input.domains {
        let normalized_domain = normalize_domain(&domain.domain)
            .map_err(|error| ApiError(StatusCode::BAD_REQUEST, error.to_string()))?;
        if !domain.sector_score.is_finite() || !(0.0..=1.0).contains(&domain.sector_score) {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "sector_score must be a finite number from 0 to 1".to_string(),
            ));
        }
        let embedding = embedding_literal(&domain.embedding)?;
        for sample in &domain.sample_urls {
            let parsed = Url::parse(&sample.url).map_err(|_| {
                ApiError(
                    StatusCode::BAD_REQUEST,
                    format!("invalid sample URL: {}", sample.url),
                )
            })?;
            if !matches!(parsed.scheme(), "http" | "https") {
                return Err(ApiError(
                    StatusCode::BAD_REQUEST,
                    "sample URLs must use HTTP or HTTPS".to_string(),
                ));
            }
        }
        wire_domains.push(serde_json::json!({
            "domain": normalized_domain,
            "sector_score": domain.sector_score,
            "embedding": embedding,
            "sample_urls": domain.sample_urls,
        }));
        domain_count += 1;
        url_count += domain.sample_urls.len();
    }
    if !wire_domains.is_empty() {
        let payload = serde_json::Value::Array(wire_domains);
        let payload = serde_json::to_string(&payload)
            .map_err(|error| ApiError(StatusCode::BAD_REQUEST, error.to_string()))?;
        state.db.execute(
            r#"WITH batch AS (
                 SELECT * FROM jsonb_to_recordset($1::jsonb)
                 AS x(domain text, sector_score real, embedding text, sample_urls jsonb)
               )
               INSERT INTO public.cc_index_domains
                 (sector, domain, sector_similarity, domain_embedding, representative_urls, collection_name, processed_at)
               SELECT $2, domain, sector_score, embedding::vector, sample_urls, $3, NOW() FROM batch
               ON CONFLICT (sector, domain) DO UPDATE SET
                 sector_similarity=EXCLUDED.sector_similarity, domain_embedding=EXCLUDED.domain_embedding,
                 representative_urls=EXCLUDED.representative_urls, collection_name=EXCLUDED.collection_name,
                 classification_status='candidate', processed_at=NOW()"#,
            &[&payload, &input.sector, &input.collection],
        ).await.map_err(|error| internal_error(error.into()))?;
        state.db.execute(
            r#"WITH domain_rows AS (
                 SELECT * FROM jsonb_to_recordset($1::jsonb)
                 AS x(domain text, sample_urls jsonb)
               ), items AS (
                 SELECT d.domain, u.* FROM domain_rows d
                 CROSS JOIN LATERAL jsonb_to_recordset(d.sample_urls) AS u(
                   url text, status integer, mime_type text, language text,
                   warc_filename text, warc_offset bigint, warc_length bigint, fetch_time text)
               )
               INSERT INTO public.cc_index_urls
                 (sector, domain, url, collection_name, http_status, mime_type, language,
                  warc_filename, warc_offset, warc_length, fetch_time)
               SELECT $2, domain, url, $3, status, mime_type, language,
                      warc_filename, warc_offset, warc_length, fetch_time FROM items
               ON CONFLICT (sector, url, collection_name) DO UPDATE SET
                 domain=EXCLUDED.domain, http_status=EXCLUDED.http_status, mime_type=EXCLUDED.mime_type,
                 language=EXCLUDED.language, warc_filename=EXCLUDED.warc_filename,
                 warc_offset=EXCLUDED.warc_offset, warc_length=EXCLUDED.warc_length, fetch_time=EXCLUDED.fetch_time"#,
            &[&payload, &input.sector, &input.collection],
        ).await.map_err(|error| internal_error(error.into()))?;
    }
    state.db.execute(
        r#"UPDATE public.cc_index_shards SET domain_count=GREATEST(domain_count,$4), updated_at=NOW()
           WHERE collection_name=$1 AND shard_path=$2 AND sector=$3"#,
        &[&input.collection, &input.shard_path, &input.sector, &input.domain_count],
    ).await.map_err(|error| internal_error(error.into()))?;
    Ok(Json(
        serde_json::json!({"domains_upserted":domain_count,"sample_urls_upserted":url_count}),
    ))
}

async fn complete_index_shard(
    State(state): State<AppState>,
    Json(input): Json<IndexCompleteRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    validate_index_sector(&input.sector)?;
    if input.status != "completed" && input.status != "failed" {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "status must be completed or failed".to_string(),
        ));
    }
    state.db.execute(
        r#"INSERT INTO public.cc_index_shards
           (collection_name, shard_path, sector, status, domain_count, last_error, started_at, updated_at, completed_at)
           VALUES ($1,$2,$3,$4,$5,$6,NOW(),NOW(),CASE WHEN $4='completed' THEN NOW() ELSE NULL END)
           ON CONFLICT (collection_name, shard_path, sector) DO UPDATE SET
             status=EXCLUDED.status, domain_count=EXCLUDED.domain_count,
             last_error=EXCLUDED.last_error, updated_at=NOW(),
             completed_at=CASE WHEN EXCLUDED.status='completed' THEN NOW() ELSE NULL END"#,
        &[&input.collection, &input.shard_path, &input.sector, &input.status,
          &input.domain_count, &input.error],
    ).await.map_err(|error| internal_error(error.into()))?;
    Ok(Json(
        serde_json::json!({"status":input.status,"shard":input.shard_path}),
    ))
}

async fn index_progress(
    State(state): State<AppState>,
    Query(query): Query<IndexProgressQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    validate_index_sector(&query.sector)?;
    let rows = state.db.query(
        "SELECT shard_path FROM public.cc_index_shards WHERE collection_name=$1 AND sector=$2 AND status='completed' ORDER BY shard_path",
        &[&query.collection, &query.sector],
    ).await.map_err(|error| internal_error(error.into()))?;
    let completed: Vec<String> = rows.into_iter().map(|row| row.get(0)).collect();
    Ok(Json(
        serde_json::json!({"collection":query.collection,"sector":query.sector,"completed_shards":completed}),
    ))
}

fn validate_index_sector(sector: &str) -> Result<(), ApiError> {
    if sector != "technology" && sector != "health" {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "sector must be technology or health".to_string(),
        ));
    }
    Ok(())
}

fn embedding_literal(values: &[f32]) -> Result<String, ApiError> {
    vector_literal(values).map_err(|error| ApiError(StatusCode::BAD_REQUEST, error.to_string()))
}

fn vector_literal(values: &[f32]) -> Result<String> {
    if values.len() != 384 || values.iter().any(|value| !value.is_finite()) {
        bail!("embedding must contain 384 finite numbers");
    }
    Ok(format!(
        "[{}]",
        values
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    ))
}

async fn extract_page(
    State(state): State<AppState>,
    Json(input): Json<ExtractRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let url = input.url.trim();
    if url.is_empty() || Url::parse(url).is_err() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "url must be a complete http(s) URL".to_string(),
        ));
    }
    let sector = normalize_sector(input.sector);
    let row = state
        .db
        .query_opt(
            r#"SELECT domain, crawl_id, capture_timestamp, digest, warc_filename,
                  warc_offset, warc_length, extracted_text, sector
           FROM public.common_crawl_urls WHERE url=$1
           ORDER BY capture_timestamp DESC LIMIT 1"#,
            &[&url],
        )
        .await
        .map_err(|error| internal_error(error.into()))?
        .ok_or_else(|| {
            ApiError(
                StatusCode::NOT_FOUND,
                "URL is not indexed in this database".to_string(),
            )
        })?;
    let domain: String = row.get(0);
    let crawl_id: String = row.get(1);
    let timestamp: String = row.get(2);
    let digest: Option<String> = row.get(3);
    let filename: Option<String> = row.get(4);
    let offset: Option<i64> = row.get(5);
    let length: Option<i64> = row.get(6);
    let cached_text: Option<String> = row.get(7);
    let stored_sector: String = row.get(8);
    let cached = cached_text.is_some();
    let content = if let Some(text) = cached_text {
        text
    } else {
        let record = CdxRecord {
            timestamp,
            url: url.to_string(),
            digest,
            filename,
            offset: offset.map(|v| v.to_string()),
            length: length.map(|v| v.to_string()),
            status: Some("200".to_string()),
            mime: Some("text/html".to_string()),
        };
        let domain_id = ensure_domain(
            &state.db,
            &domain,
            sector.as_deref().or(Some(&stored_sector)),
        )
        .await
        .map_err(internal_error)?;
        let source_id = ensure_crawl_source(&state.db, &crawl_id)
            .await
            .map_err(internal_error)?;
        fetch_and_store_page(&state, domain_id, source_id, &record)
            .await
            .map_err(internal_error)?;
        let content: Option<String> = state
            .db
            .query_opt(
                "SELECT content FROM public.web_pages WHERE url=$1 ORDER BY id DESC LIMIT 1",
                &[&url],
            )
            .await
            .map_err(|error| internal_error(error.into()))?
            .map(|row| row.get(0));
        let content = content.unwrap_or_default();
        state.db.execute(
            "UPDATE public.common_crawl_urls SET extracted_text=$1, extracted_at=NOW() WHERE url=$2 AND crawl_id=$3",
            &[&content, &url, &crawl_id],
        ).await.map_err(|error| internal_error(error.into()))?;
        content
    };
    let top_lines = preview_lines(&content, 30, 120);
    Ok(Json(
        serde_json::json!({"url":url,"domain":domain,"sector":sector.unwrap_or(stored_sector),"cached":cached,"top_lines":top_lines}),
    ))
}

fn preview_lines(text: &str, limit: usize, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        if !current.is_empty() && current.chars().count() + word.chars().count() + 1 > width {
            lines.push(std::mem::take(&mut current));
            if lines.len() >= limit {
                return lines;
            }
        }
        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(word);
    }
    if !current.is_empty() && lines.len() < limit {
        lines.push(current);
    }
    lines
}

async fn connect_database() -> Result<PgClient> {
    let host = env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = env::var("DB_PORT")
        .unwrap_or_else(|_| "5432".to_string())
        .parse::<u16>()
        .context("DB_PORT must be a valid port number")?;
    let name = env::var("DB_NAME").context("DB_NAME is required in .env")?;
    let user = env::var("DB_USER").context("DB_USER is required in .env")?;
    let password = env::var("DB_PASSWORD").context("DB_PASSWORD is required in .env")?;
    let (client, connection) = tokio_postgres::Config::new()
        .host(&host)
        .port(port)
        .dbname(&name)
        .user(&user)
        .password(&password)
        .connect(NoTls)
        .await
        .with_context(|| format!("could not connect to PostgreSQL at {host}:{port}/{name}"))?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("PostgreSQL connection ended: {error}");
        }
    });
    Ok(client)
}

fn parse_limit(name: &str, default: usize, max: usize) -> Result<usize> {
    let value = env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<usize>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if value == 0 || value > max {
        bail!("{name} must be between 1 and {max}");
    }
    Ok(value)
}

async fn search(
    State(state): State<AppState>,
    Json(mut input): Json<SearchRequest>,
) -> Result<Json<SearchResponse>, ApiError> {
    let keywords = clean_keywords(&input.keywords);
    // Natural-language queries often contain multiple distinct entities (for
    // example, Java AND Python). Match any keyword, then rank relevant pages.
    let keyword_query = keywords.join(" OR ");
    let sector = normalize_sector(input.sector.take());
    let query_embedding = if input.query_embedding.is_empty() {
        None
    } else {
        if input.query_embedding.len() != 384
            || input.query_embedding.iter().any(|value| !value.is_finite())
        {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "query_embedding must contain 384 finite numbers".to_string(),
            ));
        }
        Some(input.query_embedding.clone())
    };
    if !input.url_embeddings.is_empty() {
        let Some(target_sector) = sector.as_deref() else {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "sector is required with url_embeddings".to_string(),
            ));
        };
        store_url_embeddings(&state.db, target_sector, &input.url_embeddings)
            .await
            .map_err(internal_error)?;
    }
    let requested_domain = input
        .requested_domain
        .as_deref()
        .map(normalize_domain)
        .transpose()
        .map_err(|error| ApiError(StatusCode::BAD_REQUEST, error.to_string()))?;
    // An explicit hostname is the strongest scope. Its stored category may be
    // absent or stale, so do not let sector metadata hide pages for that host.
    let sector_filter = if requested_domain.is_some() {
        None
    } else {
        sector.as_deref()
    };

    let mut websites = db_search(
        &state.db,
        requested_domain.as_deref(),
        sector_filter,
        &keyword_query,
    )
    .await
    .map_err(internal_error)?;
    if websites.is_empty() {
        websites = db_search_url_metadata(
            &state.db,
            sector_filter,
            &keywords,
            query_embedding.as_deref(),
        )
        .await
        .map_err(internal_error)?;
    }
    let registered_domains = if websites.is_empty()
        && requested_domain.is_none()
        && input.candidate_domains.is_empty()
    {
        db_registered_domains(&state.db, &keywords)
            .await
            .map_err(internal_error)?
    } else {
        Vec::new()
    };
    let mut crawl_id = None;
    let mut crawled_pages = 0usize;
    let mut crawl_failures = 0usize;
    let mut candidate_domains = Vec::new();
    for raw_candidate in &input.candidate_domains {
        let mut candidate = match raw_candidate {
            CandidateDomainInput::Name(domain) => CandidateDomain {
                domain: domain.clone(),
                captures: None,
                matched_pages: None,
                example_url: None,
                discovery_source: None,
                category: None,
                similarity: None,
            },
            CandidateDomainInput::Details(details) => details.clone(),
        };
        candidate.domain = normalize_domain(&candidate.domain)
            .map_err(|error| ApiError(StatusCode::BAD_REQUEST, error.to_string()))?;
        if !candidate_domains
            .iter()
            .any(|existing: &CandidateDomain| existing.domain == candidate.domain)
        {
            candidate_domains.push(candidate);
        }
    }
    if candidate_domains.is_empty() {
        candidate_domains.extend(registered_domains.iter().map(|domain| CandidateDomain {
            domain: domain.clone(),
            captures: None,
            matched_pages: None,
            example_url: None,
            discovery_source: Some("postgres_registered_domain".to_string()),
            category: None,
            similarity: None,
        }));
    }
    if candidate_domains.is_empty() && requested_domain.is_none() && registered_domains.is_empty() {
        if let (Some(target_sector), Some(embedding)) =
            (sector.as_deref(), query_embedding.as_deref())
        {
            candidate_domains = db_semantic_domain_candidates(
                &state.db,
                target_sector,
                embedding,
                state.domain_similarity_threshold,
            )
            .await
            .map_err(internal_error)?;
        }
    }

    if let (Some(target_sector), Some(embedding)) = (sector.as_deref(), query_embedding.as_deref())
    {
        let candidate_names: Vec<String> = candidate_domains
            .iter()
            .map(|candidate| candidate.domain.clone())
            .collect();
        let url_embedding_candidates =
            db_unembedded_url_candidates(&state.db, target_sector, &candidate_names)
                .await
                .map_err(internal_error)?;
        if !url_embedding_candidates.is_empty() {
            let domains: Vec<String> = candidate_domains
                .iter()
                .map(|candidate| candidate.domain.clone())
                .collect();
            let original_query = if input.original_query.trim().is_empty() {
                input.normalized_query.clone()
            } else {
                input.original_query.clone()
            };
            let _ = embedding;
            return Ok(Json(SearchResponse {
                status: "embedding_required".to_string(),
                source: "pgvector-domain-index".to_string(),
                index_collection: input.index_collection.clone(),
                requested_domain: requested_domain.clone(),
                sector: sector.clone(),
                keywords: keywords.clone(),
                original_query,
                domains,
                registered_domains: registered_domains.clone(),
                candidate_domains,
                websites: Vec::new(),
                matched_count: 0,
                crawled_pages: 0,
                crawl_failures: 0,
                needs_candidates: false,
                needs_url_embeddings: true,
                url_embedding_candidates,
                message: "Candidate-domain URLs need embeddings; Go will compute them through Python and resubmit.".to_string(),
            }));
        }
    }
    let mut targets: Vec<String> = candidate_domains
        .iter()
        .map(|candidate| candidate.domain.clone())
        .collect();
    if targets.is_empty() {
        if let Some(domain) = requested_domain.as_ref() {
            targets.push(domain.clone());
        }
    }

    if websites.len() < TOP_K as usize && !targets.is_empty() {
        let (collection, index_url) = selected_index(&state, input.index_collection.as_deref())
            .await
            .map_err(internal_error)?;
        crawl_id = Some(collection.clone());
        let source_id = ensure_crawl_source(&state.db, &collection)
            .await
            .map_err(internal_error)?;
        let target_count = targets.len();
        let mut crawl_tasks = tokio::task::JoinSet::new();
        for (index, domain) in targets.into_iter().enumerate() {
            let page_budget = (state.page_limit / target_count
                + usize::from(index < state.page_limit % target_count))
            .min(6);
            if page_budget == 0 {
                continue;
            }
            let task_state = state.clone();
            let task_sector = sector.clone();
            let task_collection = collection.clone();
            let task_index_url = index_url.clone();
            let task_keywords = keywords.clone();
            let task_source_id = source_id;
            crawl_tasks.spawn(async move {
                let permit = match task_state.index_semaphore.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(error) => {
                        return (
                            domain,
                            Err(anyhow!("Common Crawl concurrency control failed: {error}")),
                        )
                    }
                };
                let result = crawl_domain(
                    &task_state,
                    &domain,
                    task_sector.as_deref(),
                    &task_collection,
                    &task_index_url,
                    &task_keywords,
                    task_source_id,
                    page_budget,
                    input.metadata_only,
                )
                .await;
                drop(permit);
                (domain, result)
            });
        }
        while let Some(task_result) = crawl_tasks.join_next().await {
            match task_result {
                Ok((_domain, Ok((saved, failed)))) => {
                    crawled_pages += saved;
                    crawl_failures += failed;
                }
                Ok((domain, Err(error))) => {
                    crawl_failures += 1;
                    eprintln!("Common Crawl failed for {domain}: {error:#}");
                }
                Err(error) => {
                    crawl_failures += 1;
                    eprintln!("Common Crawl task failed: {error}");
                }
            }
        }
        websites = db_search(
            &state.db,
            requested_domain.as_deref(),
            sector_filter,
            &keyword_query,
        )
        .await
        .map_err(internal_error)?;
        if websites.is_empty() {
            websites = db_search_url_metadata(
                &state.db,
                sector_filter,
                &keywords,
                query_embedding.as_deref(),
            )
            .await
            .map_err(internal_error)?;
        }
        if websites.is_empty() && !candidate_domains.is_empty() {
            let domains: Vec<String> = candidate_domains
                .iter()
                .map(|candidate| candidate.domain.clone())
                .collect();
            websites = db_search_urls_from_domains(
                &state.db,
                sector_filter,
                &domains,
                query_embedding.as_deref(),
            )
            .await
            .map_err(internal_error)?;
        }
    }

    let mut domains: Vec<String> = Vec::new();
    for website in &websites {
        if !domains.contains(&website.domain) && domains.len() < TOP_K as usize {
            domains.push(website.domain.clone());
        }
    }
    let needs_candidates = websites.is_empty()
        && requested_domain.is_none()
        && input.candidate_domains.is_empty()
        && candidate_domains.is_empty()
        && registered_domains.is_empty();
    let source = if crawled_pages > 0 {
        "database+common_crawl"
    } else {
        "database"
    };
    let message = if !websites.is_empty() {
        format!("Found {} matching website page(s).", websites.len())
    } else if needs_candidates {
        "No matching URL metadata is stored yet. Run Common Crawl Parquet domain discovery, then submit candidate domains for CDX URL-index metadata.".to_string()
    } else if crawl_failures > 0 {
        "No matching URLs found; Common Crawl or the metadata database was unavailable.".to_string()
    } else {
        "No matching URLs were found in the database or selected Common Crawl captures.".to_string()
    };

    Ok(Json(SearchResponse {
        status: if websites.is_empty() {
            "no_results"
        } else {
            "success"
        }
        .to_string(),
        source: source.to_string(),
        index_collection: crawl_id,
        requested_domain,
        sector,
        keywords,
        original_query: if input.original_query.trim().is_empty() {
            input.normalized_query
        } else {
            input.original_query
        },
        domains,
        registered_domains,
        candidate_domains,
        matched_count: websites.len(),
        websites,
        crawled_pages,
        crawl_failures,
        needs_candidates,
        needs_url_embeddings: false,
        url_embedding_candidates: Vec::new(),
        message,
    }))
}

async fn ensure_metadata_schema(db: &PgClient) -> Result<()> {
    db.batch_execute(
        r#"
        CREATE EXTENSION IF NOT EXISTS vector;
        CREATE TABLE IF NOT EXISTS public.common_crawl_urls (
            id BIGSERIAL PRIMARY KEY,
            sector VARCHAR(32) NOT NULL,
            domain TEXT NOT NULL,
            url TEXT NOT NULL,
            crawl_id TEXT NOT NULL,
            capture_timestamp TEXT NOT NULL,
            digest TEXT,
            warc_filename TEXT,
            warc_offset BIGINT,
            warc_length BIGINT,
            http_status INTEGER,
            mime_type TEXT,
            extracted_text TEXT,
            extracted_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (crawl_id, url, capture_timestamp)
        );
        CREATE INDEX IF NOT EXISTS common_crawl_urls_sector_domain_idx
            ON public.common_crawl_urls (sector, domain);
        CREATE INDEX IF NOT EXISTS common_crawl_urls_url_idx
            ON public.common_crawl_urls (url);
        CREATE TABLE IF NOT EXISTS public.cc_index_sectors (
            sector TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            embedding vector(384) NOT NULL,
            model_name TEXT NOT NULL DEFAULT 'BAAI/bge-small-en-v1.5',
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS public.cc_index_domains (
            sector TEXT NOT NULL,
            domain TEXT NOT NULL,
            sector_similarity REAL NOT NULL,
            domain_embedding vector(384) NOT NULL,
            representative_urls JSONB NOT NULL DEFAULT '[]'::jsonb,
            collection_name TEXT NOT NULL,
            classification_status TEXT NOT NULL DEFAULT 'candidate',
            processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (sector, domain)
        );
        CREATE INDEX IF NOT EXISTS cc_index_domains_embedding_hnsw
            ON public.cc_index_domains USING hnsw (domain_embedding vector_cosine_ops);
        CREATE INDEX IF NOT EXISTS cc_index_domains_sector_score_idx
            ON public.cc_index_domains (sector, sector_similarity DESC);
        CREATE TABLE IF NOT EXISTS public.cc_index_urls (
            sector TEXT NOT NULL,
            domain TEXT NOT NULL,
            url TEXT NOT NULL,
            collection_name TEXT NOT NULL,
            http_status INTEGER,
            mime_type TEXT,
            language TEXT,
            warc_filename TEXT,
            warc_offset BIGINT,
            warc_length BIGINT,
            fetch_time TEXT,
            url_embedding vector(384),
            embedding_model TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (sector, url, collection_name)
        );
        CREATE INDEX IF NOT EXISTS cc_index_urls_domain_idx
            ON public.cc_index_urls (sector, domain);
        CREATE INDEX IF NOT EXISTS cc_index_urls_embedding_hnsw
            ON public.cc_index_urls USING hnsw (url_embedding vector_cosine_ops)
            WHERE url_embedding IS NOT NULL;
        CREATE TABLE IF NOT EXISTS public.cc_index_shards (
            collection_name TEXT NOT NULL,
            shard_path TEXT NOT NULL,
            sector TEXT NOT NULL,
            status TEXT NOT NULL,
            domain_count BIGINT NOT NULL DEFAULT 0,
            last_error TEXT,
            started_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at TIMESTAMPTZ,
            PRIMARY KEY (collection_name, shard_path, sector)
        );
    "#,
    )
    .await
    .context("could not create Common Crawl URL metadata table")?;
    Ok(())
}

async fn db_search_url_metadata(
    db: &PgClient,
    sector: Option<&str>,
    keywords: &[String],
    query_embedding: Option<&[f32]>,
) -> Result<Vec<Website>> {
    let patterns: Vec<String> = keywords
        .iter()
        .filter(|word| word.as_str() != "c")
        .map(|word| format!("%{}%", word.to_lowercase()))
        .collect();
    if patterns.is_empty() {
        return Ok(Vec::new());
    }
    let rows = if let Some(embedding) = query_embedding {
        let vector = vector_literal(embedding)?;
        db.query(
            r#"SELECT c.domain, c.url, c.capture_timestamp, c.warc_filename, c.warc_offset, c.extracted_text
               FROM public.common_crawl_urls c
               LEFT JOIN LATERAL (
                   SELECT i.url_embedding FROM public.cc_index_urls i
                   WHERE i.sector=c.sector AND i.url=c.url AND i.url_embedding IS NOT NULL
                   ORDER BY i.created_at DESC LIMIT 1
               ) u ON TRUE
               WHERE ($1::text IS NULL OR c.sector = $1)
                 AND (lower(c.domain) LIKE ANY($2::text[]) OR lower(c.url) LIKE ANY($2::text[]))
               ORDER BY u.url_embedding <=> $3::text::vector ASC NULLS LAST, c.capture_timestamp DESC
               LIMIT 5"#,
            &[&sector, &patterns, &vector],
        ).await.context("could not vector-rank stored Common Crawl URLs")?
    } else {
        db.query(
            r#"SELECT domain, url, capture_timestamp, warc_filename, warc_offset, extracted_text
               FROM public.common_crawl_urls
               WHERE ($1::text IS NULL OR sector = $1)
                 AND (lower(domain) LIKE ANY($2::text[]) OR lower(url) LIKE ANY($2::text[]))
               ORDER BY capture_timestamp DESC LIMIT 5"#,
            &[&sector, &patterns],
        )
        .await
        .context("could not search stored Common Crawl URL metadata")?
    };
    let mut results = Vec::new();
    for row in rows {
        let domain: String = row.get(0);
        let url: String = row.get(1);
        let timestamp: String = row.get(2);
        let filename: Option<String> = row.get(3);
        let offset: Option<i64> = row.get(4);
        let text: Option<String> = row.get(5);
        let snippet = text
            .as_deref()
            .map(|body| make_snippet(body, &[]))
            .unwrap_or_else(|| {
                "Common Crawl URL metadata; page content has not been extracted.".to_string()
            });
        results.push(Website {
            domain,
            category: sector.map(str::to_string),
            url,
            title: None,
            description: None,
            snippet,
            crawl_timestamp: Some(timestamp),
            warc_filename: filename,
            warc_offset: offset,
        });
    }
    Ok(results)
}

async fn store_url_embeddings(
    db: &PgClient,
    sector: &str,
    embeddings: &[UrlEmbeddingInput],
) -> Result<usize> {
    let mut stored = 0usize;
    for item in embeddings {
        let vector = vector_literal(&item.embedding)?;
        let count = db
            .execute(
                r#"UPDATE public.cc_index_urls
               SET url_embedding=$3::text::vector, embedding_model='BAAI/bge-small-en-v1.5'
               WHERE sector=$1 AND url=$2"#,
                &[&sector, &item.url, &vector],
            )
            .await
            .context("could not cache candidate URL embedding")?;
        stored += count as usize;
    }
    Ok(stored)
}

async fn db_semantic_domain_candidates(
    db: &PgClient,
    sector: &str,
    query_embedding: &[f32],
    threshold: f32,
) -> Result<Vec<CandidateDomain>> {
    let query_vector = vector_literal(query_embedding)?;
    let rows = db
        .query(
            r#"SELECT d.domain,
                  1.0 - (d.domain_embedding <=> s.embedding) AS sector_score,
                  d.representative_urls->0->>'url' AS example_url
           FROM public.cc_index_domains d
           JOIN public.cc_index_sectors s ON s.sector=d.sector
           WHERE d.sector=$1
             AND 1.0 - (d.domain_embedding <=> s.embedding) >= $3::real
           ORDER BY d.domain_embedding <=> $2::text::vector
           LIMIT 5"#,
            &[&sector, &query_vector, &threshold],
        )
        .await
        .context("could not search the pgvector domain index")?;
    Ok(rows
        .into_iter()
        .map(|row| CandidateDomain {
            domain: row.get(0),
            captures: None,
            matched_pages: None,
            example_url: row.get(2),
            discovery_source: Some("pgvector_domain_embedding".to_string()),
            category: Some("sector_candidate".to_string()),
            similarity: Some(row.get(1)),
        })
        .collect())
}

async fn db_unembedded_url_candidates(
    db: &PgClient,
    sector: &str,
    domains: &[String],
) -> Result<Vec<String>> {
    if domains.is_empty() {
        return Ok(Vec::new());
    }
    let rows = db
        .query(
            r#"SELECT DISTINCT ON (url) url
           FROM public.cc_index_urls
           WHERE sector=$1 AND domain=ANY($2::text[]) AND url_embedding IS NULL
           ORDER BY url, fetch_time DESC NULLS LAST
           LIMIT 100"#,
            &[&sector, &domains],
        )
        .await
        .context("could not find candidate URLs requiring embeddings")?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

async fn db_search_urls_from_domains(
    db: &PgClient,
    sector: Option<&str>,
    domains: &[String],
    query_embedding: Option<&[f32]>,
) -> Result<Vec<Website>> {
    if domains.is_empty() {
        return Ok(Vec::new());
    }
    let rows = if let Some(embedding) = query_embedding {
        let vector = vector_literal(embedding)?;
        db.query(
            r#"SELECT DISTINCT ON (c.domain) c.domain, c.url, c.capture_timestamp, c.warc_filename, c.warc_offset
               FROM public.common_crawl_urls c
               LEFT JOIN LATERAL (
                   SELECT i.url_embedding FROM public.cc_index_urls i
                   WHERE i.sector=c.sector AND i.url=c.url AND i.url_embedding IS NOT NULL
                   ORDER BY i.created_at DESC LIMIT 1
               ) u ON TRUE
               WHERE ($1::text IS NULL OR c.sector = $1) AND c.domain = ANY($2::text[])
               ORDER BY c.domain, u.url_embedding <=> $3::text::vector ASC NULLS LAST, c.capture_timestamp DESC
               LIMIT 5"#,
            &[&sector, &domains, &vector],
        ).await.context("could not vector-rank candidate-domain URLs")?
    } else {
        db.query(
            r#"SELECT DISTINCT ON (domain) domain, url, capture_timestamp, warc_filename, warc_offset
               FROM public.common_crawl_urls
               WHERE ($1::text IS NULL OR sector = $1) AND domain = ANY($2::text[])
               ORDER BY domain, capture_timestamp DESC
               LIMIT 5"#,
            &[&sector, &domains],
        ).await.context("could not load URLs from discovered candidate domains")?
    };
    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let domain: String = row.get(0);
        let url: String = row.get(1);
        let timestamp: String = row.get(2);
        let filename: Option<String> = row.get(3);
        let offset: Option<i64> = row.get(4);
        results.push(Website {
            domain,
            category: sector.map(str::to_string),
            url,
            title: None,
            description: None,
            snippet: "Common Crawl URL capture found for a sector-relevant candidate domain; page text is not verified yet.".to_string(),
            crawl_timestamp: Some(timestamp),
            warc_filename: filename,
            warc_offset: offset,
        });
    }
    Ok(results)
}

fn internal_error(error: anyhow::Error) -> ApiError {
    eprintln!("Crawler request failed: {error:#}");
    ApiError(
        StatusCode::BAD_GATEWAY,
        "crawler or database operation failed".to_string(),
    )
}

fn clean_keywords(values: &[String]) -> Vec<String> {
    let mut result = Vec::new();
    for value in values {
        let word = value.trim().to_lowercase();
        if (word.chars().count() > 1 || matches!(word.as_str(), "c" | "c++" | "c#"))
            && !result.contains(&word)
        {
            result.push(word);
        }
        if result.len() >= 20 {
            break;
        }
    }
    result
}

fn normalize_sector(value: Option<String>) -> Option<String> {
    value
        .map(|v| v.trim().to_lowercase())
        .filter(|v| v == "technology" || v == "health")
}

fn normalize_domain(value: &str) -> Result<String> {
    let raw = value.trim();
    if raw.is_empty() {
        bail!("domain cannot be empty");
    }
    let parsed = Url::parse(raw)
        .or_else(|_| Url::parse(&format!("https://{raw}")))
        .with_context(|| format!("invalid domain: {raw}"))?;
    if parsed.username() != ""
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        bail!("domain must not contain credentials, query, or fragment");
    }
    if parsed.path() != "/" {
        bail!("send a hostname or origin, without a path");
    }
    Ok(parsed
        .host_str()
        .ok_or_else(|| anyhow!("domain has no hostname"))?
        .trim_end_matches('.')
        .to_ascii_lowercase())
}

async fn db_search(
    db: &PgClient,
    domain: Option<&str>,
    sector: Option<&str>,
    keywords: &str,
) -> Result<Vec<Website>> {
    let rows = db.query(
        r#"
        WITH ranked_pages AS (
            SELECT d.domain_name, d.category, p.url, p.title, p.description,
                   left(COALESCE(p.content, ''), 4000) AS page_text,
                   p.crawl_timestamp, p.warc_filename, p.warc_offset,
                   CASE WHEN $3 = '' THEN 0::real ELSE ts_rank(
                       COALESCE(p.search_vector, to_tsvector('simple', concat_ws(' ', p.title, p.description, p.content))),
                       websearch_to_tsquery('simple', $3)
                   ) END AS relevance,
                   ROW_NUMBER() OVER (
                       PARTITION BY CASE WHEN $1::text IS NULL THEN COALESCE(d.domain_name, p.url) ELSE p.url END
                       ORDER BY CASE WHEN $3 = '' THEN 0::real ELSE ts_rank(
                           COALESCE(p.search_vector, to_tsvector('simple', concat_ws(' ', p.title, p.description, p.content))),
                           websearch_to_tsquery('simple', $3)
                       ) END DESC, p.crawl_timestamp DESC NULLS LAST
                   ) AS domain_rank
            FROM public.web_pages p
            LEFT JOIN public.domains d ON d.id = p.domain_id
            WHERE ($1::text IS NULL OR lower(d.domain_name) = $1 OR lower(d.domain_name) LIKE '%.' || $1)
              AND ($2::text IS NULL OR lower(d.category) = $2)
              AND ($3 = '' OR COALESCE(p.search_vector, to_tsvector('simple', concat_ws(' ', p.title, p.description, p.content))) @@ websearch_to_tsquery('simple', $3))
        )
        SELECT domain_name, category, url, title, description, page_text,
               crawl_timestamp, warc_filename, warc_offset
        FROM ranked_pages
        WHERE domain_rank = 1
        ORDER BY relevance DESC, crawl_timestamp DESC NULLS LAST
        LIMIT 5
        "#,
        &[&domain, &sector, &keywords],
    ).await.context("database search failed")?;
    let mut pages = Vec::new();
    for row in rows {
        let domain_name: Option<String> = row.get(0);
        let category: Option<String> = row.get(1);
        let url: String = row.get(2);
        let title: Option<String> = row.get(3);
        let description: Option<String> = row.get(4);
        let text: String = row.get(5);
        let timestamp: Option<String> = row.get(6);
        let filename: Option<String> = row.get(7);
        let offset: Option<i64> = row.get(8);
        let snippet = make_snippet(&text, &keywords.split_whitespace().collect::<Vec<_>>());
        pages.push(Website {
            domain: domain_name.unwrap_or_else(|| host_from_url(&url)),
            category,
            url,
            title,
            description,
            snippet,
            crawl_timestamp: timestamp,
            warc_filename: filename,
            warc_offset: offset,
        });
    }
    pages.truncate(TOP_K as usize);
    Ok(pages)
}

async fn db_registered_domains(db: &PgClient, keywords: &[String]) -> Result<Vec<String>> {
    let patterns: Vec<String> = keywords
        .iter()
        .filter(|keyword| keyword.as_str() != "c")
        .map(|keyword| format!("%{}%", keyword.to_lowercase()))
        .collect();
    if patterns.is_empty() {
        return Ok(Vec::new());
    }
    let rows = db
        .query(
            "SELECT domain_name FROM public.domains WHERE lower(domain_name) LIKE ANY($1::text[]) ORDER BY id LIMIT 5",
            &[&patterns],
        )
        .await
        .context("could not find registered domains related to the query")?;
    Ok(rows
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect())
}

fn make_snippet(text: &str, keywords: &[&str]) -> String {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let lower = normalized.to_lowercase();
    let pos = keywords
        .iter()
        .filter_map(|word| lower.find(word))
        .min()
        .unwrap_or(0);
    let start = pos.saturating_sub(160).min(normalized.len());
    let end = (start + 500).min(normalized.len());
    let mut snippet = normalized
        .get(start..end)
        .unwrap_or(&normalized)
        .to_string();
    if start > 0 {
        snippet.insert_str(0, "…");
    }
    if end < normalized.len() {
        snippet.push('…');
    }
    snippet
}

fn host_from_url(value: &str) -> String {
    Url::parse(value)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_default()
}

async fn selected_index(
    state: &AppState,
    requested_collection: Option<&str>,
) -> Result<(String, String)> {
    if let Some(url) = state.configured_index.as_ref() {
        return Ok((index_id_from_url(url), url.clone()));
    }
    let response = state
        .http
        .get(COLLINFO_URL)
        .send()
        .await?
        .error_for_status()
        .context("Common Crawl collection list request failed")?;
    let collections: Vec<CrawlInfo> = response
        .json()
        .await
        .context("invalid Common Crawl collection JSON")?;
    let selected = if let Some(id) = requested_collection {
        collections
            .iter()
            .find(|collection| collection.id == id)
            .ok_or_else(|| anyhow!("requested Common Crawl collection {id} is unavailable"))?
    } else {
        collections
            .first()
            .ok_or_else(|| anyhow!("Common Crawl returned no collections"))?
    };
    Ok((selected.id.clone(), selected.cdx_api.clone()))
}

fn index_id_from_url(url: &str) -> String {
    url.trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("unknown")
        .trim_end_matches("-index")
        .to_string()
}

async fn crawl_domain(
    state: &AppState,
    domain: &str,
    sector: Option<&str>,
    crawl_id: &str,
    index_url: &str,
    keywords: &[String],
    source_id: i64,
    page_budget: usize,
    metadata_only: bool,
) -> Result<(usize, usize)> {
    let domain_id = ensure_domain(&state.db, domain, sector).await?;
    let job_id: i64 = state.db.query_one(
        "INSERT INTO public.crawl_jobs (seed_url, collection_name, status, pages_processed, pages_failed, started_at, created_at) VALUES ($1, $2, 'running', 0, 0, NOW(), NOW()) RETURNING id",
        &[&format!("https://{domain}/"), &crawl_id],
    ).await.context("could not create crawl job")?.get(0);

    let result = crawl_domain_records(
        state,
        domain,
        sector,
        domain_id,
        source_id,
        crawl_id,
        index_url,
        keywords,
        page_budget,
        metadata_only,
    )
    .await;
    match result {
        Ok((saved, failed)) => {
            state.db.execute("UPDATE public.crawl_jobs SET status='completed', pages_processed=$1, pages_failed=$2, completed_at=NOW() WHERE id=$3", &[&(saved as i32), &(failed as i32), &job_id]).await?;
            Ok((saved, failed))
        }
        Err(error) => {
            let _ = state
                .db
                .execute(
                    "UPDATE public.crawl_jobs SET status='failed', completed_at=NOW() WHERE id=$1",
                    &[&job_id],
                )
                .await;
            Err(error)
        }
    }
}

async fn ensure_domain(db: &PgClient, domain: &str, sector: Option<&str>) -> Result<i64> {
    let row = db.query_one(
        "INSERT INTO public.domains AS d (domain_name, category, created_at) VALUES ($1, $2, NOW()) ON CONFLICT (domain_name) DO UPDATE SET category=COALESCE(d.category, EXCLUDED.category) RETURNING id",
        &[&domain, &sector],
    ).await.context("could not create or retrieve registered domain")?;
    Ok(row.get(0))
}

async fn ensure_crawl_source(db: &PgClient, crawl_id: &str) -> Result<i64> {
    // The existing schema check constraint allows only WET and WARC.
    if let Some(row) = db.query_opt("SELECT id FROM public.crawl_sources WHERE collection_name=$1 AND record_type='WARC' ORDER BY id LIMIT 1", &[&crawl_id]).await? {
        return Ok(row.get(0));
    }
    let row = db.query_one("INSERT INTO public.crawl_sources (collection_name, record_type, manifest_path, created_at) VALUES ($1, 'WARC', NULL, NOW()) RETURNING id", &[&crawl_id]).await?;
    Ok(row.get(0))
}

async fn crawl_domain_records(
    state: &AppState,
    domain: &str,
    _sector: Option<&str>,
    domain_id: i64,
    source_id: i64,
    crawl_id: &str,
    index_url: &str,
    _keywords: &[String],
    page_budget: usize,
    metadata_only: bool,
) -> Result<(usize, usize)> {
    let records = query_index(&state.http, index_url, domain, state.record_limit).await?;
    if metadata_only {
        let mut stored = 0usize;
        let mut failed = 0usize;
        for record in records.iter().take(page_budget) {
            match store_url_metadata(&state.db, domain, _sector, crawl_id, record).await {
                Ok(()) => stored += 1,
                Err(error) => {
                    failed += 1;
                    eprintln!("Could not store Common Crawl URL {}: {error:#}", record.url);
                }
            }
        }
        return Ok((stored, failed));
    }
    let mut saved = 0usize;
    let mut failed = 0usize;
    let mut fetched = 0usize;
    let mut seen_urls = HashSet::new();
    let mut processed_urls = HashSet::new();
    let mut link_queue = VecDeque::new();

    if let Some(seed) = records.first() {
        seen_urls.insert(seed.url.clone());
        processed_urls.insert(seed.url.clone());
        let already_stored = page_capture_exists(&state.db, &seed.url, &seed.timestamp).await?;
        if !already_stored {
            fetched += 1;
            match fetch_and_store_page(state, domain_id, source_id, seed).await {
                Ok((true, links)) => {
                    saved += 1;
                    enqueue_same_domain_links(domain, links, &mut link_queue, &mut seen_urls);
                }
                Ok((false, _)) => {}
                Err(error) => {
                    failed += 1;
                    eprintln!("Could not archive page {}: {error:#}", seed.url);
                }
            }
        }
    }

    // Prefer archived URLs found in same-domain href links from the seed page.
    while fetched < page_budget {
        let Some(linked_url) = link_queue.pop_front() else {
            break;
        };
        let Some(record) = records
            .iter()
            .find(|record| strip_url_fragment(&record.url) == linked_url)
        else {
            continue;
        };
        if !processed_urls.insert(record.url.clone()) {
            continue;
        }
        if page_capture_exists(&state.db, &record.url, &record.timestamp).await? {
            continue;
        }
        fetched += 1;
        match fetch_and_store_page(state, domain_id, source_id, record).await {
            Ok((true, links)) => {
                saved += 1;
                enqueue_same_domain_links(domain, links, &mut link_queue, &mut seen_urls);
            }
            Ok((false, _)) => {}
            Err(error) => {
                failed += 1;
                eprintln!("Could not archive linked page {}: {error:#}", record.url);
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Fill any remaining page budget with recent URLs from the same CDX domain search.
    for record in &records {
        if fetched >= page_budget {
            break;
        }
        if !processed_urls.insert(record.url.clone()) {
            continue;
        }
        if page_capture_exists(&state.db, &record.url, &record.timestamp).await? {
            continue;
        }
        seen_urls.insert(record.url.clone());
        fetched += 1;
        match fetch_and_store_page(state, domain_id, source_id, record).await {
            Ok((true, links)) => {
                saved += 1;
                enqueue_same_domain_links(domain, links, &mut link_queue, &mut seen_urls);
            }
            Ok((false, _)) => {}
            Err(error) => {
                failed += 1;
                eprintln!("Could not archive page {}: {error:#}", record.url);
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = crawl_id;
    Ok((saved, failed))
}

async fn store_url_metadata(
    db: &PgClient,
    domain: &str,
    sector: Option<&str>,
    crawl_id: &str,
    record: &CdxRecord,
) -> Result<()> {
    let offset = record
        .offset
        .as_deref()
        .and_then(|value| value.parse::<i64>().ok());
    let length = record
        .length
        .as_deref()
        .and_then(|value| value.parse::<i64>().ok());
    let status = record
        .status
        .as_deref()
        .and_then(|value| value.parse::<i32>().ok());
    db.execute(
        r#"INSERT INTO public.common_crawl_urls
           (sector, domain, url, crawl_id, capture_timestamp, digest, warc_filename,
            warc_offset, warc_length, http_status, mime_type)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
           ON CONFLICT (crawl_id, url, capture_timestamp) DO UPDATE SET
             sector=EXCLUDED.sector, domain=EXCLUDED.domain, digest=EXCLUDED.digest,
             warc_filename=EXCLUDED.warc_filename, warc_offset=EXCLUDED.warc_offset,
             warc_length=EXCLUDED.warc_length, http_status=EXCLUDED.http_status,
             mime_type=EXCLUDED.mime_type"#,
        &[
            &sector.unwrap_or("technology"),
            &domain,
            &record.url,
            &crawl_id,
            &record.timestamp,
            &record.digest,
            &record.filename,
            &offset,
            &length,
            &status,
            &record.mime,
        ],
    )
    .await
    .context("could not insert Common Crawl URL metadata")?;
    Ok(())
}

async fn page_capture_exists(db: &PgClient, url: &str, timestamp: &str) -> Result<bool> {
    Ok(db.query_opt(
        "SELECT id FROM public.web_pages WHERE url=$1 AND crawl_timestamp=$2 ORDER BY id LIMIT 1",
        &[&url, &timestamp],
    ).await?.is_some())
}

fn strip_url_fragment(value: &str) -> String {
    match Url::parse(value) {
        Ok(mut url) => {
            url.set_fragment(None);
            url.to_string()
        }
        Err(_) => value.to_string(),
    }
}

fn enqueue_same_domain_links(
    domain: &str,
    links: Vec<String>,
    queue: &mut VecDeque<String>,
    seen: &mut HashSet<String>,
) {
    for link in links {
        let link = strip_url_fragment(&link);
        let host = host_from_url(&link);
        if (host == domain || host.ends_with(&format!(".{domain}"))) && seen.insert(link.clone()) {
            queue.push_back(link);
        }
    }
}

async fn query_index(
    http: &Client,
    index_url: &str,
    domain: &str,
    limit: usize,
) -> Result<Vec<CdxRecord>> {
    let mut url = Url::parse(index_url).context("Common Crawl index URL is invalid")?;
    url.query_pairs_mut()
        .append_pair("url", domain)
        .append_pair("matchType", "domain")
        .append_pair("output", "json")
        .append_pair("filter", "status:200")
        .append_pair("filter", "mime:text/html")
        .append_pair("limit", &limit.to_string());
    let mut body = None;
    const MAX_INDEX_ATTEMPTS: u32 = 3;
    for attempt in 1..=MAX_INDEX_ATTEMPTS {
        match http
            .get(url.clone())
            .timeout(Duration::from_secs(30))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                body = Some(
                    response
                        .text()
                        .await
                        .context("could not read Common Crawl index response")?,
                );
                break;
            }
            Ok(response) => {
                let status = response.status();
                let retryable = status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error();
                let retry_after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok());
                let error_body = response.text().await.unwrap_or_default();
                if status == StatusCode::NOT_FOUND
                    && error_body.to_lowercase().contains("no captures found")
                {
                    eprintln!("Common Crawl has no captures for {domain} in the selected snapshot");
                    return Ok(Vec::new());
                }
                if !retryable || attempt == MAX_INDEX_ATTEMPTS {
                    bail!(
                        "Common Crawl index returned HTTP {status}: {}",
                        error_body.chars().take(250).collect::<String>()
                    );
                }
                let backoff = retry_after
                    .unwrap_or_else(|| 2u64.pow(attempt).min(10))
                    .min(10);
                eprintln!(
                    "Common Crawl index returned HTTP {status}; retry {attempt}/{MAX_INDEX_ATTEMPTS} after {backoff}s"
                );
                tokio::time::sleep(Duration::from_secs(backoff)).await;
            }
            Err(error) => {
                if attempt == MAX_INDEX_ATTEMPTS {
                    return Err(error).context(format!(
                        "Common Crawl index request failed after {MAX_INDEX_ATTEMPTS} attempts"
                    ));
                }
                let backoff = 2u64.pow(attempt).min(10);
                eprintln!("Common Crawl index request failed; retry {attempt}/{MAX_INDEX_ATTEMPTS} after {backoff}s: {error}");
                tokio::time::sleep(Duration::from_secs(backoff)).await;
            }
        }
    }
    let body =
        body.ok_or_else(|| anyhow!("Common Crawl index request returned no response body"))?;
    let records = body
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str::<CdxRecord>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("Common Crawl returned invalid CDX JSON")?;

    // Match discover_tech_urls.py: retain the latest capture for each URL.
    let mut latest_by_url: HashMap<String, CdxRecord> = HashMap::new();
    for record in records {
        match latest_by_url.get(&record.url) {
            Some(existing) if existing.timestamp >= record.timestamp => {}
            _ => {
                latest_by_url.insert(record.url.clone(), record);
            }
        }
    }
    let mut unique: Vec<CdxRecord> = latest_by_url.into_values().collect();
    unique.sort_by(|left, right| right.timestamp.cmp(&left.timestamp));
    Ok(unique)
}

async fn fetch_and_store_page(
    state: &AppState,
    domain_id: i64,
    source_id: i64,
    record: &CdxRecord,
) -> Result<(bool, Vec<String>)> {
    let filename = match record.filename.as_deref() {
        Some(v) if !v.is_empty() => v,
        _ => return Ok((false, Vec::new())),
    };
    let offset = match record.offset.as_deref().and_then(|v| v.parse::<u64>().ok()) {
        Some(v) => v,
        None => return Ok((false, Vec::new())),
    };
    let length = match record
        .length
        .as_deref()
        .and_then(|v| v.parse::<usize>().ok())
    {
        Some(v) if v > 0 && v <= MAX_WARC_RECORD_BYTES => v,
        _ => return Ok((false, Vec::new())),
    };
    let end = offset
        .checked_add(length as u64 - 1)
        .ok_or_else(|| anyhow!("WARC byte range overflow"))?;
    let warc_url = format!(
        "https://data.commoncrawl.org/{}",
        filename.trim_start_matches('/')
    );
    let response = state
        .http
        .get(warc_url)
        .header(RANGE, format!("bytes={offset}-{end}"))
        .send()
        .await
        .context("WARC range request failed")?;
    if response.status() != StatusCode::PARTIAL_CONTENT {
        bail!(
            "WARC server did not honor byte range (HTTP {})",
            response.status()
        );
    }
    let compressed = response
        .bytes()
        .await
        .context("could not read WARC bytes")?;
    if compressed.len() > MAX_WARC_RECORD_BYTES {
        bail!("WARC record exceeded safety size limit");
    }
    let decompressed = decompress_warc(&compressed)?;
    let html = extract_http_body(&decompressed);
    if html.is_empty() {
        return Ok((false, Vec::new()));
    }
    let (title, description, content, language, links) = extract_page_metadata(&html, &record.url);
    if content.trim().is_empty() && title.as_deref().unwrap_or("").trim().is_empty() {
        return Ok((false, Vec::new()));
    }

    let id = state
        .db
        .query_opt(
            "SELECT id FROM public.web_pages WHERE url=$1 ORDER BY id LIMIT 1",
            &[&record.url],
        )
        .await?;
    let page_id = if let Some(row) = id {
        let page_id: i64 = row.get(0);
        state.db.execute(
            "UPDATE public.web_pages SET domain_id=$1, crawl_source_id=$2, title=$3, description=$4, content=$5, content_hash=$6, language=$7, crawl_timestamp=$8, warc_filename=$9, warc_offset=$10, warc_length=$11 WHERE id=$12",
            &[&domain_id, &source_id, &title, &description, &content, &record.digest, &language, &record.timestamp, &filename, &(offset as i64), &(length as i64), &page_id],
        ).await?;
        page_id
    } else {
        let row = state.db.query_one(
            "INSERT INTO public.web_pages (domain_id, crawl_source_id, url, title, description, content, content_hash, language, crawl_timestamp, warc_filename, warc_offset, warc_length, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW()) RETURNING id",
            &[&domain_id, &source_id, &record.url, &title, &description, &content, &record.digest, &language, &record.timestamp, &filename, &(offset as i64), &(length as i64)],
        ).await?;
        let inserted_id: i64 = row.get(0);
        inserted_id
    };
    let discovered_links = links.iter().map(|(url, _)| url.clone()).collect();
    store_links(&state.db, page_id, &links).await?;
    Ok((true, discovered_links))
}

fn decompress_warc(bytes: &[u8]) -> Result<Vec<u8>> {
    let decoder = GzDecoder::new(Cursor::new(bytes));
    let mut limited = decoder.take(4 * 1024 * 1024);
    let mut output = Vec::new();
    limited
        .read_to_end(&mut output)
        .context("WARC gzip record could not be decompressed")?;
    Ok(output)
}

fn extract_http_body(warc: &[u8]) -> String {
    let text = String::from_utf8_lossy(warc);
    let first = match text.find("\r\n\r\n") {
        Some(pos) => pos + 4,
        None => return String::new(),
    };
    let after_warc_headers = &text[first..];
    let second = match after_warc_headers.find("\r\n\r\n") {
        Some(pos) => pos + 4,
        None => return String::new(),
    };
    after_warc_headers[second..].to_string()
}

fn extract_page_metadata(
    html: &str,
    page_url: &str,
) -> (
    Option<String>,
    Option<String>,
    String,
    Option<String>,
    Vec<(String, Option<String>)>,
) {
    let document = Html::parse_document(html);
    let text_of = |selector: &str| -> Option<String> {
        let selector = Selector::parse(selector).ok()?;
        let element = document.select(&selector).next()?;
        let text = element
            .text()
            .collect::<Vec<_>>()
            .join(" ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        if text.is_empty() {
            None
        } else {
            Some(text.chars().take(1000).collect())
        }
    };
    let meta_value = |selector: &str| -> Option<String> {
        let selector = Selector::parse(selector).ok()?;
        document
            .select(&selector)
            .next()?
            .value()
            .attr("content")
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(|v| v.chars().take(2000).collect())
    };
    let title = text_of("title");
    let description = meta_value("meta[name='description']")
        .or_else(|| meta_value("meta[property='og:description']"));
    let body_selector = Selector::parse("body").ok();
    let mut content = body_selector
        .and_then(|s| document.select(&s).next())
        .map(|body| body.text().collect::<Vec<_>>().join(" "))
        .unwrap_or_else(|| document.root_element().text().collect::<Vec<_>>().join(" "))
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if content.chars().count() > MAX_TEXT_CHARS {
        content = content.chars().take(MAX_TEXT_CHARS).collect();
    }
    let lang = Selector::parse("html")
        .ok()
        .and_then(|s| document.select(&s).next())
        .and_then(|html| html.value().attr("lang"))
        .map(|value| value.chars().take(10).collect());
    let mut links = Vec::new();
    let mut seen = HashSet::new();
    if let (Some(base), Ok(selector)) = (Url::parse(page_url).ok(), Selector::parse("a[href]")) {
        for anchor in document.select(&selector) {
            let Some(href) = anchor.value().attr("href") else {
                continue;
            };
            let Ok(target) = base.join(href) else {
                continue;
            };
            if !matches!(target.scheme(), "http" | "https") {
                continue;
            }
            let target_url = target.to_string();
            if !seen.insert(target_url.clone()) {
                continue;
            }
            let text = anchor.text().collect::<Vec<_>>().join(" ");
            let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
            let text = if text.is_empty() {
                None
            } else {
                Some(text.chars().take(1000).collect())
            };
            links.push((target_url, text));
            if links.len() >= 500 {
                break;
            }
        }
    }
    (title, description, content, lang, links)
}

async fn store_links(
    db: &PgClient,
    source_page_id: i64,
    links: &[(String, Option<String>)],
) -> Result<()> {
    for (target_url, anchor_text) in links {
        let target_id: Option<i64> = db
            .query_opt(
                "SELECT id FROM public.web_pages WHERE url=$1 ORDER BY id LIMIT 1",
                &[target_url],
            )
            .await?
            .map(|row| row.get(0));
        if let Some(link) = db
            .query_opt(
                "SELECT id FROM public.page_links WHERE source_page_id=$1 AND target_url=$2 ORDER BY id LIMIT 1",
                &[&source_page_id, target_url],
            )
            .await?
        {
            let link_id: i64 = link.get(0);
            db.execute(
                "UPDATE public.page_links SET target_page_id=$1, anchor_text=$2 WHERE id=$3",
                &[&target_id, anchor_text, &link_id],
            )
            .await?;
        } else {
            db.execute(
                "INSERT INTO public.page_links (source_page_id, target_page_id, target_url, anchor_text, created_at) VALUES ($1,$2,$3,$4,NOW())",
                &[&source_page_id, &target_id, target_url, anchor_text],
            )
            .await?;
        }
    }
    Ok(())
}
