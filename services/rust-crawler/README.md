# Rust Common Crawl metadata service

Go routes each request to a Rust process that is connected to one sector database. Start one process for `searchengine_db` (technology) and a second for `searchengine_health` (health). Both processes share the common host, port, user, and password values from `.env`; set `DB_NAME` and `CRAWLER_LISTEN_ADDR` in each PowerShell terminal before `cargo run`.

## Search behavior

`POST /api/v1/search` checks stored pages and supplied candidate domains. Go returns candidate URLs from the Parquet URL index immediately, then Rust checks Common Crawl CDX in a background job. The job fetches matching WARC captures, extracts page text and outgoing links, and stores them in the selected sector database. Later searches can return the stored page URLs and matching related links.

On startup, Rust creates `common_crawl_urls` and indexes if they do not exist. Existing `domains`, `web_pages`, `crawl_sources`, and `crawl_jobs` tables are still required for registered-domain handling and explicit extraction.

## Explicit page extraction

`POST /api/v1/pages/extract` accepts a `url` and optional `sector`. It must find that URL in the selected database's `common_crawl_urls` table. Rust uses the stored WARC byte range to extract and cache text, then returns up to 30 non-empty lines. Repeat requests reuse `extracted_text` and do not fetch WARC again.

```json
{"sector":"technology","url":"https://example.org/page"}
```

## Start instances

Technology database:

```powershell
cd services\rust-crawler
$env:DB_NAME = 'searchengine_db'
$env:CRAWLER_LISTEN_ADDR = '127.0.0.1:8081'
cargo run
```

Health database in a separate terminal:

```powershell
cd services\rust-crawler
$env:DB_NAME = 'searchengine_health'
$env:CRAWLER_LISTEN_ADDR = '127.0.0.1:8082'
cargo run
```

`CC_RECORD_LIMIT` controls how many CDX records Rust requests per domain (default 100). `CC_PAGE_LIMIT` controls how many URL metadata records are stored per query (default 30, with at most six per domain). `CC_MAX_PARALLEL_DOMAINS` limits simultaneous domain lookups (default 2). CDX requests have a 30-second timeout and at most three attempts; a 404 stating no captures is treated as an empty result. The Common Crawl archive itself is not downloaded locally.

The project includes a restartable Common Crawl Parquet indexer in the Python service. It reads the remote WARC-subset Parquet shards with DuckDB, stores sector/domain/URL vectors in `cc_index_sectors`, `cc_index_domains`, and `cc_index_urls`, and checkpoints each shard in `cc_index_shards`. Run it separately using the instructions in `services/go-orchestrator/README.md`; starting Rust does not automatically launch the full snapshot job. Configure `CC_DOMAIN_SIMILARITY_THRESHOLD` (default `0.35`) to tune semantic domain filtering. Search can lazily cache URL vectors through the Go-to-Python embedding endpoint. The actual snapshot still needs to be indexed by running the background job, which can be long-running and resource intensive.
