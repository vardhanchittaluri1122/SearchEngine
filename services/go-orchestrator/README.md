# SearchEngineWork run guide

## What the services do

1. Python receives a natural-language query, classifies technology vs. health, extracts keywords, and returns a normalized BGE embedding.
2. Go sends the query to the matching Rust service and PostgreSQL database. It checks stored records first, then registered domains, then the persistent Common Crawl pgvector domain index. If indexed URL vectors are needed, Go asks Python to create them and resubmits the search to Rust.
3. Rust returns up to five URL metadata results. It stores Common Crawl URL/capture metadata; it only extracts archived page text when `/api/v1/pages/extract` is explicitly called (up to 30 lines).
4. If the persistent index has no match, Go falls back to Python's current Common Crawl URL-index discovery flow.

The indexer below is a separate one-time/background snapshot job. It uses DuckDB to read remote Common Crawl WARC-subset Parquet files with HTTP range access, embeds domain/URL metadata with the same BGE model, and checkpoints each shard in PostgreSQL. It does not download the whole archive locally or extract page bodies.

## Prerequisites

- Docker PostgreSQL is running on `localhost:5434` and the existing database `searchengine_db` works.
- `searchengine_health` exists. Create it once from pgAdmin Query Tool connected to the `postgres` database:

```sql
CREATE DATABASE searchengine_health WITH TEMPLATE searchengine_db OWNER searchadmin;
```

- Rust and Go are installed, and Python 3.10+ is installed.
- Set the real database password in `services\rust-crawler\.env` (do not share/commit that file). The Rust processes automatically create the Common Crawl index tables and pgvector indexes at startup. The PostgreSQL image must have the `vector` extension installed.

## Start services (Windows Command Prompt)

Keep each command window open. In CMD, use `set`, not PowerShell's `$env:` syntax.

### 1. Rust technology service

```bat
cd /d services\rust-crawler
set "DB_NAME=searchengine_db"
set "CRAWLER_LISTEN_ADDR=127.0.0.1:8081"
cargo run
```

### 2. Rust health service (second CMD window)

```bat
cd /d services\rust-crawler
set "DB_NAME=searchengine_health"
set "CRAWLER_LISTEN_ADDR=127.0.0.1:8082"
cargo run
```

### 3. Python API (third CMD window)

```bat
cd /d services\python-query-service-corrected
if not exist .venv python -m venv .venv
.venv\Scripts\activate.bat
pip install -r requirements.txt
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

The initial Python start may download the BGE model. Wait until Uvicorn says application startup is complete.

### 4. Go orchestrator (fourth CMD window)

```bat
cd /d services\go-orchestrator
set "CRAWLER_TECHNOLOGY_URL=http://localhost:8081/api/v1/search"
set "CRAWLER_HEALTH_URL=http://localhost:8082/api/v1/search"
set "COMMONCRAWL_DISCOVERY_URL=http://localhost:8000/internal/v1/commoncrawl/discover"
set "COMMONCRAWL_EMBED_URLS=http://localhost:8000/internal/v1/commoncrawl/embed-urls"
go run .
```

## Build the persistent Common Crawl index

Run this only after both Rust processes are listening and the Python environment has all requirements installed. Use a fifth CMD window. The first command is a small smoke-sized pilot over two Parquet files; it also creates the index tables/checkpoints and confirms the snapshot schema can be read.

```bat
cd /d services\python-query-service-corrected
.venv\Scripts\activate.bat
set "RUST_TECHNOLOGY_URL=http://localhost:8081"
set "RUST_HEALTH_URL=http://localhost:8082"
set "CC_DUCKDB_MEMORY_LIMIT=4GB"
python -m app.services.commoncrawl_indexer --max-files 2 --sector both
```

If that succeeds, start/continue indexing every remaining Parquet shard in the current Common Crawl snapshot:

```bat
python -m app.services.commoncrawl_indexer --sector both
```

This is a background/long-running job, not a Postman request. Common Crawl contains many remote shards; full indexing can take substantial time, CPU, memory, network, and database storage. Each finished shard is checkpointed separately for technology and health. Stop with Ctrl+C and rerun the same full command to resume. A failed shard is retried on a later run. Do not run the full index until the two-file pilot succeeds and you have enough database/storage resources.

Optional controls:

- `--snapshot CC-MAIN-YYYY-NN` fixes a snapshot instead of choosing the latest one.
- `--max-files N` limits this run to the first N manifest shards; omit it for all remaining shards.
- `--sector technology`, `--sector health`, or `--sector both` chooses destination database(s).
- `--batch-size 100` adjusts the number of unique domains embedded per batch.
- `CC_SECTOR_SIMILARITY_THRESHOLD=0.35` sets the domain-to-sector filter; lower values store more candidates and use more space.
- `CC_DUCKDB_TEMP_DIR` sets the local DuckDB spill folder; `CC_DUCKDB_MEMORY_LIMIT` and `--threads` tune local processing.

Check progress/data in pgAdmin by connecting to each database and running:

```sql
SELECT sector, status, count(*) AS shards FROM cc_index_shards GROUP BY sector, status ORDER BY sector, status;
SELECT sector, count(*) AS domains FROM cc_index_domains GROUP BY sector ORDER BY sector;
SELECT sector, count(*) AS urls FROM cc_index_urls GROUP BY sector ORDER BY sector;
SELECT * FROM cc_index_shards WHERE status = 'failed' ORDER BY updated_at DESC;
```

The full snapshot is not included in the source ZIP and is not indexed automatically by starting the APIs. The indexer command above must be run once per snapshot; rerunning it resumes from saved shard progress.

## Search from Postman

After all four service windows are running, send:

- Method: `POST`
- URL: `http://localhost:8000/api/v1/query/analyze`
- Header: `Content-Type: application/json`
- Body (raw JSON):

```json
{"query":"What is the difference between Java and Python?"}
```

To test health routing, use `{"query":"What are symptoms of a heart attack?"}`. The response includes analysis, selected sector, candidate domains, and up to five website metadata records. Page text is not fetched at search time.

## Extract page text on demand

Use a URL returned from search, via Go:

- Method: `POST`
- URL: `http://localhost:8080/api/v1/pages/extract`
- Header: `Content-Type: application/json`
- Body:

```json
{"sector":"technology","url":"https://example.org/page"}
```

Set `sector` to `health` for a health database URL. The URL must already have Common Crawl capture metadata in that database. Rust stores extracted text and returns up to 30 lines.

## Service health checks

- Go: `http://localhost:8080/health`
- Rust technology: `http://localhost:8081/health`
- Rust health: `http://localhost:8082/health`
- Python: `http://localhost:8000/docs`

If a port is already in use, close the old service window before starting another instance. Keep all service windows open while testing requests.
