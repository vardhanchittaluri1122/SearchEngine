# SearchEngine

A query-driven search project focused on technology and health. The user enters a question, Python analyzes it, Go routes it to the right Rust service, and Rust searches PostgreSQL/Common Crawl metadata. The web page is served by the Python service.

## Project structure

- `services/python-query-service-corrected`: FastAPI query analysis and the HTML search page at `/`.
- `services/go-orchestrator`: routes analyzed queries to the matching Rust service.
- `services/rust-crawler`: searches and stores Common Crawl URL metadata and extracts archived page text when requested.
- `infrastructure`: database-related project assets.

The technology and health Rust instances use separate databases. The optional Common Crawl Parquet indexer is a background job; starting the APIs does not automatically index the snapshot.

## Run locally on Windows

Prerequisites: Docker PostgreSQL with pgvector on port `5434`, Rust/Cargo, Go, and Python. The local databases expected by default are `searchengine_db` and `searchengine_health`.

1. Create `services\rust-crawler\.env` from `.env.example` and set your local PostgreSQL password. Keep `.env` private; Git ignores it.
2. Install Python dependencies once if needed:

   ```bat
   cd /d services\python-query-service-corrected
   python -m venv .venv
   .venv\Scripts\activate.bat
   pip install -r requirements.txt
   ```

3. From the project root, double-click `run_local.bat`. It starts technology Rust, health Rust, Python, and Go in separate terminal windows. Start Docker first and wait for PostgreSQL to be ready.
4. Open `http://localhost:8000/` and enter a query. The page calls the same Python API used by Postman.

To test through Postman instead, send `POST http://localhost:8000/api/v1/query/analyze` with `Content-Type: application/json` and body:

```json
{"query":"What is the difference between Java and Python?"}
```

To build the persistent Common Crawl metadata/embedding index, first run the two-file pilot from the Python service directory after Rust and Python are up:

```bat
.venv\Scripts\activate.bat
set "RUST_TECHNOLOGY_URL=http://localhost:8081"
set "RUST_HEALTH_URL=http://localhost:8082"
python -m app.services.commoncrawl_indexer --max-files 2 --sector both
```

If that succeeds, run the full background job with `python -m app.services.commoncrawl_indexer --sector both`. It can take substantial time and storage and resumes from completed shard checkpoints. Do not run it as part of a normal web request.

## Keep GitHub updates safe

The repository ignores `.env` files, virtual environments, Rust/Go build output, Python bytecode, local caches, executables, and ZIP files. Commit `.env.example` files only; never commit passwords or access tokens.

After editing files in this project folder, run these commands from the project root:

```bat
git status
git add .
git commit -m "Describe the update"
git push
```

## Later hosting plan

This repository is prepared for source control; it is not deployed yet. The current HTML page is served by Python and calls a same-origin API path. For a separate Vercel frontend and Render backend, configure the frontend to call the Render API URL and enable the appropriate FastAPI CORS origins. Go and both Rust services also need to be deployed/configured with their service URLs and database settings. Set credentials as hosting-provider environment variables, not in Git. The Common Crawl indexer should be run as a separate controlled job, not during web-service startup.
