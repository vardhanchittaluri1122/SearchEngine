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

## Run locally and on Render

The same code supports both environments. Locally, Rust reads `services\\rust-crawler\\.env` and connects to Docker PostgreSQL at `localhost:5434`. On Render, configure the deployed Rust services with Render environment variables. The credentials are never stored in the repository.

### Render database settings

Deploy two separate **Private Services** from this repository, both with **Root Directory** `services/rust-crawler`, **Build Command** `cargo build --release`, and **Start Command** `./target/release/rust-crawler`. Set `CRAWLER_LISTEN_ADDR=0.0.0.0:10000` in each private service.

In each Rust service's **Environment** page, add:

```text
DB_HOST=<host from the Render database Internal Database URL>
DB_PORT=5432
DB_USER=<current Render database username>
DB_PASSWORD=<current Render database password>
```

Set `DB_NAME=searchengine_db` on the technology Rust service, and `DB_NAME=searchengine_health` on the health Rust service. Both use the same Render database host and credentials. Use the **internal** database hostname because the Rust services run on Render; do not paste the full URL into `DB_HOST`. Keep the password in Render's Environment settings only. Local `.env` settings remain for Docker; the Render values do not change your local database connection.

Deploy the Go orchestrator as a **Private Service** from root directory `services/go-orchestrator`, with build command `go build -o app .` and start command `./app`. Set `LISTEN_ADDR=:10000`. Set these environment variables to the **internal service addresses** shown by Render after deploying the services (private services use port `10000` here):

```text
CRAWLER_TECHNOLOGY_URL=http://<rust-technology-private-host>:10000/api/v1/search
CRAWLER_HEALTH_URL=http://<rust-health-private-host>:10000/api/v1/search
COMMONCRAWL_DISCOVERY_URL=http://<python-host>:10000/internal/v1/commoncrawl/discover
COMMONCRAWL_EMBED_URLS=http://<python-host>:10000/internal/v1/commoncrawl/embed-urls
```

Deploy Python as a **Web Service** from root directory `services/python-query-service-corrected`, with build command `pip install -r requirements-render.txt` and start command `uvicorn app.main:app --host 0.0.0.0 --port $PORT`. The Render requirements install CPU-only PyTorch for the same BGE sentence-transformer classifier, avoiding CUDA packages that exceed the free instance memory. Set `GO_ORCHESTRATOR_URL=http://<go-private-host>:10000/api/v1/search`. Keep Python and Go in the same Render region as the Rust services. Add the actual Render private hostnames in these URL values after creating the services. For Python's internal URLs in the Go settings, use the Python service's internal hostname with port `10000`.

After each service deploys, check its Render **Logs**. Rust should log a successful database connection and listener startup; Go and Python should show their HTTP listeners. If Rust says the database does not exist or permission is denied, check `DB_NAME`, `DB_USER`, and that this role owns or has access to that database. Creating the Render databases by itself does not configure these service variables.

The Common Crawl indexer remains a separate controlled job; do not run it during web-service startup. The current search HTML is served by Python. Vercel hosting for the frontend is optional; first confirm the Render Python URL opens and the backend services connect successfully.
