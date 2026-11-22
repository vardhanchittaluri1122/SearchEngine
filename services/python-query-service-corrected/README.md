# Python Query Service

FastAPI accepts only a natural-language query plus optional sector and keyword hints. It extracts keywords and uses `BAAI/bge-small-en-v1.5` sentence embeddings to classify a specific topic (`programming`, `ai`, `technology`, `education`, `health`, or `general`). It maps that label to the existing Rust/database sector values (`technology`, `health`, or `general`) and forwards the metadata to Go. Users do not provide a domain.

## Run

```powershell
.\.venv\Scripts\Activate.ps1
python -m uvicorn app.main:app --reload --port 8000
```

If `.venv` is missing, create and install it first:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

After changing `requirements.txt`, activate the environment and run `pip install -r requirements.txt` again. The first service startup downloads `BAAI/bge-small-en-v1.5`; later starts use the local model cache.

## Request

```powershell
$body = @{ query = 'What is Java?' } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://localhost:8000/api/v1/query/analyze -ContentType 'application/json' -Body $body
```

The model and topic-description embeddings load once at Python service startup. The first setup downloads the model; later starts reuse the local Hugging Face cache. The response includes extracted keywords, specific topic and similarity, discovered domains, and up to five website results returned through Go and Rust. `GO_ORCHESTRATOR_URL` can override the default Go URL.

When stored pages do not answer a query, Go calls `POST /internal/v1/commoncrawl/discover`. Python returns candidate domains and matching Common Crawl Parquet URLs; the UI shows those candidate URLs while Rust checks CDX/WARC in a background job, extracts page text and links, and saves them in the selected sector database. Python also exposes `POST /internal/v1/commoncrawl/embed-urls` for on-demand URL embeddings. Explicit page extraction remains available through Go's `POST /api/v1/pages/extract` and returns up to 30 cached lines.

For the persistent snapshot index, run `python -m app.services.commoncrawl_indexer --max-files 2 --sector both` from this service's virtual environment after both Rust databases are running. If the pilot succeeds, run `python -m app.services.commoncrawl_indexer --sector both` to process the remaining snapshot shards. The job checkpoints each shard and resumes when rerun. Detailed setup, CMD commands, progress queries, and resource notes are in the Go orchestrator's README.
