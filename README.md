# SearchEngine — local setup

This local setup uses one PostgreSQL Docker container holding 200 sector databases, one Rust crawler service, one Python query service, and one Go orchestrator. Python classifies the query with the embedding model and sends its selected topic/database to Go. Go forwards it to Rust, which opens that PostgreSQL database and searches or stores data there.

## Database names

The canonical catalog is [sectors.json](services/python-query-service-corrected/app/data/sectors.json). The complete list of 200 names is [database_names.txt](services/python-query-service-corrected/app/data/database_names.txt). Names start with `searchengine_`, for example:

- `searchengine_python_programming`
- `searchengine_java_programming`
- `searchengine_databases_and_sql`
- `searchengine_cardiology`
- `searchengine_mental_health`
- `searchengine_cloud_computing`

## Create the 200 databases in Docker

Start the PostgreSQL container first. From PowerShell, at the project root, run:

```powershell
docker ps
docker start searchengine-postgres
.\infrastructure\provision-sector-databases.ps1 -HostName localhost -Port 5434 -UserName searchadmin -DockerContainer searchengine-postgres -SslMode disable
```

Use `docker start` only if `docker ps` does not show the container running. The script reads the catalog, prompts for the PostgreSQL password, creates any missing databases in the `searchengine-postgres` container, and leaves existing databases in place. `searchadmin` must own or have permission to create the databases. It does not copy data from your older `searchengine_db` or `searchengine_health` databases; those remain separate.

## Confirm they were created

In PowerShell or Command Prompt, list matching databases from the Docker container:

```bat
docker exec -it searchengine-postgres psql -U searchadmin -d postgres -c "SELECT datname FROM pg_database WHERE datname LIKE 'searchengine_%' ORDER BY datname;"
```

In pgAdmin, connect to the existing Docker PostgreSQL server on `localhost:5434`, then right-click **Databases** and choose **Refresh**. You should see the names from `database_names.txt`.

Rust creates its project tables and pgvector indexes inside a sector database the first time that topic is used. Creating the databases does not fill them with website data; Common Crawl discovery and subsequent extraction populate data as the project runs.

## Start the application locally

1. Create `services\rust-crawler\.env` from `.env.example`. For the existing Docker PostgreSQL container, set `DB_HOST=localhost`, `DB_PORT=5434`, `DB_USER=searchadmin`, your local password, and `DB_NAME=searchengine_python_programming`. Keep `.env` private.
2. Install Python dependencies once if they are not already installed:

   ```bat
   cd /d services\python-query-service-corrected
   python -m venv .venv
   .venv\Scripts\activate.bat
   pip install -r requirements.txt
   ```

3. Return to the project root and double-click `run_local.bat`. Keep the Docker container running. The batch file starts one Rust service, Python, and Go in separate windows.
4. Open `http://localhost:8000/`, or send a Postman request:

   ```http
   POST http://localhost:8000/api/v1/query/analyze
   Content-Type: application/json
   ```

   ```json
   {"query":"What is Python programming?"}
   ```

The response hides the internal `query_embedding`; the vector still passes between services for similarity search. Database names and example queries are in `app/data/sectors.json` and `app/data/sample_queries.json`.

### Search and background-crawl behavior

1. Rust checks the selected topic database first. If it already has matching page content or Common Crawl URLs, relevant page URLs are returned immediately.
2. If there is no matching URL yet, the response shows the best candidate domains immediately.
3. Rust saves a persistent `async_crawl_jobs` record in that topic database, then looks up Common Crawl CDX captures in the background. It prioritizes URLs whose paths contain query keywords and stores their URL/capture metadata in `common_crawl_urls`.
4. The page polls the crawl-job status and replaces candidate cards with matching page links when the job finishes. Later searches can return those stored links directly.

This workflow stores URL metadata; it does not download full page text on every search. A domain candidate can still be shown if Common Crawl has no matching archived capture.

## Optional Common Crawl Parquet indexing

This is a separate background task and may take a long time. Start Rust and Python first. From the Python service directory, a small pilot is:

```bat
set "RUST_SERVICE_URL=http://localhost:8081"
python -m app.services.commoncrawl_indexer --sector python_programming --max-files 2
```

The indexer accepts one topic ID, `technology`, `health`, or `both`. A full `both` run covers all catalog topics; it is not part of normal startup.

## Update GitHub

```bat
git status
git add .
git commit -m "Describe the update"
git push
```

Commit `.env.example`, but never commit your real `.env` or database password.
