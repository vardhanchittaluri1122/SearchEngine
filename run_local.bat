@echo off
setlocal
cd /d "%~dp0"
if not exist "services\python-query-service-corrected\.venv\Scripts\python.exe" (
  echo Python virtual environment not found.
  echo Create it and install requirements using the steps in README.md.
  pause
  exit /b 1
)
start "Rust Technology - 8081" cmd /k "cd /d services\rust-crawler && set DB_NAME=searchengine_db&&set CRAWLER_LISTEN_ADDR=127.0.0.1:8081&&cargo run"
start "Rust Health - 8082" cmd /k "cd /d services\rust-crawler && set DB_NAME=searchengine_health&&set CRAWLER_LISTEN_ADDR=127.0.0.1:8082&&cargo run"
start "Python Query API - 8000" cmd /k "cd /d services\python-query-service-corrected && .venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000"
start "Go Orchestrator - 8080" cmd /k "cd /d services\go-orchestrator && set CRAWLER_TECHNOLOGY_URL=http://localhost:8081/api/v1/search&&set CRAWLER_HEALTH_URL=http://localhost:8082/api/v1/search&&set COMMONCRAWL_DISCOVERY_URL=http://localhost:8000/internal/v1/commoncrawl/discover&&set COMMONCRAWL_EMBED_URLS=http://localhost:8000/internal/v1/commoncrawl/embed-urls&&go run ."
echo Started both Rust instances, Python, and Go in separate windows.
echo Wait for startup, then open http://localhost:8000/.
