from pathlib import Path

from fastapi import FastAPI # type: ignore
from fastapi.responses import FileResponse
from app.api.routes.query import router as query_router
from app.api.routes.commoncrawl import router as commoncrawl_router

app = FastAPI(
    title="Search Engine Query Service",
    description="Normalize and analyze search queries.",
    version="1.0.0",
)
app.include_router(query_router, prefix="/api/v1/query", tags=["Query Understanding"])
app.include_router(commoncrawl_router, prefix="/internal/v1/commoncrawl", tags=["Common Crawl discovery"])

UI_FILE = Path(__file__).parent / "static" / "index.html"


@app.get("/", include_in_schema=False)
def search_ui() -> FileResponse:
    return FileResponse(UI_FILE, media_type="text/html")


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "healthy", "service": "python-query-service"}
