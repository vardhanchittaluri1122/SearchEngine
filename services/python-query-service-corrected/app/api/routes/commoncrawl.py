import asyncio
import logging
from urllib.parse import urlsplit

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.commoncrawl_discovery import discover_domains
from app.services.query_understanding import embed_texts

router = APIRouter()
logger = logging.getLogger(__name__)


class DiscoveryRequest(BaseModel):
    normalized_query: str = Field(default="", max_length=2000)
    keywords: list[str] = Field(default_factory=list, max_length=20)
    sector: str | None = None
    topic: str | None = None
    limit: int = Field(default=5, ge=1, le=5)


class EmbedURLsRequest(BaseModel):
    urls: list[str] = Field(min_length=1, max_length=100)


@router.post("/discover")
async def discover(request: DiscoveryRequest):
    try:
        return await discover_domains(request.model_dump(), request.limit)
    except Exception as exc:
        logger.exception("Common Crawl Parquet URL-index discovery failed")
        raise HTTPException(status_code=502, detail="Common Crawl URL-index discovery failed") from exc


@router.post("/embed-urls")
async def embed_urls(request: EmbedURLsRequest):
    urls = list(dict.fromkeys(value.strip() for value in request.urls if value.strip()))
    if not urls:
        raise HTTPException(status_code=400, detail="At least one URL is required")
    if len(urls) > 100:
        raise HTTPException(status_code=400, detail="At most 100 URLs can be embedded per request")
    for value in urls:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise HTTPException(status_code=400, detail=f"Invalid HTTP URL: {value}")
    try:
        vectors = await asyncio.to_thread(embed_texts, urls, 64)
    except Exception as exc:
        logger.exception("Could not create candidate URL embeddings")
        raise HTTPException(status_code=500, detail="URL embedding failed") from exc
    return {"embeddings": [{"url": url, "embedding": vector} for url, vector in zip(urls, vectors)]}
