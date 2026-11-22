import logging
import os
import httpx

GO_SEARCH_URL = os.getenv("GO_ORCHESTRATOR_URL", "http://localhost:8080/api/v1/search")
logger = logging.getLogger(__name__)


async def forward_analysis_to_go(analysis: dict):
    try:
        async with httpx.AsyncClient(timeout=1800.0) as client:
            response = await client.post(GO_SEARCH_URL, json=analysis)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPError as exc:
        logger.exception("Could not forward query analysis to Go orchestrator: %s", exc)
        return None


async def forward_crawl_job_status(job_id: int, database_name: str, sector_group: str):
    endpoint = GO_SEARCH_URL.removesuffix("/api/v1/search") + f"/api/v1/crawl-jobs/{job_id}"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(endpoint, params={
                "database_name": database_name,
                "sector_group": sector_group,
            })
            response.raise_for_status()
            return response.json()
    except httpx.HTTPError as exc:
        logger.exception("Could not forward crawl-job status to Go orchestrator: %s", exc)
        return None
