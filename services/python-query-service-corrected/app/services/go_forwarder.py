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
