import asyncio
import logging
from fastapi import APIRouter, HTTPException
from app.schemas.query import QueryRequest
from app.services.query_understanding import understand_query
from app.services.go_forwarder import forward_analysis_to_go, forward_crawl_job_status

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/analyze")
async def analyze_query(request: QueryRequest):
    if not request.query.strip():
        raise HTTPException(status_code=422, detail="provide a query")
    try:
        analysis = await asyncio.to_thread(
            understand_query, request.query, request.sector, request.keywords
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    go_result = await forward_analysis_to_go(analysis)
    if go_result is None:
        raise HTTPException(status_code=502, detail="Go orchestrator or crawler service is unavailable")
    # Keep the vector in the internal Python-to-Go request for pgvector search,
    # but do not expose this implementation detail in the user-facing response.
    public_analysis = {key: value for key, value in analysis.items() if key != "query_embedding"}
    return {
        "analysis": public_analysis,
        "discovery": go_result.get("discovery"),
        "results": go_result.get("search", {}),
    }


@router.get("/crawl-jobs/{job_id}")
async def crawl_job_status(job_id: int, database_name: str, sector_group: str = "technology"):
    result = await forward_crawl_job_status(job_id, database_name, sector_group)
    if result is None:
        raise HTTPException(status_code=502, detail="Could not check the background crawl status")
    return result
