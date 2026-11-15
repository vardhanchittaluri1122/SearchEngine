"""Find candidate registered domains in Common Crawl's remote Parquet URL index.

This is URL-metadata discovery, not a full-text search. The Rust service fetches
candidate captures through CDXJ/WARC and verifies their page text.
"""

from __future__ import annotations

import asyncio
import gzip
import os
import re
import time
from typing import Any

import duckdb # type: ignore
import httpx # type: ignore
from app.services.query_understanding import HEALTH_TERMS

COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
MAX_INDEX_CANDIDATES = 5
MAX_PARQUET_FILES = max(1, min(int(os.getenv("CC_MAX_PARQUET_FILES", "3")), 20))
CACHE_SECONDS = 1800
_RESULT_CACHE: dict[tuple[str, ...], tuple[float, dict[str, Any]]] = {}

# Fast starts from the topic-to-domain map in extract_commoncrawl_domains.py.
# Rust still checks archived page text before returning a result.
CURATED_DOMAINS = {
    "programming": [
        ("developer.mozilla.org", "developer_reference"), ("stackoverflow.com", "developer_resources"),
        ("github.com", "developer_resources"), ("geeksforgeeks.org", "developer_resources"),
        ("cplusplus.com", "language_reference"),
    ],
    "java": [
        ("java.com", "official"), ("java.org", "official"),
        ("dev.java", "official"), ("oracle.com", "official"),
        ("spring.io", "developer_resources"),
    ],
    "python": [
        ("python.org", "official"), ("docs.python.org", "official"),
        ("realpython.com", "developer_resources"),
        ("geeksforgeeks.org", "developer_resources"),
        ("programiz.com", "developer_resources"),
    ],
    "sql": [
        ("postgresql.org", "official"), ("mysql.com", "official"),
        ("sqlite.org", "official"), ("sqlbolt.com", "developer_resources"),
        ("mode.com", "developer_resources"),
    ],
    "ai": [
        ("openai.com", "ai_research"), ("deepmind.google", "ai_research"),
        ("huggingface.co", "ai_tools"), ("pytorch.org", "ai_tools"),
        ("tensorflow.org", "ai_tools"),
    ],
    "technology": [
        ("cloudflare.com", "technology_reference"), ("aws.amazon.com", "cloud_platform"),
        ("developer.mozilla.org", "developer_reference"),
        ("arstechnica.com", "technology_news"), ("techcrunch.com", "technology_news"),
    ],
    "education": [
        ("harvard.edu", "university"), ("mit.edu", "university"),
        ("stanford.edu", "university"), ("khanacademy.org", "courses"),
        ("coursera.org", "courses"),
    ],
}
HEALTH_DOMAINS = [
    ("medlineplus.gov", "health_reference"), ("nhs.uk", "health_reference"),
    ("who.int", "health_reference"), ("cdc.gov", "health_reference"),
    ("mayoclinic.org", "health_reference"),
]


def _terms(analysis: dict[str, Any]) -> list[str]:
    raw = analysis.get("keywords") or []
    terms = [str(term).strip().lower() for term in raw if str(term).strip()]
    if not terms:
        terms = re.findall(r"[\w+#.-]{2,}", str(analysis.get("normalized_query", "")).lower())
    unique = list(dict.fromkeys(t for t in terms if t not in {"what", "about", "tell", "me"}))
    return unique[:8]


def _discover_sync(parquet_urls: list[str], terms: list[str], limit: int) -> list[dict[str, Any]]:
    """Query remote Parquet files in place; no local corpus is made."""
    if not terms:
        return []
    conn = duckdb.connect(database=":memory:")
    try:
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        predicates = []
        params: list[Any] = [parquet_urls]
        for term in terms:
            predicates.append(
                "(lower(url_host_registered_domain) LIKE ? "
                "OR lower(url) LIKE ? OR lower(url_path) LIKE ? OR lower(url_query) LIKE ?)"
            )
            like = f"%{term}%"
            params.extend([like, like, like, like])
        sql = f"""
            SELECT lower(url_host_registered_domain) AS domain, count(*) AS captures
            FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
            WHERE fetch_status = 200 AND content_mime_type = 'text/html'
              AND url_host_registered_domain IS NOT NULL
              AND ({' OR '.join(predicates)})
            GROUP BY 1 ORDER BY captures DESC, domain ASC LIMIT ?
        """
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        return [{"domain": row[0], "captures": row[1]} for row in rows]
    finally:
        conn.close()


async def discover_domains(analysis: dict[str, Any], limit: int = MAX_INDEX_CANDIDATES) -> dict[str, Any]:
    terms = _terms(analysis)
    if not terms:
        return {"crawl_id": None, "domains": [], "terms": [], "method": "commoncrawl-parquet-url-index"}
    cache_key = tuple([str(analysis.get("sector", "")), *terms])
    cached = _RESULT_CACHE.get(cache_key)
    if cached and time.monotonic() - cached[0] < CACHE_SECONDS:
        return cached[1]

    curated_terms = [term for term in terms if term in CURATED_DOMAINS]
    topic = str(analysis.get("topic", "")).lower()
    if topic in CURATED_DOMAINS and topic not in curated_terms:
        curated_terms.insert(0, topic)
    is_health = analysis.get("sector") == "health" or any(term in HEALTH_TERMS for term in terms)
    if curated_terms or is_health:
        if is_health:
            curated = HEALTH_DOMAINS
        else:
            # Interleave topic sources so comparison queries such as Java vs
            # Python return candidates for both sides before filling the cap.
            curated = []
            for position in range(max(len(CURATED_DOMAINS[term]) for term in curated_terms)):
                for term in curated_terms:
                    if position < len(CURATED_DOMAINS[term]):
                        curated.append(CURATED_DOMAINS[term][position])
        result = {
            "crawl_id": None,
            "domains": [
                {"domain": domain, "captures": 0, "discovery_source": "curated_seed", "category": category}
                for domain, category in curated[: min(max(limit, 1), MAX_INDEX_CANDIDATES)]
            ],
            "terms": terms,
            "method": "curated-domains",
        }
        _RESULT_CACHE[cache_key] = (time.monotonic(), result)
        return result
    timeout = httpx.Timeout(30.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        response = await client.get(COLLINFO_URL)
        response.raise_for_status()
        collections = response.json()
    if not collections:
        raise RuntimeError("Common Crawl returned no collections")
    crawl_id = str(collections[0]["id"])
    manifest_url = f"https://data.commoncrawl.org/crawl-data/{crawl_id}/cc-index-table.paths.gz"
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        manifest_response = await client.get(manifest_url)
        manifest_response.raise_for_status()
    try:
        manifest_text = gzip.decompress(manifest_response.content).decode("utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise RuntimeError("Common Crawl returned an invalid Parquet path manifest") from exc
    prefix = f"crawl={crawl_id}/subset=warc/"
    parquet_urls = []
    for item in manifest_text.splitlines():
        path = item.strip().lstrip("/")
        if path.endswith(".parquet") and prefix in path:
            parquet_urls.append("https://data.commoncrawl.org/" + path)
    if not parquet_urls:
        raise RuntimeError(f"Common Crawl manifest has no Parquet files for {crawl_id}")
    # Keep request latency bounded. The full crawl has many shards; a small
    # sample can miss domains, so increase CC_MAX_PARQUET_FILES when needed.
    parquet_urls = parquet_urls[:MAX_PARQUET_FILES]
    domains = await asyncio.to_thread(
        _discover_sync, parquet_urls, terms, min(max(limit, 1), MAX_INDEX_CANDIDATES)
    )
    result = {
        "crawl_id": crawl_id,
        "domains": domains,
        "terms": terms,
        "method": "commoncrawl-parquet-url-index",
    }
    _RESULT_CACHE[cache_key] = (time.monotonic(), result)
    return result
