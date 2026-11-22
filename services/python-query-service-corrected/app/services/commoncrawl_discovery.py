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

COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
MAX_INDEX_CANDIDATES = 5
MAX_CANDIDATE_URLS = 30
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
        ("geeksforgeeks.org", "developer_resources"), ("dev.java", "official"),
        ("java.com", "official"), ("oracle.com", "official"),
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
    "team_sports": [
        ("en.wikipedia.org", "reference"),
        ("espncricinfo.com", "cricket_reference"),
        ("prokabaddi.com", "kabaddi_reference"),
        ("britannica.com", "reference"),
    ],
    "individual_sports": [
        ("en.wikipedia.org", "reference"),
        ("olympics.com", "sports_reference"),
        ("britannica.com", "reference"),
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


def _discover_sync(parquet_urls: list[str], terms: list[str], limit: int) -> dict[str, list[dict[str, Any]]]:
    """Query remote Parquet files in place; no local corpus is made."""
    if not terms:
        return {"domains": [], "candidate_urls": []}
    conn = duckdb.connect(database=":memory:")
    try:
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        predicates = []
        where_params: list[Any] = []
        score_terms = []
        score_params: list[Any] = []
        for term in terms:
            like = f"%{term}%"
            predicates.append(
                "(lower(url_host_registered_domain) LIKE ? "
                "OR lower(url) LIKE ? OR lower(url_path) LIKE ? OR lower(url_query) LIKE ?)"
            )
            where_params.extend([like, like, like, like])
            score_terms.append(
                "(CASE WHEN lower(url_path) LIKE ? THEN 4 "
                "WHEN lower(url) LIKE ? THEN 2 "
                "WHEN lower(url_host_registered_domain) LIKE ? THEN 1 ELSE 0 END)"
            )
            score_params.extend([like, like, like])
        sql = f"""
            SELECT lower(url_host_registered_domain) AS domain, url,
                   ({' + '.join(score_terms)}) AS relevance
            FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
            WHERE fetch_status = 200 AND content_mime_type = 'text/html'
              AND url_host_registered_domain IS NOT NULL
              AND ({' OR '.join(predicates)})
            ORDER BY relevance DESC, domain ASC, url ASC LIMIT 2000
        """
        rows = conn.execute(sql, [*score_params, parquet_urls, *where_params]).fetchall()
        candidate_urls: list[dict[str, Any]] = []
        domains: dict[str, dict[str, Any]] = {}
        seen_urls: set[str] = set()
        for domain, url, relevance in rows:
            domain = str(domain or "").lower()
            url = str(url or "")
            if not domain or not url:
                continue
            score = int(relevance or 0)
            stats = domains.setdefault(domain, {"domain": domain, "captures": 0, "score": 0, "example_url": url})
            stats["captures"] += 1
            stats["score"] = max(stats["score"], score)
            if url not in seen_urls:
                seen_urls.add(url)
                candidate_urls.append({"url": url, "domain": domain, "match_score": score, "discovery_source": "commoncrawl_parquet"})
        ranked_domains = sorted(domains.values(), key=lambda item: (-item["score"], -item["captures"], item["domain"]))
        for item in ranked_domains:
            item.pop("score", None)
        return {"domains": ranked_domains[:limit], "candidate_urls": candidate_urls[:MAX_CANDIDATE_URLS]}
    finally:
        conn.close()


async def discover_domains(analysis: dict[str, Any], limit: int = MAX_INDEX_CANDIDATES) -> dict[str, Any]:
    terms = _terms(analysis)
    if not terms:
        return {"crawl_id": None, "domains": [], "candidate_urls": [], "terms": [], "method": "commoncrawl-parquet-url-index"}
    cache_key = tuple([str(analysis.get("sector", "")), *terms])
    cached = _RESULT_CACHE.get(cache_key)
    if cached and time.monotonic() - cached[0] < CACHE_SECONDS:
        return cached[1]

    curated_terms = [term for term in terms if term in CURATED_DOMAINS]
    topic = str(analysis.get("sector_id") or analysis.get("sector") or analysis.get("topic", "")).lower()
    topic = topic.replace(" ", "_")
    if topic in CURATED_DOMAINS and topic not in curated_terms:
        curated_terms.insert(0, topic)
    # Sector routing comes from the query embedding classifier. Search terms
    # may be used for URL-index lookup, but must not override that classification.
    is_health = analysis.get("sector_group", analysis.get("sector")) == "health"
    if is_health:
        curated = HEALTH_DOMAINS
    else:
        selected_topics = curated_terms or ([topic] if topic in CURATED_DOMAINS else [])
        curated = []
        for position in range(max((len(CURATED_DOMAINS[key]) for key in selected_topics), default=0)):
            for key in selected_topics:
                if position < len(CURATED_DOMAINS[key]):
                    curated.append(CURATED_DOMAINS[key][position])
    seeded_domains = [
        {"domain": domain, "captures": 0, "discovery_source": "curated_seed", "category": category}
        for domain, category in curated
    ]
    try:
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
        # Keep request latency bounded. A sampled shard set can miss URLs.
        parquet_urls = parquet_urls[:MAX_PARQUET_FILES]
        indexed = await asyncio.to_thread(
            _discover_sync, parquet_urls, terms, min(max(limit, 1), MAX_INDEX_CANDIDATES)
        )
    except Exception:
        if not seeded_domains:
            raise
        result = {
            "crawl_id": None,
            "domains": seeded_domains[: min(max(limit, 1), MAX_INDEX_CANDIDATES)],
            "candidate_urls": [],
            "terms": terms,
            "method": "curated-domains-commoncrawl-index-unavailable",
        }
        _RESULT_CACHE[cache_key] = (time.monotonic(), result)
        return result
    domains = indexed["domains"]
    candidate_urls = indexed["candidate_urls"]
    indexed_domains = list(domains)
    present = {item["domain"] for item in domains}
    domains = domains + [item for item in seeded_domains if item["domain"] not in present]
    domain_limit = min(max(limit, 1), MAX_INDEX_CANDIDATES)
    if topic in {"team_sports", "individual_sports"}:
        wikipedia = next((item for item in seeded_domains if item["domain"] == "en.wikipedia.org"), None)
        if wikipedia and not any(item["domain"] == "en.wikipedia.org" for item in indexed_domains):
            domains = domains[: max(0, domain_limit - 1)] + [wikipedia]
    domains = domains[:domain_limit]
    result = {
        "crawl_id": crawl_id,
        "domains": domains,
        "candidate_urls": candidate_urls,
        "terms": terms,
        "method": "commoncrawl-parquet-url-index",
    }
    _RESULT_CACHE[cache_key] = (time.monotonic(), result)
    return result
