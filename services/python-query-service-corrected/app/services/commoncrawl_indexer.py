"""Restartable, once-per-snapshot Common Crawl Parquet metadata indexer.

DuckDB reads remote Parquet shards using HTTP range requests. It does not build
a local copy of the Common Crawl corpus. Progress and indexed rows are saved by
the Rust service in each sector database, so rerunning this job skips completed
shards and safely upserts a shard again if the previous run stopped mid-shard.
"""

from __future__ import annotations

import argparse
import gzip
import logging
import os
import sys
import time
import json
from pathlib import Path
from typing import Any

import duckdb  # type: ignore
import httpx  # type: ignore

from app.services.query_understanding import embed_texts

LOG = logging.getLogger("commoncrawl-indexer")
COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
MANIFEST_BASE = "https://data.commoncrawl.org/crawl-data"
DATA_BASE = "https://data.commoncrawl.org/"
SECTOR_FILE = Path(__file__).resolve().parents[1] / "data" / "sectors.json"
SECTOR_CATALOG = json.loads(SECTOR_FILE.read_text(encoding="utf-8"))["sectors"]
SECTOR_BY_ID = {item["id"]: item for item in SECTOR_CATALOG}
SECTOR_DESCRIPTIONS = {item["id"]: item["description"] for item in SECTOR_CATALOG}
FAMILY_GROUPS = {
    "technology": {"programming_software", "ai_data", "engineering", "computing_infra"},
    "health": {"health_medicine", "biology"},
}
DOMAIN_COLUMNS = (
    "url_host_registered_domain",
    "url_host_private_domain",
    "url_host_name",
)
URL_COLUMNS = ("url",)
STATUS_COLUMNS = ("fetch_status", "status")
MIME_COLUMNS = ("content_mime_type", "mime")
LANGUAGE_COLUMNS = ("content_languages", "content_language", "language")
FILENAME_COLUMNS = ("warc_filename",)
OFFSET_COLUMNS = ("warc_record_offset", "warc_offset")
LENGTH_COLUMNS = ("warc_record_length", "warc_length")
TIME_COLUMNS = ("fetch_time", "timestamp")


def _choose(columns: set[str], options: tuple[str, ...]) -> str | None:
    return next((name for name in options if name in columns), None)


def _identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _first_value(columns: set[str], options: tuple[str, ...], alias: str, cast: str = "VARCHAR") -> str:
    column = _choose(columns, options)
    if column is None:
        return f"NULL::{cast} AS {_identifier(alias)}"
    return f"TRY_CAST({_identifier(column)} AS {cast}) AS {_identifier(alias)}"


def inspect_schema(connection: duckdb.DuckDBPyConnection, parquet_url: str) -> set[str]:
    sql = f"DESCRIBE SELECT * FROM read_parquet({_literal(parquet_url)}, union_by_name=true)"
    rows = connection.execute(sql).fetchall()
    columns = {str(row[0]).lower() for row in rows}
    domain_column = _choose(columns, DOMAIN_COLUMNS)
    url_column = _choose(columns, URL_COLUMNS)
    if domain_column is None or url_column is None:
        raise RuntimeError(
            "Common Crawl Parquet schema is missing a registered-domain or URL column; "
            f"found columns: {', '.join(sorted(columns))}"
        )
    LOG.info("Inspected Parquet schema: %d columns; domain=%s; url=%s", len(columns), domain_column, url_column)
    return columns


def stream_domain_batches(
    connection: duckdb.DuckDBPyConnection,
    parquet_url: str,
    columns: set[str],
    batch_size: int,
    sample_urls: int,
):
    domain_column = _choose(columns, DOMAIN_COLUMNS)
    url_column = _choose(columns, URL_COLUMNS)
    assert domain_column and url_column
    projection = [
        f"lower(trim(CAST({_identifier(domain_column)} AS VARCHAR))) AS domain",
        f"CAST({_identifier(url_column)} AS VARCHAR) AS url",
        _first_value(columns, STATUS_COLUMNS, "http_status", "INTEGER"),
        _first_value(columns, MIME_COLUMNS, "mime_type"),
        _first_value(columns, LANGUAGE_COLUMNS, "language"),
        _first_value(columns, FILENAME_COLUMNS, "warc_filename"),
        _first_value(columns, OFFSET_COLUMNS, "warc_offset", "BIGINT"),
        _first_value(columns, LENGTH_COLUMNS, "warc_length", "BIGINT"),
        _first_value(columns, TIME_COLUMNS, "fetch_time"),
    ]
    filters = [
        f"{_identifier(domain_column)} IS NOT NULL",
        f"trim(CAST({_identifier(domain_column)} AS VARCHAR)) <> ''",
        f"{_identifier(url_column)} IS NOT NULL",
        f"CAST({_identifier(url_column)} AS VARCHAR) <> ''",
    ]
    status_column = _choose(columns, STATUS_COLUMNS)
    mime_column = _choose(columns, MIME_COLUMNS)
    if status_column:
        filters.append(f"TRY_CAST({_identifier(status_column)} AS INTEGER) = 200")
    if mime_column:
        filters.append(f"lower(CAST({_identifier(mime_column)} AS VARCHAR)) = 'text/html'")
    sql = f"""
        WITH pages AS (
            SELECT {', '.join(projection)}
            FROM read_parquet(?, union_by_name=true)
            WHERE {' AND '.join(filters)}
        ),
        unique_pages AS (
            SELECT *, row_number() OVER (
                PARTITION BY domain, url ORDER BY fetch_time DESC NULLS LAST
            ) AS url_rank
            FROM pages
        )
        SELECT domain,
               list_slice(
                   list(struct_pack(
                       url := url,
                       status := http_status,
                       mime := mime_type,
                       language := language,
                       warc_filename := warc_filename,
                       warc_offset := warc_offset,
                       warc_length := warc_length,
                       fetch_time := fetch_time
                   ) ORDER BY fetch_time DESC NULLS LAST),
                   1, {int(sample_urls)}
               ) AS samples
        FROM unique_pages
        WHERE url_rank = 1
        GROUP BY domain
        ORDER BY domain
    """
    cursor = connection.execute(sql, [parquet_url])
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield rows


def _domain_text(domain: str, urls: list[dict[str, Any]]) -> str:
    paths: list[str] = []
    languages: set[str] = set()
    for item in urls:
        url = str(item.get("url") or "").strip()
        if url:
            paths.append(url)
        language = str(item.get("language") or "").strip()
        if language:
            languages.add(language)
    return (
        f"Registered website domain: {domain}.\n"
        f"Representative Common Crawl URLs and page paths: {' '.join(paths)}\n"
        f"Available page languages: {' '.join(sorted(languages))}"
    )


def _cosine_scores(vectors: list[list[float]], sector_vectors: dict[str, list[float]]) -> dict[str, list[float]]:
    scores = {sector: [] for sector in SECTOR_DESCRIPTIONS}
    for vector in vectors:
        for sector, sector_vector in sector_vectors.items():
            scores[sector].append(sum(float(a) * float(b) for a, b in zip(vector, sector_vector)))
    return scores


def _collection(client: httpx.Client, requested: str | None) -> str:
    if requested:
        return requested
    response = client.get(COLLINFO_URL)
    response.raise_for_status()
    collections = response.json()
    if not collections:
        raise RuntimeError("Common Crawl returned no available collections")
    return str(collections[0]["id"])


def _parquet_shards(client: httpx.Client, collection: str) -> list[tuple[str, str]]:
    manifest_url = f"{MANIFEST_BASE}/{collection}/cc-index-table.paths.gz"
    response = client.get(manifest_url)
    response.raise_for_status()
    try:
        manifest = gzip.decompress(response.content).decode("utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise RuntimeError(f"Invalid Parquet manifest for {collection}") from exc
    prefix = f"crawl={collection}/subset=warc/"
    shards = []
    for line in manifest.splitlines():
        path = line.strip().lstrip("/")
        if path.endswith(".parquet") and prefix in path:
            shards.append((path, DATA_BASE + path))
    if not shards:
        raise RuntimeError(f"No WARC Parquet shards found in {collection} manifest")
    return shards


def _targets(sector: str) -> dict[str, str]:
    base_url = os.getenv("RUST_SERVICE_URL", "http://localhost:8081").rstrip("/")
    if sector == "both":
        selected_ids = list(SECTOR_BY_ID)
    elif sector in FAMILY_GROUPS:
        selected_ids = [item["id"] for item in SECTOR_CATALOG if item["family_id"] in FAMILY_GROUPS[sector]]
    elif sector in SECTOR_BY_ID:
        selected_ids = [sector]
    else:
        raise ValueError("--sector must be technology, health, both, or a sector id from sectors.json")
    return {sector_id: base_url for sector_id in selected_ids}


def _progress(client: httpx.Client, base_url: str, collection: str, sector: str) -> set[str]:
    response = client.get(
        f"{base_url}/internal/v1/index/progress",
        params={"collection": collection, "sector": sector},
    )
    response.raise_for_status()
    return set(response.json().get("completed_shards", []))


def _post_batch(client: httpx.Client, base_url: str, payload: dict[str, Any]) -> None:
    response = client.post(f"{base_url}/internal/v1/index/batch", json=payload, timeout=180.0)
    response.raise_for_status()


def _complete_shard(
    client: httpx.Client, base_url: str, collection: str, path: str,
    sector: str, status: str, domains: int, error: str | None = None,
) -> None:
    response = client.post(
        f"{base_url}/internal/v1/index/complete",
        json={
            "collection": collection,
            "shard_path": path,
            "sector": sector,
            "status": status,
            "domain_count": domains,
            "error": error,
        },
    )
    response.raise_for_status()


def run(args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    target_urls = _targets(args.sector)
    rust_client = httpx.Client(timeout=httpx.Timeout(30.0, connect=10.0))
    cc_client = httpx.Client(timeout=httpx.Timeout(90.0, connect=20.0), follow_redirects=True)
    try:
        collection = _collection(cc_client, args.snapshot)
        shards = _parquet_shards(cc_client, collection)
        if args.max_files:
            shards = shards[: args.max_files]
        LOG.info("Snapshot %s has %d selected Parquet shards", collection, len(shards))
        completed = {
            sector: _progress(rust_client, base_url, collection, sector)
            for sector, base_url in target_urls.items()
        }
        pending = [
            (path, url) for path, url in shards
            if not all(path in completed[sector] for sector in target_urls)
        ]
        LOG.info("Skipping %d completed shards; %d remain", len(shards) - len(pending), len(pending))

        sector_vectors_list = embed_texts(list(SECTOR_DESCRIPTIONS.values()), batch_size=8)
        sector_vectors = dict(zip(SECTOR_DESCRIPTIONS.keys(), sector_vectors_list))
        base_url = next(iter(target_urls.values()))
        response = rust_client.post(
            f"{base_url}/internal/v1/index/sectors",
            json={
                "sectors": [
                    {"sector": sector, "description": SECTOR_DESCRIPTIONS[sector], "embedding": sector_vectors[sector]}
                    for sector in target_urls
                ]
            },
        )
        response.raise_for_status()

        temp_directory = Path(args.temp_dir).resolve()
        temp_directory.mkdir(parents=True, exist_ok=True)
        duck = duckdb.connect(database=":memory:")
        try:
            duck.execute("INSTALL httpfs")
            duck.execute("LOAD httpfs")
            duck.execute(f"SET memory_limit={_literal(args.memory_limit)}")
            duck.execute(f"SET temp_directory={_literal(str(temp_directory))}")
            duck.execute(f"SET threads={int(args.threads)}")
            for shard_number, (path, parquet_url) in enumerate(pending, start=1):
                started = time.monotonic()
                LOG.info("[%d/%d] Inspecting %s", shard_number, len(pending), path)
                try:
                    columns = inspect_schema(duck, parquet_url)
                    domain_count = 0
                    for rows in stream_domain_batches(
                        duck, parquet_url, columns, args.batch_size, args.sample_urls
                    ):
                        domain_values = [str(row[0]) for row in rows]
                        url_samples = [
                            [dict(item) for item in (row[1] or [])]
                            for row in rows
                        ]
                        texts = [_domain_text(domain, urls) for domain, urls in zip(domain_values, url_samples)]
                        vectors = embed_texts(texts, batch_size=args.embedding_batch_size)
                        scores = _cosine_scores(vectors, sector_vectors)
                        selected_by_sector: dict[str, list[dict[str, Any]]] = {sector: [] for sector in target_urls}
                        for i, (domain, urls, vector) in enumerate(zip(domain_values, url_samples, vectors)):
                            best_sector = max(target_urls, key=lambda candidate: scores[candidate][i])
                            best_score = scores[best_sector][i]
                            if best_score < args.threshold:
                                continue
                            selected_by_sector[best_sector].append({
                                "domain": domain,
                                "sector_score": best_score,
                                "embedding": vector,
                                "sample_urls": urls,
                            })
                        for sector, selected in selected_by_sector.items():
                            if selected:
                                _post_batch(
                                    rust_client,
                                    target_urls[sector],
                                    {
                                        "collection": collection,
                                        "shard_path": path,
                                        "sector": sector,
                                        "domain_count": len(domain_values),
                                        "domains": selected,
                                    },
                                )
                        domain_count += len(domain_values)
                    for sector, base_url in target_urls.items():
                        _complete_shard(rust_client, base_url, collection, path, sector, "completed", domain_count)
                    LOG.info("Completed %s: %d unique shard domains in %.1f sec", path, domain_count, time.monotonic() - started)
                except Exception as exc:
                    for sector, base_url in target_urls.items():
                        try:
                            _complete_shard(rust_client, base_url, collection, path, sector, "failed", 0, str(exc)[:2000])
                        except Exception:
                            LOG.exception("Could not record failed shard state for %s/%s", sector, path)
                    raise
        finally:
            duck.close()
        LOG.info("Indexing finished for %s (%d shards processed this run)", collection, len(pending))
    finally:
        rust_client.close()
        cc_client.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a restartable Common Crawl domain embedding index")
    parser.add_argument("--snapshot", default=os.getenv("CC_INDEX_SNAPSHOT"), help="Common Crawl snapshot ID; default is latest")
    parser.add_argument("--sector", choices=["technology", "health", "both", *SECTOR_BY_ID.keys()], default="both")
    parser.add_argument("--max-files", type=int, default=0, help="Maximum shards to consider; 0 means all remaining shards")
    parser.add_argument("--batch-size", type=int, default=100, help="Domains returned from DuckDB and embedded per batch")
    parser.add_argument("--sample-urls", type=int, default=5, help="Representative URLs saved per domain per shard")
    parser.add_argument("--embedding-batch-size", type=int, default=64)
    parser.add_argument("--threshold", type=float, default=float(os.getenv("CC_SECTOR_SIMILARITY_THRESHOLD", "0.35")))
    parser.add_argument("--memory-limit", default=os.getenv("CC_DUCKDB_MEMORY_LIMIT", "4GB"))
    parser.add_argument("--threads", type=int, default=max(1, min(4, os.cpu_count() or 1)))
    parser.add_argument("--temp-dir", default=os.getenv("CC_DUCKDB_TEMP_DIR", str(Path.cwd() / ".cc-duckdb-tmp")))
    args = parser.parse_args(argv)
    if args.max_files < 0 or not 1 <= args.batch_size <= 1000 or not 1 <= args.sample_urls <= 20:
        parser.error("max-files must be nonnegative, batch-size 1..1000, sample-urls 1..20")
    if not 0.0 <= args.threshold <= 1.0 or args.threads < 1 or args.embedding_batch_size < 1:
        parser.error("threshold must be 0..1 and thread/batch sizes must be positive")
    return args


if __name__ == "__main__":
    try:
        run(parse_args())
    except KeyboardInterrupt:
        LOG.warning("Interrupted. Completed shards are persisted; rerun the same command to resume.")
        sys.exit(130)
    except Exception:
        LOG.exception("Common Crawl snapshot indexing failed; completed shards remain checkpointed")
        sys.exit(1)
