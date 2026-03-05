"""Ingest embeddings from BigQuery into local SQLite.

Processes in prefix-based batches to avoid BigQuery 'Response too large' errors.
JP patents use multiple numbering formats:
  - JP-2000 to JP-2025: Western year (post-2000)
  - JP-S06 to JP-S64:   Showa era (1931-1989)
  - JP-H01 to JP-H11:   Heisei era (1989-1999)
  - JP-3 to JP-7:       Grant/registration numbers
  - JP-WO:              PCT national phase
  - JP-1:               Old grant numbers
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import struct
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.migrations import SCHEMA_SQL
from sources.bigquery import BigQuerySource

DEFAULT_BATCH_SIZE = 50_000
RESEARCH_TABLE = "patents-public-data.google_patents_research.publications"
COUNTRY_NAME_MAP = {
    "JP": "Japan",
    "US": "United States",
}


def _build_jp_prefixes() -> list[str]:
    """Build JP patent publication_number prefixes for batch processing.

    Only includes prefixes that exist in the local patents table:
    - JP-2000 to JP-2025: Western year publications (~8M)
    - JP-3 to JP-7: Grant/registration numbers (~5M)
    - JP-WO: PCT national phase (~415K)
    - JP-1: Old grant numbers (~298K)

    Skips Showa (JP-S) and Heisei (JP-H) era prefixes as these
    are not in the local patents table (ingested from BigQuery
    patents.publications which uses different numbering).
    """
    prefixes = []
    # Western years (post-2000): JP-2000 to JP-2025
    for year in range(2000, 2026):
        prefixes.append(f"JP-{year}")
    # Grant/registration numbers (each ~1M records)
    for n in range(3, 8):
        prefixes.append(f"JP-{n}")
    # PCT national phase
    prefixes.append("JP-WO")
    # Old grant numbers
    prefixes.append("JP-1")
    return prefixes


def _build_us_prefixes() -> list[str]:
    """Build US patent prefixes."""
    prefixes = []
    for year in range(1990, 2026):
        prefixes.append(f"US-{year}")
    return prefixes


def _build_prefix_query(limit: int | None) -> str:
    """Build query filtering by publication_number prefix."""
    query = f"""
    SELECT
        publication_number,
        title,
        abstract,
        top_terms,
        embedding_v1
    FROM `{RESEARCH_TABLE}`
    WHERE country = @country_name
      AND STARTS_WITH(publication_number, @prefix)
      AND ARRAY_LENGTH(embedding_v1) > 0
    """
    if limit is not None:
        query += f"\nLIMIT {int(limit)}"
    return query


def _pack_embedding(values: list[float] | tuple[float, ...] | None) -> bytes | None:
    if not values or len(values) != 64:
        return None
    try:
        return struct.pack("64d", *values)
    except (TypeError, struct.error):
        return None


def _ingest_prefix(
    source: BigQuerySource,
    conn: sqlite3.Connection,
    country_name: str,
    prefix: str,
    batch_size: int,
    limit: int | None,
) -> tuple[int, int]:
    """Ingest embeddings for a single prefix batch. Returns (fetched, inserted)."""
    from google.cloud import bigquery

    query = _build_prefix_query(limit=limit)
    params = [
        bigquery.ScalarQueryParameter("country_name", "STRING", country_name),
        bigquery.ScalarQueryParameter("prefix", "STRING", prefix),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params)

    result_iter = source.client.query(query, job_config=job_config).result()

    batch_rows: list[tuple[str, str | None, str | None, str, bytes]] = []
    fetched = 0
    inserted = 0

    for row in result_iter:
        row_dict = dict(row)
        fetched += 1
        publication_number = row_dict.get("publication_number")
        embedding_blob = _pack_embedding(row_dict.get("embedding_v1"))
        if not publication_number or embedding_blob is None:
            continue

        top_terms = row_dict.get("top_terms") or []
        batch_rows.append(
            (
                publication_number,
                row_dict.get("title"),
                row_dict.get("abstract"),
                json.dumps(top_terms),
                embedding_blob,
            )
        )

        if len(batch_rows) >= batch_size:
            conn.executemany(
                """
                INSERT OR REPLACE INTO patent_research_data (
                    publication_number, title_en, abstract_en, top_terms, embedding_v1
                ) VALUES (?, ?, ?, ?, ?)
                """,
                batch_rows,
            )
            conn.commit()
            inserted += len(batch_rows)
            batch_rows = []

    if batch_rows:
        conn.executemany(
            """
            INSERT OR REPLACE INTO patent_research_data (
                publication_number, title_en, abstract_en, top_terms, embedding_v1
            ) VALUES (?, ?, ?, ?, ?)
            """,
            batch_rows,
        )
        conn.commit()
        inserted += len(batch_rows)

    return fetched, inserted


def run_ingestion(
    country_code: str = "JP",
    db_path: str = "data/patents.db",
    limit_per_batch: int | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    start_from: str | None = None,
) -> dict[str, int | float]:
    country_code = country_code.upper()
    country_name = COUNTRY_NAME_MAP.get(country_code)
    if not country_name:
        raise ValueError(
            f"Unsupported country '{country_code}'. Supported: {sorted(COUNTRY_NAME_MAP)}"
        )

    if country_code == "JP":
        all_prefixes = _build_jp_prefixes()
    elif country_code == "US":
        all_prefixes = _build_us_prefixes()
    else:
        raise ValueError(f"No prefix builder for country '{country_code}'")

    # Resume support: skip prefixes until we reach start_from
    if start_from:
        try:
            idx = all_prefixes.index(start_from)
            all_prefixes = all_prefixes[idx:]
            print(f"Resuming from prefix: {start_from} ({len(all_prefixes)} remaining)")
        except ValueError:
            print(f"Warning: start_from prefix '{start_from}' not found, starting from beginning")
    sys.stdout.flush()

    source = BigQuerySource()
    db_path_obj = Path(db_path)
    db_path_obj.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path, timeout=60)
    conn.executescript(SCHEMA_SQL)

    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-512000")
    conn.execute("PRAGMA foreign_keys=OFF")

    print(
        f"Starting embedding ingestion: country={country_code} ({country_name}), "
        f"prefixes={len(all_prefixes)}, batch_size={batch_size}"
    )
    sys.stdout.flush()

    start_time = time.time()
    total_fetched = 0
    total_inserted = 0
    skipped = 0

    for i, prefix in enumerate(all_prefixes, 1):
        prefix_start = time.time()
        print(f"\n--- [{i}/{len(all_prefixes)}] Prefix: {prefix} ---")
        sys.stdout.flush()

        try:
            fetched, inserted = _ingest_prefix(
                source=source,
                conn=conn,
                country_name=country_name,
                prefix=prefix,
                batch_size=batch_size,
                limit=limit_per_batch,
            )
        except Exception as e:
            print(f"  ERROR for prefix {prefix}: {e}")
            sys.stdout.flush()
            skipped += 1
            continue

        total_fetched += fetched
        total_inserted += inserted
        prefix_elapsed = time.time() - prefix_start
        total_elapsed = time.time() - start_time

        if fetched > 0:
            print(
                f"  Prefix {prefix}: fetched={fetched:,}, inserted={inserted:,}, "
                f"time={prefix_elapsed:.1f}s"
            )
        else:
            print(f"  Prefix {prefix}: (empty) time={prefix_elapsed:.1f}s")
        print(
            f"  Total: fetched={total_fetched:,}, inserted={total_inserted:,}, "
            f"elapsed={total_elapsed:.1f}s ({total_elapsed/60:.1f}m)"
        )
        sys.stdout.flush()

    elapsed = time.time() - start_time
    conn.execute("PRAGMA foreign_keys=ON")
    conn.commit()
    conn.close()

    print(f"\nIngestion complete.")
    print(f"Fetched:  {total_fetched:,}")
    print(f"Inserted: {total_inserted:,}")
    print(f"Skipped:  {skipped} prefixes")
    print(f"Elapsed:  {elapsed:.1f}s ({elapsed/60:.1f}m)")
    if elapsed > 0:
        print(f"Rate:     {total_fetched / elapsed:,.1f}/s")
    sys.stdout.flush()

    return {
        "total_fetched": total_fetched,
        "total_inserted": total_inserted,
        "elapsed_seconds": elapsed,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest embeddings from BigQuery google_patents_research.publications"
    )
    parser.add_argument("--country", default="JP", help="Country code (JP, US)")
    parser.add_argument("--db", default="data/patents.db", help="SQLite DB path")
    parser.add_argument(
        "--limit-per-batch",
        type=int,
        default=None,
        help="Max rows per prefix batch (for testing)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows per SQLite commit (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--start-from",
        type=str,
        default=None,
        help="Resume from this prefix (e.g. JP-2000, JP-S50, JP-H01)",
    )
    args = parser.parse_args()

    run_ingestion(
        country_code=args.country,
        db_path=args.db,
        limit_per_batch=args.limit_per_batch,
        batch_size=args.batch_size,
        start_from=args.start_from,
    )


if __name__ == "__main__":
    main()
