"""Ingest forward citation counts from BigQuery into local SQLite."""
from __future__ import annotations

import argparse
import sqlite3
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


def _build_query(limit: int | None) -> str:
    query = f"""
    SELECT
        publication_number,
        ARRAY_LENGTH(cited_by) AS forward_citations
    FROM `{RESEARCH_TABLE}`
    WHERE country = @country_name
      AND STARTS_WITH(publication_number, @publication_prefix)
      AND ARRAY_LENGTH(cited_by) > 0
    ORDER BY publication_number
    """
    if limit is not None:
        query += f"\nLIMIT {int(limit)}"
    return query


def run_ingestion(
    country_code: str = "JP",
    db_path: str = "data/patents.db",
    limit: int | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, int | float]:
    country_code = country_code.upper()
    country_name = COUNTRY_NAME_MAP.get(country_code)
    if not country_name:
        raise ValueError(
            f"Unsupported country '{country_code}'. Supported: {sorted(COUNTRY_NAME_MAP)}"
        )
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be > 0")

    source = BigQuerySource()
    db_path_obj = Path(db_path)
    db_path_obj.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_SQL)

    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-512000")
    conn.execute("PRAGMA foreign_keys=OFF")

    query = _build_query(limit=limit)
    from google.cloud import bigquery

    params = [
        bigquery.ScalarQueryParameter("country_name", "STRING", country_name),
        bigquery.ScalarQueryParameter(
            "publication_prefix", "STRING", f"{country_code}-"
        ),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params)

    print(
        f"Starting citation ingestion: country={country_code} ({country_name}), "
        f"batch_size={batch_size}, limit={limit}"
    )
    sys.stdout.flush()

    start_time = time.time()
    result_iter = source.client.query(query, job_config=job_config).result()

    batch_rows: list[tuple[str, int]] = []
    total_fetched = 0
    total_inserted = 0

    for row in result_iter:
        row_dict = dict(row)
        total_fetched += 1
        publication_number = row_dict.get("publication_number")
        forward_citations = row_dict.get("forward_citations")
        if not publication_number or forward_citations is None:
            continue

        batch_rows.append((publication_number, int(forward_citations)))

        if len(batch_rows) >= batch_size:
            conn.executemany(
                """
                INSERT OR REPLACE INTO citation_counts (
                    publication_number, forward_citations
                ) VALUES (?, ?)
                """,
                batch_rows,
            )
            conn.commit()
            total_inserted += len(batch_rows)
            batch_rows = []

            elapsed = time.time() - start_time
            rate = total_fetched / elapsed if elapsed > 0 else 0.0
            print(
                f"Fetched: {total_fetched:>10,} | "
                f"Inserted: {total_inserted:>10,} | "
                f"Rate: {rate:>8,.1f}/s"
            )
            sys.stdout.flush()

    if batch_rows:
        conn.executemany(
            """
            INSERT OR REPLACE INTO citation_counts (
                publication_number, forward_citations
            ) VALUES (?, ?)
            """,
            batch_rows,
        )
        conn.commit()
        total_inserted += len(batch_rows)

    conn.execute(
        """
        UPDATE patents SET citation_count_forward = (
            SELECT forward_citations FROM citation_counts cc
            WHERE cc.publication_number = patents.publication_number
        )
        WHERE EXISTS (
            SELECT 1 FROM citation_counts cc WHERE cc.publication_number = patents.publication_number
        )
        """
    )
    conn.commit()

    elapsed = time.time() - start_time
    conn.execute("PRAGMA foreign_keys=ON")
    conn.commit()
    conn.close()

    print("\nIngestion complete.")
    print(f"Fetched:  {total_fetched:,}")
    print(f"Inserted: {total_inserted:,}")
    print(f"Elapsed:  {elapsed:.1f}s")
    print(f"Rate:     {(total_fetched / elapsed) if elapsed > 0 else 0:,.1f}/s")
    sys.stdout.flush()

    return {
        "total_fetched": total_fetched,
        "total_inserted": total_inserted,
        "elapsed_seconds": elapsed,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest citation counts from BigQuery google_patents_research.publications"
    )
    parser.add_argument("--country", default="JP", help="Country code (JP, US)")
    parser.add_argument("--db", default="data/patents.db", help="SQLite DB path")
    parser.add_argument("--limit", type=int, default=None, help="Max rows to fetch")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows per SQLite commit (default: {DEFAULT_BATCH_SIZE})",
    )
    args = parser.parse_args()

    run_ingestion(
        country_code=args.country,
        db_path=args.db,
        limit=args.limit,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
