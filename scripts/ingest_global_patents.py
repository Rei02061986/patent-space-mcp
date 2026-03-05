#!/usr/bin/env python3
"""Ingest patent METADATA from BigQuery for non-JP jurisdictions.

Fetches metadata from patents-public-data.patents.publications for
US, EP, WO, CN, KR, DE, GB, FR (configurable). Processes in batches
using publication_date ranges to avoid BigQuery "Response too large" errors.

Estimated BQ scan costs (at $5/TB):
  US patents: ~5M records, ~100GB scan -> ~$0.50
  EP patents: ~3M records, ~60GB scan  -> ~$0.30
  WO patents: ~4M records, ~80GB scan  -> ~$0.40
  CN patents: ~8M records, ~160GB scan -> ~$0.80
  KR patents: ~3M records, ~60GB scan  -> ~$0.30
  DE patents: ~2M records, ~40GB scan  -> ~$0.20
  GB patents: ~1M records, ~20GB scan  -> ~$0.10
  FR patents: ~1M records, ~20GB scan  -> ~$0.10
  Total (all 8): ~$2.70 for all jurisdictions

Usage:
    python scripts/ingest_global_patents.py \
        --db data/patents.db \
        --country-codes US,EP \
        --date-from 2015-01-01

    # Dry run to see estimated cost/record count:
    python scripts/ingest_global_patents.py \
        --db data/patents.db \
        --country-codes US,EP,WO,CN,KR \
        --dry-run

    # Resume from last ingested publication_date:
    python scripts/ingest_global_patents.py \
        --db data/patents.db \
        --country-codes US \
        --resume
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.migrations import SCHEMA_SQL
from normalize.schema import normalize_bigquery_row
from sources.bigquery import BigQuerySource, PUBLICATIONS_TABLE

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_BATCH_SIZE = 50_000
DEFAULT_COUNTRY_CODES = "US,EP,WO,CN,KR"
PROGRESS_INTERVAL = 10_000  # Log every N records

# Estimated record counts and scan sizes per country (for dry-run display)
COUNTRY_ESTIMATES: dict[str, dict[str, float]] = {
    "US": {"records_millions": 5.0, "scan_gb": 100.0},
    "EP": {"records_millions": 3.0, "scan_gb": 60.0},
    "WO": {"records_millions": 4.0, "scan_gb": 80.0},
    "CN": {"records_millions": 8.0, "scan_gb": 160.0},
    "KR": {"records_millions": 3.0, "scan_gb": 60.0},
    "DE": {"records_millions": 2.0, "scan_gb": 40.0},
    "GB": {"records_millions": 1.0, "scan_gb": 20.0},
    "FR": {"records_millions": 1.0, "scan_gb": 20.0},
}
BQ_COST_PER_TB = 5.0  # USD

# Publication date ranges for batch processing (YYYYMMDD).
# Each chunk covers a 1-year window. Adjust if needed for very large
# jurisdictions (e.g. CN may need 6-month windows).
MIN_YEAR = 1990
MAX_YEAR = 2026


# ---------------------------------------------------------------------------
# BigQuery query builder
# ---------------------------------------------------------------------------
def _build_metadata_query(
    country_code: str,
    date_lo: int,
    date_hi: int,
) -> tuple[str, list]:
    """Build parameterized BigQuery query for patent metadata.

    Returns (sql, params) where params is a list of bigquery.ScalarQueryParameter.
    Uses publication_date range for pagination to avoid Response too large errors.
    """
    from google.cloud import bigquery

    sql = f"""
    SELECT
        p.publication_number,
        p.application_number,
        p.family_id,
        p.country_code,
        p.kind_code,
        p.filing_date,
        p.publication_date,
        p.grant_date,
        p.entity_status,
        (SELECT t.text FROM UNNEST(p.title_localized) t
         WHERE t.language = 'ja' LIMIT 1) AS title_ja,
        (SELECT t.text FROM UNNEST(p.title_localized) t
         WHERE t.language = 'en' LIMIT 1) AS title_en,
        (SELECT a.text FROM UNNEST(p.abstract_localized) a
         WHERE a.language = 'ja' LIMIT 1) AS abstract_ja,
        (SELECT a.text FROM UNNEST(p.abstract_localized) a
         WHERE a.language = 'en' LIMIT 1) AS abstract_en,
        ARRAY(SELECT AS STRUCT c.code, c.inventive, c.first
              FROM UNNEST(p.cpc) c) AS cpc_codes,
        ARRAY(SELECT AS STRUCT a.name, a.country_code
              FROM UNNEST(p.assignee_harmonized) a) AS assignees,
        ARRAY(SELECT AS STRUCT i.name, i.country_code
              FROM UNNEST(p.inventor_harmonized) i) AS inventors
    FROM `{PUBLICATIONS_TABLE}` p
    WHERE p.country_code = @country_code
      AND p.publication_date >= @date_lo
      AND p.publication_date < @date_hi
    ORDER BY p.publication_date ASC
    """

    params = [
        bigquery.ScalarQueryParameter("country_code", "STRING", country_code),
        bigquery.ScalarQueryParameter("date_lo", "INT64", date_lo),
        bigquery.ScalarQueryParameter("date_hi", "INT64", date_hi),
    ]
    return sql, params


def _build_count_query(
    country_code: str,
    date_from_int: int | None,
    date_to_int: int | None,
) -> tuple[str, list]:
    """Build a COUNT(*) query for dry-run estimation."""
    from google.cloud import bigquery

    conditions = ["p.country_code = @country_code"]
    params = [
        bigquery.ScalarQueryParameter("country_code", "STRING", country_code),
    ]

    if date_from_int:
        conditions.append("p.publication_date >= @date_from")
        params.append(
            bigquery.ScalarQueryParameter("date_from", "INT64", date_from_int)
        )
    if date_to_int:
        conditions.append("p.publication_date <= @date_to")
        params.append(
            bigquery.ScalarQueryParameter("date_to", "INT64", date_to_int)
        )

    where = " AND ".join(conditions)
    sql = f"""
    SELECT COUNT(*) AS total_count
    FROM `{PUBLICATIONS_TABLE}` p
    WHERE {where}
    """
    return sql, params


# ---------------------------------------------------------------------------
# Date range helpers
# ---------------------------------------------------------------------------
def _parse_date_to_int(date_str: str) -> int:
    """Parse YYYY-MM-DD to YYYYMMDD integer."""
    parts = date_str.replace("/", "-").split("-")
    if len(parts) != 3:
        raise ValueError(f"Expected YYYY-MM-DD, got: {date_str}")
    return int(parts[0]) * 10000 + int(parts[1]) * 100 + int(parts[2])


def _build_year_ranges(
    date_from_int: int | None,
    date_to_int: int | None,
) -> list[tuple[int, int]]:
    """Build (lo, hi) publication_date ranges in 1-year windows.

    Each range covers [lo, hi) where lo/hi are YYYYMMDD integers.
    The year windows align to Jan 1 boundaries.
    """
    start_year = MIN_YEAR
    end_year = MAX_YEAR

    if date_from_int:
        start_year = max(start_year, date_from_int // 10000)
    if date_to_int:
        end_year = min(end_year, (date_to_int // 10000) + 1)

    ranges = []
    for year in range(start_year, end_year):
        lo = year * 10000 + 101  # Jan 1
        hi = (year + 1) * 10000 + 101  # Jan 1 of next year

        # Clamp to user-specified bounds
        if date_from_int and lo < date_from_int:
            lo = date_from_int
        if date_to_int and hi > date_to_int + 1:
            hi = date_to_int + 1

        if lo < hi:
            ranges.append((lo, hi))

    return ranges


# ---------------------------------------------------------------------------
# Entity resolution (optional, best-effort)
# ---------------------------------------------------------------------------
def _try_load_entity_resolver():
    """Attempt to load EntityResolver for firm_id linking.

    Returns ApplicantNormalizer or None if entity data is not available.
    """
    try:
        from entity.registry import EntityRegistry
        from entity.loader import load_registry
        from normalize.applicant import ApplicantNormalizer

        registry = load_registry()
        return ApplicantNormalizer(registry)
    except Exception:
        return None


def _resolve_assignees(
    normalizer, patents: list[dict],
) -> list[dict]:
    """Run entity resolution on assignees to link firm_ids.

    Modifies patents in-place and returns them.
    """
    if normalizer is None:
        return patents
    try:
        return normalizer.link_firm_ids(patents)
    except Exception:
        return patents


# ---------------------------------------------------------------------------
# SQLite batch upsert (bypasses PatentStore for performance)
# ---------------------------------------------------------------------------
def _upsert_batch_raw(
    conn: sqlite3.Connection,
    patents: list[dict],
) -> int:
    """Batch-insert normalized patents directly into SQLite.

    Uses raw SQL instead of PatentStore.upsert_batch() for better
    performance with large batches (avoids per-patent connection overhead).
    Returns count of successfully inserted records.
    """
    count = 0
    for p in patents:
        try:
            pub = p["publication_number"]

            # Main patents table
            conn.execute(
                """INSERT OR REPLACE INTO patents (
                    publication_number, application_number, family_id,
                    country_code, kind_code, title_ja, title_en,
                    abstract_ja, abstract_en, filing_date, publication_date,
                    grant_date, entity_status, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    pub,
                    p.get("application_number"),
                    p.get("family_id"),
                    p.get("country_code", ""),
                    p.get("kind_code"),
                    p.get("title_ja"),
                    p.get("title_en"),
                    p.get("abstract_ja"),
                    p.get("abstract_en"),
                    p.get("filing_date"),
                    p.get("publication_date"),
                    p.get("grant_date"),
                    p.get("entity_status"),
                    p.get("source", "bigquery"),
                ),
            )

            # CPC codes
            conn.execute(
                "DELETE FROM patent_cpc WHERE publication_number = ?", (pub,)
            )
            for c in p.get("cpc_codes", []):
                if isinstance(c, dict):
                    code = c.get("code", "")
                    inventive = 1 if c.get("inventive") or c.get("is_inventive") else 0
                    first = 1 if c.get("first") or c.get("is_first") else 0
                elif isinstance(c, str):
                    code = c
                    inventive = 0
                    first = 0
                else:
                    continue
                if code:
                    conn.execute(
                        """INSERT OR IGNORE INTO patent_cpc
                           (publication_number, cpc_code, is_inventive, is_first)
                           VALUES (?, ?, ?, ?)""",
                        (pub, code, inventive, first),
                    )

            # Assignees (harmonized)
            conn.execute(
                "DELETE FROM patent_assignees WHERE publication_number = ?",
                (pub,),
            )
            for a in p.get("applicants", []):
                if isinstance(a, dict):
                    conn.execute(
                        """INSERT INTO patent_assignees
                           (publication_number, raw_name, harmonized_name,
                            country_code, firm_id)
                           VALUES (?, ?, ?, ?, ?)""",
                        (
                            pub,
                            a.get("raw_name", ""),
                            a.get("harmonized_name"),
                            a.get("country_code"),
                            a.get("firm_id"),
                        ),
                    )

            # Raw assignees fallback
            for name in p.get("raw_assignees", []):
                if name:
                    conn.execute(
                        """INSERT OR IGNORE INTO patent_assignees
                           (publication_number, raw_name, harmonized_name)
                           VALUES (?, ?, ?)""",
                        (pub, name, name),
                    )

            # Inventors
            conn.execute(
                "DELETE FROM patent_inventors WHERE publication_number = ?",
                (pub,),
            )
            for inv in p.get("inventors", []):
                if isinstance(inv, dict):
                    name = inv.get("name", "")
                    cc = inv.get("country_code")
                elif isinstance(inv, str):
                    name = inv
                    cc = None
                else:
                    continue
                if name:
                    conn.execute(
                        """INSERT INTO patent_inventors
                           (publication_number, name, country_code)
                           VALUES (?, ?, ?)""",
                        (pub, name, cc),
                    )

            # Citations (backward)
            conn.execute(
                "DELETE FROM patent_citations WHERE citing_publication = ?",
                (pub,),
            )
            for cited in p.get("citations_backward", []):
                if cited:
                    conn.execute(
                        """INSERT OR IGNORE INTO patent_citations
                           (citing_publication, cited_publication, citation_type)
                           VALUES (?, ?, ?)""",
                        (pub, cited, "patent"),
                    )

            count += 1
        except Exception:
            # Skip individual record failures, continue with batch
            continue

    return count


# ---------------------------------------------------------------------------
# Resume support
# ---------------------------------------------------------------------------
def _get_resume_date(
    conn: sqlite3.Connection, country_code: str,
) -> int | None:
    """Find the last publication_date ingested for a country.

    Queries ingestion_log first (last_publication_date), then falls back
    to MAX(publication_date) in the patents table.
    """
    # Try ingestion_log
    row = conn.execute(
        """SELECT last_publication_date
           FROM ingestion_log
           WHERE country_code = ? AND source = 'bigquery_global'
           ORDER BY started_at DESC LIMIT 1""",
        (country_code,),
    ).fetchone()
    if row and row[0]:
        return row[0]

    # Fallback: scan patents table
    row = conn.execute(
        """SELECT MAX(publication_date) AS max_pub
           FROM patents
           WHERE country_code = ?""",
        (country_code,),
    ).fetchone()
    if row and row[0]:
        return row[0]

    return None


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------
def run_dry_run(
    source: BigQuerySource,
    country_codes: list[str],
    date_from_int: int | None,
    date_to_int: int | None,
) -> None:
    """Show estimated record counts and costs without ingesting."""
    print("=" * 60)
    print("DRY RUN: Estimated record counts and BigQuery scan costs")
    print("=" * 60)

    total_records = 0
    total_cost = 0.0

    for cc in country_codes:
        print(f"\n--- {cc} ---")

        # Run actual count query against BigQuery
        try:
            from google.cloud import bigquery

            sql, params = _build_count_query(cc, date_from_int, date_to_int)
            job_config = bigquery.QueryJobConfig(query_parameters=params)
            result = source.client.query(sql, job_config=job_config).result()
            row = next(iter(result))
            actual_count = row.total_count
            print(f"  Actual record count: {actual_count:,}")
        except Exception as e:
            actual_count = None
            print(f"  Count query failed: {e}")

        # Show estimate for comparison
        est = COUNTRY_ESTIMATES.get(cc, {"records_millions": 1.0, "scan_gb": 20.0})
        est_records = int(est["records_millions"] * 1_000_000)
        est_cost = est["scan_gb"] / 1000.0 * BQ_COST_PER_TB

        if actual_count is not None:
            # Use actual count, scale estimated scan proportionally
            ratio = actual_count / est_records if est_records > 0 else 1.0
            adjusted_cost = est_cost * ratio
            total_records += actual_count
            total_cost += adjusted_cost
            print(f"  Estimated scan cost: ~${adjusted_cost:.2f}")
        else:
            total_records += est_records
            total_cost += est_cost
            print(f"  Estimated records: ~{est_records:,}")
            print(f"  Estimated scan cost: ~${est_cost:.2f}")

    print(f"\n{'=' * 60}")
    print(f"TOTAL estimated records: {total_records:,}")
    print(f"TOTAL estimated cost:    ~${total_cost:.2f}")
    print(f"{'=' * 60}")
    print("\nNote: First 1TB/month of BigQuery scanning is free.")
    print("Run without --dry-run to start ingestion.")


# ---------------------------------------------------------------------------
# Main ingestion loop
# ---------------------------------------------------------------------------
def run_ingestion(
    db_path: str,
    country_codes: list[str],
    date_from: str | None = None,
    date_to: str | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    resume: bool = False,
    dry_run: bool = False,
) -> dict[str, int | float]:
    """Ingest global patent metadata from BigQuery.

    Returns dict with total_fetched, total_inserted, elapsed_seconds.
    """
    # Parse date filters
    date_from_int = _parse_date_to_int(date_from) if date_from else None
    date_to_int = _parse_date_to_int(date_to) if date_to else None

    # Connect to BigQuery
    source = BigQuerySource()

    # Dry run: show estimates and exit
    if dry_run:
        run_dry_run(source, country_codes, date_from_int, date_to_int)
        return {"total_fetched": 0, "total_inserted": 0, "elapsed_seconds": 0.0}

    # Open SQLite database
    db_path_obj = Path(db_path)
    db_path_obj.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path_obj), timeout=120)
    conn.executescript(SCHEMA_SQL)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-1024000")  # 1GB cache
    conn.execute("PRAGMA foreign_keys=OFF")

    # Try to load entity resolver for firm_id linking
    normalizer = _try_load_entity_resolver()
    if normalizer:
        print("Entity resolver loaded: firm_id linking enabled")
    else:
        print("Entity resolver not available: skipping firm_id linking")
    sys.stdout.flush()

    start_time = time.time()
    grand_total_fetched = 0
    grand_total_inserted = 0

    for cc in country_codes:
        cc_start_time = time.time()
        cc_fetched = 0
        cc_inserted = 0

        # Generate a batch_id for ingestion logging
        batch_id = f"global_{cc}_{uuid.uuid4().hex[:8]}"

        # Log ingestion start
        try:
            conn.execute(
                """INSERT INTO ingestion_log
                   (batch_id, source, country_code, status)
                   VALUES (?, ?, ?, 'running')""",
                (batch_id, "bigquery_global", cc),
            )
            conn.commit()
        except Exception:
            pass

        # Determine effective date range
        effective_from = date_from_int
        if resume:
            resume_date = _get_resume_date(conn, cc)
            if resume_date:
                # Start from the day after the last ingested date
                if effective_from is None or resume_date >= effective_from:
                    effective_from = resume_date
                    print(
                        f"Resuming {cc} from publication_date={resume_date}"
                    )
                    sys.stdout.flush()

        # Build year-based date ranges
        year_ranges = _build_year_ranges(effective_from, date_to_int)

        print(
            f"\n{'='*60}\n"
            f"Ingesting {cc} patents: {len(year_ranges)} year-batches, "
            f"batch_size={batch_size:,}\n"
            f"{'='*60}"
        )
        sys.stdout.flush()

        for range_idx, (date_lo, date_hi) in enumerate(year_ranges, 1):
            range_start = time.time()
            year_label = f"{date_lo // 10000}"

            print(
                f"\n--- [{range_idx}/{len(year_ranges)}] {cc} "
                f"year={year_label} ({date_lo}-{date_hi}) ---"
            )
            sys.stdout.flush()

            try:
                from google.cloud import bigquery

                sql, params = _build_metadata_query(cc, date_lo, date_hi)
                job_config = bigquery.QueryJobConfig(query_parameters=params)
                result_iter = source.client.query(
                    sql, job_config=job_config
                ).result()
            except Exception as e:
                print(f"  ERROR querying {cc} year={year_label}: {e}")
                sys.stdout.flush()
                continue

            # Process rows in memory batches
            batch_buffer: list[dict] = []
            range_fetched = 0
            range_inserted = 0
            last_pub_date = None

            for row in result_iter:
                row_dict = dict(row)
                range_fetched += 1

                # Normalize row to UnifiedPatent dict
                patent = normalize_bigquery_row(row_dict)

                # Track publication_date for resume support
                pub_date = patent.get("publication_date")
                if pub_date:
                    last_pub_date = pub_date

                batch_buffer.append(patent)

                # Flush batch when buffer is full
                if len(batch_buffer) >= batch_size:
                    # Entity resolution on batch
                    batch_buffer = _resolve_assignees(normalizer, batch_buffer)

                    inserted = _upsert_batch_raw(conn, batch_buffer)
                    conn.commit()
                    range_inserted += inserted
                    batch_buffer = []

                    # WAL checkpoint to keep WAL file manageable
                    conn.execute("PRAGMA wal_checkpoint(PASSIVE)")

                # Progress logging
                if range_fetched % PROGRESS_INTERVAL == 0:
                    elapsed = time.time() - range_start
                    rate = range_fetched / elapsed if elapsed > 0 else 0
                    print(
                        f"  {cc} year={year_label}: "
                        f"fetched={range_fetched:,}, "
                        f"inserted={range_inserted:,}, "
                        f"rate={rate:,.0f}/s"
                    )
                    sys.stdout.flush()

            # Flush remaining records in buffer
            if batch_buffer:
                batch_buffer = _resolve_assignees(normalizer, batch_buffer)
                inserted = _upsert_batch_raw(conn, batch_buffer)
                conn.commit()
                range_inserted += inserted

                # WAL checkpoint after final batch
                conn.execute("PRAGMA wal_checkpoint(PASSIVE)")

            range_elapsed = time.time() - range_start
            cc_fetched += range_fetched
            cc_inserted += range_inserted

            # Update ingestion log with progress
            try:
                conn.execute(
                    """UPDATE ingestion_log
                       SET records_fetched = ?,
                           last_publication_date = ?
                       WHERE batch_id = ?""",
                    (cc_fetched, last_pub_date, batch_id),
                )
                conn.commit()
            except Exception:
                pass

            if range_fetched > 0:
                rate = range_fetched / range_elapsed if range_elapsed > 0 else 0
                print(
                    f"  Completed: fetched={range_fetched:,}, "
                    f"inserted={range_inserted:,}, "
                    f"time={range_elapsed:.1f}s, rate={rate:,.0f}/s"
                )
            else:
                print(f"  (empty) time={range_elapsed:.1f}s")
            sys.stdout.flush()

        # Country summary
        cc_elapsed = time.time() - cc_start_time
        grand_total_fetched += cc_fetched
        grand_total_inserted += cc_inserted

        # Mark ingestion complete in log
        try:
            conn.execute(
                """UPDATE ingestion_log
                   SET completed_at = datetime('now'),
                       records_inserted = ?,
                       status = 'completed'
                   WHERE batch_id = ?""",
                (cc_inserted, batch_id),
            )
            conn.commit()
        except Exception:
            pass

        print(
            f"\n--- {cc} Summary ---\n"
            f"  Fetched:  {cc_fetched:,}\n"
            f"  Inserted: {cc_inserted:,}\n"
            f"  Elapsed:  {cc_elapsed:.1f}s ({cc_elapsed / 60:.1f}m)\n"
            f"  Rate:     {cc_fetched / cc_elapsed:,.0f}/s"
            if cc_elapsed > 0
            else f"\n--- {cc} Summary ---\n"
            f"  Fetched:  {cc_fetched:,}\n"
            f"  Inserted: {cc_inserted:,}\n"
            f"  Elapsed:  0.0s"
        )
        sys.stdout.flush()

    # Final cleanup
    conn.execute("PRAGMA foreign_keys=ON")
    conn.commit()

    # Final WAL checkpoint (full)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    total_elapsed = time.time() - start_time

    # DB total counts
    for cc in country_codes:
        row = conn.execute(
            "SELECT COUNT(*) FROM patents WHERE country_code = ?", (cc,)
        ).fetchone()
        print(f"  DB total {cc}: {row[0]:,}")

    conn.close()

    print(f"\n{'='*60}")
    print(f"Global Ingestion Complete")
    print(f"{'='*60}")
    print(f"Countries:     {', '.join(country_codes)}")
    print(f"Total fetched: {grand_total_fetched:,}")
    print(f"Total inserted:{grand_total_inserted:,}")
    print(f"Elapsed:       {total_elapsed:.1f}s ({total_elapsed / 60:.1f}m)")
    if total_elapsed > 0:
        print(f"Rate:          {grand_total_fetched / total_elapsed:,.0f}/s")
    sys.stdout.flush()

    return {
        "total_fetched": grand_total_fetched,
        "total_inserted": grand_total_inserted,
        "elapsed_seconds": total_elapsed,
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest global patent metadata from BigQuery "
            "(patents-public-data.patents.publications)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest US and EP patents from 2015 onwards:
  python scripts/ingest_global_patents.py --db data/patents.db \\
      --country-codes US,EP --date-from 2015-01-01

  # Dry run to see estimated costs:
  python scripts/ingest_global_patents.py --db data/patents.db \\
      --country-codes US,EP,WO,CN,KR --dry-run

  # Resume from last ingested date:
  python scripts/ingest_global_patents.py --db data/patents.db \\
      --country-codes US --resume

  # All default jurisdictions, all dates:
  python scripts/ingest_global_patents.py --db data/patents.db
        """,
    )
    parser.add_argument(
        "--db",
        default="data/patents.db",
        help="SQLite database path (default: data/patents.db)",
    )
    parser.add_argument(
        "--country-codes",
        default=DEFAULT_COUNTRY_CODES,
        help=(
            f"Comma-separated country codes "
            f"(default: {DEFAULT_COUNTRY_CODES})"
        ),
    )
    parser.add_argument(
        "--date-from",
        default=None,
        help="Start date in YYYY-MM-DD format (default: 1990-01-01)",
    )
    parser.add_argument(
        "--date-to",
        default=None,
        help="End date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Records per SQLite commit (default: {DEFAULT_BATCH_SIZE:,})",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Resume from last ingested publication_date for each country. "
            "Checks ingestion_log and patents table."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Show estimated record count and BQ scan cost without "
            "actually ingesting data."
        ),
    )
    args = parser.parse_args()

    # Parse country codes
    codes = [c.strip().upper() for c in args.country_codes.split(",") if c.strip()]
    if not codes:
        parser.error("No country codes specified")

    # Validate country codes
    valid_codes = {"US", "EP", "WO", "CN", "KR", "DE", "GB", "FR", "CA", "AU", "IN"}
    for cc in codes:
        if cc not in valid_codes:
            print(
                f"Warning: '{cc}' is not in the standard set "
                f"({', '.join(sorted(valid_codes))}). Proceeding anyway."
            )

    run_ingestion(
        db_path=args.db,
        country_codes=codes,
        date_from=args.date_from,
        date_to=args.date_to,
        batch_size=args.batch_size,
        resume=args.resume,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
