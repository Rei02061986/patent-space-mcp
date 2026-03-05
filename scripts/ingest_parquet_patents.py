#!/usr/bin/env python3
"""Ingest global patent data from Parquet files exported by bq extract.

Usage:
    python scripts/ingest_parquet_patents.py \
        --parquet-dir ~/exports/patents/ \
        --db data/patents.db \
        [--skip-existing] [--batch-size 10000]

This script reads Parquet files from bq extract of
patents-public-data:patents.publications and inserts them into the
local SQLite database, deduplicating by publication_number.

Performance-optimized for ~130M patents:
  - executemany bulk inserts
  - Indexes deferred until after ingestion
  - WAL mode with large page cache
  - mmap I/O for large DB
"""
from __future__ import annotations

import argparse
import glob
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

CREATE_TABLES_SQL = """
    -- Match MCP migrations.py schema exactly
    CREATE TABLE IF NOT EXISTS patents (
        publication_number TEXT PRIMARY KEY,
        application_number TEXT,
        family_id TEXT,
        country_code TEXT NOT NULL,
        kind_code TEXT,
        title_ja TEXT,
        title_en TEXT,
        abstract_ja TEXT,
        abstract_en TEXT,
        filing_date INTEGER,
        publication_date INTEGER,
        grant_date INTEGER,
        entity_status TEXT,
        citation_count_forward INTEGER DEFAULT 0,
        source TEXT DEFAULT 'bigquery',
        ingested_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS patent_cpc (
        publication_number TEXT NOT NULL,
        cpc_code TEXT NOT NULL,
        is_inventive INTEGER DEFAULT 0,
        is_first INTEGER DEFAULT 0,
        PRIMARY KEY (publication_number, cpc_code)
    );

    CREATE TABLE IF NOT EXISTS patent_assignees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        publication_number TEXT NOT NULL,
        raw_name TEXT NOT NULL,
        harmonized_name TEXT,
        country_code TEXT,
        firm_id TEXT
    );

    CREATE TABLE IF NOT EXISTS patent_inventors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        publication_number TEXT NOT NULL,
        name TEXT NOT NULL,
        country_code TEXT
    );

    CREATE TABLE IF NOT EXISTS patent_citations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        citing_publication TEXT NOT NULL,
        cited_publication TEXT NOT NULL,
        citation_type TEXT
    );
"""

CREATE_INDEXES_SQL = """
    CREATE INDEX IF NOT EXISTS idx_patents_family ON patents(family_id);
    CREATE INDEX IF NOT EXISTS idx_patents_country ON patents(country_code);
    CREATE INDEX IF NOT EXISTS idx_patents_filing_date ON patents(filing_date);
    CREATE INDEX IF NOT EXISTS idx_patents_pub_date ON patents(publication_date);
    CREATE INDEX IF NOT EXISTS idx_cpc_code ON patent_cpc(cpc_code);
    CREATE INDEX IF NOT EXISTS idx_cpc_class ON patent_cpc(substr(cpc_code, 1, 4));
    CREATE INDEX IF NOT EXISTS idx_assignee_pub ON patent_assignees(publication_number);
    CREATE INDEX IF NOT EXISTS idx_assignee_harmonized ON patent_assignees(harmonized_name);
    CREATE INDEX IF NOT EXISTS idx_assignee_firm ON patent_assignees(firm_id);
    CREATE INDEX IF NOT EXISTS idx_inventor_pub ON patent_inventors(publication_number);
    CREATE INDEX IF NOT EXISTS idx_citation_citing ON patent_citations(citing_publication);
    CREATE INDEX IF NOT EXISTS idx_citation_cited ON patent_citations(cited_publication);
"""


def _ensure_tables(conn: sqlite3.Connection) -> None:
    """Create tables (without indexes for fast bulk insert)."""
    conn.executescript(CREATE_TABLES_SQL)
    conn.commit()


def _create_indexes(conn: sqlite3.Connection) -> None:
    """Create indexes after bulk ingestion."""
    log.info("Creating indexes...")
    t0 = time.time()
    conn.executescript(CREATE_INDEXES_SQL)
    conn.commit()
    log.info("Indexes created in %.0f seconds", time.time() - t0)


# ---------------------------------------------------------------------------
# Parquet row extraction — optimized
# ---------------------------------------------------------------------------

def _extract_localized(field, lang: str) -> str | None:
    """Extract text for a given language from a localized repeated field."""
    if field is None:
        return None
    for item in field:
        if item and isinstance(item, dict) and item.get("language") == lang:
            return item.get("text")
    return None


def _date_to_int(date_val) -> int | None:
    """Convert date value to YYYYMMDD int."""
    if date_val is None:
        return None
    if hasattr(date_val, 'year'):
        return date_val.year * 10000 + date_val.month * 100 + date_val.day
    if isinstance(date_val, (int, float)):
        val = int(date_val)
        if val == 0:
            return None
        # BigQuery exports dates as YYYYMMDD integers (e.g., 20231201, 18491106)
        # Values >= 10000000 are 8-digit YYYYMMDD (covers dates from 1000 AD)
        if val >= 10000000:
            return val
    return None


# ---------------------------------------------------------------------------
# Batch insert — bulk executemany
# ---------------------------------------------------------------------------

def _insert_batch(
    conn: sqlite3.Connection,
    patents: list[dict],
) -> int:
    """Insert a batch of patents using executemany. Returns inserted count."""
    if not patents:
        return 0

    # Collect all rows for bulk insert
    patent_rows = []
    cpc_rows = []
    assignee_rows = []
    inventor_rows = []
    citation_rows = []

    for p in patents:
        pub = p["publication_number"]
        if not pub:
            continue

        patent_rows.append((
            pub,
            p.get("application_number"),
            p.get("family_id"),
            p.get("country_code"),
            p.get("kind_code"),
            p.get("title_ja"),
            p.get("title_en"),
            p.get("abstract_ja"),
            p.get("abstract_en"),
            p.get("filing_date"),
            p.get("publication_date"),
            p.get("grant_date"),
            p.get("entity_status"),
            "bigquery",
        ))

        # CPC codes
        seen_cpc = set()
        for cpc_item in (p.get("cpc_codes") or []):
            code = cpc_item.get("code") or ""
            if code and code not in seen_cpc:
                seen_cpc.add(code)
                cpc_rows.append((
                    pub, code,
                    int(cpc_item.get("inventive", False)),
                    int(cpc_item.get("first", False)),
                ))

        # Assignees
        for a in (p.get("assignees") or []):
            name = a.get("raw_name") or a.get("name") or ""
            if name:
                assignee_rows.append((
                    pub, name, a.get("harmonized_name") or name,
                    a.get("country_code") or "",
                ))

        # Inventors
        for inv in (p.get("inventors") or []):
            name = inv.get("name") or ""
            if name:
                inventor_rows.append((pub, name, inv.get("country_code") or ""))

        # Citations
        for cited in (p.get("citations") or []):
            if cited:
                citation_rows.append((pub, cited, "patent"))

    # Bulk inserts — count before/after for accurate inserted count
    before = conn.execute("SELECT total_changes()").fetchone()[0]
    conn.executemany(
        """INSERT OR IGNORE INTO patents
           (publication_number, application_number, family_id,
            country_code, kind_code, title_ja, title_en,
            abstract_ja, abstract_en, filing_date, publication_date,
            grant_date, entity_status, source)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        patent_rows,
    )
    after = conn.execute("SELECT total_changes()").fetchone()[0]
    inserted = after - before

    if cpc_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO patent_cpc (publication_number, cpc_code, is_inventive, is_first) VALUES (?,?,?,?)",
            cpc_rows,
        )
    if assignee_rows:
        conn.executemany(
            "INSERT INTO patent_assignees (publication_number, raw_name, harmonized_name, country_code) VALUES (?,?,?,?)",
            assignee_rows,
        )
    if inventor_rows:
        conn.executemany(
            "INSERT INTO patent_inventors (publication_number, name, country_code) VALUES (?,?,?)",
            inventor_rows,
        )
    if citation_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO patent_citations (citing_publication, cited_publication, citation_type) VALUES (?,?,?)",
            citation_rows,
        )

    return inserted


# ---------------------------------------------------------------------------
# Process single Parquet file
# ---------------------------------------------------------------------------

def _process_parquet_batch(table_dict: dict, n_rows: int) -> list[dict]:
    """Extract patent records from a pyarrow batch dict."""
    patents = []
    pub_nums = table_dict.get("publication_number", [])
    app_nums = table_dict.get("application_number", [])
    countries = table_dict.get("country_code", [])
    kind_codes = table_dict.get("kind_code", [])
    family_ids = table_dict.get("family_id", [])
    entity_statuses = table_dict.get("entity_status", [])
    filing_dates = table_dict.get("filing_date", [])
    pub_dates = table_dict.get("publication_date", [])
    grant_dates = table_dict.get("grant_date", [])
    titles = table_dict.get("title_localized", [])
    abstracts = table_dict.get("abstract_localized", [])
    cpcs = table_dict.get("cpc", [])
    assignees = table_dict.get("assignee_harmonized", [])
    inventors = table_dict.get("inventor_harmonized", [])
    citations = table_dict.get("citation", [])

    for i in range(n_rows):
        pub_num = pub_nums[i] if i < len(pub_nums) else None
        if not pub_num:
            continue

        country = countries[i] if i < len(countries) else None
        if not country and pub_num:
            parts = pub_num.split("-")
            if parts:
                country = parts[0]

        title_loc = titles[i] if i < len(titles) else None
        abstract_loc = abstracts[i] if i < len(abstracts) else None

        # Extract CPC inline
        cpc_field = cpcs[i] if i < len(cpcs) else None
        cpc_codes = []
        if cpc_field:
            seen = set()
            for item in cpc_field:
                if item and isinstance(item, dict):
                    code = item.get("code") or ""
                    if code and code not in seen:
                        seen.add(code)
                        cpc_codes.append({
                            "code": code,
                            "inventive": bool(item.get("inventive", False)),
                            "first": bool(item.get("first", False)),
                        })

        # Extract assignees inline
        asgn_field = assignees[i] if i < len(assignees) else None
        asgn_list = []
        if asgn_field:
            for item in asgn_field:
                if item and isinstance(item, dict):
                    name = item.get("name") or ""
                    if name:
                        asgn_list.append({
                            "raw_name": name,
                            "harmonized_name": name,
                            "country_code": item.get("country_code") or "",
                        })

        # Extract inventors inline
        inv_field = inventors[i] if i < len(inventors) else None
        inv_list = []
        if inv_field:
            for item in inv_field:
                if item and isinstance(item, dict):
                    name = item.get("name") or ""
                    if name:
                        inv_list.append({
                            "name": name,
                            "country_code": item.get("country_code") or "",
                        })

        # Extract citations inline
        cit_field = citations[i] if i < len(citations) else None
        cit_list = []
        if cit_field:
            for item in cit_field:
                if item and isinstance(item, dict):
                    pub = item.get("publication_number") or ""
                    if pub:
                        cit_list.append(pub)

        fid = family_ids[i] if i < len(family_ids) else None

        patents.append({
            "publication_number": pub_num,
            "application_number": app_nums[i] if i < len(app_nums) else None,
            "family_id": str(fid or ""),
            "country_code": country or "",
            "kind_code": (kind_codes[i] if i < len(kind_codes) else None) or "",
            "title_ja": _extract_localized(title_loc, "ja"),
            "title_en": _extract_localized(title_loc, "en"),
            "abstract_ja": _extract_localized(abstract_loc, "ja"),
            "abstract_en": _extract_localized(abstract_loc, "en"),
            "filing_date": _date_to_int(filing_dates[i] if i < len(filing_dates) else None),
            "publication_date": _date_to_int(pub_dates[i] if i < len(pub_dates) else None),
            "grant_date": _date_to_int(grant_dates[i] if i < len(grant_dates) else None),
            "entity_status": (entity_statuses[i] if i < len(entity_statuses) else None) or "",
            "cpc_codes": cpc_codes,
            "assignees": asgn_list,
            "inventors": inv_list,
            "citations": cit_list,
        })

    return patents


def process_parquet_file(
    filepath: str,
    conn: sqlite3.Connection,
    batch_size: int,
) -> tuple[int, int]:
    """Process one Parquet file. Returns (total_rows, inserted)."""
    pf = pq.ParquetFile(filepath)
    total_rows = pf.metadata.num_rows
    total_inserted = 0

    batch_buffer: list[dict] = []

    for batch in pf.iter_batches(batch_size=batch_size):
        table = batch.to_pydict()
        n_rows = len(table.get("publication_number", []))
        patents = _process_parquet_batch(table, n_rows)
        batch_buffer.extend(patents)

        if len(batch_buffer) >= batch_size:
            inserted = _insert_batch(conn, batch_buffer)
            total_inserted += inserted
            conn.commit()
            batch_buffer.clear()

    # Flush remaining
    if batch_buffer:
        inserted = _insert_batch(conn, batch_buffer)
        total_inserted += inserted
        conn.commit()

    return total_rows, total_inserted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest global patents from Parquet files into SQLite."
    )
    parser.add_argument(
        "--parquet-dir", required=True,
        help="Directory containing publications-*.parquet files",
    )
    parser.add_argument(
        "--db", default="data/patents.db",
        help="Path to SQLite database (default: data/patents.db)",
    )
    parser.add_argument(
        "--skip-existing", action="store_true",
        help="Skip patents that already exist (by publication_number)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=10000,
        help="Batch size for inserts (default: 10000)",
    )
    parser.add_argument(
        "--limit-files", type=int, default=0,
        help="Process only first N files (0 = all)",
    )
    parser.add_argument(
        "--start-from", type=int, default=0,
        help="Start from file index N (for resuming)",
    )
    parser.add_argument(
        "--no-indexes", action="store_true",
        help="Skip index creation (create them later for faster bulk insert)",
    )
    args = parser.parse_args()

    parquet_files = sorted(glob.glob(os.path.join(args.parquet_dir, "publications-*.parquet")))
    if not parquet_files:
        log.error("No publications-*.parquet files found in %s", args.parquet_dir)
        sys.exit(1)

    if args.start_from > 0:
        parquet_files = parquet_files[args.start_from:]
        log.info("Resuming from file index %d", args.start_from)

    if args.limit_files > 0:
        parquet_files = parquet_files[:args.limit_files]

    log.info("=== Parquet → SQLite Patent Ingestion ===")
    log.info("Parquet dir: %s", args.parquet_dir)
    log.info("Database: %s", args.db)
    log.info("Files to process: %d", len(parquet_files))
    log.info("Batch size: %d", args.batch_size)

    # Ensure parent dir exists
    os.makedirs(os.path.dirname(args.db) or ".", exist_ok=True)

    conn = sqlite3.connect(args.db, timeout=300)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-4000000")  # 4GB page cache
    conn.execute("PRAGMA mmap_size=8589934592")  # 8GB mmap
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA page_size=32768")  # 32KB pages

    _ensure_tables(conn)

    # Get existing count before
    before_count = conn.execute("SELECT COUNT(*) FROM patents").fetchone()[0]
    log.info("Existing patents in DB: %d", before_count)

    t0 = time.time()
    grand_total = 0
    grand_inserted = 0

    for idx, filepath in enumerate(parquet_files, 1):
        fname = os.path.basename(filepath)
        fsize_mb = os.path.getsize(filepath) / (1024**2)
        file_t0 = time.time()

        try:
            total, inserted = process_parquet_file(filepath, conn, args.batch_size)
        except Exception as e:
            log.error("  ERROR processing %s: %s", fname, e)
            continue

        file_elapsed = time.time() - file_t0
        grand_total += total
        grand_inserted += inserted

        rate = total / max(file_elapsed, 0.01)
        elapsed_total = time.time() - t0
        files_remaining = len(parquet_files) - idx
        avg_per_file = elapsed_total / idx
        eta_seconds = files_remaining * avg_per_file

        if idx % 10 == 0 or idx <= 5:
            log.info(
                "[%d/%d] %s: %d rows, %d new in %.1fs (%.0f r/s) | Total: %d inserted | ETA: %.0fm",
                idx, len(parquet_files), fname, total, inserted,
                file_elapsed, rate, grand_inserted,
                eta_seconds / 60,
            )

        # WAL checkpoint every 50 files
        if idx % 50 == 0:
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")

    # Final checkpoint
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    # Create indexes unless --no-indexes
    if not args.no_indexes:
        _create_indexes(conn)

    after_count = conn.execute("SELECT COUNT(*) FROM patents").fetchone()[0]
    elapsed = time.time() - t0

    conn.close()

    # Cleanup test db if exists
    test_db = os.path.join(os.path.dirname(args.db) or ".", "test.db")
    if os.path.exists(test_db):
        os.remove(test_db)

    log.info("")
    log.info("=== Ingestion Complete ===")
    log.info("Elapsed: %.0f seconds (%.1f minutes / %.1f hours)", elapsed, elapsed / 60, elapsed / 3600)
    log.info("Files processed: %d", len(parquet_files))
    log.info("Total rows in Parquet: %d", grand_total)
    log.info("Inserted: %d", grand_inserted)
    log.info("DB before: %d patents", before_count)
    log.info("DB after: %d patents", after_count)
    log.info("Net new: %d patents", after_count - before_count)
    if elapsed > 0:
        log.info("Avg rate: %.0f rows/sec", grand_total / elapsed)


if __name__ == "__main__":
    main()
