#!/usr/bin/env python3
"""Ingest PTAB trial data and litigation cases from Parquet into SQLite.

Reads two Parquet files exported from BigQuery:
  - PTAB trials (TrialNumber, PatentNumber, etc.)
  - Litigation cases (case_number, court, plaintiff, defendant, etc.)

Creates three tables: ptab_trials, litigation_cases, litigation_patents.
The litigation_patents link table normalises the patent_numbers array column
so that each patent mentioned in a case gets its own row.

Usage:
    python ingest_ptab.py \\
        --trials /mnt/nvme/patent-exports-ptab/trials-000000000000.parquet \\
        --litigation /mnt/nvme/patent-exports-ptab/litigation-cases-000000000000.parquet \\
        --db /app/data/patents.db
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq

BATCH_SIZE = 5_000

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_PTAB_SCHEMA = """
CREATE TABLE IF NOT EXISTS ptab_trials (
    trial_number TEXT PRIMARY KEY,
    patent_number TEXT,
    publication_number TEXT,
    filing_date TEXT,
    institution_decision_date TEXT,
    prosecution_status TEXT,
    accorded_filing_date TEXT,
    petitioner TEXT,
    patent_owner TEXT,
    inventor_name TEXT,
    application_number TEXT
);
"""

_PTAB_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_ptab_patent ON ptab_trials(patent_number);",
    "CREATE INDEX IF NOT EXISTS idx_ptab_pub ON ptab_trials(publication_number);",
    "CREATE INDEX IF NOT EXISTS idx_ptab_petitioner ON ptab_trials(petitioner);",
    "CREATE INDEX IF NOT EXISTS idx_ptab_owner ON ptab_trials(patent_owner);",
    "CREATE INDEX IF NOT EXISTS idx_ptab_status ON ptab_trials(prosecution_status);",
]

_LITIGATION_SCHEMA = """
CREATE TABLE IF NOT EXISTS litigation_cases (
    case_id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_number TEXT,
    court TEXT,
    judge TEXT,
    date_filed TEXT,
    date_terminated TEXT,
    plaintiff TEXT,
    defendant TEXT,
    nature_of_suit TEXT,
    outcome TEXT
);
"""

_LITIGATION_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_lit_number ON litigation_cases(case_number);",
    "CREATE INDEX IF NOT EXISTS idx_lit_plaintiff ON litigation_cases(plaintiff);",
    "CREATE INDEX IF NOT EXISTS idx_lit_defendant ON litigation_cases(defendant);",
    "CREATE INDEX IF NOT EXISTS idx_lit_date ON litigation_cases(date_filed);",
]

_LITIGATION_PATENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS litigation_patents (
    case_id INTEGER,
    patent_number TEXT,
    PRIMARY KEY (case_id, patent_number)
);
"""

_LITIGATION_PATENTS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_litpat_patent ON litigation_patents(patent_number);",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _val(table, col_name: str, row_idx: int):
    """Extract a scalar Python value from a PyArrow table cell, returning None
    for missing columns or null values."""
    try:
        cell = table.column(col_name)[row_idx]
    except KeyError:
        return None
    if not cell.is_valid:
        return None
    return cell.as_py()


def _normalise_patent_number(raw: str | None) -> str | None:
    """Minimal normalisation: strip whitespace, upper-case."""
    if not raw:
        return None
    return raw.strip().upper()


def _extract_patent_numbers(raw) -> list[str]:
    """Parse patent_numbers field which may be a list, JSON array, or
    comma / semicolon-separated string.  Returns a deduplicated list."""
    if raw is None:
        return []
    if isinstance(raw, list):
        nums = [str(x).strip() for x in raw if x]
    elif isinstance(raw, str):
        # Try JSON parse first
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                nums = [str(x).strip() for x in parsed if x]
            else:
                nums = [str(parsed).strip()]
        except (json.JSONDecodeError, ValueError):
            # Comma or semicolon separated
            nums = re.split(r"[,;\|]+", raw)
            nums = [n.strip() for n in nums if n.strip()]
    else:
        nums = [str(raw).strip()]

    # Normalise and deduplicate preserving order
    seen: set[str] = set()
    result: list[str] = []
    for n in nums:
        normed = n.upper()
        if normed and normed not in seen:
            seen.add(normed)
            result.append(normed)
    return result


# ---------------------------------------------------------------------------
# Ingestion routines
# ---------------------------------------------------------------------------

def _create_schema(conn: sqlite3.Connection) -> None:
    """Create all tables and indexes if they do not already exist."""
    conn.execute(_PTAB_SCHEMA)
    for idx in _PTAB_INDEXES:
        conn.execute(idx)
    conn.execute(_LITIGATION_SCHEMA)
    for idx in _LITIGATION_INDEXES:
        conn.execute(idx)
    conn.execute(_LITIGATION_PATENTS_SCHEMA)
    for idx in _LITIGATION_PATENTS_INDEXES:
        conn.execute(idx)
    conn.commit()
    print("[schema] Tables and indexes created / verified.")


def _ingest_ptab_trials(conn: sqlite3.Connection, parquet_path: str) -> int:
    """Ingest PTAB trials from a single Parquet file.  Returns row count."""
    path = Path(parquet_path)
    if not path.exists():
        print(f"[ptab] File not found: {parquet_path}")
        return 0

    table = pq.read_table(path)
    num_rows = table.num_rows
    print(f"[ptab] Reading {num_rows:,} rows from {path.name} ...")

    inserted = 0
    batch: list[tuple] = []

    for i in range(num_rows):
        trial_number = _val(table, "TrialNumber", i)
        if not trial_number:
            # TrialNumber is the primary key; skip rows without it
            continue

        patent_number = _normalise_patent_number(_val(table, "PatentNumber", i))
        publication_number = _val(table, "publication_number", i)
        filing_date = _val(table, "FilingDate", i)
        institution_date = _val(table, "InstitutionDecisionDate", i)
        status = _val(table, "ProsecutionStatus", i)
        accorded_date = _val(table, "AccordedFilingDate", i)
        petitioner = _val(table, "PetitionerPartyName", i)
        patent_owner = _val(table, "PatentOwnerName", i)
        inventor = _val(table, "InventorName", i)
        app_number = _val(table, "ApplicationNumber", i)

        # Convert date-like objects to ISO string
        for date_val_name in ("filing_date", "institution_date",
                              "accorded_date"):
            v = locals()[date_val_name]
            if v is not None and not isinstance(v, str):
                locals()[date_val_name] = str(v)
        filing_date = str(filing_date) if filing_date and not isinstance(filing_date, str) else filing_date
        institution_date = str(institution_date) if institution_date and not isinstance(institution_date, str) else institution_date
        accorded_date = str(accorded_date) if accorded_date and not isinstance(accorded_date, str) else accorded_date

        batch.append((
            trial_number,
            patent_number,
            publication_number,
            filing_date,
            institution_date,
            status,
            accorded_date,
            petitioner,
            patent_owner,
            inventor,
            app_number,
        ))

        if len(batch) >= BATCH_SIZE:
            conn.executemany(
                "INSERT OR REPLACE INTO ptab_trials "
                "(trial_number, patent_number, publication_number, "
                "filing_date, institution_decision_date, prosecution_status, "
                "accorded_filing_date, petitioner, patent_owner, "
                "inventor_name, application_number) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch,
            )
            conn.commit()
            inserted += len(batch)
            print(f"  [ptab] {inserted:,} / {num_rows:,} inserted ...")
            batch.clear()

    # Final flush
    if batch:
        conn.executemany(
            "INSERT OR REPLACE INTO ptab_trials "
            "(trial_number, patent_number, publication_number, "
            "filing_date, institution_decision_date, prosecution_status, "
            "accorded_filing_date, petitioner, patent_owner, "
            "inventor_name, application_number) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        conn.commit()
        inserted += len(batch)

    print(f"[ptab] Done. {inserted:,} trials ingested.")
    return inserted


def _ingest_litigation(conn: sqlite3.Connection, parquet_path: str) -> int:
    """Ingest litigation cases and build the litigation_patents link table.
    Returns the number of case rows inserted."""
    path = Path(parquet_path)
    if not path.exists():
        print(f"[litigation] File not found: {parquet_path}")
        return 0

    table = pq.read_table(path)
    num_rows = table.num_rows
    print(f"[litigation] Reading {num_rows:,} rows from {path.name} ...")

    # Pre-clear existing data to allow re-runs (idempotent)
    conn.execute("DELETE FROM litigation_patents")
    conn.execute("DELETE FROM litigation_cases")
    conn.commit()
    print("[litigation] Cleared existing data for clean re-ingest.")

    inserted = 0
    patent_links = 0
    case_batch: list[tuple] = []
    link_batch: list[tuple] = []

    # We need to track auto-increment IDs.  Since we cleared the table,
    # we can rely on rowid == autoincrement starting from 1.
    # However, to be safe across partial runs, we use lastrowid.
    for i in range(num_rows):
        case_number = _val(table, "case_number", i)
        court = _val(table, "court", i)
        judge = _val(table, "judge", i)
        date_filed = _val(table, "date_filed", i)
        date_terminated = _val(table, "date_terminated", i)
        plaintiff = _val(table, "plaintiff", i)
        defendant = _val(table, "defendant", i)
        nature_of_suit = _val(table, "nature_of_suit", i)
        outcome = _val(table, "outcome", i)
        patent_numbers_raw = _val(table, "patent_numbers", i)

        # Convert dates to string if needed
        if date_filed and not isinstance(date_filed, str):
            date_filed = str(date_filed)
        if date_terminated and not isinstance(date_terminated, str):
            date_terminated = str(date_terminated)

        # Insert the case row individually so we can capture lastrowid
        cursor = conn.execute(
            "INSERT INTO litigation_cases "
            "(case_number, court, judge, date_filed, date_terminated, "
            "plaintiff, defendant, nature_of_suit, outcome) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (case_number, court, judge, date_filed, date_terminated,
             plaintiff, defendant, nature_of_suit, outcome),
        )
        case_id = cursor.lastrowid
        inserted += 1

        # Parse patent numbers and create link rows
        patents = _extract_patent_numbers(patent_numbers_raw)
        for pn in patents:
            link_batch.append((case_id, pn))
            patent_links += 1

        # Batch commit periodically
        if inserted % BATCH_SIZE == 0:
            if link_batch:
                conn.executemany(
                    "INSERT OR IGNORE INTO litigation_patents "
                    "(case_id, patent_number) VALUES (?, ?)",
                    link_batch,
                )
                link_batch.clear()
            conn.commit()
            print(f"  [litigation] {inserted:,} / {num_rows:,} cases, "
                  f"{patent_links:,} patent links ...")

    # Final flush
    if link_batch:
        conn.executemany(
            "INSERT OR IGNORE INTO litigation_patents "
            "(case_id, patent_number) VALUES (?, ?)",
            link_batch,
        )
    conn.commit()

    print(f"[litigation] Done. {inserted:,} cases, "
          f"{patent_links:,} patent-case links.")
    return inserted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Ingest PTAB trials and litigation cases into SQLite.",
    )
    parser.add_argument(
        "--trials",
        default="/mnt/nvme/patent-exports-ptab/trials-000000000000.parquet",
        help="Path to PTAB trials Parquet file",
    )
    parser.add_argument(
        "--litigation",
        default="/mnt/nvme/patent-exports-ptab/litigation-cases-000000000000.parquet",
        help="Path to litigation cases Parquet file",
    )
    parser.add_argument(
        "--db",
        default="/app/data/patents.db",
        help="Path to SQLite database (default: /app/data/patents.db)",
    )
    parser.add_argument(
        "--skip-trials",
        action="store_true",
        help="Skip PTAB trials ingestion",
    )
    parser.add_argument(
        "--skip-litigation",
        action="store_true",
        help="Skip litigation cases ingestion",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.parent.exists():
        print(f"Error: parent directory {db_path.parent} does not exist.")
        sys.exit(1)

    print(f"Database: {db_path}")
    print(f"Trials:   {args.trials}")
    print(f"Litigation: {args.litigation}")
    print()

    t0 = time.time()

    # Use isolation_level=None for WAL-safe manual transaction control
    conn = sqlite3.connect(str(db_path), timeout=300, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-500000")  # ~500 MB page cache
    conn.execute("PRAGMA temp_store=MEMORY")

    _create_schema(conn)

    # Re-enable auto-commit grouping via BEGIN/COMMIT
    conn.execute("BEGIN")

    ptab_count = 0
    lit_count = 0

    if not args.skip_trials:
        ptab_count = _ingest_ptab_trials(conn, args.trials)

    conn.execute("BEGIN")

    if not args.skip_litigation:
        lit_count = _ingest_litigation(conn, args.litigation)

    elapsed = time.time() - t0
    print()
    print("=" * 60)
    print(f"Ingestion complete in {elapsed:.1f}s")
    print(f"  PTAB trials:      {ptab_count:>10,}")
    print(f"  Litigation cases: {lit_count:>10,}")
    print("=" * 60)

    # Verify
    conn2 = sqlite3.connect(str(db_path), timeout=30)
    for tbl in ("ptab_trials", "litigation_cases", "litigation_patents"):
        row = conn2.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()
        print(f"  {tbl}: {row[0]:,} rows")
    conn2.close()

    conn.close()


if __name__ == "__main__":
    main()
