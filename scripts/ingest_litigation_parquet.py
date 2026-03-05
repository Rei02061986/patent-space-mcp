#!/usr/bin/env python3
"""Ingest litigation data from Parquet into SQLite."""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq

BATCH_SIZE = 10_000


def ingest_parquet_dir(parquet_dir: str, db_path: str):
    parquet_path = Path(parquet_dir)
    files = sorted(parquet_path.glob("*.parquet"))
    if not files:
        print(f"No .parquet files found in {parquet_dir}")
        return

    print(f"Found {len(files)} Parquet files")

    conn = sqlite3.connect(db_path, timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS patent_litigation (
            case_id TEXT PRIMARY KEY,
            patent_number TEXT,
            plaintiff TEXT,
            defendant TEXT,
            filing_date TEXT,
            court TEXT,
            outcome TEXT,
            damages_amount REAL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pl_patent ON patent_litigation(patent_number)")
    conn.commit()

    total = 0
    for pf in files:
        table = pq.read_table(pf)
        cols = table.column_names
        for i in range(table.num_rows):
            row = {c: table.column(c)[i].as_py() if table.column(c)[i].is_valid else None for c in cols}
            case_id = row.get("case_id") or row.get("id") or f"case_{total}"
            conn.execute(
                """INSERT OR REPLACE INTO patent_litigation
                   (case_id, patent_number, plaintiff, defendant, filing_date, court, outcome, damages_amount)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (case_id,
                 row.get("patent_number") or row.get("patent_id"),
                 row.get("plaintiff") or row.get("plaintiff_name"),
                 row.get("defendant") or row.get("defendant_name"),
                 row.get("filing_date"),
                 row.get("court") or row.get("court_name"),
                 row.get("outcome") or row.get("disposition"),
                 row.get("damages_amount") or row.get("damages")),
            )
            total += 1
        conn.commit()
        print(f"  {pf.name}: {table.num_rows} rows")

    conn.close()
    print(f"Total litigation records: {total:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet-dir", required=True)
    parser.add_argument("--db", required=True)
    args = parser.parse_args()
    ingest_parquet_dir(args.parquet_dir, args.db)
