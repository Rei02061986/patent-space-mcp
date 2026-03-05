"""Compute patent family sizes from patents.family_id.

Groups patents by family_id and records the count for each member patent.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.migrations import SCHEMA_SQL


def run_compute(db_path: str = "data/patents.db") -> int:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.executescript(SCHEMA_SQL)
    conn.execute("PRAGMA journal_mode=WAL")

    t0 = time.time()
    print("Computing patent family sizes ...")
    sys.stdout.flush()

    # Clear existing
    conn.execute("DELETE FROM patent_family")
    conn.commit()

    # Single INSERT ... SELECT to compute and insert all at once
    cursor = conn.execute(
        """
        INSERT INTO patent_family (publication_number, family_id, family_size)
        SELECT p.publication_number, p.family_id, fs.cnt
        FROM patents p
        JOIN (
            SELECT family_id, COUNT(*) AS cnt
            FROM patents
            WHERE family_id IS NOT NULL AND family_id != ''
            GROUP BY family_id
        ) fs ON fs.family_id = p.family_id
        WHERE p.family_id IS NOT NULL AND p.family_id != ''
        """
    )
    inserted = cursor.rowcount
    conn.commit()

    conn.close()
    elapsed = time.time() - t0
    print(f"\nDone: {inserted:,} rows in {elapsed:.1f}s ({elapsed / 60:.1f}m)")
    sys.stdout.flush()
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute patent family sizes."
    )
    parser.add_argument("--db", default="data/patents.db", help="SQLite DB path")
    args = parser.parse_args()

    count = run_compute(db_path=args.db)
    print(f"Family rows: {count}")

    # Print stats
    conn = sqlite3.connect(args.db, timeout=60)
    stats = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            AVG(family_size) AS avg_size,
            MAX(family_size) AS max_size,
            COUNT(CASE WHEN family_size = 1 THEN 1 END) AS singletons,
            COUNT(CASE WHEN family_size > 10 THEN 1 END) AS large_families
        FROM patent_family
        """
    ).fetchone()
    conn.close()
    print(f"\nStats:")
    print(f"  Total: {stats[0]:,}")
    print(f"  Avg family size: {stats[1]:.1f}")
    print(f"  Max family size: {stats[2]:,}")
    print(f"  Singletons (size=1): {stats[3]:,}")
    print(f"  Large families (size>10): {stats[4]:,}")


if __name__ == "__main__":
    main()
