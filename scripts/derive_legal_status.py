"""Derive patent legal status from existing patents table data.

Uses kind_code + filing_date + grant_date to approximate
legal status without requiring EPO OPS API access.

JP kind_code rules:
  - B1, B2, B6 = Patent grant → filing_date + 20yr term
  - S, S3 = Utility model registration → filing_date + 10yr term
  - A, A1, A5 = Patent application (unexamined) → 'pending' if recent, else 'abandoned'
  - U = Utility model application → 'pending' if recent, else 'abandoned'
  - Y2 = Utility model gazette → same as S

For non-JP (future):
  - entity_status == 'GRANT' → alive/expired
  - entity_status == 'APPLICATION' → pending
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.migrations import SCHEMA_SQL


def run_derive(db_path: str = "data/patents.db", batch_size: int = 50000) -> int:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.executescript(SCHEMA_SQL)
    conn.execute("PRAGMA journal_mode=WAL")

    # Current date as YYYYMMDD integer
    import datetime

    today_int = int(datetime.date.today().strftime("%Y%m%d"))

    t0 = time.time()
    print("Deriving patent legal status ...")
    sys.stdout.flush()

    # Clear existing
    conn.execute("DELETE FROM patent_legal_status")
    conn.commit()

    # Process in batches using rowid
    total_inserted = 0
    last_rowid = 0

    while True:
        rows = conn.execute(
            """
            SELECT rowid, publication_number, entity_status, filing_date, grant_date
            FROM patents
            WHERE rowid > ?
            ORDER BY rowid
            LIMIT ?
            """,
            (last_rowid, batch_size),
        ).fetchall()

        if not rows:
            break

        batch = []
        for rowid, pub, entity_status, filing_date, grant_date in rows:
            last_rowid = rowid

            status = "abandoned"
            expiry_date = None

            # Extract kind_code from publication_number (e.g. "JP-2020123456-A" → "A")
            kind_code = ""
            if pub:
                parts = pub.rsplit("-", 1)
                if len(parts) == 2:
                    kind_code = parts[1].strip().upper()

            # Granted patents: B1, B2, B6 (patent), S, S3, Y2 (utility model)
            is_patent_grant = kind_code in ("B1", "B2", "B6")
            is_utility_grant = kind_code in ("S", "S3", "Y2")
            is_application = kind_code in ("A", "A1", "A5")
            is_utility_app = kind_code == "U"

            if is_patent_grant or is_utility_grant:
                term_years = 20 if is_patent_grant else 10
                base_date = filing_date
                if base_date and base_date > 19000000:
                    base_year = base_date // 10000
                    base_month = (base_date % 10000) // 100
                    base_day = base_date % 100
                    expiry_year = base_year + term_years
                    expiry_date = expiry_year * 10000 + base_month * 100 + base_day
                    status = "alive" if expiry_date > today_int else "expired"
                else:
                    status = "alive"  # granted but no filing date → assume alive
            elif is_application or is_utility_app:
                # Applications: 'pending' if filed within last 5 years, else 'abandoned'
                if filing_date and filing_date > 19000000:
                    filing_year = filing_date // 10000
                    current_year = today_int // 10000
                    if current_year - filing_year <= 5:
                        status = "pending"
                    else:
                        status = "abandoned"
                else:
                    status = "pending"
            elif entity_status == "GRANT":
                # Fallback for non-JP patents with entity_status
                if filing_date and filing_date > 19000000:
                    filing_year = filing_date // 10000
                    expiry_year = filing_year + 20
                    filing_month = (filing_date % 10000) // 100
                    filing_day = filing_date % 100
                    expiry_date = expiry_year * 10000 + filing_month * 100 + filing_day
                    status = "alive" if expiry_date > today_int else "expired"
                else:
                    status = "alive"
            elif entity_status == "APPLICATION":
                status = "pending"

            batch.append((pub, status, expiry_date))

        conn.executemany(
            """
            INSERT OR REPLACE INTO patent_legal_status
            (publication_number, status, expiry_date)
            VALUES (?, ?, ?)
            """,
            batch,
        )
        conn.commit()
        total_inserted += len(batch)

        elapsed = time.time() - t0
        if total_inserted % 500000 < batch_size:
            print(
                f"  {total_inserted:,} rows inserted, {elapsed:.1f}s elapsed"
            )
            sys.stdout.flush()

    conn.close()
    elapsed = time.time() - t0
    print(f"\nDone: {total_inserted:,} rows in {elapsed:.1f}s ({elapsed / 60:.1f}m)")
    sys.stdout.flush()
    return total_inserted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Derive patent legal status from existing data."
    )
    parser.add_argument("--db", default="data/patents.db", help="SQLite DB path")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50000,
        help="Batch size for processing",
    )
    args = parser.parse_args()

    count = run_derive(db_path=args.db, batch_size=args.batch_size)
    print(f"Legal status rows: {count}")

    # Print distribution
    conn = sqlite3.connect(args.db, timeout=60)
    rows = conn.execute(
        "SELECT status, COUNT(*) FROM patent_legal_status GROUP BY status ORDER BY COUNT(*) DESC"
    ).fetchall()
    conn.close()
    print("\nDistribution:")
    for status, cnt in rows:
        print(f"  {status}: {cnt:,}")


if __name__ == "__main__":
    main()
