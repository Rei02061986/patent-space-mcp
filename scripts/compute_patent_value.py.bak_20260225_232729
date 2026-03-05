"""Compute Patent Value Index for all patents.

Formula:
  value_score = normalize(
      w1 * log(1 + forward_citations) +
      w2 * log(family_size) +
      w3 * remaining_life_ratio +
      w4 * cluster_momentum
  )

Where:
  - forward_citations: from citation_counts table
  - family_size: from patent_family table
  - remaining_life_ratio: (filing_date + 20yr - today) / 20, clamped [0,1]
  - cluster_momentum: from tech_cluster_momentum (latest year)

Output is normalized to [0, 1] range using min-max normalization.
"""
from __future__ import annotations

import argparse
import math
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.migrations import SCHEMA_SQL

# Component weights
W_CITATION = 0.35
W_FAMILY = 0.20
W_RECENCY = 0.25
W_MOMENTUM = 0.20


def run_compute(db_path: str = "data/patents.db", batch_size: int = 50000) -> int:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.executescript(SCHEMA_SQL)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row

    import datetime
    today_int = int(datetime.date.today().strftime("%Y%m%d"))
    current_year = today_int // 10000

    t0 = time.time()
    print("Computing Patent Value Index ...")
    sys.stdout.flush()

    # Preload cluster momentum into dict
    print("  Loading cluster momentum ...")
    momentum_rows = conn.execute(
        """
        SELECT cluster_id, growth_rate
        FROM tech_cluster_momentum
        WHERE year = (SELECT MAX(year) FROM tech_cluster_momentum)
        """
    ).fetchall()
    cluster_momentum = {r["cluster_id"]: r["growth_rate"] for r in momentum_rows}
    print(f"  {len(cluster_momentum)} clusters with momentum data")

    # Clear existing
    conn.execute("DELETE FROM patent_value_index")
    conn.commit()

    # Pass 1: Compute raw scores
    total = 0
    last_rowid = 0
    raw_scores = []

    while True:
        rows = conn.execute(
            """
            SELECT p.rowid, p.publication_number, p.filing_date,
                   COALESCE(cc.forward_citations, 0) AS fwd_cite,
                   COALESCE(pf.family_size, 1) AS fam_size,
                   pcm.cluster_id
            FROM patents p
            LEFT JOIN citation_counts cc ON p.publication_number = cc.publication_number
            LEFT JOIN patent_family pf ON p.publication_number = pf.publication_number
            LEFT JOIN patent_cluster_mapping pcm ON p.publication_number = pcm.publication_number
            WHERE p.rowid > ?
            ORDER BY p.rowid
            LIMIT ?
            """,
            (last_rowid, batch_size),
        ).fetchall()

        if not rows:
            break

        for r in rows:
            last_rowid = r["rowid"]

            # Citation component
            cite_score = math.log1p(r["fwd_cite"])

            # Family component
            fam_score = math.log(max(r["fam_size"], 1))

            # Recency component (remaining life ratio)
            recency = 0.0
            filing_date = r["filing_date"]
            if filing_date and filing_date > 19000000:
                filing_year = filing_date // 10000
                remaining = (filing_year + 20 - current_year) / 20.0
                recency = max(0.0, min(1.0, remaining))

            # Cluster momentum component
            momentum = 0.0
            if r["cluster_id"] and r["cluster_id"] in cluster_momentum:
                momentum = max(0.0, cluster_momentum[r["cluster_id"]])

            raw = (
                W_CITATION * cite_score
                + W_FAMILY * fam_score
                + W_RECENCY * recency
                + W_MOMENTUM * momentum
            )

            raw_scores.append((
                r["publication_number"],
                raw,
                cite_score,
                fam_score,
                recency,
                momentum,
            ))

        total += len(rows)
        if total % 500000 < batch_size:
            elapsed = time.time() - t0
            print(f"  {total:,} rows scanned, {elapsed:.1f}s elapsed")
            sys.stdout.flush()

    if not raw_scores:
        conn.close()
        print("No patents found.")
        return 0

    # Pass 2: Normalize to [0, 1]
    print(f"\n  Normalizing {len(raw_scores):,} scores ...")
    raw_vals = [s[1] for s in raw_scores]
    min_raw = min(raw_vals)
    max_raw = max(raw_vals)
    range_raw = max_raw - min_raw if max_raw > min_raw else 1.0

    # Insert normalized scores in batches
    inserted = 0
    batch = []
    for pub, raw, cite, fam, recency, momentum in raw_scores:
        normalized = (raw - min_raw) / range_raw
        batch.append((pub, normalized, cite, fam, recency, momentum))

        if len(batch) >= batch_size:
            conn.executemany(
                """
                INSERT OR REPLACE INTO patent_value_index
                (publication_number, value_score, citation_component, family_component,
                 recency_component, cluster_momentum_component)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                batch,
            )
            conn.commit()
            inserted += len(batch)
            batch = []

            if inserted % 500000 < batch_size:
                elapsed = time.time() - t0
                print(f"  {inserted:,} rows inserted, {elapsed:.1f}s elapsed")
                sys.stdout.flush()

    if batch:
        conn.executemany(
            """
            INSERT OR REPLACE INTO patent_value_index
            (publication_number, value_score, citation_component, family_component,
             recency_component, cluster_momentum_component)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            batch,
        )
        conn.commit()
        inserted += len(batch)

    conn.close()
    elapsed = time.time() - t0
    print(f"\nDone: {inserted:,} rows in {elapsed:.1f}s ({elapsed / 60:.1f}m)")
    sys.stdout.flush()
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute Patent Value Index."
    )
    parser.add_argument("--db", default="data/patents.db", help="SQLite DB path")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50000,
        help="Batch size for processing",
    )
    args = parser.parse_args()

    count = run_compute(db_path=args.db, batch_size=args.batch_size)
    print(f"Value index rows: {count}")

    # Print stats
    conn = sqlite3.connect(args.db, timeout=60)
    row = conn.execute(
        """
        SELECT MIN(value_score), AVG(value_score), MAX(value_score),
               COUNT(*) AS cnt
        FROM patent_value_index
        """
    ).fetchone()
    conn.close()
    if row:
        print(f"\nStats:")
        print(f"  Count: {row[3]:,}")
        print(f"  Min: {row[0]:.4f}")
        print(f"  Avg: {row[1]:.4f}")
        print(f"  Max: {row[2]:.4f}")


if __name__ == "__main__":
    main()
