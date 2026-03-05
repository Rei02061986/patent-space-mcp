#!/usr/bin/env python3
"""Ingest research embeddings from Parquet files into SQLite.

Reads Parquet files exported via `bq extract` from
google_patents_research.publications and inserts into
the patent_research_data table.

Usage:
    python3 scripts/ingest_research_parquet.py \
        --parquet-dir ~/exports/research/ \
        --db /home/deploy/patent-space-mcp/data/patents.db
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import struct
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq

BATCH_SIZE = 50_000


def _pack_embedding(values) -> bytes | None:
    """Pack a 64-dim float vector into binary blob."""
    if values is None:
        return None
    # Handle pyarrow list/array types
    if hasattr(values, "as_py"):
        values = values.as_py()
    if not values or len(values) != 64:
        return None
    try:
        return struct.pack("64d", *[float(v) for v in values])
    except (TypeError, struct.error, ValueError):
        return None


def ingest_parquet_dir(
    parquet_dir: str,
    db_path: str,
    country_filter: str | None = None,
    resume_from: int = 0,
):
    """Ingest all Parquet files from a directory into SQLite."""
    parquet_path = Path(parquet_dir)
    files = sorted(parquet_path.glob("*.parquet"))
    if not files:
        print(f"No .parquet files found in {parquet_dir}")
        return

    print(f"Found {len(files)} Parquet files in {parquet_dir}")
    if resume_from > 0:
        files = files[resume_from:]
        print(f"Resuming from file index {resume_from} ({len(files)} remaining)")

    # Connect to DB
    conn = sqlite3.connect(db_path, timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-1024000")  # 1GB cache
    conn.execute("PRAGMA foreign_keys=OFF")

    # Ensure table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS patent_research_data (
            publication_number TEXT PRIMARY KEY,
            title_en TEXT,
            abstract_en TEXT,
            top_terms TEXT,
            embedding_v1 BLOB
        )
    """)
    conn.commit()

    start_time = time.time()
    total_inserted = 0
    total_skipped = 0
    total_files = len(files)

    for file_idx, pf in enumerate(files):
        file_start = time.time()
        try:
            table = pq.read_table(pf)
        except Exception as e:
            print(f"  ERROR reading {pf.name}: {e}")
            continue

        n_rows = table.num_rows
        batch_rows = []
        file_inserted = 0
        file_skipped = 0

        # Get column indices
        col_names = table.column_names
        pub_idx = col_names.index("publication_number") if "publication_number" in col_names else None
        if pub_idx is None:
            print(f"  SKIP {pf.name}: no publication_number column")
            continue

        # Read columns
        pub_col = table.column("publication_number")
        title_col = table.column("title") if "title" in col_names else None
        abstract_col = table.column("abstract") if "abstract" in col_names else None
        top_terms_col = table.column("top_terms") if "top_terms" in col_names else None
        embedding_col = table.column("embedding_v1") if "embedding_v1" in col_names else None
        country_col = table.column("country") if "country" in col_names else None

        for i in range(n_rows):
            # Country filter
            if country_filter and country_col is not None:
                country = country_col[i].as_py() if country_col[i].is_valid else None
                if country and country_filter not in country:
                    file_skipped += 1
                    continue

            pub_num = pub_col[i].as_py() if pub_col[i].is_valid else None
            if not pub_num:
                file_skipped += 1
                continue

            # Pack embedding
            emb_val = embedding_col[i] if embedding_col is not None else None
            embedding_blob = _pack_embedding(emb_val)
            if embedding_blob is None:
                file_skipped += 1
                continue

            title = title_col[i].as_py() if title_col is not None and title_col[i].is_valid else None
            abstract = abstract_col[i].as_py() if abstract_col is not None and abstract_col[i].is_valid else None

            top_terms = None
            if top_terms_col is not None and top_terms_col[i].is_valid:
                tt = top_terms_col[i].as_py()
                if tt:
                    top_terms = json.dumps(tt)

            batch_rows.append((pub_num, title, abstract, top_terms, embedding_blob))

            if len(batch_rows) >= BATCH_SIZE:
                conn.executemany(
                    """INSERT OR REPLACE INTO patent_research_data
                       (publication_number, title_en, abstract_en, top_terms, embedding_v1)
                       VALUES (?, ?, ?, ?, ?)""",
                    batch_rows,
                )
                conn.commit()
                file_inserted += len(batch_rows)
                batch_rows = []

        # Final batch for this file
        if batch_rows:
            conn.executemany(
                """INSERT OR REPLACE INTO patent_research_data
                   (publication_number, title_en, abstract_en, top_terms, embedding_v1)
                   VALUES (?, ?, ?, ?, ?)""",
                batch_rows,
            )
            conn.commit()
            file_inserted += len(batch_rows)

        total_inserted += file_inserted
        total_skipped += file_skipped
        file_elapsed = time.time() - file_start
        total_elapsed = time.time() - start_time
        rate = total_inserted / total_elapsed if total_elapsed > 0 else 0

        print(
            f"  [{file_idx + 1 + resume_from}/{total_files + resume_from}] {pf.name}: "
            f"rows={n_rows:,}, inserted={file_inserted:,}, skipped={file_skipped:,}, "
            f"time={file_elapsed:.1f}s | "
            f"Total: {total_inserted:,} ({rate:,.0f}/s)"
        )
        sys.stdout.flush()

    elapsed = time.time() - start_time
    conn.execute("PRAGMA foreign_keys=ON")
    conn.commit()

    # Show final stats
    cursor = conn.execute("SELECT COUNT(*) FROM patent_research_data")
    db_total = cursor.fetchone()[0]
    conn.close()

    print(f"\n=== Ingestion Complete ===")
    print(f"Files processed: {total_files}")
    print(f"Rows inserted:   {total_inserted:,}")
    print(f"Rows skipped:    {total_skipped:,}")
    print(f"Elapsed:         {elapsed:.1f}s ({elapsed/60:.1f}m)")
    if elapsed > 0:
        print(f"Rate:            {total_inserted / elapsed:,.0f}/s")
    print(f"DB total rows:   {db_total:,}")


def main():
    parser = argparse.ArgumentParser(
        description="Ingest research embeddings from Parquet into SQLite"
    )
    parser.add_argument(
        "--parquet-dir",
        required=True,
        help="Directory containing Parquet files from bq extract",
    )
    parser.add_argument(
        "--db",
        required=True,
        help="SQLite database path",
    )
    parser.add_argument(
        "--country",
        default=None,
        help="Filter by country name (e.g. 'Japan', 'United States'). None = all countries.",
    )
    parser.add_argument(
        "--resume-from",
        type=int,
        default=0,
        help="Resume from file index N (skip first N files)",
    )
    args = parser.parse_args()

    ingest_parquet_dir(
        parquet_dir=args.parquet_dir,
        db_path=args.db,
        country_filter=args.country,
        resume_from=args.resume_from,
    )


if __name__ == "__main__":
    main()
