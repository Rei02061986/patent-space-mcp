#!/usr/bin/env python3
"""Build citation_index table from patent_citations.

Pre-computes forward and backward citation counts per patent
for fast lookups. Also updates patents.citation_count_forward.

Usage:
    python3 build_citation_index.py --db /app/data/patents.db
"""
import argparse
import sqlite3
import time

DB_DEFAULT = "/app/data/patents.db"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DB_DEFAULT)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db, timeout=300, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=120000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-512000")  # 512MB cache

    print("=== Building citation_index ===")
    print(f"DB: {args.db}")

    # Step 1: Create citation_index table
    print("\n[1/5] Creating citation_index table...")
    conn.execute("DROP TABLE IF EXISTS citation_index")
    conn.execute("""
        CREATE TABLE citation_index (
            publication_number TEXT PRIMARY KEY,
            citing_count INTEGER DEFAULT 0,
            cited_by_count INTEGER DEFAULT 0
        )
    """)

    # Step 2: Populate citing_count (how many patents does this one cite)
    t0 = time.time()
    print("[2/5] Computing citing counts (forward references)...")
    conn.execute("""
        INSERT INTO citation_index (publication_number, citing_count, cited_by_count)
        SELECT citing_publication, COUNT(*), 0
        FROM patent_citations
        GROUP BY citing_publication
    """)
    citing_rows = conn.execute("SELECT COUNT(*) FROM citation_index").fetchone()[0]
    print(f"  Citing counts: {citing_rows:,} patents ({time.time()-t0:.0f}s)")

    # Step 3: Update cited_by_count (how many patents cite this one)
    t1 = time.time()
    print("[3/5] Computing cited_by counts (backward references)...")
    conn.execute("""
        INSERT OR REPLACE INTO citation_index (publication_number, citing_count, cited_by_count)
        SELECT
            COALESCE(ci.publication_number, cb.cited_publication),
            COALESCE(ci.citing_count, 0),
            COALESCE(cb.cnt, 0)
        FROM (
            SELECT cited_publication, COUNT(*) as cnt
            FROM patent_citations
            GROUP BY cited_publication
        ) cb
        LEFT JOIN citation_index ci ON ci.publication_number = cb.cited_publication
    """)
    total_rows = conn.execute("SELECT COUNT(*) FROM citation_index").fetchone()[0]
    print(f"  Total citation_index: {total_rows:,} patents ({time.time()-t1:.0f}s)")

    # Step 4: Create index on cited_by_count for "most cited" queries
    t2 = time.time()
    print("[4/5] Creating indexes...")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_citidx_cited_by ON citation_index(cited_by_count DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_citidx_citing ON citation_index(citing_count DESC)")
    print(f"  Indexes created ({time.time()-t2:.0f}s)")

    # Step 5: Update patents.citation_count_forward
    t3 = time.time()
    print("[5/5] Updating patents.citation_count_forward...")
    conn.execute("""
        UPDATE patents
        SET citation_count_forward = COALESCE(
            (SELECT cited_by_count FROM citation_index WHERE citation_index.publication_number = patents.publication_number),
            0
        )
        WHERE EXISTS (
            SELECT 1 FROM citation_index WHERE citation_index.publication_number = patents.publication_number
        )
    """)
    updated = conn.execute("SELECT changes()").fetchone()[0]
    print(f"  Updated {updated:,} patents ({time.time()-t3:.0f}s)")

    # Summary
    stats = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN cited_by_count > 0 THEN 1 ELSE 0 END) as has_citations,
            MAX(cited_by_count) as max_cited,
            AVG(cited_by_count) as avg_cited
        FROM citation_index
    """).fetchone()

    print(f"\n=== Summary ===")
    print(f"Total patents in index: {stats[0]:,}")
    print(f"Patents with citations: {stats[1]:,}")
    print(f"Max cited_by_count: {stats[2]:,}")
    print(f"Avg cited_by_count: {stats[3]:.1f}")
    print(f"Total time: {time.time()-t0:.0f}s")

    # Top 10 most cited
    print(f"\nTop 10 most cited patents:")
    for row in conn.execute(
        "SELECT publication_number, cited_by_count FROM citation_index ORDER BY cited_by_count DESC LIMIT 10"
    ):
        print(f"  {row[0]}: {row[1]:,} citations")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
