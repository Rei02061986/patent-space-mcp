"""Compute yearly firm technology vectors and summary metrics.

Memory-efficient version: uses GROUP BY for CPC aggregation and
streaming cursor for embeddings to avoid OOM on large datasets.
"""
from __future__ import annotations

import argparse
import math
import sqlite3
import struct
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.migrations import SCHEMA_SQL

EMBED_DIM = 64


def _unpack_embedding(blob: bytes | None) -> tuple[float, ...] | None:
    if blob is None:
        return None
    try:
        values = struct.unpack("64d", blob)
    except (struct.error, TypeError):
        return None
    if len(values) != EMBED_DIM:
        return None
    return values


def _pack_embedding(values: list[float]) -> bytes:
    return struct.pack("64d", *values)


def run_compute(
    db_path: str = "data/patents.db",
    year_from: int = 2015,
    year_to: int = 2024,
) -> int:
    if year_from > year_to:
        raise ValueError("year_from must be <= year_to")

    conn = sqlite3.connect(db_path, timeout=60)
    conn.executescript(SCHEMA_SQL)
    conn.execute("PRAGMA journal_mode=WAL")

    years = list(range(year_from, year_to + 1))
    t0 = time.time()

    # --- Phase 1: Aggregate patent counts per (firm_id, filing_year) ---
    print("Phase 1: Loading patent counts per firm×year ...")
    sys.stdout.flush()
    patent_counts: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    cur = conn.execute("""
        SELECT pa.firm_id, p.filing_date / 10000 AS filing_year,
               COUNT(DISTINCT pa.publication_number) AS cnt
        FROM patent_assignees pa
        JOIN patents p ON p.publication_number = pa.publication_number
        WHERE pa.firm_id IS NOT NULL AND pa.firm_id <> ''
          AND p.filing_date IS NOT NULL AND p.filing_date > 0
        GROUP BY pa.firm_id, filing_year
    """)
    firm_ids_set: set[str] = set()
    for row in cur:
        firm_id, fy, cnt = row[0], row[1], row[2]
        if fy and fy > 0:
            patent_counts[firm_id][fy] = cnt
            firm_ids_set.add(firm_id)
    print(f"  {len(firm_ids_set)} firms loaded in {time.time()-t0:.1f}s")
    sys.stdout.flush()

    # --- Phase 2: Aggregate CPC distribution per (firm_id, filing_year, cpc_class) ---
    print("Phase 2: Loading CPC distribution per firm×year×class ...")
    sys.stdout.flush()
    t1 = time.time()
    # Structure: {firm_id: {filing_year: Counter({cpc_class: count})}}
    cpc_by_firm_year: dict[str, dict[int, Counter]] = defaultdict(
        lambda: defaultdict(Counter)
    )
    cur = conn.execute("""
        SELECT pa.firm_id, p.filing_date / 10000 AS filing_year,
               substr(pc.cpc_code, 1, 4) AS cpc_class, COUNT(*) AS cnt
        FROM patent_assignees pa
        JOIN patents p ON p.publication_number = pa.publication_number
        JOIN patent_cpc pc ON pc.publication_number = pa.publication_number
        WHERE pa.firm_id IS NOT NULL AND pa.firm_id <> ''
          AND p.filing_date IS NOT NULL AND p.filing_date > 0
          AND pc.cpc_code IS NOT NULL AND length(pc.cpc_code) >= 4
        GROUP BY pa.firm_id, filing_year, cpc_class
    """)
    cpc_rows_count = 0
    for row in cur:
        firm_id, fy, cpc_class, cnt = row[0], row[1], row[2], row[3]
        if fy and fy > 0 and cpc_class:
            cpc_by_firm_year[firm_id][fy][cpc_class] = cnt
            cpc_rows_count += 1
    print(f"  {cpc_rows_count:,} CPC aggregates loaded in {time.time()-t1:.1f}s")
    sys.stdout.flush()

    # --- Phase 3: Stream embeddings ordered by firm_id, compute tech vectors ---
    print("Phase 3: Streaming embeddings and computing tech vectors ...")
    sys.stdout.flush()
    t2 = time.time()

    conn.execute(
        "DELETE FROM firm_tech_vectors WHERE year BETWEEN ? AND ?",
        (year_from, year_to),
    )
    conn.commit()

    # Use a separate connection for reading to avoid cursor interference
    read_conn = sqlite3.connect(db_path, timeout=60)
    read_conn.execute("PRAGMA journal_mode=WAL")

    embed_cursor = read_conn.execute("""
        SELECT pa.firm_id,
               p.filing_date / 10000 AS filing_year,
               prd.embedding_v1,
               COALESCE(cc.forward_citations, 0) AS forward_citations
        FROM patent_assignees pa
        JOIN patents p ON p.publication_number = pa.publication_number
        JOIN patent_research_data prd ON prd.publication_number = pa.publication_number
        LEFT JOIN citation_counts cc ON cc.publication_number = pa.publication_number
        WHERE pa.firm_id IS NOT NULL AND pa.firm_id <> ''
          AND p.filing_date IS NOT NULL AND p.filing_date > 0
          AND prd.embedding_v1 IS NOT NULL
        ORDER BY pa.firm_id
    """)

    inserted = 0
    firms_processed = 0
    upsert_buf: list[tuple] = []

    def _process_firm(fid: str, embeds: list[tuple[int, tuple[float, ...], int]]) -> None:
        nonlocal inserted, upsert_buf
        embeds.sort(key=lambda x: x[0])

        yearly_cpc = cpc_by_firm_year.get(fid, {})
        yearly_patents = patent_counts.get(fid, {})

        # Incremental accumulation using sorted filing years
        all_fys = sorted(set(list(yearly_patents.keys()) + list(yearly_cpc.keys())))
        fy_idx = 0
        cpc_cumulative: Counter = Counter()
        cumulative_patent_count = 0

        for year in years:
            # Add data from all filing years up to current year
            while fy_idx < len(all_fys) and all_fys[fy_idx] <= year:
                fy = all_fys[fy_idx]
                cumulative_patent_count += yearly_patents.get(fy, 0)
                if fy in yearly_cpc:
                    cpc_cumulative += yearly_cpc[fy]
                fy_idx += 1

            # Compute weighted embedding average
            weight_sum = 0.0
            weighted = [0.0] * EMBED_DIM
            for filing_year, emb_vec, forward_citations in embeds:
                if filing_year > year:
                    break
                decay = math.exp(-0.1 * (year - filing_year))
                weight = (max(forward_citations, 1) ** 0.5) * decay
                weight_sum += weight
                for i in range(EMBED_DIM):
                    weighted[i] += weight * emb_vec[i]

            tech_vector_blob = None
            if weight_sum > 0:
                avg = [v / weight_sum for v in weighted]
                tech_vector_blob = _pack_embedding(avg)

            dominant_cpc = None
            tech_diversity = 0.0
            tech_concentration = 0.0

            total_cpc = sum(cpc_cumulative.values())
            if total_cpc > 0:
                dominant_cpc = cpc_cumulative.most_common(1)[0][0]
                probs = [count / total_cpc for count in cpc_cumulative.values()]
                tech_diversity = -sum(p * math.log(p) for p in probs if p > 0)
                top3 = sum(count for _, count in cpc_cumulative.most_common(3))
                tech_concentration = top3 / total_cpc

            upsert_buf.append((
                fid, year, tech_vector_blob, cumulative_patent_count,
                dominant_cpc, float(tech_diversity), float(tech_concentration),
            ))

        if len(upsert_buf) >= 1000:
            conn.executemany("""
                INSERT OR REPLACE INTO firm_tech_vectors (
                    firm_id, year, tech_vector, patent_count, dominant_cpc,
                    tech_diversity, tech_concentration
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, upsert_buf)
            conn.commit()
            inserted += len(upsert_buf)
            upsert_buf = []

    # Stream through embeddings, grouping by firm_id
    current_firm: str | None = None
    firm_embeds: list[tuple[int, tuple[float, ...], int]] = []

    for row in embed_cursor:
        firm_id = row[0]
        filing_year = row[1]
        emb_blob = row[2]
        fc = row[3]

        if filing_year is None or filing_year <= 0:
            continue

        embedding = _unpack_embedding(emb_blob)
        if embedding is None:
            continue

        if firm_id != current_firm:
            if current_firm is not None:
                _process_firm(current_firm, firm_embeds)
                firms_processed += 1
                if firms_processed % 100 == 0:
                    elapsed = time.time() - t2
                    print(
                        f"  [{firms_processed} firms] {inserted} rows inserted, "
                        f"{elapsed:.0f}s elapsed"
                    )
                    sys.stdout.flush()
            current_firm = firm_id
            firm_embeds = []

        firm_embeds.append((filing_year, embedding, int(fc)))

    # Process last firm
    if current_firm is not None:
        _process_firm(current_firm, firm_embeds)
        firms_processed += 1

    # Flush remaining buffer
    if upsert_buf:
        conn.executemany("""
            INSERT OR REPLACE INTO firm_tech_vectors (
                firm_id, year, tech_vector, patent_count, dominant_cpc,
                tech_diversity, tech_concentration
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, upsert_buf)
        conn.commit()
        inserted += len(upsert_buf)

    read_conn.close()

    # Also insert rows for firms that have patents but no embeddings
    firms_with_embeds = set()
    for row in conn.execute(
        "SELECT DISTINCT firm_id FROM firm_tech_vectors"
    ):
        firms_with_embeds.add(row[0])

    no_embed_buf: list[tuple] = []
    for fid in firm_ids_set - firms_with_embeds:
        yearly_patents = patent_counts.get(fid, {})
        yearly_cpc = cpc_by_firm_year.get(fid, {})

        for year in years:
            cumulative_patent_count = sum(
                cnt for fy, cnt in yearly_patents.items() if fy <= year
            )
            if cumulative_patent_count == 0:
                continue

            cpc_cumulative: Counter = Counter()
            for fy in yearly_cpc:
                if fy <= year:
                    cpc_cumulative += yearly_cpc[fy]

            dominant_cpc = None
            tech_diversity = 0.0
            tech_concentration = 0.0
            total_cpc = sum(cpc_cumulative.values())
            if total_cpc > 0:
                dominant_cpc = cpc_cumulative.most_common(1)[0][0]
                probs = [count / total_cpc for count in cpc_cumulative.values()]
                tech_diversity = -sum(p * math.log(p) for p in probs if p > 0)
                top3 = sum(count for _, count in cpc_cumulative.most_common(3))
                tech_concentration = top3 / total_cpc

            no_embed_buf.append((
                fid, year, None, cumulative_patent_count,
                dominant_cpc, float(tech_diversity), float(tech_concentration),
            ))

    if no_embed_buf:
        conn.executemany("""
            INSERT OR REPLACE INTO firm_tech_vectors (
                firm_id, year, tech_vector, patent_count, dominant_cpc,
                tech_diversity, tech_concentration
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, no_embed_buf)
        conn.commit()
        inserted += len(no_embed_buf)

    conn.close()
    total_elapsed = time.time() - t0
    print(
        f"\nDone: {inserted} rows for {firms_processed} firms "
        f"(+{len(firm_ids_set) - len(firms_with_embeds)} without embeddings) "
        f"in {total_elapsed:.1f}s ({total_elapsed/60:.1f}m)"
    )
    sys.stdout.flush()
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute weighted firm-year technology vectors and CPC metrics."
    )
    parser.add_argument("--db", default="data/patents.db", help="SQLite DB path")
    parser.add_argument("--year-from", type=int, default=2015, help="Start year")
    parser.add_argument("--year-to", type=int, default=2024, help="End year")
    args = parser.parse_args()

    inserted = run_compute(
        db_path=args.db,
        year_from=args.year_from,
        year_to=args.year_to,
    )
    print(f"Computed firm tech vectors rows: {inserted}")


if __name__ == "__main__":
    main()
