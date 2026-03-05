"""Build tech clusters for a SINGLE CPC 4-char class.

Optimized: skips the expensive Step-1 discovery query.
Usage: python build_single_cluster.py --db data/patents.db --cpc B01D
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import struct
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.migrations import SCHEMA_SQL

import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

EMBED_DIM = 64


def _unpack(blob: bytes | None) -> np.ndarray | None:
    if blob is None:
        return None
    try:
        v = struct.unpack("64d", blob)
    except (TypeError, struct.error):
        return None
    return np.array(v, dtype=np.float64) if len(v) == EMBED_DIM else None


def _pack(arr: np.ndarray) -> bytes:
    return struct.pack("64d", *arr.tolist())


def _year(yyyymmdd: int | None) -> int | None:
    if yyyymmdd is None:
        return None
    y = yyyymmdd // 10000
    return y if y > 0 else None


def _safe_terms(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    return [v for v in parsed if isinstance(v, str) and v] if isinstance(parsed, list) else []


def _best_kmeans(embs: np.ndarray) -> tuple[np.ndarray, int]:
    n = len(embs)
    if n < 3:
        return np.zeros(n, dtype=int), 1
    max_k = min(5, n - 1)
    best_labels = np.zeros(n, dtype=int)
    best_score = -1.0
    best_k = 1
    for k in range(2, max_k + 1):
        labels = KMeans(n_clusters=k, random_state=42, n_init=10).fit_predict(embs)
        if len(set(labels.tolist())) < 2:
            continue
        score = float(silhouette_score(embs, labels))
        if score > best_score:
            best_score = score
            best_labels = labels
            best_k = k
    return (best_labels, best_k) if best_k > 1 else (np.zeros(n, dtype=int), 1)


def run(db_path: str, cpc_class: str, min_patents: int = 50) -> tuple[int, int]:
    assert len(cpc_class) == 4, "CPC class must be exactly 4 characters (e.g., B01D)"

    conn = sqlite3.connect(db_path, timeout=120)
    conn.executescript(SCHEMA_SQL)
    conn.execute("PRAGMA journal_mode=WAL")
    t0 = time.time()

    # Step 1: Get publication_numbers in this CPC class
    print(f"Loading patents for CPC class {cpc_class} ...")
    sys.stdout.flush()

    rows = conn.execute("""
        SELECT DISTINCT pc.publication_number
        FROM patent_cpc pc
        WHERE pc.cpc_code LIKE ? || '%'
    """, (cpc_class,)).fetchall()
    pub_numbers = [r[0] for r in rows]
    print(f"  {len(pub_numbers)} patents in {cpc_class}")
    sys.stdout.flush()

    if len(pub_numbers) < min_patents:
        print(f"  Not enough patents (need {min_patents}). Skipping.")
        conn.close()
        return 0, 0

    # Step 2: Load embeddings for these patents (batch lookup)
    print("Loading embeddings ...")
    sys.stdout.flush()

    records = []
    batch_size = 500
    for i in range(0, len(pub_numbers), batch_size):
        batch = pub_numbers[i:i + batch_size]
        placeholders = ",".join("?" * len(batch))
        emb_rows = conn.execute(f"""
            SELECT prd.publication_number, prd.embedding_v1, prd.top_terms
            FROM patent_research_data prd
            WHERE prd.publication_number IN ({placeholders})
              AND prd.embedding_v1 IS NOT NULL
        """, batch).fetchall()

        for row in emb_rows:
            emb = _unpack(row[1])
            if emb is not None:
                records.append({
                    "publication_number": row[0],
                    "embedding": emb,
                    "top_terms": _safe_terms(row[2]),
                })

    print(f"  {len(records)} patents with embeddings")
    sys.stdout.flush()

    if len(records) < min_patents:
        print(f"  Not enough embeddings (need {min_patents}). Skipping.")
        conn.close()
        return 0, 0

    # Step 3: Load metadata (filing_date, cpc_code, firm_id) for matched patents
    print("Loading metadata ...")
    sys.stdout.flush()

    matched_pubs = [r["publication_number"] for r in records]
    meta_map = {}
    for i in range(0, len(matched_pubs), batch_size):
        batch = matched_pubs[i:i + batch_size]
        placeholders = ",".join("?" * len(batch))

        # Filing dates
        prows = conn.execute(f"""
            SELECT publication_number, filing_date
            FROM patents WHERE publication_number IN ({placeholders})
        """, batch).fetchall()
        for r in prows:
            meta_map.setdefault(r[0], {})["filing_year"] = _year(r[1])

        # CPC codes
        crows = conn.execute(f"""
            SELECT publication_number, cpc_code
            FROM patent_cpc WHERE publication_number IN ({placeholders})
              AND cpc_code LIKE ? || '%'
        """, batch + [cpc_class]).fetchall()
        for r in crows:
            meta_map.setdefault(r[0], {})["cpc_code"] = r[1]

        # Firm IDs
        frows = conn.execute(f"""
            SELECT publication_number, MIN(firm_id) AS firm_id
            FROM patent_assignees
            WHERE publication_number IN ({placeholders})
              AND firm_id IS NOT NULL AND firm_id <> ''
            GROUP BY publication_number
        """, batch).fetchall()
        for r in frows:
            meta_map.setdefault(r[0], {})["firm_id"] = r[1]

    for rec in records:
        m = meta_map.get(rec["publication_number"], {})
        rec["filing_year"] = m.get("filing_year")
        rec["cpc_code"] = m.get("cpc_code", cpc_class)
        rec["firm_id"] = m.get("firm_id")

    # Step 4: Cluster
    print("Clustering ...")
    sys.stdout.flush()

    embeddings = np.stack([r["embedding"] for r in records], axis=0)
    avg_std = float(np.std(embeddings, axis=0).mean())
    print(f"  avg embedding std: {avg_std:.4f}")

    if avg_std > 0.3:
        labels, n_clusters = _best_kmeans(embeddings)
        print(f"  KMeans: {n_clusters} sub-clusters")
    else:
        labels = np.zeros(len(records), dtype=int)
        n_clusters = 1
        print("  Homogeneous: 1 cluster")
    sys.stdout.flush()

    # Step 5: Write results
    print("Writing results ...")
    sys.stdout.flush()

    # Clear existing data for this CPC class
    conn.execute(
        "DELETE FROM patent_cluster_mapping WHERE cluster_id IN "
        "(SELECT cluster_id FROM tech_clusters WHERE cpc_class = ?)",
        (cpc_class,),
    )
    conn.execute("DELETE FROM tech_clusters WHERE cpc_class = ?", (cpc_class,))

    cluster_count = 0
    mapped_count = 0

    for cidx in range(n_clusters):
        cluster_id = f"{cpc_class}_{cidx}"
        members = np.where(labels == cidx)[0]
        if members.size == 0:
            continue

        center = embeddings[members].mean(axis=0)

        yearly: Counter[int] = Counter()
        applicants: Counter[str] = Counter()
        terms: Counter[str] = Counter()
        cpc_codes: set[str] = set()

        for idx in members.tolist():
            rec = records[idx]
            if rec["filing_year"]:
                yearly[rec["filing_year"]] += 1
            if rec["firm_id"]:
                applicants[str(rec["firm_id"])] += 1
            for t in rec["top_terms"]:
                terms[t] += 1
            cpc_codes.add(str(rec["cpc_code"]))

        # Growth rate (5y CAGR fallback to 3y, 2y)
        growth_rate = 0.0
        if yearly:
            sy = sorted(yearly)
            for w in (5, 3, 2):
                if len(sy) >= w + 1:
                    c0 = yearly[sy[-1 - w]]
                    c1 = yearly[sy[-1]]
                    if c0 > 0:
                        growth_rate = float((c1 / c0) ** (1.0 / w) - 1.0)
                    break

        conn.execute("""
            INSERT OR REPLACE INTO tech_clusters (
                cluster_id, label, cpc_class, cpc_codes, center_vector,
                patent_count, yearly_counts, growth_rate,
                top_applicants, top_terms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            cluster_id, cpc_class, cpc_class,
            json.dumps(sorted(cpc_codes)),
            _pack(center),
            int(members.size),
            json.dumps({str(y): yearly[y] for y in sorted(yearly)}),
            growth_rate,
            json.dumps([{"firm_id": f, "count": c} for f, c in applicants.most_common(10)]),
            json.dumps([t for t, _ in terms.most_common(20)]),
        ))

        for idx in members.tolist():
            rec = records[idx]
            dist = float(np.linalg.norm(rec["embedding"] - center))
            conn.execute("""
                INSERT OR REPLACE INTO patent_cluster_mapping (
                    publication_number, cluster_id, distance
                ) VALUES (?, ?, ?)
            """, (rec["publication_number"], cluster_id, dist))

        cluster_count += 1
        mapped_count += members.size

    conn.commit()
    conn.close()

    elapsed = time.time() - t0
    print(f"\nDone: {cluster_count} clusters, {mapped_count:,} patents mapped in {elapsed:.1f}s")
    return cluster_count, mapped_count


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="data/patents.db")
    p.add_argument("--cpc", required=True, help="4-char CPC class (e.g., B01D)")
    p.add_argument("--min-patents", type=int, default=50)
    args = p.parse_args()
    run(args.db, args.cpc, args.min_patents)


if __name__ == "__main__":
    main()
