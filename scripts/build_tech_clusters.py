"""Build technology clusters from CPC classes and patent embeddings.

Memory-efficient version: processes one CPC class at a time instead of
loading all embeddings into memory at once.
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

try:
    import numpy as np
except ImportError as exc:
    np = None
    _IMPORT_ERR = exc
else:
    _IMPORT_ERR = None

try:
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score
except ImportError as exc:
    KMeans = None
    silhouette_score = None
    _SKLEARN_IMPORT_ERR = exc
else:
    _SKLEARN_IMPORT_ERR = None

EMBED_DIM = 64


def _require_deps() -> None:
    if np is None:
        raise SystemExit(
            f"numpy is required for build_tech_clusters.py: {_IMPORT_ERR}"
        )
    if KMeans is None or silhouette_score is None:
        raise SystemExit(
            "scikit-learn is required for build_tech_clusters.py: "
            f"{_SKLEARN_IMPORT_ERR}"
        )


def _unpack_embedding(blob: bytes | None) -> "np.ndarray | None":
    if blob is None:
        return None
    try:
        values = struct.unpack("64d", blob)
    except (TypeError, struct.error):
        return None
    if len(values) != EMBED_DIM:
        return None
    return np.array(values, dtype=np.float64)


def _pack_embedding(arr: "np.ndarray") -> bytes:
    return struct.pack("64d", *arr.tolist())


def _extract_year(yyyymmdd: int | None) -> int | None:
    if yyyymmdd is None:
        return None
    year = yyyymmdd // 10000
    if year <= 0:
        return None
    return year


def _safe_load_top_terms(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [v for v in parsed if isinstance(v, str) and v]


def _best_kmeans_labels(
    embeddings: "np.ndarray",
) -> "tuple[np.ndarray, int]":
    n = len(embeddings)
    if n < 3:
        return np.zeros(n, dtype=int), 1

    max_k = min(5, n - 1)
    best_labels = np.zeros(n, dtype=int)
    best_score = -1.0
    best_k = 1

    for k in range(2, max_k + 1):
        model = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = model.fit_predict(embeddings)
        if len(set(labels.tolist())) < 2:
            continue
        score = float(silhouette_score(embeddings, labels))
        if score > best_score:
            best_score = score
            best_labels = labels
            best_k = k

    if best_k == 1:
        return np.zeros(n, dtype=int), 1
    return best_labels, best_k


def run_build(
    db_path: str = "data/patents.db",
    min_patents: int = 100,
    variance_threshold: float = 0.3,
    cpc_prefix: str | None = None,
) -> tuple[int, int]:
    _require_deps()
    if min_patents <= 0:
        raise ValueError("min_patents must be > 0")
    if variance_threshold < 0:
        raise ValueError("variance_threshold must be >= 0")

    conn = sqlite3.connect(db_path, timeout=60)
    conn.executescript(SCHEMA_SQL)
    conn.execute("PRAGMA journal_mode=WAL")
    t0 = time.time()

    # Step 1: Find qualifying CPC classes (with enough patents that have embeddings)
    prefix_filter = ""
    params: list = [min_patents]
    if cpc_prefix:
        prefix_filter = "AND substr(pc.cpc_code, 1, ?) = ?"
        params = [len(cpc_prefix), cpc_prefix, min_patents]
        print(f"Step 1: Finding qualifying CPC classes (prefix={cpc_prefix}) ...")
    else:
        print("Step 1: Finding qualifying CPC classes ...")
    sys.stdout.flush()

    sql = f"""
        SELECT substr(pc.cpc_code, 1, 4) AS cpc_class, COUNT(DISTINCT pc.publication_number) AS cnt
        FROM patent_cpc pc
        JOIN patent_research_data prd ON prd.publication_number = pc.publication_number
        WHERE pc.cpc_code IS NOT NULL AND length(pc.cpc_code) >= 4
          AND prd.embedding_v1 IS NOT NULL
          {prefix_filter}
        GROUP BY cpc_class
        HAVING cnt >= ?
        ORDER BY cpc_class
    """
    if cpc_prefix:
        cpc_classes = conn.execute(sql, (len(cpc_prefix), cpc_prefix, min_patents)).fetchall()
    else:
        cpc_classes = conn.execute(sql, (min_patents,)).fetchall()
    print(f"  {len(cpc_classes)} CPC classes qualify (>= {min_patents} patents)")
    sys.stdout.flush()

    # Clear existing data (only for the target prefix, or all if no prefix)
    if cpc_prefix:
        conn.execute(
            "DELETE FROM patent_cluster_mapping WHERE cluster_id IN "
            "(SELECT cluster_id FROM tech_clusters WHERE substr(cpc_class, 1, ?) = ?)",
            (len(cpc_prefix), cpc_prefix),
        )
        conn.execute(
            "DELETE FROM tech_clusters WHERE substr(cpc_class, 1, ?) = ?",
            (len(cpc_prefix), cpc_prefix),
        )
    else:
        conn.execute("DELETE FROM patent_cluster_mapping")
        conn.execute("DELETE FROM tech_clusters")
    conn.commit()

    # Use a read connection for queries
    read_conn = sqlite3.connect(db_path, timeout=60)
    read_conn.execute("PRAGMA journal_mode=WAL")

    cluster_count = 0
    mapped_count = 0

    # Step 2: Process each CPC class independently
    for ci, (cpc_class, patent_count) in enumerate(cpc_classes):
        t1 = time.time()

        # Load patents for this CPC class
        # Use primary CPC assignment (first by priority ranking)
        rows = read_conn.execute("""
            SELECT
                pc.publication_number,
                pc.cpc_code,
                p.filing_date,
                prd.embedding_v1,
                prd.top_terms,
                pa.firm_id
            FROM patent_cpc pc
            JOIN patent_research_data prd ON prd.publication_number = pc.publication_number
            LEFT JOIN patents p ON p.publication_number = pc.publication_number
            LEFT JOIN (
                SELECT publication_number, MIN(firm_id) AS firm_id
                FROM patent_assignees
                WHERE firm_id IS NOT NULL AND firm_id <> ''
                GROUP BY publication_number
            ) pa ON pa.publication_number = pc.publication_number
            WHERE substr(pc.cpc_code, 1, 4) = ?
              AND prd.embedding_v1 IS NOT NULL
        """, (cpc_class,)).fetchall()

        records = []
        seen_pubs = set()
        for row in rows:
            pub = row[0]
            if pub in seen_pubs:
                continue
            seen_pubs.add(pub)
            emb = _unpack_embedding(row[3])
            if emb is None:
                continue
            records.append({
                "publication_number": pub,
                "cpc_code": row[1],
                "filing_year": _extract_year(row[2]),
                "embedding": emb,
                "firm_id": row[5],
                "top_terms": _safe_load_top_terms(row[4]),
            })

        if len(records) < min_patents:
            continue

        embeddings = np.stack([r["embedding"] for r in records], axis=0)
        avg_std = float(np.std(embeddings, axis=0).mean())

        if avg_std > variance_threshold:
            labels, n_clusters = _best_kmeans_labels(embeddings)
        else:
            labels = np.zeros(len(records), dtype=int)
            n_clusters = 1

        cluster_rows = []
        mapping_rows = []

        for cluster_idx in range(n_clusters):
            cluster_id = f"{cpc_class}_{cluster_idx}"
            member_indices = np.where(labels == cluster_idx)[0]
            if member_indices.size == 0:
                continue

            member_embeddings = embeddings[member_indices]
            center = member_embeddings.mean(axis=0)
            center_blob = _pack_embedding(center)

            yearly_counter: Counter[int] = Counter()
            applicant_counter: Counter[str] = Counter()
            term_counter: Counter[str] = Counter()
            cpc_codes: set[str] = set()

            for idx in member_indices.tolist():
                rec = records[idx]
                fy = rec["filing_year"]
                if fy is not None:
                    yearly_counter[fy] += 1
                fid = rec["firm_id"]
                if fid:
                    applicant_counter[str(fid)] += 1
                for term in rec["top_terms"]:
                    term_counter[term] += 1
                cpc_codes.add(str(rec["cpc_code"]))

            if yearly_counter:
                sorted_years = sorted(yearly_counter)
                # Use 5-year CAGR for normalized growth rate
                # Fall back to 3-year or 2-year if not enough data
                growth_rate = 0.0
                for window in (5, 3, 2):
                    if len(sorted_years) >= window + 1:
                        recent_end = sorted_years[-1]
                        recent_start = sorted_years[-1 - window]
                        c_start = yearly_counter[recent_start]
                        c_end = yearly_counter[recent_end]
                        if c_start > 0:
                            growth_rate = float(
                                (c_end / c_start) ** (1.0 / window) - 1.0
                            )
                        break
            else:
                growth_rate = 0.0

            yearly_counts_json = json.dumps(
                {str(y): yearly_counter[y] for y in sorted(yearly_counter)}
            )
            top_applicants_json = json.dumps([
                {"firm_id": fid, "count": cnt}
                for fid, cnt in applicant_counter.most_common(10)
            ])
            top_terms_json = json.dumps(
                [term for term, _ in term_counter.most_common(20)]
            )
            cpc_codes_json = json.dumps(sorted(cpc_codes))

            cluster_rows.append((
                cluster_id, cpc_class, cpc_class, cpc_codes_json,
                center_blob, int(member_indices.size),
                yearly_counts_json, growth_rate,
                top_applicants_json, top_terms_json,
            ))

            for idx in member_indices.tolist():
                rec = records[idx]
                emb = rec["embedding"]
                distance = float(np.linalg.norm(emb - center))
                mapping_rows.append((
                    str(rec["publication_number"]),
                    cluster_id,
                    distance,
                ))

            cluster_count += 1

        mapped_count += len(mapping_rows)

        # Write this class's results
        if cluster_rows:
            conn.executemany("""
                INSERT OR REPLACE INTO tech_clusters (
                    cluster_id, label, cpc_class, cpc_codes, center_vector,
                    patent_count, yearly_counts, growth_rate,
                    top_applicants, top_terms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, cluster_rows)
        if mapping_rows:
            conn.executemany("""
                INSERT OR REPLACE INTO patent_cluster_mapping (
                    publication_number, cluster_id, distance
                ) VALUES (?, ?, ?)
            """, mapping_rows)
        conn.commit()

        elapsed = time.time() - t1
        if (ci + 1) % 10 == 0 or elapsed > 30:
            total_elapsed = time.time() - t0
            print(
                f"  [{ci+1}/{len(cpc_classes)}] {cpc_class}: "
                f"{len(records)} patents, {n_clusters} sub-clusters, "
                f"{elapsed:.1f}s (total: {cluster_count} clusters, "
                f"{mapped_count:,} mapped, {total_elapsed:.0f}s)"
            )
            sys.stdout.flush()

        # Free memory
        del records, embeddings, labels, cluster_rows, mapping_rows

    read_conn.close()
    conn.close()
    total_elapsed = time.time() - t0
    print(
        f"\nDone: {cluster_count} clusters, {mapped_count:,} patents mapped "
        f"in {total_elapsed:.1f}s ({total_elapsed/60:.1f}m)"
    )
    sys.stdout.flush()
    return cluster_count, mapped_count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build technology clusters from CPC classes and embeddings."
    )
    parser.add_argument("--db", default="data/patents.db", help="SQLite DB path")
    parser.add_argument(
        "--min-patents", type=int, default=100,
        help="Minimum patents per CPC class to build clusters",
    )
    parser.add_argument(
        "--variance-threshold", type=float, default=0.3,
        help="Average embedding std threshold to trigger KMeans subdivision",
    )
    parser.add_argument(
        "--cpc-prefix", default=None,
        help="Only process CPC classes starting with this prefix (e.g., B01D, G06)",
    )
    args = parser.parse_args()

    clusters, mapped = run_build(
        db_path=args.db,
        min_patents=args.min_patents,
        variance_threshold=args.variance_threshold,
        cpc_prefix=args.cpc_prefix,
    )
    print(f"Built clusters: {clusters}")
    print(f"Mapped patents: {mapped}")


if __name__ == "__main__":
    main()
