"""similar_firms tool — discover similar companies by patent portfolio.

v5: Use top-K startability clusters (by SCORE) for Jaccard similarity.
Full cluster_ids (e.g., "B60K_0") rather than CPC prefixes.
Jaccard-dominant weighting (0.1/0.9) because the pre-computed tech_vectors
produce unreliable cosine similarities (Honda=0.0 with Toyota, but random
gaming company=0.996). Top-K cluster Jaccard is the reliable signal.
"""
from __future__ import annotations

import struct
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver

_TOP_CPC_K = 15  # Top clusters for Jaccard
_COSINE_WEIGHT = 0.1
_JACCARD_WEIGHT = 0.9


def _unpack_vec(blob: bytes | None) -> list[float] | None:
    if not blob or len(blob) < 8:
        return None
    n = len(blob) // 4
    try:
        return list(struct.unpack(f"{n}f", blob))
    except struct.error:
        n = len(blob) // 8
        if n == 0:
            return None
        return list(struct.unpack(f"{n}d", blob))


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    if na < 1e-12 or nb < 1e-12:
        return 0.0
    return dot / (na * nb)


def _jaccard(set_a: set, set_b: set) -> float:
    if not set_a and not set_b:
        return 0.0
    union = set_a | set_b
    if not union:
        return 0.0
    return len(set_a & set_b) / len(union)


def similar_firms(
    store: PatentStore,
    resolver: EntityResolver,
    firm_query: str,
    top_n: int = 10,
    year: int = 2024,
) -> dict[str, Any]:
    """Find firms with similar patent portfolios via cosine + Jaccard similarity."""
    store._relax_timeout()
    resolved = resolver.resolve(firm_query, country_hint="JP")
    if resolved is None:
        return {
            "error": f"Could not resolve firm: '{firm_query}'",
            "suggestion": "Try the exact company name, Japanese name, or stock ticker",
        }

    firm_id = resolved.entity.canonical_id
    conn = store._conn()

    # Find best available year for tech_vectors
    actual_year = year
    row = conn.execute(
        "SELECT tech_vector, dominant_cpc FROM firm_tech_vectors WHERE firm_id = ? AND year = ?",
        (firm_id, year),
    ).fetchone()
    if row is None:
        best = conn.execute(
            "SELECT MAX(year) as y FROM firm_tech_vectors WHERE firm_id = ?",
            (firm_id,),
        ).fetchone()
        if best and best["y"]:
            actual_year = best["y"]
            row = conn.execute(
                "SELECT tech_vector, dominant_cpc FROM firm_tech_vectors WHERE firm_id = ? AND year = ?",
                (firm_id, actual_year),
            ).fetchone()

    if row is None:
        return {
            "error": f"No tech vector found for '{firm_id}'",
            "suggestion": "This firm may not have enough patents for a technology profile.",
        }

    target_vec = _unpack_vec(row["tech_vector"])
    if target_vec is None:
        return {"error": "Could not decode tech vector"}

    # Find best year for startability_surface (may differ from tech_vectors year)
    ss_year = _find_best_ss_year(conn, firm_id, actual_year)

    # Get target firm's top-K clusters by score
    target_top_clusters = _get_top_clusters(conn, firm_id, ss_year, _TOP_CPC_K)

    # Load all firm vectors for the same year
    all_rows = conn.execute(
        "SELECT firm_id, tech_vector, patent_count, dominant_cpc, tech_diversity "
        "FROM firm_tech_vectors WHERE year = ?",
        (actual_year,),
    ).fetchall()

    # Batch load top clusters for all candidate firms
    candidate_firm_ids = [r["firm_id"] for r in all_rows if r["firm_id"] != firm_id]
    cluster_cache = _batch_get_top_clusters(conn, candidate_firm_ids, ss_year, _TOP_CPC_K)

    similarities = []
    for r in all_rows:
        fid = r["firm_id"]
        if fid == firm_id:
            continue
        vec = _unpack_vec(r["tech_vector"])
        if vec is None:
            continue
        cos_sim = _cosine(target_vec, vec)

        # Jaccard on top-K clusters by score
        candidate_clusters = cluster_cache.get(fid, set())
        jac_sim = _jaccard(target_top_clusters, candidate_clusters)

        # Combined score
        final_score = _COSINE_WEIGHT * cos_sim + _JACCARD_WEIGHT * jac_sim

        if final_score > 0.01:
            similarities.append({
                "firm_id": fid,
                "similarity": round(final_score, 4),
                "cosine_similarity": round(cos_sim, 4),
                "cpc_jaccard": round(jac_sim, 4),
                "patent_count": r["patent_count"],
                "dominant_cpc": r["dominant_cpc"],
                "tech_diversity": round((r["tech_diversity"] or 0) / 5.0, 3),
            })

    similarities.sort(key=lambda x: x["similarity"], reverse=True)
    top = similarities[:top_n]

    # Enrich top firms with shared/unique clusters
    for item in top:
        fid = item["firm_id"]
        candidate_clusters = cluster_cache.get(fid, set())
        shared = target_top_clusters & candidate_clusters
        unique = candidate_clusters - target_top_clusters
        item["shared_clusters"] = sorted(shared)[:10]
        item["shared_cluster_count"] = len(shared)
        item["unique_strengths"] = sorted(unique)[:5]

    return {
        "endpoint": "similar_firms",
        "firm_id": firm_id,
        "year": actual_year,
        "startability_year": ss_year,
        "total_firms_compared": len(all_rows) - 1,
        "scoring_method": f"cosine({_COSINE_WEIGHT}) + top{_TOP_CPC_K}_cluster_jaccard({_JACCARD_WEIGHT})",
        "target_top_clusters": sorted(target_top_clusters),
        "results": top,
        "result_count": len(top),
        "visualization_hint": {
            "recommended_chart": "radar",
            "title": f"{firm_id}の類似企業分析",
            "axes": {"x": "similarity", "y": "tech_diversity", "size": "patent_count"},
        },
    }


def _find_best_ss_year(conn, firm_id: str, preferred_year: int) -> int:
    """Find the best startability_surface year with substantial data."""
    # Check preferred year
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM startability_surface WHERE firm_id = ? AND year = ?",
        (firm_id, preferred_year),
    ).fetchone()
    if row and row["cnt"] > 10:
        return preferred_year
    # Fall back to the year with most data
    row = conn.execute(
        "SELECT year, COUNT(*) as cnt FROM startability_surface "
        "WHERE firm_id = ? GROUP BY year ORDER BY cnt DESC LIMIT 1",
        (firm_id,),
    ).fetchone()
    if row:
        return row["year"]
    # Default to 2023 (where most data lives)
    return 2023


def _get_top_clusters(conn, firm_id: str, year: int, k: int) -> set[str]:
    """Get top-K cluster_ids by score for a firm."""
    rows = conn.execute(
        "SELECT cluster_id FROM startability_surface "
        "WHERE firm_id = ? AND year = ? "
        "ORDER BY score DESC LIMIT ?",
        (firm_id, year, k),
    ).fetchall()
    return {r["cluster_id"] for r in rows}


def _batch_get_top_clusters(conn, firm_ids: list[str], year: int, k: int) -> dict[str, set[str]]:
    """Batch get top-K clusters for multiple firms.

    Uses a single query per chunk, then groups and trims in Python.
    """
    if not firm_ids:
        return {}

    result: dict[str, set[str]] = {}
    chunk_size = 500
    for i in range(0, len(firm_ids), chunk_size):
        chunk = firm_ids[i:i + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT firm_id, cluster_id, score FROM startability_surface "
            f"WHERE firm_id IN ({placeholders}) AND year = ? "
            f"ORDER BY firm_id, score DESC",
            (*chunk, year),
        ).fetchall()

        current_firm = None
        count = 0
        for r in rows:
            fid = r["firm_id"]
            if fid != current_firm:
                current_firm = fid
                count = 0
            if count < k:
                result.setdefault(fid, set()).add(r["cluster_id"])
                count += 1

    return result
