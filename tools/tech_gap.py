"""tech_gap tool — analyze technology gap between two firms.

Quantifies complementarity, overlap, and synergy potential using
pre-computed startability_surface data.
"""
from __future__ import annotations

import struct
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver


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


def _resolve_firm(resolver, name):
    r = resolver.resolve(name, country_hint="JP")
    if r is None:
        return None, None
    return r.entity.canonical_id, r.entity.canonical_name


def _get_scores(conn, firm_id, year):
    """Get startability scores for a firm, with year fallback."""
    rows = conn.execute(
        "SELECT cluster_id, score, gate_open FROM startability_surface "
        "WHERE firm_id = ? AND year = ? ORDER BY score DESC",
        (firm_id, year),
    ).fetchall()
    if not rows:
        best = conn.execute(
            "SELECT MAX(year) as y FROM startability_surface WHERE firm_id = ?",
            (firm_id,),
        ).fetchone()
        if best and best["y"]:
            year = best["y"]
            rows = conn.execute(
                "SELECT cluster_id, score, gate_open FROM startability_surface "
                "WHERE firm_id = ? AND year = ? ORDER BY score DESC",
                (firm_id, year),
            ).fetchall()
    return {r["cluster_id"]: {"score": r["score"], "gate_open": r["gate_open"]} for r in rows}, year


def _cluster_label(conn, cluster_id):
    row = conn.execute(
        "SELECT label FROM tech_clusters WHERE cluster_id = ?",
        (cluster_id,),
    ).fetchone()
    return row["label"] if row else cluster_id


def tech_gap(
    store: PatentStore,
    resolver: EntityResolver,
    firm_a: str,
    firm_b: str,
    year: int = 2024,
) -> dict[str, Any]:
    """Analyze technology gap between two firms."""
    fid_a, name_a = _resolve_firm(resolver, firm_a)
    if fid_a is None:
        return {"error": f"Could not resolve firm: '{firm_a}'",
                "suggestion": "Try the exact company name, Japanese name, or stock ticker"}
    fid_b, name_b = _resolve_firm(resolver, firm_b)
    if fid_b is None:
        return {"error": f"Could not resolve firm: '{firm_b}'",
                "suggestion": "Try the exact company name, Japanese name, or stock ticker"}

    conn = store._conn()

    scores_a, year_a = _get_scores(conn, fid_a, year)
    scores_b, year_b = _get_scores(conn, fid_b, year)
    actual_year = min(year_a, year_b) if year_a and year_b else year

    all_clusters = set(scores_a.keys()) | set(scores_b.keys())

    overlap = []
    a_stronger = []
    b_stronger = []

    for cid in sorted(all_clusters):
        sa = scores_a.get(cid, {}).get("score", 0.0)
        sb = scores_b.get(cid, {}).get("score", 0.0)
        gap = abs(sa - sb)
        label = _cluster_label(conn, cid)
        entry = {
            "cluster_id": cid,
            "label": label,
            "score_a": round(sa, 3),
            "score_b": round(sb, 3),
            "gap": round(gap, 3),
        }

        # Both have significant presence (>0.3) → overlap
        if sa > 0.3 and sb > 0.3:
            overlap.append(entry)
        elif sa > sb + 0.1:
            a_stronger.append(entry)
        elif sb > sa + 0.1:
            b_stronger.append(entry)
        elif sa > 0.1 or sb > 0.1:
            overlap.append(entry)

    # Sort by gap descending
    overlap.sort(key=lambda x: min(x["score_a"], x["score_b"]), reverse=True)
    a_stronger.sort(key=lambda x: x["gap"], reverse=True)
    b_stronger.sort(key=lambda x: x["gap"], reverse=True)

    # Calculate synergy and overlap scores
    # Synergy = how much one fills the other's gaps
    synergy_pairs = 0
    for cid in all_clusters:
        sa = scores_a.get(cid, {}).get("score", 0.0)
        sb = scores_b.get(cid, {}).get("score", 0.0)
        if (sa > 0.5 and sb < 0.2) or (sb > 0.5 and sa < 0.2):
            synergy_pairs += 1

    synergy_score = min(synergy_pairs / max(len(all_clusters), 1) * 5.0, 1.0)

    # Overlap = proportion where both are strong
    overlap_count = sum(
        1 for cid in all_clusters
        if scores_a.get(cid, {}).get("score", 0) > 0.3
        and scores_b.get(cid, {}).get("score", 0) > 0.3
    )
    overlap_score = overlap_count / max(len(all_clusters), 1)

    # Tech distance from vectors
    tech_distance = None
    vec_a_row = conn.execute(
        "SELECT tech_vector FROM firm_tech_vectors WHERE firm_id = ? AND year = ?",
        (fid_a, actual_year),
    ).fetchone()
    vec_b_row = conn.execute(
        "SELECT tech_vector FROM firm_tech_vectors WHERE firm_id = ? AND year = ?",
        (fid_b, actual_year),
    ).fetchone()
    if vec_a_row and vec_b_row:
        va = _unpack_vec(vec_a_row["tech_vector"])
        vb = _unpack_vec(vec_b_row["tech_vector"])
        if va and vb:
            tech_distance = round(1.0 - _cosine(va, vb), 4)

    # Acquisition fit classification
    if synergy_score > 0.5 and overlap_score < 0.3:
        acquisition_fit = "high_synergy"
    elif overlap_score > 0.5:
        acquisition_fit = "high_overlap"
    elif synergy_score > 0.3:
        acquisition_fit = "moderate_synergy"
    else:
        acquisition_fit = "low_relevance"

    return {
        "endpoint": "tech_gap",
        "firm_a": {"firm_id": fid_a, "name": name_a},
        "firm_b": {"firm_id": fid_b, "name": name_b},
        "year": actual_year,
        "overlap": overlap[:20],
        "a_stronger": a_stronger[:20],
        "b_stronger": b_stronger[:20],
        "synergy_score": round(synergy_score, 3),
        "overlap_score": round(overlap_score, 3),
        "tech_distance": tech_distance,
        "acquisition_fit": acquisition_fit,
        "total_clusters_analyzed": len(all_clusters),
        "visualization_hint": {
            "recommended_chart": "bubble",
            "title": f"{name_a} vs {name_b} 技術ギャップ",
            "axes": {"x": "score_a", "y": "score_b", "size": "gap"},
        },
    }
