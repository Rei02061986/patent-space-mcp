"""startability and startability_ranking tool implementations.

v3: Smarter year auto-fallback. When requested year returns sparse data
(e.g., year=2024 has only 1 cluster per firm), falls back to the year
with the most data (typically 2023 with 607 clusters per firm).
"""
from __future__ import annotations

import json
import math
import struct
from typing import Any

import numpy as np

from db.sqlite_store import PatentStore


def _resolve_firm_id_with_fallback(resolved, conn, table="startability_surface"):
    """Resolve entity to DB firm_id, with company_XXXX fallback."""
    firm_id = resolved.entity.canonical_id
    # Check if this firm_id exists in the target table
    row = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE firm_id = ?", (firm_id,)
    ).fetchone()
    if row and row[0] > 0:
        return firm_id
    # Fallback: try company_{ticker}
    ticker = getattr(resolved.entity, 'ticker', None)
    if ticker:
        alt_id = f"company_{ticker}"
        row2 = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE firm_id = ?", (alt_id,)
        ).fetchone()
        if row2 and row2[0] > 0:
            return alt_id
    # Fallback: LIKE match
    like_row = conn.execute(
        f"SELECT DISTINCT firm_id FROM {table} WHERE firm_id LIKE ? LIMIT 1",
        (f"{firm_id}%",)
    ).fetchone()
    if like_row:
        return like_row[0]
    return firm_id  # Return original even if not found
from entity.resolver import EntityResolver
from tools.pagination import paginate
from space.startability import (
    gate,
    phi_tech_cosine,
    phi_tech_cpc_jaccard,
    phi_tech_distance,
    startability_score,
    unpack_embedding,
)


def _normalize_cpc_codes(codes: list[str]) -> set[str]:
    out: set[str] = set()
    for code in codes:
        cleaned = (code or "").strip().upper()
        if not cleaned:
            continue
        out.add(cleaned[:4] if len(cleaned) >= 4 else cleaned)
    return out


def _parse_json_codes(raw: str | None) -> set[str]:
    if not raw:
        return set()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return set()
    if not isinstance(data, list):
        return set()
    return _normalize_cpc_codes([str(v) for v in data])


def _unpack_blob(blob: bytes | None):
    if not blob:
        return None
    try:
        return unpack_embedding(blob)
    except Exception:
        if len(blob) % 8 != 0:
            return None
        count = len(blob) // 8
        return struct.unpack(f"{count}d", blob)


def _resolve_cluster(conn, tech_query_or_cluster_id: str):
    q = (tech_query_or_cluster_id or "").strip()
    looks_like_id = "_" in q and len(q) <= 40

    if looks_like_id:
        row = conn.execute(
            """
            SELECT cluster_id, label, cpc_class, cpc_codes, center_vector
            FROM tech_clusters
            WHERE cluster_id = ?
            """,
            (q,),
        ).fetchone()
        if row is not None:
            return row

    like = f"%{q}%"
    return conn.execute(
        """
        SELECT cluster_id, label, cpc_class, cpc_codes, center_vector
        FROM tech_clusters
        WHERE label LIKE ? OR cpc_class LIKE ? || '%'
        ORDER BY patent_count DESC
        LIMIT 1
        """,
        (like, q),
    ).fetchone()


def _latest_year_ss(conn, cluster_id=None, firm_id=None):
    """Find latest year with data in startability_surface.

    Uses indexed lookups (idx_ss_cluster_year_score or idx_ss_firm_year)
    so this is fast even on HDD.
    """
    if cluster_id:
        row = conn.execute(
            "SELECT MAX(year) as y FROM startability_surface WHERE cluster_id = ?",
            (cluster_id,),
        ).fetchone()
    elif firm_id:
        row = conn.execute(
            "SELECT MAX(year) as y FROM startability_surface WHERE firm_id = ?",
            (firm_id,),
        ).fetchone()
    else:
        return None
    return row["y"] if row and row["y"] else None


def _best_year_ss(conn, cluster_id=None, firm_id=None):
    """Find the latest year with substantial data in startability_surface.

    Picks the latest year whose row count is >= 90% of the max count.
    This avoids picking ancient years when counts are nearly equal
    (e.g., H01L_0 has 2762 in 2018 vs 2732 in 2023 — we want 2023).
    """
    if cluster_id:
        rows = conn.execute(
            "SELECT year, COUNT(*) as cnt FROM startability_surface "
            "WHERE cluster_id = ? GROUP BY year ORDER BY cnt DESC",
            (cluster_id,),
        ).fetchall()
    elif firm_id:
        rows = conn.execute(
            "SELECT year, COUNT(*) as cnt FROM startability_surface "
            "WHERE firm_id = ? GROUP BY year ORDER BY cnt DESC",
            (firm_id,),
        ).fetchall()
    else:
        return None
    if not rows:
        return None
    max_cnt = rows[0]["cnt"]
    threshold = max(1, int(max_cnt * 0.9))
    # Among years with >= 90% of max count, pick the latest
    best = max(
        (r["year"] for r in rows if r["cnt"] >= threshold),
        default=rows[0]["year"],
    )
    return best


def _latest_year_ftv(conn, firm_id):
    """Find latest year with data in firm_tech_vectors."""
    row = conn.execute(
        "SELECT MAX(year) as y FROM firm_tech_vectors WHERE firm_id = ?",
        (firm_id,),
    ).fetchone()
    return row["y"] if row and row["y"] else None


def _firm_cpc_codes(conn, firm_id: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT c.cpc_code
        FROM patent_cpc c
        JOIN patent_assignees a
          ON a.publication_number = c.publication_number
        WHERE a.firm_id = ?
        """,
        (firm_id,),
    ).fetchall()
    return _normalize_cpc_codes([row["cpc_code"] for row in rows])


def _compute_phi(
    store: PatentStore,
    resolver: EntityResolver,
    firm_query: str,
    tech_query_or_cluster_id: str,
    year: int,
) -> dict[str, Any]:
    resolved = resolver.resolve(firm_query, country_hint="JP")
    if resolved is None:
        return {
            "error": f"Could not resolve firm: '{firm_query}'",
            "suggestion": "Try the exact company name, Japanese name, or stock ticker",
        }

    firm_id = resolved.entity.canonical_id

    with store._conn() as conn:
        firm_vec_row = conn.execute(
            """
            SELECT tech_vector
            FROM firm_tech_vectors
            WHERE firm_id = ? AND year = ?
            """,
            (firm_id, year),
        ).fetchone()

        # Year fallback: try latest available year if requested year has no data
        actual_year = year
        if firm_vec_row is None:
            best = _latest_year_ftv(conn, firm_id)
            if best and best != year:
                actual_year = best
                firm_vec_row = conn.execute(
                    "SELECT tech_vector FROM firm_tech_vectors WHERE firm_id = ? AND year = ?",
                    (firm_id, actual_year),
                ).fetchone()

        if firm_vec_row is None:
            return {
                "error": f"No firm_tech_vector found for firm_id='{firm_id}' and year={year}",
                "firm_id": firm_id,
                "year": year,
            }

        cluster_row = _resolve_cluster(conn, tech_query_or_cluster_id)
        if cluster_row is None:
            return {
                "error": f"No tech cluster found for query: '{tech_query_or_cluster_id}'"
            }

        firm_vec = _unpack_blob(firm_vec_row["tech_vector"])
        cluster_vec = _unpack_blob(cluster_row["center_vector"])
        if firm_vec is None or cluster_vec is None:
            # Fallback: use pre-computed startability_surface if vectors unavailable
            ss_row = conn.execute(
                """
                SELECT score, gate_open, phi_tech_cos, phi_tech_dist, phi_tech_cpc
                FROM startability_surface
                WHERE firm_id = ? AND cluster_id = ? AND year = ?
                """,
                (firm_id, cluster_row["cluster_id"], actual_year),
            ).fetchone()
            if ss_row is None and actual_year != year:
                # Try original year too
                ss_row = conn.execute(
                    """
                    SELECT score, gate_open, phi_tech_cos, phi_tech_dist, phi_tech_cpc
                    FROM startability_surface
                    WHERE firm_id = ? AND cluster_id = ?
                    ORDER BY year DESC LIMIT 1
                    """,
                    (firm_id, cluster_row["cluster_id"]),
                ).fetchone()
            if ss_row:
                return {
                    "firm_id": firm_id,
                    "cluster_id": cluster_row["cluster_id"],
                    "year": actual_year,
                    "phi_tech_cosine": ss_row["phi_tech_cos"],
                    "phi_tech_distance": ss_row["phi_tech_dist"],
                    "phi_tech_cpc_jaccard": ss_row["phi_tech_cpc"],
                    "gate_open": bool(ss_row["gate_open"]),
                    "data_source": "startability_surface",
                }
            return {
                "error": "Could not unpack one or both vectors",
                "firm_id": firm_id,
                "cluster_id": cluster_row["cluster_id"],
                "year": year,
            }

        firm_cpc_codes = _firm_cpc_codes(conn, firm_id)
        cluster_cpc_codes = _parse_json_codes(cluster_row["cpc_codes"])

        phi_cos = float(phi_tech_cosine(firm_vec, cluster_vec))
        phi_dist = float(phi_tech_distance(firm_vec, cluster_vec))
        phi_cpc = float(phi_tech_cpc_jaccard(cluster_cpc_codes, firm_cpc_codes))
        gate_open = bool(gate(phi_cos, phi_cpc, 0.0))

    return {
        "firm_id": firm_id,
        "cluster_id": cluster_row["cluster_id"],
        "year": actual_year,
        "phi_tech_cosine": phi_cos,
        "phi_tech_distance": phi_dist,
        "phi_tech_cpc_jaccard": phi_cpc,
        "gate_open": gate_open,
    }


def startability(
    store: PatentStore,
    resolver: EntityResolver,
    firm_query: str,
    tech_query_or_cluster_id: str,
    year: int = 2024,
) -> dict[str, Any]:
    """Compute startability score for one firm and one technology cluster.

    v2: Falls back to pre-computed startability_surface when vectors are
    unavailable, and auto-detects the best available year.
    """
    # Try _compute_phi first (uses vectors if available)
    fit = _compute_phi(
        store=store,
        resolver=resolver,
        firm_query=firm_query,
        tech_query_or_cluster_id=tech_query_or_cluster_id,
        year=year,
    )

    if "error" in fit:
        # Fallback: check pre-computed startability_surface directly
        resolved = resolver.resolve(firm_query, country_hint="JP")
        if resolved is None:
            return fit  # Can't resolve firm, return original error

        conn = store._conn()
        firm_id = _resolve_firm_id_with_fallback(resolved, conn)

        # Resolve cluster
        cluster_row = _resolve_cluster(conn, tech_query_or_cluster_id)
        if cluster_row is None:
            return fit

        cluster_id = cluster_row["cluster_id"]

        # Try requested year first, then latest
        ss_row = conn.execute(
            """
            SELECT score, gate_open, phi_tech_cos, phi_tech_dist, phi_tech_cpc, year
            FROM startability_surface
            WHERE firm_id = ? AND cluster_id = ? AND year = ?
            """,
            (firm_id, cluster_id, year),
        ).fetchone()

        if ss_row is None:
            # Try latest available year
            ss_row = conn.execute(
                """
                SELECT score, gate_open, phi_tech_cos, phi_tech_dist, phi_tech_cpc, year
                FROM startability_surface
                WHERE firm_id = ? AND cluster_id = ?
                ORDER BY year DESC LIMIT 1
                """,
                (firm_id, cluster_id),
            ).fetchone()

        if ss_row is None:
            # Graceful fallback: return score=0 with explanation
            # Also find what clusters ARE available for this firm
            available = []
            try:
                avail_rows = conn.execute(
                    "SELECT cluster_id, score FROM startability_surface "
                    "WHERE firm_id = ? ORDER BY score DESC LIMIT 10",
                    (firm_id,),
                ).fetchall()
                available = [{"cluster_id": r["cluster_id"], "score": round(r["score"], 4)} for r in avail_rows]
            except Exception:
                pass

            return {
                "endpoint": "startability",
                "firm_id": firm_id,
                "cluster_id": cluster_id,
                "year": year,
                "score": 0.0,
                "gate_open": False,
                "phi_tech_cosine": None,
                "phi_tech_distance": None,
                "phi_tech_cpc_jaccard": None,
                "rank": None,
                "note": "No pre-computed data available for this firm-cluster combination. The firm may not have filings in this technology area, or may not be in the pre-computed dataset.",
                "available_clusters": available,
                "suggestion": (
                    f"This firm ({firm_id}) has data for {len(available)} clusters."
                    if available else
                    f"This firm ({firm_id}) is not in the pre-computed startability dataset."
                ),
            }

        actual_year = ss_row["year"]
        return {
            "firm_id": firm_id,
            "cluster_id": cluster_id,
            "year": actual_year,
            "score": ss_row["score"],
            "gate_open": bool(ss_row["gate_open"]),
            "phi_tech_cosine": ss_row["phi_tech_cos"],
            "phi_tech_distance": ss_row["phi_tech_dist"],
            "phi_tech_cpc_jaccard": ss_row["phi_tech_cpc"],
            "rank": None,
            "data_source": "startability_surface",
            "explanation": f"Pre-computed from startability_surface (year={actual_year}).",
        }

    # Normal path: compute from phi components
    phi_vec = np.array(
        [
            fit["phi_tech_cosine"],
            fit["phi_tech_distance"],
            fit["phi_tech_cpc_jaccard"],
            0.0,
        ]
    )

    if not fit["gate_open"]:
        return {
            "firm_id": fit["firm_id"],
            "cluster_id": fit["cluster_id"],
            "year": fit["year"],
            "score": 0.0,
            "gate_open": False,
            "phi_tech_cosine": fit["phi_tech_cosine"],
            "phi_tech_distance": fit["phi_tech_distance"],
            "phi_tech_cpc_jaccard": fit["phi_tech_cpc_jaccard"],
            "rank": None,
            "explanation": "Gate is closed, so startability score is 0.",
        }

    score = float(startability_score(phi_vec))
    rank = None
    with store._conn() as conn:
        rank_row = conn.execute(
            """
            SELECT 1 + COUNT(*) AS rank_pos
            FROM startability_surface
            WHERE cluster_id = ? AND year = ? AND score > ?
            """,
            (fit["cluster_id"], fit["year"], score),
        ).fetchone()
        if rank_row is not None:
            rank = rank_row["rank_pos"]

    return {
        "firm_id": fit["firm_id"],
        "cluster_id": fit["cluster_id"],
        "year": fit["year"],
        "score": score,
        "gate_open": True,
        "phi_tech_cosine": fit["phi_tech_cosine"],
        "phi_tech_distance": fit["phi_tech_distance"],
        "phi_tech_cpc_jaccard": fit["phi_tech_cpc_jaccard"],
        "rank": rank,
        "explanation": "Gate is open, score computed from phi_tech components.",
    }


def startability_ranking(
    store: PatentStore,
    resolver: EntityResolver,
    mode: str,
    query: str,
    year: int = 2024,
    top_n: int = 20,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """Rank technologies for a firm or firms for a technology by startability.

    v2: Auto-detects best available year when requested year has no data.
    """
    top_n = max(1, min(int(top_n), 200))

    if mode not in {"by_firm", "by_tech"}:
        return {"error": "mode must be 'by_firm' or 'by_tech'"}

    with store._conn() as conn:
        if mode == "by_firm":
            resolved = resolver.resolve(query, country_hint="JP")
            if resolved is None:
                return {
                    "error": f"Could not resolve firm: '{query}'",
                    "suggestion": "Try the exact company name, Japanese name, or stock ticker",
                }
            firm_id = _resolve_firm_id_with_fallback(resolved, conn)

            rows = conn.execute(
                """
                SELECT cluster_id, score, gate_open,
                       phi_tech_cos, phi_tech_dist, phi_tech_cpc
                FROM startability_surface
                WHERE firm_id = ? AND year = ?
                ORDER BY score DESC
                LIMIT ?
                """,
                (firm_id, year, top_n),
            ).fetchall()

            # Year fallback: trigger when results are empty OR sparse (<3)
            # This handles year=2024 which has only 1 cluster per firm
            actual_year = year
            if len(rows) < min(3, top_n):
                best = _best_year_ss(conn, firm_id=firm_id)
                if best and best != year:
                    actual_year = best
                    rows = conn.execute(
                        """
                        SELECT cluster_id, score, gate_open,
                               phi_tech_cos, phi_tech_dist, phi_tech_cpc
                        FROM startability_surface
                        WHERE firm_id = ? AND year = ?
                        ORDER BY score DESC
                        LIMIT ?
                        """,
                        (firm_id, actual_year, top_n),
                    ).fetchall()

            results = [
                {
                    "cluster_id": row["cluster_id"],
                    "score": row["score"],
                    "gate_open": bool(row["gate_open"]),
                    "phi_tech_cosine": row["phi_tech_cos"],
                    "phi_tech_distance": row["phi_tech_dist"],
                    "phi_tech_cpc_jaccard": row["phi_tech_cpc"],
                }
                for row in rows
            ]
            paged = paginate(results, page=page, page_size=page_size)
            page_size_clamped = paged["page_size"]
            pages = math.ceil(len(results) / page_size_clamped) if results else 1
            return {
                "mode": mode,
                "firm_id": firm_id,
                "year": actual_year,
                "total": len(results),
                "page": paged["page"],
                "page_size": page_size_clamped,
                "pages": pages,
                "results": paged["results"],
                "summary": {
                    "top_applicants": [],
                    "date_range": {"earliest": actual_year, "latest": actual_year},
                    "cpc_distribution": [],
                    "avg_score": round(
                        sum(r["score"] for r in results) / len(results), 4
                    ) if results else None,
                },
                "all_results": results,
            }

        cluster_row = _resolve_cluster(conn, query)
        if cluster_row is None:
            return {"error": f"No tech cluster found for query: '{query}'"}
        cluster_id = cluster_row["cluster_id"]

        rows = conn.execute(
            """
            SELECT ss.firm_id, ss.score, ss.gate_open,
                   ss.phi_tech_cos, ss.phi_tech_dist, ss.phi_tech_cpc,
                   COALESCE(ftv.patent_count, 0) as patent_count
            FROM startability_surface ss
            LEFT JOIN firm_tech_vectors ftv 
                ON ss.firm_id = ftv.firm_id AND ftv.year = ss.year
            WHERE ss.cluster_id = ? AND ss.year = ?
              AND COALESCE(ftv.patent_count, 0) > 50
            ORDER BY ss.score DESC, COALESCE(ftv.patent_count, 0) DESC
            LIMIT ?
            """,
            (cluster_id, year, top_n),
        ).fetchall()

        # Year fallback: trigger when results are empty OR sparse (<3)
        actual_year = year
        if len(rows) < min(3, top_n):
            best = _best_year_ss(conn, cluster_id=cluster_id)
            if best and best != year:
                actual_year = best
                rows = conn.execute(
                    """
                    SELECT ss.firm_id, ss.score, ss.gate_open,
                           ss.phi_tech_cos, ss.phi_tech_dist, ss.phi_tech_cpc,
                           COALESCE(ftv.patent_count, 0) as patent_count
                    FROM startability_surface ss
                    LEFT JOIN firm_tech_vectors ftv
                        ON ss.firm_id = ftv.firm_id AND ftv.year = ss.year
                    WHERE ss.cluster_id = ? AND ss.year = ?
                      AND COALESCE(ftv.patent_count, 0) > 50
                    ORDER BY ss.score DESC, COALESCE(ftv.patent_count, 0) DESC
                    LIMIT ?
                    """,
                    (cluster_id, actual_year, top_n),
                ).fetchall()

    results = [
        {
            "firm_id": row["firm_id"],
            "score": row["score"],
            "gate_open": bool(row["gate_open"]),
            "phi_tech_cosine": row["phi_tech_cos"],
            "phi_tech_distance": row["phi_tech_dist"],
            "phi_tech_cpc_jaccard": row["phi_tech_cpc"],
        }
        for row in rows
    ]

    # Tie-break: when multiple firms have identical scores (e.g., 1.0),
    # re-rank by patent_count from firm_tech_vectors (higher = more relevant)
    if results and len(set(r["score"] for r in results)) < len(results):
        tie_firms = [r["firm_id"] for r in results]
        ph = ",".join("?" * len(tie_firms))
        pc_rows = conn.execute(
            f"SELECT firm_id, patent_count FROM firm_tech_vectors "
            f"WHERE firm_id IN ({ph}) AND year = ? ORDER BY patent_count DESC",
            tie_firms + [actual_year],
        ).fetchall()
        pc_map = {r["firm_id"]: r["patent_count"] or 0 for r in pc_rows}
        results.sort(key=lambda r: (-r["score"], -pc_map.get(r["firm_id"], 0)))
    paged = paginate(results, page=page, page_size=page_size)
    page_size_clamped = paged["page_size"]
    pages = math.ceil(len(results) / page_size_clamped) if results else 1
    top_firms = sorted(results, key=lambda r: r["score"], reverse=True)[:5]

    return {
        "mode": mode,
        "cluster_id": cluster_id,
        "year": actual_year,
        "total": len(results),
        "page": paged["page"],
        "page_size": page_size_clamped,
        "pages": pages,
        "results": paged["results"],
        "summary": {
            "top_applicants": [
                {"firm_id": r["firm_id"], "count": r["score"]}
                for r in top_firms
            ],
            "date_range": {"earliest": actual_year, "latest": actual_year},
            "cpc_distribution": [],
            "avg_score": round(
                sum(r["score"] for r in results) / len(results), 4
            ) if results else None,
        },
        "all_results": [
            {
                "firm_id": row["firm_id"],
                "score": row["score"],
                "gate_open": bool(row["gate_open"]),
                "phi_tech_cosine": row["phi_tech_cos"],
                "phi_tech_distance": row["phi_tech_dist"],
                "phi_tech_cpc_jaccard": row["phi_tech_cpc"],
            }
            for row in rows
        ],
    }
