"""startability and startability_ranking tool implementations.

v2: Auto-detect best available year when requested year has no data.
This prevents empty results when data only covers 2016-2023 but
MCP schema defaults to year=2024.
"""
from __future__ import annotations

import json
import math
import struct
from typing import Any

import numpy as np

from db.sqlite_store import PatentStore
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

        firm_id = resolved.entity.canonical_id
        conn = store._conn()

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
            return {
                "error": f"No pre-computed data in startability_surface for firm_id='{firm_id}' cluster='{cluster_id}'",
                "firm_id": firm_id,
                "cluster_id": cluster_id,
                "suggestion": "This firm-technology pair may not have been pre-computed.",
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
            firm_id = resolved.entity.canonical_id

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

            # Year fallback
            actual_year = year
            if not rows:
                best = _latest_year_ss(conn, firm_id=firm_id)
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
            SELECT firm_id, score, gate_open,
                   phi_tech_cos, phi_tech_dist, phi_tech_cpc
            FROM startability_surface
            WHERE cluster_id = ? AND year = ?
            ORDER BY score DESC
            LIMIT ?
            """,
            (cluster_id, year, top_n),
        ).fetchall()

        # Year fallback
        actual_year = year
        if not rows:
            best = _latest_year_ss(conn, cluster_id=cluster_id)
            if best and best != year:
                actual_year = best
                rows = conn.execute(
                    """
                    SELECT firm_id, score, gate_open,
                           phi_tech_cos, phi_tech_dist, phi_tech_cpc
                    FROM startability_surface
                    WHERE cluster_id = ? AND year = ?
                    ORDER BY score DESC
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
