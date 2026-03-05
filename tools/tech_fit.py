"""tech_fit tool implementation.

v2: Auto-detect best year + fallback to startability_surface when
vectors (center_vector or tech_vector) are unavailable.
"""
from __future__ import annotations

import json
import struct
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver
from space.startability import (
    gate,
    phi_tech_cosine,
    phi_tech_cpc_jaccard,
    phi_tech_distance,
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


def tech_fit(
    store: PatentStore,
    resolver: EntityResolver,
    firm_query: str,
    tech_query_or_cluster_id: str,
    year: int = 2024,
) -> dict[str, Any]:
    """Compute phi_tech fit components for a firm and a technology cluster.

    v2: Falls back to pre-computed startability_surface when vectors
    are unavailable, and auto-detects the best available year.
    """
    resolved = resolver.resolve(firm_query, country_hint="JP")
    if resolved is None:
        return {
            "error": f"Could not resolve firm: '{firm_query}'",
            "suggestion": "Try the exact company name, Japanese name, or stock ticker",
        }

    firm_id = resolved.entity.canonical_id

    with store._conn() as conn:
        cluster_row = _resolve_cluster(conn, tech_query_or_cluster_id)
        if cluster_row is None:
            return {
                "error": f"No tech cluster found for query: '{tech_query_or_cluster_id}'"
            }

        cluster_id = cluster_row["cluster_id"]

        # Try to get vectors for on-the-fly computation
        firm_vec_row = conn.execute(
            "SELECT tech_vector FROM firm_tech_vectors WHERE firm_id = ? AND year = ?",
            (firm_id, year),
        ).fetchone()

        # Year fallback for firm_tech_vectors
        actual_year = year
        if firm_vec_row is None:
            best_row = conn.execute(
                "SELECT MAX(year) as y FROM firm_tech_vectors WHERE firm_id = ?",
                (firm_id,),
            ).fetchone()
            best = best_row["y"] if best_row and best_row["y"] else None
            if best and best != year:
                actual_year = best
                firm_vec_row = conn.execute(
                    "SELECT tech_vector FROM firm_tech_vectors WHERE firm_id = ? AND year = ?",
                    (firm_id, actual_year),
                ).fetchone()

        firm_vec = _unpack_blob(firm_vec_row["tech_vector"]) if firm_vec_row else None
        cluster_vec = _unpack_blob(cluster_row["center_vector"])

        # If both vectors available, compute on-the-fly
        if firm_vec is not None and cluster_vec is not None:
            firm_cpc_codes = _firm_cpc_codes(conn, firm_id)
            cluster_cpc_codes = _parse_json_codes(cluster_row["cpc_codes"])

            phi_cos = float(phi_tech_cosine(firm_vec, cluster_vec))
            phi_dist = float(phi_tech_distance(firm_vec, cluster_vec))
            phi_cpc = float(phi_tech_cpc_jaccard(cluster_cpc_codes, firm_cpc_codes))
            gate_open = bool(gate(phi_cos, phi_cpc, 0.0))

            explanation = (
                f"Tech fit for {firm_id} vs {cluster_id}: "
                f"cosine={phi_cos:.3f}, distance={phi_dist:.3f}, cpc_jaccard={phi_cpc:.3f}. "
                f"Gate is {'open' if gate_open else 'closed'} based on phi_tech thresholds."
            )

            return {
                "firm_id": firm_id,
                "cluster_id": cluster_id,
                "year": actual_year,
                "phi_tech_cosine": phi_cos,
                "phi_tech_distance": phi_dist,
                "phi_tech_cpc_jaccard": phi_cpc,
                "gate_open": gate_open,
                "data_source": "computed",
                "explanation": explanation,
            }

        # Fallback: use pre-computed startability_surface
        ss_row = conn.execute(
            """
            SELECT score, gate_open, phi_tech_cos, phi_tech_dist, phi_tech_cpc, year
            FROM startability_surface
            WHERE firm_id = ? AND cluster_id = ? AND year = ?
            """,
            (firm_id, cluster_id, actual_year),
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
                "error": f"No tech fit data available for {firm_id} x {cluster_id}",
                "firm_id": firm_id,
                "cluster_id": cluster_id,
                "year": year,
                "suggestion": "This firm-technology pair may not have been pre-computed.",
            }

        phi_cos = ss_row["phi_tech_cos"]
        phi_dist = ss_row["phi_tech_dist"]
        phi_cpc = ss_row["phi_tech_cpc"]
        gate_open = bool(ss_row["gate_open"])
        data_year = ss_row["year"]

        explanation = (
            f"Tech fit for {firm_id} vs {cluster_id} (pre-computed, year={data_year}): "
            f"cosine={phi_cos:.3f}, distance={phi_dist:.3f}, cpc_jaccard={phi_cpc:.3f}. "
            f"Gate is {'open' if gate_open else 'closed'}."
        )

        return {
            "firm_id": firm_id,
            "cluster_id": cluster_id,
            "year": data_year,
            "phi_tech_cosine": phi_cos,
            "phi_tech_distance": phi_dist,
            "phi_tech_cpc_jaccard": phi_cpc,
            "gate_open": gate_open,
            "data_source": "startability_surface",
            "explanation": explanation,
        }
