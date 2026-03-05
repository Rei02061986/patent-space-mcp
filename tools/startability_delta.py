"""Startability Delta tool implementation.

v4: Fix delta=0 bug — increase precision, filter negligible deltas,
properly separate gainers/losers, add diagnostic when data lacks variation.
"""
from __future__ import annotations

import math
import sqlite3
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver
from tools.pagination import paginate

# Deltas below this threshold are considered negligible (data noise)
_DELTA_THRESHOLD = 1e-4


def startability_delta(
    store: PatentStore,
    resolver: EntityResolver,
    mode: str,
    query: str,
    year_from: int = 2020,
    year_to: int = 2023,
    top_n: int = 20,
    direction: str = "gainers",
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """Compute startability delta (change over time).

    Args:
        mode: "by_firm" (show cluster deltas for one firm) or
              "by_tech" (show firm deltas for one cluster).
        query: Firm name/ticker (by_firm) or cluster_id/CPC (by_tech).
        year_from: Start year for delta calculation.
        year_to: End year for delta calculation.
        top_n: Number of results to return.
        direction: "gainers", "losers", or "both".
    """
    top_n = max(1, min(int(top_n), 200))

    if mode not in {"by_firm", "by_tech"}:
        return {"error": "mode must be 'by_firm' or 'by_tech'"}

    if direction not in {"gainers", "losers", "both"}:
        return {"error": "direction must be 'gainers', 'losers', or 'both'"}

    conn = store._conn()

    if mode == "by_firm":
        return _by_firm(conn, resolver, query, year_from, year_to, top_n, direction, page, page_size)
    else:
        store._relax_timeout()
        return _by_tech(conn, query, year_from, year_to, top_n, direction, page, page_size)


def _compute_deltas(start_rows, end_rows, id_key: str) -> tuple[list[dict], int]:
    """Compute deltas between start and end rows.
    
    Returns (deltas_list, negligible_count).
    deltas_list only contains entries with abs(delta) >= threshold.
    """
    start_map = {r[id_key]: (r["score"], r["gate_open"]) for r in start_rows}
    end_map = {r[id_key]: (r["score"], r["gate_open"]) for r in end_rows}

    all_ids = set(start_map.keys()) | set(end_map.keys())
    deltas = []
    negligible_count = 0
    
    for rid in all_ids:
        s_start, go_start = start_map.get(rid, (0.0, 0))
        s_end, go_end = end_map.get(rid, (0.0, 0))
        delta = s_end - s_start
        
        if abs(delta) < _DELTA_THRESHOLD:
            negligible_count += 1
            continue
            
        deltas.append({
            id_key: rid,
            "score_start": round(s_start, 6),
            "score_end": round(s_end, 6),
            "delta": round(delta, 6),
            "gate_open_start": bool(go_start),
            "gate_open_end": bool(go_end),
        })
    
    return deltas, negligible_count


def _filter_by_direction(deltas: list[dict], direction: str, top_n: int) -> tuple[list[dict], list[dict] | None]:
    """Filter and sort deltas by direction.
    
    Returns (results, losers_or_none).
    For direction="both", returns (gainers, losers).
    For "gainers" or "losers", returns (results, None).
    """
    if direction == "gainers":
        filtered = [d for d in deltas if d["delta"] > 0]
        filtered.sort(key=lambda x: x["delta"], reverse=True)
        return filtered[:top_n], None
    elif direction == "losers":
        filtered = [d for d in deltas if d["delta"] < 0]
        filtered.sort(key=lambda x: x["delta"])
        return filtered[:top_n], None
    else:  # both
        gainers = [d for d in deltas if d["delta"] > 0]
        gainers.sort(key=lambda x: x["delta"], reverse=True)
        losers = [d for d in deltas if d["delta"] < 0]
        losers.sort(key=lambda x: x["delta"])
        return gainers[:top_n], losers[:top_n]


def _build_diagnostic(negligible_count: int, total_count: int, year_from: int, year_to: int) -> dict | None:
    """Build diagnostic message when most deltas are negligible."""
    if negligible_count == 0:
        return None
    if negligible_count == total_count:
        return {
            "warning": "no_meaningful_change",
            "message": (
                f"All {total_count} entries had negligible delta (<{_DELTA_THRESHOLD}) "
                f"between {year_from} and {year_to}. "
                "The pre-computed startability surface may lack year-specific variation "
                "for this period. Try a wider range (e.g., 2016-2024) or different year endpoints."
            ),
        }
    if negligible_count > total_count * 0.8:
        return {
            "note": f"{negligible_count}/{total_count} entries had negligible delta and were filtered out.",
        }
    return None


def _by_firm(
    conn, resolver, query, year_from, year_to, top_n, direction, page, page_size,
) -> dict[str, Any]:
    """Delta by firm: which clusters did this firm gain/lose most in?"""
    resolved = resolver.resolve(query, country_hint="JP")
    if resolved is None:
        return {
            "error": f"Could not resolve firm: '{query}'",
            "suggestion": "Try the exact company name, Japanese name, or stock ticker",
        }
    firm_id = resolved.entity.canonical_id

    try:
        start_rows = conn.execute(
            "SELECT cluster_id, score, gate_open FROM startability_surface WHERE firm_id = ? AND year = ?",
            (firm_id, year_from),
        ).fetchall()
    except sqlite3.OperationalError as e:
        if "interrupted" in str(e):
            raise
        start_rows = []

    try:
        end_rows = conn.execute(
            "SELECT cluster_id, score, gate_open FROM startability_surface WHERE firm_id = ? AND year = ?",
            (firm_id, year_to),
        ).fetchall()
    except sqlite3.OperationalError as e:
        if "interrupted" in str(e):
            raise
        end_rows = []

    if not start_rows and not end_rows:
        available = conn.execute(
            "SELECT DISTINCT year FROM startability_surface WHERE firm_id = ? ORDER BY year",
            (firm_id,),
        ).fetchall()
        return {
            "error": f"Need data for both {year_from} and {year_to}",
            "available_years": [r["year"] for r in available],
        }

    total_count = len(set(r["cluster_id"] for r in start_rows) | set(r["cluster_id"] for r in end_rows))
    deltas, negligible_count = _compute_deltas(start_rows, end_rows, "cluster_id")
    diagnostic = _build_diagnostic(negligible_count, total_count, year_from, year_to)

    if direction == "both":
        gainers, losers = _filter_by_direction(deltas, direction, top_n)
        gainers_paged = paginate(gainers, page=page, page_size=page_size)
        losers_paged = paginate(losers, page=page, page_size=page_size)
        page_size_clamped = gainers_paged["page_size"]
        result = {
            "endpoint": "startability_delta",
            "mode": "by_firm",
            "firm_id": firm_id,
            "year_from": year_from,
            "year_to": year_to,
            "gainers_count": len(gainers),
            "losers_count": len(losers),
            "negligible_filtered": negligible_count,
            "page": gainers_paged["page"],
            "page_size": page_size_clamped,
            "pages": math.ceil(len(gainers) / page_size_clamped) if gainers else 1,
            "gainers": gainers_paged["results"],
            "losers": losers_paged["results"],
            "results": gainers_paged["results"],
            "summary": {
                "top_applicants": [],
                "date_range": {"earliest": year_from, "latest": year_to},
                "cpc_distribution": [],
            },
        }
        if diagnostic:
            result["diagnostic"] = diagnostic
        return result

    results, _ = _filter_by_direction(deltas, direction, top_n)
    paged = paginate(results, page=page, page_size=page_size)
    page_size_clamped = paged["page_size"]
    result = {
        "endpoint": "startability_delta",
        "mode": "by_firm",
        "firm_id": firm_id,
        "year_from": year_from,
        "year_to": year_to,
        "direction": direction,
        "total": len(results),
        "negligible_filtered": negligible_count,
        "page": paged["page"],
        "page_size": page_size_clamped,
        "pages": math.ceil(len(results) / page_size_clamped) if results else 1,
        "results": paged["results"],
        "summary": {
            "top_applicants": [],
            "date_range": {"earliest": year_from, "latest": year_to},
            "cpc_distribution": [],
        },
    }
    if diagnostic:
        result["diagnostic"] = diagnostic
    return result


def _by_tech(
    conn, query, year_from, year_to, top_n, direction, page, page_size,
) -> dict[str, Any]:
    """Delta by tech: which firms gained/lost most in this cluster?"""
    q = (query or "").strip()
    looks_like_id = "_" in q and len(q) <= 40

    if looks_like_id:
        cluster_row = conn.execute(
            "SELECT cluster_id FROM tech_clusters WHERE cluster_id = ?",
            (q,),
        ).fetchone()
    else:
        like = f"%{q}%"
        cluster_row = conn.execute(
            """
            SELECT cluster_id FROM tech_clusters
            WHERE label LIKE ? OR cpc_class LIKE ? || '%'
            ORDER BY patent_count DESC LIMIT 1
            """,
            (like, q),
        ).fetchone()

    if cluster_row is None:
        return {"error": f"No tech cluster found for query: '{query}'"}

    cluster_id = cluster_row["cluster_id"]

    try:
        start_rows = conn.execute(
            "SELECT firm_id, score, gate_open FROM startability_surface WHERE cluster_id = ? AND year = ?",
            (cluster_id, year_from),
        ).fetchall()
    except sqlite3.OperationalError as e:
        if "interrupted" in str(e):
            raise
        start_rows = []

    try:
        end_rows = conn.execute(
            "SELECT firm_id, score, gate_open FROM startability_surface WHERE cluster_id = ? AND year = ?",
            (cluster_id, year_to),
        ).fetchall()
    except sqlite3.OperationalError as e:
        if "interrupted" in str(e):
            raise
        end_rows = []

    total_count = len(set(r["firm_id"] for r in start_rows) | set(r["firm_id"] for r in end_rows))
    deltas, negligible_count = _compute_deltas(start_rows, end_rows, "firm_id")
    diagnostic = _build_diagnostic(negligible_count, total_count, year_from, year_to)

    if direction == "both":
        gainers, losers = _filter_by_direction(deltas, direction, top_n)
        gainers_paged = paginate(gainers, page=page, page_size=page_size)
        losers_paged = paginate(losers, page=page, page_size=page_size)
        page_size_clamped = gainers_paged["page_size"]
        result = {
            "endpoint": "startability_delta",
            "mode": "by_tech",
            "cluster_id": cluster_id,
            "year_from": year_from,
            "year_to": year_to,
            "gainers_count": len(gainers),
            "losers_count": len(losers),
            "negligible_filtered": negligible_count,
            "page": gainers_paged["page"],
            "page_size": page_size_clamped,
            "pages": math.ceil(len(gainers) / page_size_clamped) if gainers else 1,
            "gainers": gainers_paged["results"],
            "losers": losers_paged["results"],
            "results": gainers_paged["results"],
            "summary": {
                "top_applicants": [
                    {"firm_id": r["firm_id"], "count": r["delta"]}
                    for r in sorted(gainers, key=lambda x: x["delta"], reverse=True)[:5]
                ],
                "date_range": {"earliest": year_from, "latest": year_to},
                "cpc_distribution": [],
            },
        }
        if diagnostic:
            result["diagnostic"] = diagnostic
        return result

    results, _ = _filter_by_direction(deltas, direction, top_n)
    paged = paginate(results, page=page, page_size=page_size)
    page_size_clamped = paged["page_size"]
    result = {
        "endpoint": "startability_delta",
        "mode": "by_tech",
        "cluster_id": cluster_id,
        "year_from": year_from,
        "year_to": year_to,
        "direction": direction,
        "total": len(results),
        "negligible_filtered": negligible_count,
        "page": paged["page"],
        "page_size": page_size_clamped,
        "pages": math.ceil(len(results) / page_size_clamped) if results else 1,
        "results": paged["results"],
        "summary": {
            "top_applicants": [
                {"firm_id": r["firm_id"], "count": r["delta"]}
                for r in sorted(results, key=lambda x: x["delta"], reverse=True)[:5]
            ],
            "date_range": {"earliest": year_from, "latest": year_to},
            "cpc_distribution": [],
        },
    }
    if diagnostic:
        result["diagnostic"] = diagnostic
    return result
