"""tech_clusters_list tool implementation."""
from __future__ import annotations

import json
import math
from typing import Any

from db.sqlite_store import PatentStore
from tools.pagination import paginate


_ALLOWED_SORT = {"patent_count", "growth_rate"}


def _parse_json_list(raw: str | None) -> list[Any]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        return []


def tech_clusters_list(
    store: PatentStore,
    sort_by: str = "patent_count",
    top_n: int = 200,
    cpc_filter: str | None = None,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """List technology clusters with optional CPC filtering and ranking."""
    sort_col = sort_by if sort_by in _ALLOWED_SORT else "patent_count"
    top_n = max(1, min(int(top_n), 2000))

    sql = """
        SELECT cluster_id, label, cpc_class, patent_count, growth_rate,
               top_applicants, top_terms
        FROM tech_clusters
    """
    params: list[Any] = []

    if cpc_filter:
        sql += " WHERE cpc_class LIKE ? || '%'"
        params.append(cpc_filter)

    sql += f" ORDER BY {sort_col} DESC LIMIT ?"
    params.append(top_n)

    with store._conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    clusters = []
    for row in rows:
        clusters.append(
            {
                "cluster_id": row["cluster_id"],
                "label": row["label"],
                "cpc_class": row["cpc_class"],
                "patent_count": row["patent_count"],
                "growth_rate": row["growth_rate"],
                "top_applicants": _parse_json_list(row["top_applicants"]),
                "top_terms": _parse_json_list(row["top_terms"]),
            }
        )

    paged = paginate(clusters, page=page, page_size=page_size)
    page_size_clamped = paged["page_size"]
    pages = math.ceil(len(clusters) / page_size_clamped) if clusters else 1
    top_applicants = []
    for c in clusters[:5]:
        for a in c.get("top_applicants", []):
            top_applicants.append({"name": str(a), "firm_id": None, "count": c["patent_count"]})
            if len(top_applicants) >= 5:
                break
        if len(top_applicants) >= 5:
            break

    return {
        "sort_by": sort_col,
        "top_n": top_n,
        "cpc_filter": cpc_filter,
        "total": len(clusters),
        "page": paged["page"],
        "page_size": page_size_clamped,
        "pages": pages,
        "results": paged["results"],
        "summary": {
            "top_applicants": top_applicants,
            "date_range": {"earliest": None, "latest": None},
            "cpc_distribution": [
                {"cpc_class": c["cpc_class"], "count": c["patent_count"]}
                for c in sorted(clusters, key=lambda x: x["patent_count"], reverse=True)[:10]
            ],
        },
        "clusters": paged["results"],
    }
