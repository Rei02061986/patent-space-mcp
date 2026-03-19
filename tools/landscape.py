"""tech_landscape tool implementation."""
from __future__ import annotations

import sqlite3

import math
from typing import Any

from db.sqlite_store import PatentStore
from tools.pagination import paginate


def tech_landscape(
    store: PatentStore,
    cpc_prefix: str | None = None,
    query: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    granularity: str = "year",
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """Analyze technology landscape by CPC trend, applicants, and growth."""
    date_from_int = int(date_from.replace("-", "")) if date_from else None
    date_to_int = int(date_to.replace("-", "")) if date_to else None

    if granularity not in {"year", "quarter"}:
        granularity = "year"

    try:
        total_patents = store.count(
            query=query,
            cpc_prefix=cpc_prefix,
            date_from=date_from_int,
            date_to=date_to_int,
        )
    except sqlite3.OperationalError as e:
        if "interrupt" in str(e).lower():
            total_patents = -1  # unknown due to timeout
        else:
            raise

    try:
        cpc_trend = store.get_cpc_trend(
            cpc_prefix=cpc_prefix,
            date_from=date_from_int,
            date_to=date_to_int,
            granularity=granularity,
            query=query,
        )
    except sqlite3.OperationalError as e:
        if "interrupt" in str(e).lower():
            cpc_trend = []  # partial: no trend data
        else:
            raise

    try:
        top_rows = store.get_top_applicants_for_cpc(
            cpc_prefix=cpc_prefix,
            date_from=date_from_int,
            date_to=date_to_int,
            limit=20,
            query=query,
        )
    except sqlite3.OperationalError as e:
        if "interrupt" in str(e).lower():
            top_rows = []  # partial: no applicant data
        else:
            raise
    top_applicants = []
    for row in top_rows:
        share = 0.0
        if total_patents > 0:
            share = round((row["count"] * 100.0) / total_patents, 2)
        top_applicants.append(
            {
                "name": row["name"],
                "firm_id": row["firm_id"],
                "count": row["count"],
                "share_pct": share,
            }
        )

    growth_areas: list[dict[str, Any]] = []
    if cpc_trend:
        yearly = [r for r in cpc_trend if isinstance(r["period"], int)]
        if yearly:
            max_year = max(r["period"] for r in yearly)
            recent_from = max_year - 4
            prev_from = max_year - 9
            prev_to = max_year - 5

            by_cpc: dict[str, dict[int, int]] = {}
            for row in yearly:
                cpc_class = row["cpc_class"]
                period = row["period"]
                if cpc_class not in by_cpc:
                    by_cpc[cpc_class] = {}
                by_cpc[cpc_class][period] = by_cpc[cpc_class].get(period, 0) + row[
                    "count"
                ]

            for cpc_class, years in by_cpc.items():
                recent_count = sum(
                    cnt
                    for year, cnt in years.items()
                    if recent_from <= year <= max_year
                )
                previous_count = sum(
                    cnt
                    for year, cnt in years.items()
                    if prev_from <= year <= prev_to
                )
                if recent_count == 0 and previous_count == 0:
                    continue

                if previous_count == 0:
                    growth_rate = None
                else:
                    growth_rate = round(
                        (recent_count - previous_count) / previous_count,
                        3,
                    )

                growth_areas.append(
                    {
                        "cpc_class": cpc_class,
                        "growth_rate": growth_rate,
                        "recent_count": recent_count,
                        "previous_count": previous_count,
                    }
                )

            growth_areas.sort(
                key=lambda x: (x["growth_rate"] is None, -(x["growth_rate"] or 0.0), -x["recent_count"]),
            )

    paged = paginate(cpc_trend, page=page, page_size=page_size)
    page_size_clamped = paged["page_size"]
    pages = math.ceil(len(cpc_trend) / page_size_clamped) if cpc_trend else 1

    summary = {
        "total_patents": total_patents,
        "top_applicants": top_applicants[:5],
        "date_range": {
            "earliest": date_from_int,
            "latest": date_to_int,
        },
        "cpc_distribution": [
            {"cpc_class": row["cpc_class"], "count": row["count"]}
            for row in sorted(cpc_trend, key=lambda r: r["count"], reverse=True)[:10]
        ],
    }

    return {
        "total": len(cpc_trend),
        "page": paged["page"],
        "page_size": page_size_clamped,
        "pages": pages,
        "results": paged["results"],
        "summary": summary,
        "cpc_trend": paged["results"],
        "top_applicants": top_applicants,
        "growth_areas": growth_areas,
        "total_patents": total_patents,
    }
