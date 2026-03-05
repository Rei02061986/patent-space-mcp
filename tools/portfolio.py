"""firm_patent_portfolio tool implementation."""
from __future__ import annotations

from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver


def firm_patent_portfolio(
    store: PatentStore,
    resolver: EntityResolver,
    firm: str,
    date: str | None = None,
    include_expired: bool = False,
    detail_patents: bool = False,
) -> dict[str, Any]:
    """Get patent portfolio for a firm."""
    # Resolve firm name to canonical entity
    result = resolver.resolve(firm, country_hint="JP")
    if result is None:
        return {
            "error": f"Could not resolve firm: '{firm}'",
            "suggestion": "Try the exact company name, Japanese name, or stock ticker",
        }

    entity = result.entity

    # Get portfolio from SQLite
    date_to = int(date.replace("-", "")) if date else None
    portfolio = store.get_firm_portfolio(
        firm_id=entity.canonical_id,
        date_to=date_to,
    )

    patents: list[dict[str, Any]] = []
    if detail_patents and portfolio["count"] <= 1000:
        with store._conn() as conn:
            date_cond = " AND p.publication_date <= ?" if date_to else ""
            params: list[Any] = [entity.canonical_id]
            if date_to:
                params.append(date_to)
            rows = conn.execute(
                f"""
                SELECT p.publication_number, p.title_ja, p.title_en,
                       p.filing_date, p.publication_date, p.grant_date
                FROM patents p
                JOIN patent_assignees a ON p.publication_number = a.publication_number
                WHERE a.firm_id = ? {date_cond}
                GROUP BY p.publication_number
                ORDER BY p.publication_date DESC
                LIMIT 1000
                """,
                params,
            ).fetchall()
            patents = [dict(r) for r in rows]

    return {
        "entity": {
            "canonical_id": entity.canonical_id,
            "canonical_name": entity.canonical_name,
            "country_code": entity.country_code,
            "industry": entity.industry,
            "ticker": entity.ticker,
            "tse_section": entity.tse_section,
            "resolution_confidence": result.confidence,
            "match_level": result.match_level,
        },
        "patent_count": portfolio["count"],
        "cpc_distribution": portfolio["cpc_distribution"],
        "top_technologies": portfolio["top_technologies"],
        "co_applicants": portfolio["co_applicants"],
        "filing_trend": portfolio["filing_trend"],
        "patents": patents if detail_patents and portfolio["count"] <= 1000 else [],
        "summary": {
            "total_patents": portfolio["count"],
            "top_applicants": [
                {"name": row["name"], "firm_id": None, "count": row["count"]}
                for row in portfolio["co_applicants"][:5]
            ],
            "date_range": {
                "earliest": min([r["year"] for r in portfolio["filing_trend"]], default=None),
                "latest": max([r["year"] for r in portfolio["filing_trend"]], default=None),
            },
            "cpc_distribution": [
                {"cpc_class": row["code"], "count": row["count"]}
                for row in portfolio["cpc_distribution"][:20]
            ],
            "detail_patents_included": bool(detail_patents and portfolio["count"] <= 1000),
        },
    }
