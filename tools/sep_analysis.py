"""Standard Essential Patent (SEP) analysis tools.

Provides search, landscape, portfolio, and FRAND analysis over the
sep_declarations table.  All four functions are called from server.py
via ``_safe_call(fn, store=_store, resolver=_resolver, ...)``.
"""
from __future__ import annotations

import sqlite3
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver
from tools.pagination import paginate

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_FRAND_DISCLAIMER = (
    "This analysis is based on declared SEPs only. Actual essentiality "
    "and FRAND terms require legal review."
)


def _hhi(shares: list[float]) -> float:
    """Compute Herfindahl-Hirschman Index from a list of fractional shares."""
    return round(sum(s * s for s in shares), 4)


def _concentration_label(hhi: float) -> str:
    if hhi < 0.15:
        return "competitive"
    if hhi <= 0.25:
        return "moderate"
    return "concentrated"


def _top_n_share(counts: list[int], n: int) -> float:
    """Return the combined share of the top-*n* entries."""
    total = sum(counts)
    if total == 0:
        return 0.0
    top = sum(sorted(counts, reverse=True)[:n])
    return round(top / total, 4)


def _enrich_with_titles(
    conn: sqlite3.Connection,
    declarations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Batch LEFT JOIN patent titles from the patents table."""
    pub_numbers = [
        d["publication_number"]
        for d in declarations
        if d.get("publication_number")
    ]
    if not pub_numbers:
        return declarations

    title_map: dict[str, dict[str, str | None]] = {}
    for i in range(0, len(pub_numbers), 500):
        batch = pub_numbers[i : i + 500]
        ph = ",".join("?" * len(batch))
        try:
            rows = conn.execute(
                f"SELECT publication_number, title_ja, title_en "
                f"FROM patents WHERE publication_number IN ({ph})",
                batch,
            ).fetchall()
            for r in rows:
                title_map[r["publication_number"]] = {
                    "title_ja": r["title_ja"],
                    "title_en": r["title_en"],
                }
        except sqlite3.OperationalError:
            pass

    for d in declarations:
        pub = d.get("publication_number")
        if pub and pub in title_map:
            d["title_ja"] = title_map[pub].get("title_ja")
            d["title_en"] = title_map[pub].get("title_en")
        else:
            d["title_ja"] = None
            d["title_en"] = None
    return declarations


# ---------------------------------------------------------------------------
# Tool 1: sep_search
# ---------------------------------------------------------------------------

def sep_search(
    store: PatentStore,
    query: str | None = None,
    standard: str | None = None,
    declarant: str | None = None,
    patent_number: str | None = None,
    max_results: int = 20,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """Search SEP declarations with optional filters (AND logic).

    ``query`` performs a LIKE match on standard_name, declarant,
    technical_area, and patent_number simultaneously.
    """
    page = max(int(page), 1)
    page_size = min(max(int(page_size), 1), 100)
    max_results = max(1, int(max_results))

    conditions: list[str] = []
    params: list[Any] = []

    if query:
        conditions.append(
            "(standard_name LIKE '%' || ? || '%' "
            "OR declarant LIKE '%' || ? || '%' "
            "OR technical_area LIKE '%' || ? || '%' "
            "OR patent_number LIKE '%' || ? || '%')"
        )
        params.extend([query, query, query, query])

    if standard:
        conditions.append("standard_name LIKE '%' || ? || '%'")
        params.append(standard)

    if declarant:
        conditions.append("declarant LIKE '%' || ? || '%'")
        params.append(declarant)

    if patent_number:
        conditions.append("patent_number LIKE '%' || ? || '%'")
        params.append(patent_number)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    filters_applied = {
        "query": query,
        "standard": standard,
        "declarant": declarant,
        "patent_number": patent_number,
    }

    conn = store._conn()

    try:
        count_row = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM sep_declarations {where}",
            params,
        ).fetchone()
        total = count_row["cnt"] if count_row else 0

        limit = max(max_results, page * page_size)
        rows = conn.execute(
            f"SELECT declaration_id, patent_number, standard_name, "
            f"standard_org, sso_project, declarant, declaration_date, "
            f"technical_area, publication_number "
            f"FROM sep_declarations {where} "
            f"ORDER BY declaration_date DESC, declaration_id DESC "
            f"LIMIT ?",
            params + [limit],
        ).fetchall()
    except sqlite3.OperationalError:
        return {
            "endpoint": "sep_search",
            "error": "Query timed out. Try narrower filters.",
            "filters_applied": filters_applied,
        }

    all_results = [dict(r) for r in rows]

    # Enrich with patent titles
    all_results = _enrich_with_titles(conn, all_results)

    paged = paginate(all_results, page=page, page_size=page_size)

    return {
        "endpoint": "sep_search",
        "declarations": paged["results"],
        "total": total,
        "page": paged["page"],
        "page_size": paged["page_size"],
        "pages": paged["pages"],
        "filters_applied": filters_applied,
    }


# ---------------------------------------------------------------------------
# Tool 2: sep_landscape
# ---------------------------------------------------------------------------

def sep_landscape(
    store: PatentStore,
    standard: str | None = None,
    standard_org: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """Landscape view of SEP declarations.

    If *standard* is given, drills into that standard (top declarants,
    yearly trend, tech areas).  Otherwise returns an overview across all
    standards.
    """
    page = max(int(page), 1)
    page_size = min(max(int(page_size), 1), 100)

    conn = store._conn()

    # ----- date filter fragment -----
    date_conditions: list[str] = []
    date_params: list[Any] = []
    if date_from:
        date_conditions.append("declaration_date >= ?")
        date_params.append(date_from)
    if date_to:
        date_conditions.append("declaration_date <= ?")
        date_params.append(date_to)
    if standard_org:
        date_conditions.append("standard_org = ?")
        date_params.append(standard_org)

    date_where = (" AND ".join(date_conditions)) if date_conditions else ""

    try:
        if standard:
            # ---- Drill-down for a specific standard ----
            std_cond = "standard_name LIKE '%' || ? || '%'"
            base_params: list[Any] = [standard]
            full_where = f"WHERE {std_cond}"
            if date_where:
                full_where += f" AND {date_where}"
                base_params.extend(date_params)

            # Total declarations for this standard
            total_row = conn.execute(
                f"SELECT COUNT(*) AS cnt FROM sep_declarations {full_where}",
                base_params,
            ).fetchone()
            total = total_row["cnt"] if total_row else 0

            # Top declarants
            top_declarant_rows = conn.execute(
                f"SELECT declarant, COUNT(*) AS cnt "
                f"FROM sep_declarations {full_where} "
                f"GROUP BY declarant ORDER BY cnt DESC LIMIT 50",
                base_params,
            ).fetchall()
            declarant_counts = [r["cnt"] for r in top_declarant_rows]
            shares = [c / total for c in declarant_counts] if total > 0 else []
            hhi = _hhi(shares)
            top3_share = _top_n_share(declarant_counts, 3)

            top_declarants = [
                {
                    "declarant": r["declarant"],
                    "declaration_count": r["cnt"],
                    "share_pct": round(r["cnt"] / total * 100, 2) if total > 0 else 0.0,
                }
                for r in top_declarant_rows
            ]

            # Yearly trend
            trend_rows = conn.execute(
                f"SELECT SUBSTR(declaration_date, 1, 4) AS year, "
                f"COUNT(*) AS cnt "
                f"FROM sep_declarations {full_where} "
                f"AND declaration_date IS NOT NULL "
                f"GROUP BY year ORDER BY year",
                base_params,
            ).fetchall()
            declaration_trend = [
                {"year": r["year"], "count": r["cnt"]} for r in trend_rows
            ]

            # Technical areas
            area_rows = conn.execute(
                f"SELECT technical_area, COUNT(*) AS cnt "
                f"FROM sep_declarations {full_where} "
                f"AND technical_area IS NOT NULL "
                f"GROUP BY technical_area ORDER BY cnt DESC LIMIT 20",
                base_params,
            ).fetchall()
            tech_areas = [
                {"technical_area": r["technical_area"], "count": r["cnt"]}
                for r in area_rows
            ]

            paged = paginate(top_declarants, page=page, page_size=page_size)

            return {
                "endpoint": "sep_landscape",
                "mode": "standard_detail",
                "standard": standard,
                "total_declarations": total,
                "standard_summary": [
                    {"standard_name": standard, "declaration_count": total}
                ],
                "top_declarants": paged["results"],
                "declaration_trend": declaration_trend,
                "technical_areas": tech_areas,
                "concentration": {
                    "hhi": hhi,
                    "hhi_label": _concentration_label(hhi),
                    "top3_share": top3_share,
                },
                "page": paged["page"],
                "page_size": paged["page_size"],
                "pages": paged["pages"],
            }

        else:
            # ---- Overview: counts per standard ----
            overview_where = f"WHERE {date_where}" if date_where else ""

            std_rows = conn.execute(
                f"SELECT standard_name, COUNT(*) AS cnt "
                f"FROM sep_declarations {overview_where} "
                f"GROUP BY standard_name ORDER BY cnt DESC",
                date_params,
            ).fetchall()
            standard_summary = [
                {"standard_name": r["standard_name"], "declaration_count": r["cnt"]}
                for r in std_rows
            ]

            # Global top declarants
            global_total_row = conn.execute(
                f"SELECT COUNT(*) AS cnt FROM sep_declarations {overview_where}",
                date_params,
            ).fetchone()
            global_total = global_total_row["cnt"] if global_total_row else 0

            global_declarant_rows = conn.execute(
                f"SELECT declarant, COUNT(*) AS cnt "
                f"FROM sep_declarations {overview_where} "
                f"GROUP BY declarant ORDER BY cnt DESC LIMIT 50",
                date_params,
            ).fetchall()
            declarant_counts = [r["cnt"] for r in global_declarant_rows]
            shares = (
                [c / global_total for c in declarant_counts]
                if global_total > 0
                else []
            )
            hhi = _hhi(shares)
            top3_share = _top_n_share(declarant_counts, 3)

            top_declarants = [
                {
                    "declarant": r["declarant"],
                    "declaration_count": r["cnt"],
                    "share_pct": round(
                        r["cnt"] / global_total * 100, 2
                    ) if global_total > 0 else 0.0,
                }
                for r in global_declarant_rows
            ]

            # Global yearly trend
            if overview_where:
                trend_where = f"{overview_where} AND declaration_date IS NOT NULL"
            else:
                trend_where = "WHERE declaration_date IS NOT NULL"
            trend_rows = conn.execute(
                f"SELECT SUBSTR(declaration_date, 1, 4) AS year, "
                f"COUNT(*) AS cnt "
                f"FROM sep_declarations {trend_where} "
                f"GROUP BY year ORDER BY year",
                date_params,
            ).fetchall()
            declaration_trend = [
                {"year": r["year"], "count": r["cnt"]} for r in trend_rows
            ]

            paged = paginate(standard_summary, page=page, page_size=page_size)

            return {
                "endpoint": "sep_landscape",
                "mode": "overview",
                "total_declarations": global_total,
                "standard_summary": paged["results"],
                "top_declarants": top_declarants[:20],
                "declaration_trend": declaration_trend,
                "concentration": {
                    "hhi": hhi,
                    "hhi_label": _concentration_label(hhi),
                    "top3_share": top3_share,
                },
                "page": paged["page"],
                "page_size": paged["page_size"],
                "pages": paged["pages"],
            }

    except sqlite3.OperationalError:
        return {
            "endpoint": "sep_landscape",
            "error": "Query timed out. Try a specific standard filter.",
        }


# ---------------------------------------------------------------------------
# Tool 3: sep_portfolio
# ---------------------------------------------------------------------------

def sep_portfolio(
    store: PatentStore,
    firm_query: str | None = None,
    resolver: EntityResolver | None = None,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """Analyse a firm's SEP declaration portfolio.

    Resolves the firm name via *resolver*, then queries sep_declarations
    with a LIKE match on the canonical name.
    """
    if not firm_query:
        return {
            "endpoint": "sep_portfolio",
            "error": "firm_query is required.",
        }

    page = max(int(page), 1)
    page_size = min(max(int(page_size), 1), 100)

    # Resolve firm
    search_name = firm_query
    firm_display = firm_query
    if resolver is not None:
        resolved = resolver.resolve(firm_query, country_hint="JP")
        if resolved is not None:
            search_name = resolved.entity.canonical_name
            firm_display = resolved.entity.canonical_name

    conn = store._conn()

    try:
        # Total declarations for this declarant
        total_row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM sep_declarations "
            "WHERE declarant LIKE '%' || ? || '%'",
            (search_name,),
        ).fetchone()
        total_declarations = total_row["cnt"] if total_row else 0

        if total_declarations == 0:
            # Try original query as fallback
            if search_name != firm_query:
                total_row = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM sep_declarations "
                    "WHERE declarant LIKE '%' || ? || '%'",
                    (firm_query,),
                ).fetchone()
                total_declarations = total_row["cnt"] if total_row else 0
                if total_declarations > 0:
                    search_name = firm_query

        if total_declarations == 0:
            return {
                "endpoint": "sep_portfolio",
                "firm_name": firm_display,
                "total_declarations": 0,
                "standards_covered": [],
                "yearly_trend": [],
                "peer_comparison": [],
                "note": (
                    f"No SEP declarations found for '{firm_query}'. "
                    "The declarant name in the database may differ."
                ),
            }

        # Group by standard
        std_rows = conn.execute(
            "SELECT standard_name, COUNT(*) AS cnt "
            "FROM sep_declarations "
            "WHERE declarant LIKE '%' || ? || '%' "
            "GROUP BY standard_name ORDER BY cnt DESC",
            (search_name,),
        ).fetchall()
        standards_covered = [
            {"standard_name": r["standard_name"], "declaration_count": r["cnt"]}
            for r in std_rows
        ]

        # Yearly trend
        trend_rows = conn.execute(
            "SELECT SUBSTR(declaration_date, 1, 4) AS year, "
            "COUNT(*) AS cnt "
            "FROM sep_declarations "
            "WHERE declarant LIKE '%' || ? || '%' "
            "AND declaration_date IS NOT NULL "
            "GROUP BY year ORDER BY year",
            (search_name,),
        ).fetchall()
        yearly_trend = [
            {"year": r["year"], "count": r["cnt"]} for r in trend_rows
        ]

        # Peer comparison: top 10 declarants in the same standards
        standard_names = [r["standard_name"] for r in std_rows]
        peer_comparison: list[dict[str, Any]] = []
        if standard_names:
            ph = ",".join("?" * len(standard_names))
            peer_rows = conn.execute(
                f"SELECT declarant, COUNT(*) AS cnt "
                f"FROM sep_declarations "
                f"WHERE standard_name IN ({ph}) "
                f"GROUP BY declarant ORDER BY cnt DESC LIMIT 11",
                standard_names,
            ).fetchall()
            for pr in peer_rows:
                peer_comparison.append({
                    "declarant": pr["declarant"],
                    "declaration_count": pr["cnt"],
                    "is_target": (
                        search_name.lower() in pr["declarant"].lower()
                    ),
                })
            # Ensure we return at most 10 peers (excluding a duplicate of
            # the target, if it appears twice due to fuzzy match)
            peer_comparison = peer_comparison[:10]

        paged = paginate(standards_covered, page=page, page_size=page_size)

        return {
            "endpoint": "sep_portfolio",
            "firm_name": firm_display,
            "total_declarations": total_declarations,
            "standards_covered": paged["results"],
            "yearly_trend": yearly_trend,
            "peer_comparison": peer_comparison,
            "page": paged["page"],
            "page_size": paged["page_size"],
            "pages": paged["pages"],
        }

    except sqlite3.OperationalError:
        return {
            "endpoint": "sep_portfolio",
            "firm_name": firm_display,
            "error": "Query timed out. Try again later.",
        }


# ---------------------------------------------------------------------------
# Tool 4: frand_analysis
# ---------------------------------------------------------------------------

def frand_analysis(
    store: PatentStore,
    standard: str | None = None,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """FRAND licensing landscape analysis for a standard.

    Computes HHI concentration, top holder shares, and a heuristic
    royalty-stack estimate based on declaration counts.
    """
    if not standard:
        return {
            "endpoint": "frand_analysis",
            "error": "standard parameter is required.",
            "disclaimer": _FRAND_DISCLAIMER,
        }

    page = max(int(page), 1)
    page_size = min(max(int(page_size), 1), 100)

    conn = store._conn()

    try:
        # Total declarations
        total_row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM sep_declarations "
            "WHERE standard_name LIKE '%' || ? || '%'",
            (standard,),
        ).fetchone()
        total_declarations = total_row["cnt"] if total_row else 0

        if total_declarations == 0:
            return {
                "endpoint": "frand_analysis",
                "standard": standard,
                "total_declarations": 0,
                "total_declarants": 0,
                "concentration": {},
                "top_holders": [],
                "licensing_landscape": {},
                "royalty_stack_estimate": {},
                "note": f"No declarations found for standard '{standard}'.",
                "disclaimer": _FRAND_DISCLAIMER,
            }

        # Declarant breakdown
        declarant_rows = conn.execute(
            "SELECT declarant, COUNT(*) AS cnt "
            "FROM sep_declarations "
            "WHERE standard_name LIKE '%' || ? || '%' "
            "GROUP BY declarant ORDER BY cnt DESC",
            (standard,),
        ).fetchall()
        total_declarants = len(declarant_rows)
        declarant_counts = [r["cnt"] for r in declarant_rows]

        # Concentration metrics
        shares = (
            [c / total_declarations for c in declarant_counts]
            if total_declarations > 0
            else []
        )
        hhi = _hhi(shares)
        top3_share = _top_n_share(declarant_counts, 3)
        top5_share = _top_n_share(declarant_counts, 5)
        top10_share = _top_n_share(declarant_counts, 10)
        label = _concentration_label(hhi)

        concentration = {
            "hhi": hhi,
            "hhi_label": label,
            "top3_share": top3_share,
            "top5_share": top5_share,
            "top10_share": top10_share,
        }

        # Top holders
        top_holders = [
            {
                "declarant": r["declarant"],
                "declaration_count": r["cnt"],
                "share_pct": round(r["cnt"] / total_declarations * 100, 2)
                if total_declarations > 0
                else 0.0,
            }
            for r in declarant_rows
        ]

        # Licensing landscape heuristic
        # Assume each unique declarant would seek licensing revenue.
        # A simplified royalty model: total royalty burden proportional to
        # number of distinct licensors.
        licensing_landscape = {
            "unique_licensors": total_declarants,
            "avg_declarations_per_licensor": round(
                total_declarations / total_declarants, 2
            )
            if total_declarants > 0
            else 0.0,
            "concentration_type": label,
            "negotiation_complexity": (
                "high" if total_declarants > 20
                else "moderate" if total_declarants > 5
                else "low"
            ),
        }

        # Royalty stack estimate (simplified heuristic)
        # Industry assumption: cumulative SEP royalty ~5-15% of device price
        # for major standards (e.g., 4G/5G).  Distribute proportionally.
        assumed_total_royalty_pct = 10.0  # midpoint assumption
        royalty_per_declaration = round(
            assumed_total_royalty_pct / total_declarations, 4
        ) if total_declarations > 0 else 0.0

        top_holder_royalties = []
        for r in declarant_rows[:10]:
            holder_royalty = round(r["cnt"] * royalty_per_declaration, 4)
            top_holder_royalties.append({
                "declarant": r["declarant"],
                "estimated_royalty_pct": holder_royalty,
                "declaration_count": r["cnt"],
            })

        royalty_stack_estimate = {
            "assumed_total_royalty_pct": assumed_total_royalty_pct,
            "per_declaration_pct": royalty_per_declaration,
            "top_holder_royalties": top_holder_royalties,
            "methodology": (
                "Proportional allocation of assumed total royalty "
                f"({assumed_total_royalty_pct}%) across {total_declarations} "
                "declarations. This is a simplified heuristic."
            ),
        }

        paged = paginate(top_holders, page=page, page_size=page_size)

        return {
            "endpoint": "frand_analysis",
            "standard": standard,
            "total_declarations": total_declarations,
            "total_declarants": total_declarants,
            "concentration": concentration,
            "top_holders": paged["results"],
            "licensing_landscape": licensing_landscape,
            "royalty_stack_estimate": royalty_stack_estimate,
            "page": paged["page"],
            "page_size": paged["page_size"],
            "pages": paged["pages"],
            "disclaimer": _FRAND_DISCLAIMER,
        }

    except sqlite3.OperationalError:
        return {
            "endpoint": "frand_analysis",
            "standard": standard,
            "error": "Query timed out. Try again later.",
            "disclaimer": _FRAND_DISCLAIMER,
        }
