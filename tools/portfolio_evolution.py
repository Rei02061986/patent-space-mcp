"""portfolio_evolution tool — track firm technology strategy changes over time.

Uses firm_tech_vectors year-over-year to identify emerging/declining
technology areas and strategic shifts.
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
        return list(struct.unpack(f"{n}d", blob)) if n > 0 else None


def portfolio_evolution(
    store: PatentStore,
    resolver: EntityResolver,
    firm_query: str,
    year_from: int = 2016,
    year_to: int = 2024,
) -> dict[str, Any]:
    """Track how a firm's patent portfolio evolved over time."""
    resolved = resolver.resolve(firm_query, country_hint="JP")
    if resolved is None:
        return {"error": f"Could not resolve firm: '{firm_query}'",
                "suggestion": "Try the exact company name, Japanese name, or stock ticker"}

    firm_id = resolved.entity.canonical_id
    conn = store._conn()

    # Get all years available for this firm
    year_rows = conn.execute(
        "SELECT year, patent_count, dominant_cpc, tech_diversity, tech_concentration, tech_vector "
        "FROM firm_tech_vectors WHERE firm_id = ? AND year BETWEEN ? AND ? ORDER BY year",
        (firm_id, year_from, year_to),
    ).fetchall()

    if not year_rows:
        return {"error": f"No tech vector data for '{firm_id}' between {year_from}-{year_to}",
                "suggestion": "Try a different date range or check firm_tech_vector for available years."}

    timeline = []
    for r in year_rows:
        timeline.append({
            "year": r["year"],
            "patent_count": r["patent_count"],
            "dominant_cpc": r["dominant_cpc"],
            "tech_diversity": round((r["tech_diversity"] or 0) / 5.0, 3),
            "tech_concentration": round(r["tech_concentration"] or 0, 3),
        })

    # Get startability scores across years for emerging/declining analysis
    ss_rows = conn.execute(
        "SELECT cluster_id, year, score FROM startability_surface "
        "WHERE firm_id = ? AND year BETWEEN ? AND ? ORDER BY cluster_id, year",
        (firm_id, year_from, year_to),
    ).fetchall()

    # Build cluster trajectories
    cluster_scores: dict[str, dict[int, float]] = {}
    for r in ss_rows:
        cid = r["cluster_id"]
        if cid not in cluster_scores:
            cluster_scores[cid] = {}
        cluster_scores[cid][r["year"]] = r["score"]

    # Find earliest and latest available years
    all_years = sorted({r["year"] for r in ss_rows})
    if len(all_years) < 2:
        emerging = []
        declining = []
    else:
        early_year = all_years[0]
        late_year = all_years[-1]

        changes = []
        for cid, year_scores in cluster_scores.items():
            early = year_scores.get(early_year, 0)
            late = year_scores.get(late_year, 0)
            delta = late - early
            if abs(delta) > 0.05:
                label_row = conn.execute(
                    "SELECT label FROM tech_clusters WHERE cluster_id = ?",
                    (cid,),
                ).fetchone()
                label = label_row["label"] if label_row else cid
                changes.append({
                    "cluster_id": cid,
                    "label": label,
                    "score_start": round(early, 3),
                    "score_end": round(late, 3),
                    "score_change": round(delta, 3),
                })

        changes.sort(key=lambda x: x["score_change"], reverse=True)
        emerging = [c for c in changes if c["score_change"] > 0][:15]
        declining = [c for c in changes if c["score_change"] < 0][:15]

    # Generate strategic shift summary
    shifts = []
    for i in range(1, len(timeline)):
        prev = timeline[i - 1]
        curr = timeline[i]
        if prev["dominant_cpc"] != curr["dominant_cpc"]:
            shifts.append(
                f"{prev['year']}-{curr['year']}: "
                f"{prev['dominant_cpc']}→{curr['dominant_cpc']}重点化"
            )
        div_change = curr["tech_diversity"] - prev["tech_diversity"]
        if abs(div_change) > 0.1:
            direction = "多角化" if div_change > 0 else "集中化"
            shifts.append(f"{curr['year']}: 技術{direction}傾向 (多様性{div_change:+.2f})")

    if emerging:
        top_emerging = emerging[0]
        shifts.append(
            f"成長注力: {top_emerging['label']} "
            f"(+{top_emerging['score_change']:.2f})"
        )
    if declining:
        top_declining = declining[0]
        shifts.append(
            f"撤退傾向: {top_declining['label']} "
            f"({top_declining['score_change']:.2f})"
        )

    strategic_shift_summary = "。".join(shifts) if shifts else "期間中の顕著な戦略変化なし。"

    return {
        "endpoint": "portfolio_evolution",
        "firm_id": firm_id,
        "year_from": year_from,
        "year_to": year_to,
        "timeline": timeline,
        "emerging": emerging,
        "declining": declining,
        "strategic_shift_summary": strategic_shift_summary,
        "visualization_hint": {
            "recommended_chart": "heatmap",
            "title": f"{firm_id} 技術ポートフォリオ推移",
            "axes": {"x": "year", "y": "cluster_id", "value": "score"},
        },
    }
