"""sales_prospect tool — identify licensing sales targets.

Combines startability_ranking, tech_gap, portfolio_evolution,
and GDELT signals to identify and rank potential licensees,
generating why_they_need_it narratives and approach guides.
"""
from __future__ import annotations

from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver


TEMPLATES = {
    "entering_weak": (
        "{firm_name}は{entering_area}への出願を直近で増加させているが、"
        "{weak_area}のstartabilityは{score:.2f}と低い。"
        "御社の{strong_area}特許群は、この技術ギャップを直接補完する。"
    ),
    "competitor_pressure": (
        "{firm_name}の主力領域{main_area}では競合が急速に出願を増やしており、"
        "差別化のために{weak_area}の技術強化が必要な状況にある。"
    ),
    "new_market_entry": (
        "{firm_name}は{new_direction}へのシフトが明確。"
        "この新領域で御社は{your_score:.2f}のstartabilityを持つ。"
    ),
}


def _resolve_firm(resolver, name):
    r = resolver.resolve(name, country_hint="JP")
    if r is None:
        return None, None
    return r.entity.canonical_id, r.entity.canonical_name


def _get_cluster_label(conn, cluster_id):
    row = conn.execute(
        "SELECT label, cpc_class FROM tech_clusters WHERE cluster_id = ?",
        (cluster_id,),
    ).fetchone()
    if row:
        return row["label"], row["cpc_class"]
    return cluster_id, cluster_id.split("_")[0] if "_" in cluster_id else ""


def _get_scores(conn, firm_id):
    """Get startability scores for a firm (year with most data).

    Uses the year with the highest cluster count, NOT MAX(year).
    This is critical because year=2024 may have only 1 cluster (B01D_0)
    while year=2023 has 607 clusters with full coverage.
    """
    # Find year with most data for this firm
    best_year_row = conn.execute(
        "SELECT year, COUNT(*) as cnt FROM startability_surface "
        "WHERE firm_id = ? GROUP BY year ORDER BY cnt DESC LIMIT 1",
        (firm_id,),
    ).fetchone()
    if not best_year_row:
        return {}

    best_year = best_year_row["year"]
    rows = conn.execute(
        "SELECT cluster_id, score, gate_open, year FROM startability_surface "
        "WHERE firm_id = ? AND year = ? ORDER BY score DESC",
        (firm_id, best_year),
    ).fetchall()
    return {r["cluster_id"]: r["score"] for r in rows}


def _get_delta(conn, firm_id, cluster_id):
    """Get startability change for a firm-cluster pair."""
    rows = conn.execute(
        "SELECT year, score FROM startability_surface "
        "WHERE firm_id = ? AND cluster_id = ? ORDER BY year",
        (firm_id, cluster_id),
    ).fetchall()
    if len(rows) < 2:
        return 0.0
    return rows[-1]["score"] - rows[0]["score"]


def _determine_deal_structure(synergy_score, their_startability):
    if synergy_score > 0.7:
        return "クロスライセンス（双方の技術を相互利用）"
    elif their_startability < 0.3:
        return "一方向ライセンス（技術提供型）"
    else:
        return "技術提携（共同研究+ライセンス）"


# Royalty reference (simplified from patent_valuation)
_ROYALTY_RATES = {
    "A61": ("製薬・バイオ", 3.0, 7.0, 25.0),
    "G06": ("ソフトウェア・IT", 1.0, 3.5, 10.0),
    "H01": ("電子部品・半導体", 1.5, 3.0, 7.0),
    "H04": ("通信", 1.0, 3.0, 8.0),
    "B60": ("自動車", 1.0, 3.0, 5.0),
    "C08": ("化学・素材", 2.0, 4.0, 8.0),
}


def sales_prospect(
    store: PatentStore,
    resolver: EntityResolver,
    firm_query: str,
    patent_or_tech: str,
    query_type: str = "cluster",
    target_count: int = 10,
) -> dict[str, Any]:
    """Identify and rank potential licensing targets."""
    store._relax_timeout()
    # Resolve licensor firm
    fid, fname = _resolve_firm(resolver, firm_query)
    if fid is None:
        return {"error": f"Could not resolve firm: '{firm_query}'",
                "suggestion": "Try the exact company name, Japanese name, or stock ticker"}

    conn = store._conn()

    # Determine target technology clusters
    target_clusters = []

    if query_type == "cluster":
        cluster_id = patent_or_tech.strip()
        label, cpc = _get_cluster_label(conn, cluster_id)
        target_clusters = [(cluster_id, label, cpc)]

    elif query_type == "patent":
        # Get patent's CPC codes → find matching clusters
        cpc_rows = conn.execute(
            "SELECT cpc_code FROM patent_cpc WHERE publication_number = ? LIMIT 5",
            (patent_or_tech,),
        ).fetchall()
        for cr in cpc_rows:
            cpc4 = cr["cpc_code"][:4]
            cl_row = conn.execute(
                "SELECT cluster_id, label, cpc_class FROM tech_clusters "
                "WHERE cpc_class = ? LIMIT 1",
                (cpc4,),
            ).fetchone()
            if cl_row:
                target_clusters.append(
                    (cl_row["cluster_id"], cl_row["label"], cl_row["cpc_class"])
                )

    elif query_type == "text":
        # Text → cluster label matching
        keywords = patent_or_tech.lower().split()[:5]
        like_parts = " OR ".join(
            "(label LIKE '%' || ? || '%' OR top_terms LIKE '%' || ? || '%')"
            for _ in keywords
        )
        params = []
        for kw in keywords:
            params.extend([kw, kw])
        cl_rows = conn.execute(
            f"SELECT cluster_id, label, cpc_class FROM tech_clusters "
            f"WHERE {like_parts} ORDER BY patent_count DESC LIMIT 3",
            params,
        ).fetchall()
        target_clusters = [(r["cluster_id"], r["label"], r["cpc_class"]) for r in cl_rows]

    if not target_clusters:
        return {"error": f"No matching technology cluster found for: '{patent_or_tech}'",
                "suggestion": "Use tech_clusters_list to browse available cluster IDs."}

    # Get licensor's own scores
    our_scores = _get_scores(conn, fid)

    # Source summary
    source_summary = {
        "firm_id": fid,
        "firm_name": fname,
        "relevant_clusters": [tc[0] for tc in target_clusters],
        "technology_description": ", ".join(tc[1] for tc in target_clusters),
        "technology_strength": round(
            max((our_scores.get(tc[0], 0) for tc in target_clusters), default=0), 3
        ),
    }

    # Find prospect firms: those with LOW startability in target clusters
    # but HIGH growth trajectory (entering related areas)
    prospect_scores: dict[str, dict] = {}

    for cluster_id, label, cpc in target_clusters:
        # Find year with most firms for this cluster
        best_year_row = conn.execute(
            "SELECT year, COUNT(*) as cnt FROM startability_surface "
            "WHERE cluster_id = ? GROUP BY year ORDER BY cnt DESC LIMIT 1",
            (cluster_id,),
        ).fetchone()
        prospect_year = best_year_row["year"] if best_year_row else 2023

        # Best licensing prospects: firms with gate_open=1 (they have CPC
        # overlap / active interest) but score < 0.75 (not yet strong).
        # This filters out: (1) zero-tech firms (gate_open=0, score<0.1)
        # like bookstores/publishers, and (2) top players who don't need help.
        rows = conn.execute(
            "SELECT firm_id, score, gate_open FROM startability_surface "
            "WHERE cluster_id = ? AND year = ? "
            "AND firm_id != ? "
            "AND gate_open = 1 AND score < 0.75 "
            "ORDER BY score ASC LIMIT ?",
            (cluster_id, prospect_year, fid, target_count * 5),
        ).fetchall()

        for r in rows:
            pfid = r["firm_id"]
            if pfid not in prospect_scores:
                prospect_scores[pfid] = {
                    "firm_id": pfid,
                    "target_score": r["score"],
                    "target_cluster": cluster_id,
                    "target_label": label,
                    "gate_open": r["gate_open"],
                }
            # Keep the one with lowest score (most need)
            if r["score"] < prospect_scores[pfid]["target_score"]:
                prospect_scores[pfid]["target_score"] = r["score"]
                prospect_scores[pfid]["target_cluster"] = cluster_id
                prospect_scores[pfid]["target_label"] = label

    # Score and rank prospects
    prospects = []
    for pfid, pdata in prospect_scores.items():
        their_target_score = pdata["target_score"]

        # Get their overall profile
        their_scores = _get_scores(conn, pfid)

        # Fit score: how much they NEED our tech (inverse of their score)
        need_score = (1.0 - their_target_score) * 100

        # Urgency: are they actively entering related areas?
        related_growth = 0.0
        entering_areas = []
        for tc_id, tc_label, tc_cpc in target_clusters:
            delta = _get_delta(conn, pfid, tc_id)
            if delta > 0.05:
                related_growth += delta
                entering_areas.append(tc_label)

        urgency_score = min(related_growth * 200 + need_score * 0.3, 100)

        # Get their dominant area for context
        their_dominant = max(their_scores.items(), key=lambda x: x[1], default=(None, 0))
        dominant_cluster = their_dominant[0]
        dominant_label, _ = _get_cluster_label(conn, dominant_cluster) if dominant_cluster else ("", "")

        # Generate why_they_need_it
        our_strong = source_summary["technology_description"]
        their_weak = pdata["target_label"]

        if entering_areas:
            why = TEMPLATES["entering_weak"].format(
                firm_name=pfid,
                entering_area="・".join(entering_areas[:2]),
                weak_area=their_weak,
                score=their_target_score,
                strong_area=our_strong,
            )
        elif need_score > 70:
            why = TEMPLATES["new_market_entry"].format(
                firm_name=pfid,
                new_direction=their_weak,
                your_score=source_summary["technology_strength"],
            )
        else:
            why = TEMPLATES["competitor_pressure"].format(
                firm_name=pfid,
                main_area=dominant_label,
                weak_area=their_weak,
            )

        # Evidence
        evidence = {
            "tech_gap": {
                "their_weak_area": their_weak,
                "your_strong_area": our_strong,
                "gap_magnitude": round(source_summary["technology_strength"] - their_target_score, 3),
            },
            "their_trajectory": {
                "entering_clusters": entering_areas,
                "growth_direction": "expanding" if related_growth > 0.1 else "stable",
            },
            "overlap": {
                "cpc_overlap_ratio": round(
                    len(set(their_scores.keys()) & set(our_scores.keys()))
                    / max(len(set(their_scores.keys()) | set(our_scores.keys())), 1), 3
                ),
            },
        }

        # GDELT market signals (if available)
        try:
            gdelt_row = conn.execute(
                "SELECT tone, investment_signal FROM gdelt_company_features "
                "WHERE firm_id = ? ORDER BY period_end DESC LIMIT 1",
                (pfid,),
            ).fetchone()
            if gdelt_row:
                evidence["market_signals"] = {
                    "gdelt_investment_signal": gdelt_row["investment_signal"],
                    "news_direction": "positive" if (gdelt_row["tone"] or 0) > 0 else "neutral",
                }
        except Exception:
            pass

        # Approach guide
        synergy = evidence["overlap"]["cpc_overlap_ratio"]
        deal = _determine_deal_structure(synergy, their_target_score)

        talking_points = [
            f"御社の{their_weak}領域への参入をサポートする特許ポートフォリオ",
            f"startability分析で{their_target_score:.2f}→技術補完の余地大",
        ]
        if entering_areas:
            talking_points.append(f"御社が注力中の{entering_areas[0]}との相乗効果")
        if evidence.get("market_signals", {}).get("gdelt_investment_signal", 0) > 0.5:
            talking_points.append("メディア分析でも積極的な技術投資の兆候あり")
        talking_points.append("業界動向データに基づく客観的な技術適合性分析を提示可能")

        approach = {
            "pitch_angle": f"{their_weak}領域の技術競争力強化",
            "key_talking_points": talking_points,
            "potential_deal_structure": deal,
        }

        fit_score = need_score * 0.5 + urgency_score * 0.3 + synergy * 20

        prospects.append({
            "firm_id": pfid,
            "fit_score": round(min(fit_score, 100), 1),
            "urgency_score": round(urgency_score, 1),
            "why_they_need_it": why,
            "evidence": evidence,
            "approach_guide": approach,
        })

    # Sort by fit_score
    prospects.sort(key=lambda x: x["fit_score"], reverse=True)
    ranked = []
    for i, p in enumerate(prospects[:target_count]):
        p["rank"] = i + 1
        ranked.append(p)

    # Summary with royalty reference
    primary_cpc = target_clusters[0][2][:3] if target_clusters else ""
    rate = _ROYALTY_RATES.get(primary_cpc, ("その他", 1.0, 3.0, 7.0))

    return {
        "endpoint": "sales_prospect",
        "source_summary": source_summary,
        "prospects": ranked,
        "summary": {
            "total_prospects": len(ranked),
            "avg_fit_score": round(sum(p["fit_score"] for p in ranked) / max(len(ranked), 1), 1),
            "market_timing": "積極的" if any(p["urgency_score"] > 60 for p in ranked) else "標準的",
            "royalty_reference": {
                "industry": rate[0],
                "typical_range_pct": [rate[1], rate[3]],
                "source": "業界別公開統計データ",
            },
        },
        "visualization_hint": {
            "recommended_chart": "funnel",
            "title": f"{fname} ライセンス営業ターゲット",
            "axes": {"categories": "rank", "value": "fit_score", "color": "urgency_score"},
        },
    }
