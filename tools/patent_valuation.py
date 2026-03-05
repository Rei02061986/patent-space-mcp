"""patent_valuation tool — patent value scoring + royalty rate reference.

Scores patents or portfolios on citation_impact, family_breadth,
technology_relevance, remaining_life, and market_size_proxy.
Provides industry-specific royalty rate reference from static table.
"""
from __future__ import annotations

from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver


# Static royalty rate table: CPC prefix → (industry, low%, median%, high%)
ROYALTY_RATES = {
    "A61": ("製薬・バイオ", 3.0, 7.0, 25.0),
    "G06": ("ソフトウェア・IT", 1.0, 3.5, 10.0),
    "H01": ("電子部品・半導体", 1.5, 3.0, 7.0),
    "H04": ("通信", 1.0, 3.0, 8.0),
    "B60": ("自動車", 1.0, 3.0, 5.0),
    "C08": ("化学・素材", 2.0, 4.0, 8.0),
    "F01": ("機械・エンジン", 1.0, 3.0, 6.0),
    "G01": ("計測・センサー", 2.0, 4.0, 7.0),
    "A01": ("農業・食品", 1.0, 3.0, 6.0),
    "B01": ("化学プロセス", 2.0, 4.0, 8.0),
    "C12": ("バイオテクノロジー", 3.0, 8.0, 20.0),
    "G16": ("ヘルスケアIT", 2.0, 5.0, 12.0),
    "H02": ("電力・発電", 1.5, 3.0, 6.0),
    "B25": ("ロボティクス", 1.5, 3.5, 7.0),
    "G02": ("光学", 1.5, 3.5, 7.0),
    "A23": ("食品加工", 1.0, 3.0, 6.0),
    "C07": ("有機化学", 2.5, 5.0, 12.0),
    "F16": ("機械要素", 1.0, 2.5, 5.0),
}

_DEFAULT_RATE = ("その他", 1.0, 3.0, 7.0)

CURRENT_YEAR = 2024  # Analysis reference year


def _get_royalty(cpc_prefix: str) -> dict:
    """Match CPC prefix to royalty rate reference."""
    # Try exact 3-char match, then 2-char, then 1-char
    for length in [3, 2]:
        prefix = cpc_prefix[:length].upper()
        if prefix in ROYALTY_RATES:
            industry, low, med, high = ROYALTY_RATES[prefix]
            return {
                "industry": industry,
                "typical_range_pct": [low, high],
                "median_pct": med,
                "source": "業界別公開統計データ（CPC→産業分類マッピング）",
                "note": "参考値です。実際のレートは個別交渉により異なります。",
            }
    industry, low, med, high = _DEFAULT_RATE
    return {
        "industry": industry,
        "typical_range_pct": [low, high],
        "median_pct": med,
        "source": "業界別公開統計データ（CPC→産業分類マッピング）",
        "note": "参考値です。実際のレートは個別交渉により異なります。",
    }


def _score_patent(conn, pub_num: str) -> dict[str, Any] | None:
    """Score a single patent."""
    row = conn.execute(
        "SELECT publication_number, filing_date, grant_date, country_code, "
        "family_id, title_ja, title_en FROM patents WHERE publication_number = ?",
        (pub_num,),
    ).fetchone()
    if row is None:
        return None

    # Citation impact (forward citations)
    fwd_count = 0
    try:
        cit_row = conn.execute(
            "SELECT COUNT(*) as cnt FROM patent_citations "
            "WHERE cited_publication = ?",
            (pub_num,),
        ).fetchone()
        fwd_count = cit_row["cnt"] if cit_row else 0
    except Exception:
        pass  # Citations table may not exist or be populated

    citation_impact = min(fwd_count / 20.0, 1.0) * 100  # 20+ citations = max

    # Family breadth (count distinct jurisdictions in same family)
    family_id = row["family_id"]
    family_breadth_score = 0.0
    family_countries = []
    if family_id:
        try:
            fam_rows = conn.execute(
                "SELECT DISTINCT country_code FROM patents WHERE family_id = ? AND country_code IS NOT NULL",
                (family_id,),
            ).fetchall()
            family_countries = [r["country_code"] for r in fam_rows]
            n_countries = len(family_countries)
            family_breadth_score = min(n_countries / 8.0, 1.0) * 100  # 8+ countries = max
        except Exception:
            pass

    # Technology relevance (cluster growth rate)
    cpc_rows = conn.execute(
        "SELECT cpc_code FROM patent_cpc WHERE publication_number = ? LIMIT 5",
        (pub_num,),
    ).fetchall()
    cpc_codes = [r["cpc_code"][:4] for r in cpc_rows]
    primary_cpc = cpc_codes[0] if cpc_codes else ""

    tech_relevance = 50.0  # default
    try:
        for cpc in cpc_codes[:3]:
            cluster_row = conn.execute(
                "SELECT cluster_id FROM tech_clusters WHERE cpc_class = ? LIMIT 1",
                (cpc,),
            ).fetchone()
            if cluster_row:
                mom_row = conn.execute(
                    "SELECT growth_rate FROM tech_cluster_momentum "
                    "WHERE cluster_id = ? AND year = (SELECT MAX(year) FROM tech_cluster_momentum)",
                    (cluster_row["cluster_id"],),
                ).fetchone()
                if mom_row:
                    gr = mom_row["growth_rate"] or 0
                    tech_relevance = min(max((gr + 0.1) / 0.3 * 100, 0), 100)
                    break
    except Exception:
        pass

    # Remaining life
    filing_date = row["filing_date"]
    remaining_life = 0.0
    if filing_date:
        try:
            year = int(str(filing_date)[:4])
            expiry_year = year + 20
            remaining_years = expiry_year - CURRENT_YEAR
            remaining_life = min(max(remaining_years / 20.0, 0), 1.0) * 100
        except (ValueError, TypeError):
            remaining_life = 50.0

    # Market size proxy (based on CPC → industry mapping)
    market_score = 50.0
    high_market_cpc = {"G06", "H01", "H04", "A61", "B60", "C12"}
    if any(c[:3] in high_market_cpc for c in cpc_codes):
        market_score = 80.0
    elif any(c[:3] in {"C08", "G01", "F01", "B01"} for c in cpc_codes):
        market_score = 60.0

    overall = (
        citation_impact * 0.25
        + family_breadth_score * 0.15
        + tech_relevance * 0.25
        + remaining_life * 0.20
        + market_score * 0.15
    )

    return {
        "publication_number": pub_num,
        "title": row["title_en"] or row["title_ja"],
        "overall": round(overall, 1),
        "components": {
            "citation_impact": round(citation_impact, 1),
            "family_breadth": round(family_breadth_score, 1),
            "technology_relevance": round(tech_relevance, 1),
            "remaining_life": round(remaining_life, 1),
            "market_size_proxy": round(market_score, 1),
        },
        "details": {
            "forward_citations": fwd_count,
            "family_countries": family_countries,
            "primary_cpc": primary_cpc,
            "filing_year": int(str(filing_date)[:4]) if filing_date else None,
            "citation_data_note": "被引用数データは一部のみ取得済み。値が0でも未取得の可能性があります。" if fwd_count == 0 else None,
        },
    }


def patent_valuation(
    store: PatentStore,
    resolver: EntityResolver,
    query: str,
    query_type: str = "firm",
    purpose: str = "portfolio_ranking",
) -> dict[str, Any]:
    """Score patent or portfolio value with royalty rate reference."""
    store._relax_timeout()
    conn = store._conn()

    if query_type == "patent":
        score = _score_patent(conn, query)
        if score is None:
            return {"error": f"Patent not found: '{query}'"}

        primary_cpc = score["details"].get("primary_cpc", "")
        royalty = _get_royalty(primary_cpc)

        return {
            "endpoint": "patent_valuation",
            "query_type": "patent",
            "patent_score": score,
            "royalty_reference": royalty,
            "visualization_hint": {
                "recommended_chart": "gauge_with_bar",
                "title": f"特許価値スコア: {query}",
                "axes": {"value": "overall", "components": "components"},
            },
        }

    elif query_type == "firm":
        resolved = resolver.resolve(query, country_hint="JP")
        if resolved is None:
            return {"error": f"Could not resolve firm: '{query}'",
                    "suggestion": "Try the exact company name, Japanese name, or stock ticker"}

        firm_id = resolved.entity.canonical_id

        # ── Fast-path: use pre-computed tables only ──
        ftv = conn.execute(
            "SELECT patent_count, dominant_cpc, tech_diversity "
            "FROM firm_tech_vectors WHERE firm_id = ? "
            "ORDER BY year DESC LIMIT 1",
            (firm_id,),
        ).fetchone()

        if ftv is None:
            return {"error": f"No patent data found for firm: '{firm_id}'",
                    "suggestion": "This firm may not have enough patents for analysis."}

        total_patents = ftv["patent_count"] or 0
        primary_cpc = ftv["dominant_cpc"] or ""
        tech_diversity = round((ftv["tech_diversity"] or 0) / 5.0, 3)
        royalty = _get_royalty(primary_cpc)

        # Get startability scores to assess portfolio strength
        s_rows = conn.execute(
            "SELECT cluster_id, score, gate_open FROM startability_surface "
            "WHERE firm_id = ? AND year = ("
            "  SELECT MAX(year) FROM startability_surface WHERE firm_id = ?"
            ") ORDER BY score DESC LIMIT 30",
            (firm_id, firm_id),
        ).fetchall()

        # Technology relevance: weighted average of cluster growth rates
        tech_relevance_sum = 0.0
        tech_relevance_count = 0
        top_clusters = []
        for sr in s_rows[:15]:
            cid = sr["cluster_id"]
            mom_row = conn.execute(
                "SELECT growth_rate FROM tech_cluster_momentum "
                "WHERE cluster_id = ? AND year = ("
                "  SELECT MAX(year) FROM tech_cluster_momentum WHERE cluster_id = ?"
                ")",
                (cid, cid),
            ).fetchone()
            growth = (mom_row["growth_rate"] or 0) if mom_row else 0
            tech_relevance_sum += growth
            tech_relevance_count += 1

            label_row = conn.execute(
                "SELECT label, cpc_class FROM tech_clusters WHERE cluster_id = ? LIMIT 1",
                (cid,),
            ).fetchone()
            label = label_row["label"] if label_row else cid
            top_clusters.append({
                "cluster_id": cid,
                "label": label,
                "score": round(sr["score"], 3),
                "gate_open": sr["gate_open"],
                "growth_rate": round(growth, 3),
            })

        avg_growth = (tech_relevance_sum / tech_relevance_count) if tech_relevance_count else 0
        tech_relevance_score = min(max((avg_growth + 0.1) / 0.3 * 100, 0), 100)

        # Portfolio breadth score (based on diversity)
        breadth_score = min(tech_diversity * 100, 100)

        # Startability strength (avg top scores)
        avg_startability = 0.0
        if s_rows:
            avg_startability = sum(r["score"] for r in s_rows[:10]) / min(len(s_rows), 10)
        strength_score = avg_startability * 100

        # Volume score (normalized by log)
        import math
        volume_score = min(math.log10(max(total_patents, 1)) / 5.0 * 100, 100)

        # Market score based on CPC
        market_score = 50.0
        high_market_cpc = {"G06", "H01", "H04", "A61", "B60", "C12"}
        if primary_cpc[:3] in high_market_cpc:
            market_score = 80.0
        elif primary_cpc[:3] in {"C08", "G01", "F01", "B01"}:
            market_score = 60.0

        overall = (
            strength_score * 0.30
            + tech_relevance_score * 0.25
            + breadth_score * 0.15
            + volume_score * 0.15
            + market_score * 0.15
        )

        # Determine value tier
        if overall >= 70:
            tier = "top_10%"
        elif overall >= 55:
            tier = "top_25%"
        elif overall >= 40:
            tier = "top_50%"
        else:
            tier = "below_50%"

        return {
            "endpoint": "patent_valuation",
            "query_type": "firm",
            "firm_id": firm_id,
            "portfolio_score": {
                "total_patents": total_patents,
                "avg_score": round(overall, 1),
                "estimated_value_tier": tier,
                "components": {
                    "technology_strength": round(strength_score, 1),
                    "technology_relevance": round(tech_relevance_score, 1),
                    "portfolio_breadth": round(breadth_score, 1),
                    "volume": round(volume_score, 1),
                    "market_size_proxy": round(market_score, 1),
                },
                "primary_cpc": primary_cpc,
                "tech_diversity": tech_diversity,
                "top_technology_areas": top_clusters[:5],
            },
            "royalty_reference": royalty,
            "purpose": purpose,
            "visualization_hint": {
                "recommended_chart": "bar_with_gauge",
                "title": f"{firm_id} ポートフォリオ価値評価",
                "axes": {"categories": "components", "value": "score"},
            },
        }
    else:
        return {"error": f"Invalid query_type: '{query_type}'. Use 'firm' or 'patent'."}
