"""ip_due_diligence tool -- integrated IP due diligence for VC/PE investment analysis.

Combines patent portfolio analysis with market signals to generate an
investment memo-style output.  Seven analysis sections:

1. Portfolio Overview (firm_tech_vectors fast path)
2. Technology Moat Assessment (startability_surface)
3. IP Quality Score (citation_counts + sampled patents)
4. Geographic Coverage (family_id sample)
5. Competitive Position (startability_surface ranking)
6. Market Signal Integration (gdelt_company_features)
7. Investment Scoring (weighted composite)

Performance budget: <10 seconds on HDD.  All queries use pre-computed
tables or small LIMIT-ed samples.  Never scans patent_assignees without
a firm_id filter.
"""
from __future__ import annotations

import math
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver
from tools.royalty_benchmarks import get_royalty_rate, get_sector, get_wacc, get_tax_rate, ROYALTY_RATES
from tools.cpc_labels_ja import CPC_CLASS_JA

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAJOR_JURISDICTIONS = ("JP", "US", "EP", "CN", "KR")

# Weight profiles per investment_type.
# Keys: moat, quality, coverage, position, growth, signals
_WEIGHT_PROFILES: dict[str, dict[str, float]] = {
    "venture": {
        "moat": 0.30, "quality": 0.25, "coverage": 0.05,
        "position": 0.10, "growth": 0.20, "signals": 0.10,
    },
    "growth": {
        "moat": 0.10, "quality": 0.20, "coverage": 0.10,
        "position": 0.30, "growth": 0.25, "signals": 0.05,
    },
    "buyout": {
        "moat": 0.25, "quality": 0.20, "coverage": 0.20,
        "position": 0.15, "growth": 0.10, "signals": 0.10,
    },
    "licensing": {
        "moat": 0.10, "quality": 0.30, "coverage": 0.25,
        "position": 0.20, "growth": 0.05, "signals": 0.10,
    },
}

_DEFAULT_WEIGHTS: dict[str, float] = {
    "moat": 0.25, "quality": 0.15, "coverage": 0.10,
    "position": 0.25, "growth": 0.15, "signals": 0.10,
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_firm(resolver: EntityResolver, name: str):
    """Resolve a firm name to (firm_id, canonical_name) or (None, None)."""
    r = resolver.resolve(name, country_hint="JP")
    if r is None:
        return None, None
    return r.entity.canonical_id, r.entity.canonical_name


def _cpc_label(cpc_code: str) -> str:
    """Return Japanese label for a CPC code, falling back to the code itself."""
    if not cpc_code:
        return ""
    # Try exact 4-char match, then 3-char
    for length in (4, 3):
        prefix = cpc_code[:length]
        if prefix in CPC_CLASS_JA:
            return CPC_CLASS_JA[prefix]
    return cpc_code


# ---------------------------------------------------------------------------
# Section 1: Portfolio Overview
# ---------------------------------------------------------------------------

def _portfolio_overview(conn, firm_id: str) -> dict[str, Any]:
    """Fast-path portfolio overview using firm_tech_vectors only."""
    rows = conn.execute(
        "SELECT year, patent_count, dominant_cpc, tech_diversity "
        "FROM firm_tech_vectors "
        "WHERE firm_id = ? ORDER BY year ASC",
        (firm_id,),
    ).fetchall()

    if not rows:
        return {
            "patent_count": 0,
            "dominant_technology": None,
            "tech_diversity": 0.0,
            "filing_trend": [],
            "data_available": False,
        }

    latest = rows[-1]
    # Use cumulative count across all years (not just latest year)
    patent_count = sum(r["patent_count"] or 0 for r in rows)
    latest_year_count = latest["patent_count"] or 0
    dominant_cpc = latest["dominant_cpc"] or ""
    # Entropy normalised to 0-1 range (raw entropy can be 0-6)
    tech_diversity = round((latest["tech_diversity"] or 0) / 5.0, 3)

    filing_trend = [
        {"year": r["year"], "count": r["patent_count"] or 0}
        for r in rows
    ]

    return {
        "patent_count": patent_count,
        "dominant_technology": dominant_cpc,
        "dominant_technology_label": _cpc_label(dominant_cpc),
        "tech_diversity": tech_diversity,
        "filing_trend": filing_trend,
        "data_available": True,
    }


# ---------------------------------------------------------------------------
# Section 2: Technology Moat Assessment
# ---------------------------------------------------------------------------

def _technology_moat(conn, firm_id: str) -> dict[str, Any]:
    """Assess defensive moat via startability_surface.

    High-score clusters where the firm has gate_open=1 are "protected".
    Moat width depends on how few competitors also hold those positions.
    """
    # Get latest year for the firm
    year_row = conn.execute(
        "SELECT MAX(year) AS y FROM startability_surface WHERE firm_id = ?",
        (firm_id,),
    ).fetchone()
    latest_year = year_row["y"] if year_row else None
    if latest_year is None:
        return {
            "score": 0.0,
            "classification": "no_moat",
            "protected_clusters": [],
            "vulnerability_areas": [],
            "data_available": False,
        }

    # Firm's clusters with high score
    firm_rows = conn.execute(
        "SELECT cluster_id, score, gate_open FROM startability_surface "
        "WHERE firm_id = ? AND year = ? ORDER BY score DESC",
        (firm_id, latest_year),
    ).fetchall()

    if not firm_rows:
        return {
            "score": 0.0,
            "classification": "no_moat",
            "protected_clusters": [],
            "vulnerability_areas": [],
            "data_available": False,
        }

    total_clusters = len(firm_rows)
    high_score_clusters: list[dict[str, Any]] = []
    vulnerability_areas: list[dict[str, Any]] = []

    for r in firm_rows:
        if r["gate_open"] == 1 and r["score"] > 0.5:
            cluster_id = r["cluster_id"]
            # Count competitors in same cluster with gate_open=1
            comp_row = conn.execute(
                "SELECT COUNT(DISTINCT firm_id) AS cnt FROM startability_surface "
                "WHERE cluster_id = ? AND year = ? AND gate_open = 1 "
                "AND firm_id != ?",
                (cluster_id, latest_year, firm_id),
            ).fetchone()
            competitor_count = comp_row["cnt"] if comp_row else 0

            # Look up cluster label
            label_row = conn.execute(
                "SELECT label FROM tech_clusters WHERE cluster_id = ? LIMIT 1",
                (cluster_id,),
            ).fetchone()
            label = label_row["label"] if label_row else cluster_id

            high_score_clusters.append({
                "cluster_id": cluster_id,
                "label": label,
                "score": round(r["score"], 3),
                "competitor_count": competitor_count,
            })
        elif r["score"] < 0.3 and r["gate_open"] == 0:
            label_row = conn.execute(
                "SELECT label FROM tech_clusters WHERE cluster_id = ? LIMIT 1",
                (r["cluster_id"],),
            ).fetchone()
            label = label_row["label"] if label_row else r["cluster_id"]
            vulnerability_areas.append({
                "cluster_id": r["cluster_id"],
                "label": label,
                "score": round(r["score"], 3),
            })

    # Calculate moat score using RANK-based approach
    # A firm has a moat if it ranks in the top percentile within its clusters
    n_protected = len(high_score_clusters)
    if n_protected == 0 or total_clusters == 0:
        moat_score = 0.0
    else:
        # For each protected cluster, compute the firm's percentile rank
        rank_scores = []
        for c in high_score_clusters:
            firm_score = c["score"]
            competitors = c["competitor_count"]
            if competitors == 0:
                rank_scores.append(1.0)  # sole player = perfect moat
            else:
                # Count how many competitors score HIGHER than this firm
                higher_row = conn.execute(
                    "SELECT COUNT(DISTINCT firm_id) AS cnt FROM startability_surface "
                    "WHERE cluster_id = ? AND year = ? AND score > ? AND firm_id != ?",
                    (c["cluster_id"], latest_year, firm_score, firm_id),
                ).fetchone()
                higher_count = higher_row["cnt"] if higher_row else 0
                # Percentile: 1.0 = #1, 0.0 = last
                percentile = 1.0 - (higher_count / max(competitors, 1))
                rank_scores.append(max(0, percentile))
                c["rank_percentile"] = round(percentile, 3)
                c["firms_above"] = higher_count

        # Moat = fraction of clusters where firm is in top 10% × coverage
        top_10pct_count = sum(1 for rs in rank_scores if rs >= 0.9)
        top_25pct_count = sum(1 for rs in rank_scores if rs >= 0.75)
        avg_percentile = sum(rank_scores) / len(rank_scores) if rank_scores else 0

        # Coverage: fraction of all clusters where firm has gate_open + high score
        coverage = n_protected / total_clusters

        # Moat = weighted combination of elite positioning and coverage
        moat_score = (0.5 * avg_percentile + 0.3 * (top_10pct_count / max(n_protected, 1)) + 0.2 * coverage)

    moat_score = round(min(moat_score, 1.0), 3)

    if moat_score > 0.5:
        classification = "wide_moat"
    elif moat_score >= 0.2:
        classification = "narrow_moat"
    else:
        classification = "no_moat"

    return {
        "score": moat_score,
        "classification": classification,
        "protected_clusters": high_score_clusters[:10],
        "vulnerability_areas": vulnerability_areas[:5],
        "data_available": True,
    }


# ---------------------------------------------------------------------------
# Section 3: IP Quality Score
# ---------------------------------------------------------------------------

def _ip_quality(conn, firm_id: str) -> dict[str, Any]:
    """Assess IP quality from citation_counts + sampled patents.

    Uses a LIMIT-100 sample of the firm's most-cited patents via
    citation_counts joined with patent_assignees (filtered by firm_id).
    """
    # Sample top-cited patents for this firm (fast: firm_id indexed)
    try:
        rows = conn.execute(
            """
            SELECT cc.publication_number, cc.forward_citations
            FROM citation_counts cc
            JOIN patent_assignees a ON a.publication_number = cc.publication_number
            WHERE a.firm_id = ?
            ORDER BY cc.forward_citations DESC
            LIMIT 100
            """,
            (firm_id,),
        ).fetchall()
    except Exception:
        rows = []

    if not rows:
        return {
            "score": 0,
            "citation_percentile": 0,
            "innovation_distribution": {
                "revolutionary": 0, "major": 0, "minor": 0,
            },
            "top_patents": [],
            "data_available": False,
        }

    citations = [r["forward_citations"] or 0 for r in rows]
    citations_sorted = sorted(citations)
    n = len(citations_sorted)

    median_cit = citations_sorted[n // 2] if n > 0 else 0
    p75_cit = citations_sorted[int(n * 0.75)] if n >= 4 else median_cit
    p90_cit = citations_sorted[int(n * 0.90)] if n >= 10 else p75_cit

    # Global citation benchmark: compute the median citation for all patents
    # that have at least 1 citation (fast aggregate on citation_counts).
    global_row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM citation_counts WHERE forward_citations > 0"
    ).fetchone()
    global_total = global_row["cnt"] if global_row else 1

    # Percentile: how many of the global patents have fewer citations than
    # our firm's median.
    if global_total > 0 and median_cit > 0:
        below_row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM citation_counts "
            "WHERE forward_citations > 0 AND forward_citations < ?",
            (median_cit,),
        ).fetchone()
        below_count = below_row["cnt"] if below_row else 0
        citation_percentile = round(below_count / global_total * 100, 1)
    else:
        citation_percentile = 0.0

    # Innovation classification thresholds (based on sample)
    revolutionary_threshold = p90_cit * 1.5 if p90_cit > 0 else 20
    major_threshold = p75_cit if p75_cit > 0 else 5

    revolutionary = sum(1 for c in citations if c >= revolutionary_threshold)
    major = sum(1 for c in citations if major_threshold <= c < revolutionary_threshold)
    minor = n - revolutionary - major

    # Top patents details
    top_patents: list[dict[str, Any]] = []
    for r in rows[:10]:
        pub = r["publication_number"]
        # Lightweight title lookup (direct index hit)
        title_row = conn.execute(
            "SELECT title_ja, title_en FROM patents "
            "WHERE publication_number = ?",
            (pub,),
        ).fetchone()
        title = ""
        if title_row:
            title = title_row["title_ja"] or title_row["title_en"] or ""
        top_patents.append({
            "publication_number": pub,
            "title": title,
            "forward_citations": r["forward_citations"],
        })

    # Quality score (0-100)
    # Combines: citation percentile (60%) + innovation ratio (40%)
    innovation_ratio = (revolutionary * 3 + major) / max(n, 1)
    quality_score = round(
        citation_percentile * 0.6
        + min(innovation_ratio * 100, 100) * 0.4,
        1,
    )

    return {
        "score": quality_score,
        "citation_percentile": citation_percentile,
        "citation_distribution": {
            "median": median_cit,
            "p75": p75_cit,
            "p90": p90_cit,
        },
        "innovation_distribution": {
            "revolutionary": revolutionary,
            "major": major,
            "minor": minor,
        },
        "top_patents": top_patents,
        "data_available": True,
    }


# ---------------------------------------------------------------------------
# Section 4: Geographic Coverage
# ---------------------------------------------------------------------------

def _geographic_coverage(conn, firm_id: str) -> dict[str, Any]:
    """Assess geographic coverage using a sample of top patents' families.

    Queries patent_assignees (firm_id filtered) for top patents, then
    checks family_id coverage across jurisdictions.  LIMIT 50.
    """
    try:
        sample_rows = conn.execute(
            """
            SELECT p.publication_number, p.family_id, p.country_code
            FROM patents p
            JOIN patent_assignees a ON a.publication_number = p.publication_number
            WHERE a.firm_id = ?
              AND p.family_id IS NOT NULL
            ORDER BY p.filing_date DESC
            LIMIT 50
            """,
            (firm_id,),
        ).fetchall()
    except Exception:
        sample_rows = []

    if not sample_rows:
        return {
            "score": 0.0,
            "countries_covered": [],
            "missing_jurisdictions": list(_MAJOR_JURISDICTIONS),
            "family_coverage_stats": {},
            "data_available": False,
        }

    # Collect unique family_ids
    family_ids = list({r["family_id"] for r in sample_rows if r["family_id"]})

    # Batch-query distinct country_codes per family
    all_countries: set[str] = set()
    per_family_countries: dict[str, set[str]] = {}

    for i in range(0, len(family_ids), 200):
        batch = family_ids[i : i + 200]
        ph = ",".join("?" * len(batch))
        try:
            fam_rows = conn.execute(
                f"SELECT family_id, country_code FROM patents "
                f"WHERE family_id IN ({ph}) AND country_code IS NOT NULL",
                batch,
            ).fetchall()
            for fr in fam_rows:
                fid = fr["family_id"]
                cc = fr["country_code"]
                all_countries.add(cc)
                per_family_countries.setdefault(fid, set()).add(cc)
        except Exception:
            pass

    covered_major = [j for j in _MAJOR_JURISDICTIONS if j in all_countries]
    missing_major = [j for j in _MAJOR_JURISDICTIONS if j not in all_countries]
    coverage_score = round(len(covered_major) / len(_MAJOR_JURISDICTIONS), 2)

    # Per-family stats: how many families cover 3+ jurisdictions
    families_broad = sum(
        1 for cs in per_family_countries.values() if len(cs) >= 3
    )
    families_analyzed = len(per_family_countries)

    return {
        "score": coverage_score,
        "countries_covered": sorted(all_countries),
        "major_jurisdictions_covered": covered_major,
        "missing_jurisdictions": missing_major,
        "family_coverage_stats": {
            "families_analyzed": families_analyzed,
            "families_with_3plus_jurisdictions": families_broad,
            "broad_coverage_ratio": round(
                families_broad / max(families_analyzed, 1), 3
            ),
        },
        "data_available": True,
    }


# ---------------------------------------------------------------------------
# Section 5: Competitive Position
# ---------------------------------------------------------------------------

def _competitive_position(
    conn, firm_id: str, benchmark_firm_ids: list[str] | None,
) -> dict[str, Any]:
    """Rank firm vs competitors in its top technology clusters.

    Uses startability_surface exclusively.
    """
    year_row = conn.execute(
        "SELECT MAX(year) AS y FROM startability_surface WHERE firm_id = ?",
        (firm_id,),
    ).fetchone()
    latest_year = year_row["y"] if year_row else None
    if latest_year is None:
        return {
            "leading_areas": [],
            "competitive_areas": [],
            "lagging_areas": [],
            "vs_benchmarks": [],
            "data_available": False,
        }

    # Top clusters for target firm
    firm_clusters = conn.execute(
        "SELECT cluster_id, score FROM startability_surface "
        "WHERE firm_id = ? AND year = ? ORDER BY score DESC LIMIT 20",
        (firm_id, latest_year),
    ).fetchall()

    leading: list[dict[str, Any]] = []
    competitive: list[dict[str, Any]] = []
    lagging: list[dict[str, Any]] = []

    for fc in firm_clusters:
        cid = fc["cluster_id"]
        firm_score = fc["score"]

        # Get all firms' scores in this cluster, ordered descending
        rank_rows = conn.execute(
            "SELECT firm_id, score FROM startability_surface "
            "WHERE cluster_id = ? AND year = ? AND score > 0 "
            "ORDER BY score DESC LIMIT 50",
            (cid, latest_year),
        ).fetchall()

        total_firms = len(rank_rows)
        firm_rank = None
        for idx, rr in enumerate(rank_rows):
            if rr["firm_id"] == firm_id:
                firm_rank = idx + 1
                break

        if firm_rank is None:
            firm_rank = total_firms + 1

        # Cluster label
        label_row = conn.execute(
            "SELECT label FROM tech_clusters WHERE cluster_id = ? LIMIT 1",
            (cid,),
        ).fetchone()
        label = label_row["label"] if label_row else cid

        entry = {
            "cluster_id": cid,
            "label": label,
            "score": round(firm_score, 3),
            "rank": firm_rank,
            "total_firms": total_firms,
        }

        # Classify position
        if total_firms > 0:
            percentile = firm_rank / total_firms
            if percentile <= 0.1:
                leading.append(entry)
            elif percentile <= 0.4:
                competitive.append(entry)
            else:
                lagging.append(entry)
        else:
            competitive.append(entry)

    # Benchmark comparison
    vs_benchmarks: list[dict[str, Any]] = []
    if benchmark_firm_ids:
        # For each benchmark, compare scores in target's top clusters
        top_cluster_ids = [fc["cluster_id"] for fc in firm_clusters[:10]]
        for bfid in benchmark_firm_ids:
            comparisons: list[dict[str, Any]] = []
            for cid in top_cluster_ids:
                bench_row = conn.execute(
                    "SELECT score FROM startability_surface "
                    "WHERE firm_id = ? AND cluster_id = ? AND year = ?",
                    (bfid, cid, latest_year),
                ).fetchone()
                target_row = conn.execute(
                    "SELECT score FROM startability_surface "
                    "WHERE firm_id = ? AND cluster_id = ? AND year = ?",
                    (firm_id, cid, latest_year),
                ).fetchone()
                t_score = target_row["score"] if target_row else 0
                b_score = bench_row["score"] if bench_row else 0
                comparisons.append({
                    "cluster_id": cid,
                    "target_score": round(t_score, 3),
                    "benchmark_score": round(b_score, 3),
                    "delta": round(t_score - b_score, 3),
                })
            wins = sum(1 for c in comparisons if c["delta"] > 0)
            vs_benchmarks.append({
                "benchmark_firm_id": bfid,
                "clusters_compared": len(comparisons),
                "wins": wins,
                "losses": len(comparisons) - wins,
                "details": comparisons,
            })

    return {
        "leading_areas": leading[:5],
        "competitive_areas": competitive[:5],
        "lagging_areas": lagging[:5],
        "vs_benchmarks": vs_benchmarks,
        "data_available": True,
    }


# ---------------------------------------------------------------------------
# Section 6: Market Signal Integration
# ---------------------------------------------------------------------------

def _market_signals(conn, firm_id: str) -> dict[str, Any]:
    """Pull GDELT media signals if available."""
    try:
        rows = conn.execute(
            "SELECT quarter, tone, event_count, theme_diversity, "
            "geographic_spread, source_diversity "
            "FROM gdelt_company_features "
            "WHERE firm_id = ? ORDER BY quarter ASC",
            (firm_id,),
        ).fetchall()
    except Exception:
        rows = []

    if not rows:
        return {
            "status": "data_unavailable",
            "tone_trend": None,
            "media_coverage": None,
            "details": [],
        }

    tones = [r["tone"] for r in rows if r["tone"] is not None]
    event_counts = [r["event_count"] for r in rows if r["event_count"] is not None]

    # Tone trend: compare last quarter vs first quarter
    tone_trend = None
    if len(tones) >= 2:
        recent_avg = sum(tones[-3:]) / min(len(tones), 3)
        early_avg = sum(tones[:3]) / min(len(tones), 3)
        if early_avg != 0:
            tone_trend = round((recent_avg - early_avg) / abs(early_avg), 3)
        else:
            tone_trend = round(recent_avg, 3)

    # Media coverage volume trend
    media_coverage = None
    if len(event_counts) >= 2:
        recent_events = sum(event_counts[-3:])
        early_events = sum(event_counts[:3])
        if early_events > 0:
            media_coverage = round(
                (recent_events - early_events) / early_events, 3
            )
        else:
            media_coverage = 1.0 if recent_events > 0 else 0.0

    # Latest values
    latest = rows[-1]
    latest_signals = {
        "quarter": latest["quarter"],
        "tone": round(latest["tone"], 3) if latest["tone"] is not None else None,
        "event_count": latest["event_count"],
        "theme_diversity": (
            round(latest["theme_diversity"], 3)
            if latest["theme_diversity"] is not None
            else None
        ),
        "geographic_spread": (
            round(latest["geographic_spread"], 3)
            if latest["geographic_spread"] is not None
            else None
        ),
        "source_diversity": (
            round(latest["source_diversity"], 3)
            if latest["source_diversity"] is not None
            else None
        ),
    }

    return {
        "status": "available",
        "tone_trend": tone_trend,
        "media_coverage": media_coverage,
        "latest_signals": latest_signals,
        "quarters_available": len(rows),
    }


# ---------------------------------------------------------------------------
# Section 7: Investment Scoring
# ---------------------------------------------------------------------------

def _compute_investment_score(
    portfolio: dict[str, Any],
    moat: dict[str, Any],
    quality: dict[str, Any],
    coverage: dict[str, Any],
    position: dict[str, Any],
    signals: dict[str, Any],
    investment_type: str,
) -> dict[str, Any]:
    """Weighted composite score (0-100) with recommendation."""

    weights = _WEIGHT_PROFILES.get(investment_type, _DEFAULT_WEIGHTS)

    # -- Component scores (each 0-100) --

    # Moat: already 0-1 scale
    moat_score = round(moat.get("score", 0) * 100, 1)

    # Quality: already 0-100
    quality_score = round(quality.get("score", 0), 1)

    # Coverage: 0-1 scale
    coverage_score = round(coverage.get("score", 0) * 100, 1)

    # Position: ratio of leading+competitive vs total analysed clusters
    leading_n = len(position.get("leading_areas", []))
    competitive_n = len(position.get("competitive_areas", []))
    lagging_n = len(position.get("lagging_areas", []))
    total_position = leading_n + competitive_n + lagging_n
    if total_position > 0:
        position_score = round(
            (leading_n * 1.0 + competitive_n * 0.5) / total_position * 100, 1
        )
    else:
        position_score = 0.0

    # Portfolio growth: YoY change in last 3 years of filing trend
    trend = portfolio.get("filing_trend", [])
    growth_score = 50.0  # neutral default
    if len(trend) >= 3:
        recent = trend[-1]["count"]
        older = trend[-3]["count"]
        if older > 0:
            growth_rate = (recent - older) / older
            # Map growth rate to 0-100 (growth_rate of -0.5 -> 0, +0.5 -> 100)
            growth_score = round(min(max((growth_rate + 0.5) / 1.0 * 100, 0), 100), 1)
        elif recent > 0:
            growth_score = 80.0  # new entrant with filings = positive
    elif len(trend) >= 1 and trend[-1]["count"] > 0:
        growth_score = 60.0  # has patents but insufficient trend data

    # Market signals: combine tone trend + coverage trend
    signals_score = 50.0  # neutral default
    if signals.get("status") == "available":
        tone = signals.get("tone_trend")
        media = signals.get("media_coverage")
        if tone is not None and media is not None:
            # Tone: positive tone trend = good, map -1..+1 to 0..100
            tone_component = min(max((tone + 1.0) / 2.0 * 100, 0), 100)
            # Media: growing coverage = good, map -0.5..+1.0 to 0..100
            media_component = min(max((media + 0.5) / 1.5 * 100, 0), 100)
            signals_score = round(tone_component * 0.6 + media_component * 0.4, 1)
        elif tone is not None:
            signals_score = round(min(max((tone + 1.0) / 2.0 * 100, 0), 100), 1)

    # -- Weighted overall --
    components = {
        "technology_moat": moat_score,
        "ip_quality": quality_score,
        "geographic_coverage": coverage_score,
        "competitive_position": position_score,
        "portfolio_growth": growth_score,
        "market_signals": signals_score,
    }

    overall = round(
        moat_score * weights["moat"]
        + quality_score * weights["quality"]
        + coverage_score * weights["coverage"]
        + position_score * weights["position"]
        + growth_score * weights["growth"]
        + signals_score * weights["signals"],
        1,
    )

    # -- Recommendation --
    if overall >= 65:
        recommendation = "invest"
    elif overall >= 40:
        recommendation = "monitor"
    else:
        recommendation = "pass"

    # -- Risk factors and strengths --
    risk_factors: list[str] = []
    strengths: list[str] = []

    if moat_score < 30:
        risk_factors.append("技術的堀が弱い - 競合参入リスク高")
    elif moat_score >= 60:
        strengths.append("強固な技術的堀 - 参入障壁高")

    if quality_score < 30:
        risk_factors.append("IP品質が低い - 被引用数が少なく影響力限定的")
    elif quality_score >= 70:
        strengths.append("高品質IP - 被引用数が多く技術的影響力大")

    if coverage_score < 40:
        risk_factors.append("地理的カバレッジ不足 - 主要市場での保護が不十分")
    elif coverage_score >= 80:
        strengths.append("グローバル特許保護 - 主要5法域をカバー")

    if growth_score < 30:
        risk_factors.append("出願減少傾向 - R&D投資低下の兆候")
    elif growth_score >= 70:
        strengths.append("出願増加傾向 - 積極的R&D投資")

    if position_score < 30:
        risk_factors.append("競争ポジション弱 - 主要技術領域でリーダーシップ欠如")
    elif position_score >= 70:
        strengths.append("競争優位 - 複数技術領域でリーダーポジション")

    if signals.get("status") == "data_unavailable":
        risk_factors.append("市場シグナルデータ未取得 - メディア動向分析不可")

    return {
        "overall": overall,
        "components": components,
        "weights_used": weights,
        "recommendation": recommendation,
        "risk_factors": risk_factors,
        "strengths": strengths,
    }


# ---------------------------------------------------------------------------
# Interpretation generator
# ---------------------------------------------------------------------------

def _generate_interpretation(
    firm_id: str,
    investment_type: str,
    portfolio: dict[str, Any],
    moat: dict[str, Any],
    quality: dict[str, Any],
    coverage: dict[str, Any],
    position: dict[str, Any],
    score: dict[str, Any],
) -> str:
    """Generate a Japanese-language summary interpretation."""
    parts: list[str] = []

    patent_count = portfolio.get("patent_count", 0)
    dominant = portfolio.get("dominant_technology_label") or portfolio.get(
        "dominant_technology", "不明"
    )
    diversity = portfolio.get("tech_diversity", 0)

    parts.append(
        f"{firm_id}は特許{patent_count:,}件を保有し、"
        f"主力技術は{dominant}です（技術多様性: {diversity:.2f}）。"
    )

    moat_class = moat.get("classification", "no_moat")
    moat_map = {
        "wide_moat": "広い技術的堀を有しており、競合参入が困難な領域を複数確保しています",
        "narrow_moat": "一定の技術的堀を有していますが、一部領域では競合の脅威があります",
        "no_moat": "技術的堀が弱く、競合参入に対する防御力は限定的です",
    }
    parts.append(moat_map.get(moat_class, ""))

    quality_score = quality.get("score", 0)
    if quality_score >= 70:
        parts.append("IP品質は高水準で、被引用数の観点から技術的影響力が認められます。")
    elif quality_score >= 40:
        parts.append("IP品質は中程度です。")
    else:
        parts.append("IP品質は改善の余地があります。")

    missing = coverage.get("missing_jurisdictions", [])
    if missing:
        parts.append(
            f"地理的カバレッジに関しては、{', '.join(missing)}での特許保護が不足しています。"
        )
    else:
        parts.append("主要5法域（JP, US, EP, CN, KR）全てをカバーしています。")

    overall = score.get("overall", 0)
    rec = score.get("recommendation", "monitor")
    rec_map = {
        "invest": "投資適格と評価されます",
        "monitor": "継続モニタリングを推奨します",
        "pass": "現時点での投資は慎重な判断が必要です",
    }
    inv_type_ja = {
        "venture": "ベンチャー投資",
        "growth": "グロース投資",
        "buyout": "バイアウト",
        "licensing": "ライセンシング",
    }
    parts.append(
        f"{inv_type_ja.get(investment_type, investment_type)}の観点から"
        f"総合スコアは{overall:.1f}/100で、{rec_map.get(rec, rec)}。"
    )

    return "".join(parts)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def ip_due_diligence(
    store: PatentStore,
    resolver: EntityResolver,
    target_firm: str,
    investment_type: str = "venture",
    benchmark_firms: list[str] | None = None,
) -> dict[str, Any]:
    """Integrated IP due diligence for VC/PE investment decisions.

    Combines patent portfolio analysis with market signals to generate
    an investment memo-style output across seven analysis sections.

    Args:
        store: PatentStore instance (SQLite connection provider).
        resolver: EntityResolver for company name resolution.
        target_firm: Company name (any language), stock ticker, or firm_id.
        investment_type: One of "venture", "growth", "buyout", "licensing".
            Adjusts scoring weights to match the investment strategy.
        benchmark_firms: Optional list of competitor firm names for direct
            comparison in the competitive position section.

    Returns:
        Dict with seven analysis sections plus investment score and
        Japanese-language interpretation.
    """
    store._relax_timeout()
    conn = store._conn()

    # -- Validate investment_type --
    if investment_type not in _WEIGHT_PROFILES:
        investment_type = "venture"

    # -- Resolve target firm --
    firm_id, firm_name = _resolve_firm(resolver, target_firm)
    if firm_id is None:
        return {
            "error": f"Could not resolve firm: '{target_firm}'",
            "suggestion": (
                "Try the exact company name, Japanese name, or stock ticker. "
                "Use entity_resolve tool to check available names."
            ),
        }

    # -- Resolve benchmark firms --
    benchmark_firm_ids: list[str] | None = None
    benchmark_resolution: list[dict[str, Any]] = []
    if benchmark_firms:
        benchmark_firm_ids = []
        for bf in benchmark_firms:
            bf_id, bf_name = _resolve_firm(resolver, bf)
            if bf_id:
                benchmark_firm_ids.append(bf_id)
                benchmark_resolution.append({
                    "input": bf,
                    "firm_id": bf_id,
                    "name": bf_name,
                    "resolved": True,
                })
            else:
                benchmark_resolution.append({
                    "input": bf,
                    "firm_id": None,
                    "name": None,
                    "resolved": False,
                })

    # -- Run all seven analysis sections --
    portfolio = _portfolio_overview(conn, firm_id)
    moat = _technology_moat(conn, firm_id)
    quality = _ip_quality(conn, firm_id)
    coverage = _geographic_coverage(conn, firm_id)
    position = _competitive_position(conn, firm_id, benchmark_firm_ids)
    signals = _market_signals(conn, firm_id)
    score = _compute_investment_score(
        portfolio, moat, quality, coverage, position, signals,
        investment_type,
    )

    # -- Interpretation --
    interpretation = _generate_interpretation(
        firm_name or firm_id,
        investment_type,
        portfolio,
        moat,
        quality,
        coverage,
        position,
        score,
    )

    # -- Royalty reference for dominant technology --
    dominant_cpc = portfolio.get("dominant_technology", "")
    royalty_info = None
    if dominant_cpc:
        rr = get_royalty_rate(dominant_cpc)
        sector = get_sector(dominant_cpc)
        royalty_info = {
            "dominant_cpc": dominant_cpc,
            "sector": sector,
            "royalty_rate": {
                "min": rr[0],
                "typical": rr[1],
                "max": rr[2],
                "description": rr[3],
            },
            "wacc": get_wacc(dominant_cpc),
        }

    return {
        "endpoint": "ip_due_diligence",
        "target_firm": firm_id,
        "target_firm_name": firm_name,
        "investment_type": investment_type,
        "portfolio_overview": portfolio,
        "technology_moat": moat,
        "ip_quality": quality,
        "geographic_coverage": coverage,
        "competitive_position": position,
        "market_signals": signals,
        "investment_score": score,
        "royalty_reference": royalty_info,
        "benchmark_resolution": benchmark_resolution if benchmark_firms else None,
        "interpretation": interpretation,
        "disclaimer": (
            "本分析は特許データに基づく定量分析であり、投資助言ではありません。"
            "投資判断には法務・財務等の総合的DDが必要です。"
        ),
    }
