"""tech_trend_alert tool — detect hot/cooling technology trends.

Combines tech_cluster_momentum, startability_delta, and GDELT signals
to identify rapidly growing clusters and rising firms.
"""
from __future__ import annotations

from typing import Any

from db.sqlite_store import PatentStore


def tech_trend_alert(
    store: PatentStore,
    year_from: int = 2020,
    year_to: int = 2024,
    min_growth: float = 0.3,
    top_n: int = 20,
) -> dict[str, Any]:
    """Detect hot and cooling technology trends."""
    store._relax_timeout()
    conn = store._conn()

    # Auto-detect best year range
    best_row = conn.execute(
        "SELECT MIN(year) as y_min, MAX(year) as y_max FROM tech_cluster_momentum"
    ).fetchone()
    if best_row and best_row["y_max"]:
        actual_year_to = min(year_to, best_row["y_max"])
        actual_year_from = max(year_from, best_row["y_min"] or year_from)
    else:
        actual_year_to = year_to
        actual_year_from = year_from

    # Hot clusters: high growth rate in latest year
    hot_rows = conn.execute(
        "SELECT m.cluster_id, m.growth_rate, m.acceleration, "
        "       t.label, t.patent_count, t.cpc_class "
        "FROM tech_cluster_momentum m "
        "JOIN tech_clusters t ON t.cluster_id = m.cluster_id "
        "WHERE m.year = ? AND m.growth_rate > ? "
        "ORDER BY m.growth_rate DESC LIMIT ?",
        (actual_year_to, min_growth, top_n),
    ).fetchall()

    hot_clusters = []
    for r in hot_rows:
        cid = r["cluster_id"]
        # Get top entrants using two indexed queries (no LEFT JOIN)
        end_rows = conn.execute(
            "SELECT firm_id, score FROM startability_surface "
            "WHERE cluster_id = ? AND year = ? ORDER BY score DESC LIMIT 30",
            (cid, actual_year_to),
        ).fetchall()
        start_map = {}
        if end_rows:
            start_rows = conn.execute(
                "SELECT firm_id, score FROM startability_surface "
                "WHERE cluster_id = ? AND year = ?",
                (cid, actual_year_from),
            ).fetchall()
            start_map = {r2["firm_id"]: r2["score"] for r2 in start_rows}

        entrants = []
        for e in end_rows:
            delta = e["score"] - start_map.get(e["firm_id"], 0)
            if delta > 0:
                entrants.append({"firm_id": e["firm_id"], "delta": round(delta, 3)})
        entrants.sort(key=lambda x: x["delta"], reverse=True)
        top_entrants = entrants[:5]

        hot_clusters.append({
            "cluster_id": cid,
            "label": r["label"],
            "cpc_class": r["cpc_class"],
            "growth_rate": round(r["growth_rate"], 3),
            "acceleration": round(r["acceleration"] or 0, 3),
            "patent_count": r["patent_count"],
            "momentum": round((r["growth_rate"] + (r["acceleration"] or 0)) / 2, 3),
            "top_entrants": top_entrants,
        })

    # Cooling clusters: negative growth
    cooling_rows = conn.execute(
        "SELECT m.cluster_id, m.growth_rate, m.acceleration, "
        "       t.label, t.patent_count, t.cpc_class "
        "FROM tech_cluster_momentum m "
        "JOIN tech_clusters t ON t.cluster_id = m.cluster_id "
        "WHERE m.year = ? AND m.growth_rate < -0.1 "
        "ORDER BY m.growth_rate ASC LIMIT ?",
        (actual_year_to, top_n),
    ).fetchall()

    cooling_clusters = [
        {
            "cluster_id": r["cluster_id"],
            "label": r["label"],
            "cpc_class": r["cpc_class"],
            "decline_rate": round(r["growth_rate"], 3),
            "patent_count": r["patent_count"],
        }
        for r in cooling_rows
    ]

    # Rising firms: aggregate from hot_clusters' top_entrants (no SQL JOIN)
    firm_delta_map: dict[str, list[float]] = {}
    for hc in hot_clusters[:10]:
        for ent in hc.get("top_entrants", []):
            fid = ent["firm_id"]
            if fid not in firm_delta_map:
                firm_delta_map[fid] = []
            firm_delta_map[fid].append(ent["delta"])

    rising_firms = []
    for fid, deltas in firm_delta_map.items():
        if len(deltas) >= 1 and sum(deltas) / len(deltas) > 0.05:
            rising_firms.append({
                "firm_id": fid,
                "entering_clusters": len(deltas),
                "avg_delta": round(sum(deltas) / len(deltas), 3),
            })
    rising_firms.sort(key=lambda x: (x["entering_clusters"], x["avg_delta"]), reverse=True)
    rising_firms = rising_firms[:top_n]

    # Build signals from multiple sources
    signals = []

    # Signal: cluster convergence (multiple firms entering same area)
    for hc in hot_clusters[:5]:
        n_entrants = len(hc["top_entrants"])
        if n_entrants >= 3:
            signals.append({
                "type": "cluster_convergence",
                "confidence": min(n_entrants / 5.0, 1.0),
                "description": (
                    f"{hc['label']}({hc['cpc_class']})に{n_entrants}社以上が"
                    f"同時参入の兆候。成長率{hc['growth_rate']:.1%}。"
                ),
                "evidence": {
                    "cluster_id": hc["cluster_id"],
                    "growth_rate": hc["growth_rate"],
                    "entrant_count": n_entrants,
                },
            })

    # Signal: acceleration (growth rate itself is accelerating)
    for hc in hot_clusters[:10]:
        if hc["acceleration"] > 0.1:
            signals.append({
                "type": "acceleration",
                "confidence": min(hc["acceleration"] / 0.5, 1.0),
                "description": (
                    f"{hc['label']}の成長が加速中（加速度: {hc['acceleration']:.3f}）。"
                    f"今後さらに出願増加を示唆するデータがある。"
                ),
                "evidence": {
                    "cluster_id": hc["cluster_id"],
                    "acceleration": hc["acceleration"],
                },
            })

    # Signal: GDELT market signals (if available)
    try:
        for rf in rising_firms[:5]:
            gdelt_row = conn.execute(
                "SELECT tone, investment_signal FROM gdelt_company_features "
                "WHERE firm_id = ? ORDER BY period_end DESC LIMIT 1",
                (rf["firm_id"],),
            ).fetchone()
            if gdelt_row and gdelt_row["investment_signal"]:
                inv_signal = gdelt_row["investment_signal"]
                if inv_signal > 0.5:
                    signals.append({
                        "type": "gdelt_investment",
                        "confidence": round(min(inv_signal, 1.0), 2),
                        "description": (
                            f"{rf['firm_id']}のメディア投資シグナルが高い"
                            f"（スコア: {inv_signal:.2f}）。積極的な技術投資を示唆するデータがある。"
                        ),
                        "evidence": {
                            "firm_id": rf["firm_id"],
                            "investment_signal": inv_signal,
                            "tone": gdelt_row["tone"],
                        },
                    })
    except Exception:
        pass  # GDELT data may not be available for all firms

    return {
        "endpoint": "tech_trend_alert",
        "year_from": actual_year_from,
        "year_to": actual_year_to,
        "hot_clusters": hot_clusters,
        "rising_firms": rising_firms,
        "cooling_clusters": cooling_clusters,
        "signals": signals,
        "summary": {
            "hot_count": len(hot_clusters),
            "cooling_count": len(cooling_clusters),
            "rising_firms_count": len(rising_firms),
            "signal_count": len(signals),
        },
        "visualization_hint": {
            "recommended_chart": "bubble",
            "title": "技術トレンドアラート",
            "axes": {"x": "growth_rate", "y": "entering_count", "size": "patent_count"},
        },
    }
