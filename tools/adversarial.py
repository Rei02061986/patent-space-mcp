"""Adversarial Strategy tool implementation.

Compares two firms' patent portfolios and generates game-theoretic
attack/defend/preempt scenarios based on cluster overlap analysis.

OPTIMIZED VERSION v3: Uses pre-computed startability_surface.
Uses score-difference based territory analysis (not binary in/out sets)
since large firms have scores across nearly all 607 clusters.
"""
from __future__ import annotations

import sqlite3
import struct
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver


def _find_best_ss_year(conn: sqlite3.Connection, firm_id: str, target_year: int) -> int:
    """Find the best available year in startability_surface for a firm.

    Year 2024 has very limited data (~50 rows total). Falls back to the
    latest year that has data for this firm, or the year with most data overall.
    """
    row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM startability_surface WHERE firm_id = ? AND year = ?",
        (firm_id, target_year),
    ).fetchone()
    if row and row["cnt"] > 5:
        return target_year

    row = conn.execute(
        """
        SELECT year, COUNT(*) AS cnt
        FROM startability_surface
        WHERE firm_id = ?
        GROUP BY year
        HAVING cnt > 5
        ORDER BY year DESC
        LIMIT 1
        """,
        (firm_id,),
    ).fetchone()
    if row:
        return row["year"]

    row = conn.execute(
        "SELECT year FROM startability_surface GROUP BY year ORDER BY COUNT(*) DESC LIMIT 1"
    ).fetchone()
    return row["year"] if row else target_year


def _get_both_firms_scores(
    conn: sqlite3.Connection, fid_a: str, fid_b: str, year: int
) -> dict[str, dict[str, float]]:
    """Get startability scores for both firms across all clusters in one query.

    Returns: {cluster_id: {"a": score_a, "b": score_b}}
    """
    rows = conn.execute(
        """
        SELECT cluster_id, firm_id, score
        FROM startability_surface
        WHERE firm_id IN (?, ?) AND year = ? AND score > 0.01
        """,
        (fid_a, fid_b, year),
    ).fetchall()

    scores: dict[str, dict[str, float]] = {}
    for r in rows:
        cid = r["cluster_id"]
        if cid not in scores:
            scores[cid] = {"a": 0.0, "b": 0.0}
        if r["firm_id"] == fid_a:
            scores[cid]["a"] = r["score"]
        else:
            scores[cid]["b"] = r["score"]

    return scores


def _get_cluster_metadata_batch(
    conn: sqlite3.Connection, cluster_ids: set[str]
) -> dict[str, dict]:
    """Fetch cluster metadata for a set of cluster IDs."""
    if not cluster_ids:
        return {}
    placeholders = ",".join("?" for _ in cluster_ids)
    rows = conn.execute(
        f"""
        SELECT tc.cluster_id, tc.label, tc.cpc_class, tc.patent_count,
               tc.growth_rate,
               tcm.growth_rate AS latest_momentum,
               tcm.acceleration
        FROM tech_clusters tc
        LEFT JOIN tech_cluster_momentum tcm
            ON tc.cluster_id = tcm.cluster_id
            AND tcm.year = (SELECT year FROM tech_cluster_momentum GROUP BY year HAVING AVG(growth_rate) > -0.3 ORDER BY year DESC LIMIT 1)
        WHERE tc.cluster_id IN ({placeholders})
        """,
        list(cluster_ids),
    ).fetchall()
    return {r["cluster_id"]: dict(r) for r in rows}


def _get_tech_distance(conn: sqlite3.Connection, firm_a: str, firm_b: str, year: int) -> dict:
    """Compute tech distance between two firms using their tech vectors."""
    import numpy as np

    rows = conn.execute(
        """
        SELECT firm_id, tech_vector
        FROM firm_tech_vectors
        WHERE firm_id IN (?, ?) AND year = ?
        """,
        (firm_a, firm_b, year),
    ).fetchall()

    vecs = {}
    for r in rows:
        blob = r["tech_vector"]
        if blob:
            try:
                vecs[r["firm_id"]] = np.array(struct.unpack("64d", blob), dtype=np.float64)
            except (struct.error, TypeError):
                pass

    if firm_a not in vecs or firm_b not in vecs:
        return {"distance": None, "cosine_similarity": None}

    va, vb = vecs[firm_a], vecs[firm_b]
    dist = float(np.linalg.norm(va - vb))
    na, nb = np.linalg.norm(va), np.linalg.norm(vb)
    cos_sim = float(np.dot(va, vb) / (na * nb)) if na > 0 and nb > 0 else 0.0

    return {"distance": round(dist, 4), "cosine_similarity": round(cos_sim, 4)}


def _get_convergence_trend(
    conn: sqlite3.Connection, firm_a: str, firm_b: str, years: int = 5
) -> float | None:
    """Compute trend in tech distance over time. Negative = converging."""
    import numpy as np

    rows = conn.execute(
        """
        SELECT a.year, a.tech_vector AS vec_a, b.tech_vector AS vec_b
        FROM firm_tech_vectors a
        JOIN firm_tech_vectors b ON a.year = b.year
        WHERE a.firm_id = ? AND b.firm_id = ?
        ORDER BY a.year DESC
        LIMIT ?
        """,
        (firm_a, firm_b, years),
    ).fetchall()

    if len(rows) < 2:
        return None

    distances = []
    for r in rows:
        try:
            va = np.array(struct.unpack("64d", r["vec_a"]), dtype=np.float64)
            vb = np.array(struct.unpack("64d", r["vec_b"]), dtype=np.float64)
            distances.append(float(np.linalg.norm(va - vb)))
        except (struct.error, TypeError):
            continue

    if len(distances) < 2:
        return None

    x = np.arange(len(distances), dtype=np.float64)
    slope = float(np.polyfit(x, distances, 1)[0])
    return round(slope, 6)


def adversarial_strategy(
    store: PatentStore,
    resolver: EntityResolver,
    firm_a: str,
    firm_b: str,
    year: int = 2024,
    scenario_count: int = 3,
) -> dict[str, Any]:
    """Compare two firms' patent portfolios and generate strategic scenarios.

    Uses score-difference based territory analysis: instead of binary
    in/out sets, classifies clusters by the gap between firms' scores.
    """

    store._relax_timeout()

    # Resolve firms
    res_a = resolver.resolve(firm_a, country_hint="JP")
    if res_a is None:
        return {"error": f"Could not resolve firm_a: '{firm_a}'"}
    res_b = resolver.resolve(firm_b, country_hint="JP")
    if res_b is None:
        return {"error": f"Could not resolve firm_b: '{firm_b}'"}

    fid_a = res_a.entity.canonical_id
    fid_b = res_b.entity.canonical_id

    conn = store._conn()

    # Find best available year
    best_year_a = _find_best_ss_year(conn, fid_a, year)
    best_year_b = _find_best_ss_year(conn, fid_b, year)
    effective_year = min(best_year_a, best_year_b)

    # Get ALL scores for both firms in one batch (FAST: single query)
    all_scores = _get_both_firms_scores(conn, fid_a, fid_b, effective_year)

    # Classify clusters by score difference
    # GAP_THRESHOLD determines when a difference is "significant"
    GAP_THRESHOLD = 0.05
    # MIN_SCORE: only consider clusters where at least one firm has meaningful presence
    MIN_SCORE = 0.3

    a_strong = []  # A significantly leads
    b_strong = []  # B significantly leads
    contested = []  # Both strong, close scores

    all_cluster_ids = set(all_scores.keys())
    cluster_meta = _get_cluster_metadata_batch(conn, all_cluster_ids)

    for cid, scores in all_scores.items():
        sa, sb = scores["a"], scores["b"]
        max_score = max(sa, sb)
        if max_score < MIN_SCORE:
            continue  # Neither firm has meaningful presence

        delta = sa - sb
        meta = cluster_meta.get(cid, {})
        entry = {
            "cluster_id": cid,
            "cpc_class": meta.get("cpc_class", ""),
            "label": meta.get("label", ""),
            "firm_a_score": round(sa, 3),
            "firm_b_score": round(sb, 3),
            "score_gap": round(delta, 3),
            "momentum": meta.get("latest_momentum") or 0,
        }

        # Use percentile-rank-adjusted gap for firms with high absolute scores
        # If both firms are in top 5% of a cluster, the gap matters less
        both_high = sa > 0.8 and sb > 0.8
        effective_threshold = GAP_THRESHOLD * 3 if both_high else GAP_THRESHOLD

        if delta > effective_threshold:
            entry["dominant"] = "firm_a"
            a_strong.append(entry)
        elif delta < -effective_threshold:
            entry["dominant"] = "firm_b"
            b_strong.append(entry)
        else:
            entry["dominant"] = "contested"
            entry["combined_strength"] = round(sa + sb, 3)
            contested.append(entry)

    # Sort: a_strong by A's lead, b_strong by B's lead, contested by combined strength
    a_strong.sort(key=lambda x: x["score_gap"], reverse=True)
    b_strong.sort(key=lambda x: x["score_gap"])  # Most negative first
    contested.sort(key=lambda x: x.get("combined_strength", 0), reverse=True)

    # Emerging opportunity clusters: highest momentum, both firms have weak presence
    # For large firms (presence everywhere), use relative weakness instead of absence
    unclaimed_rows = conn.execute(
        """
        SELECT tc.cluster_id, tc.cpc_class, tc.label,
               tcm.growth_rate AS momentum
        FROM tech_clusters tc
        JOIN tech_cluster_momentum tcm ON tc.cluster_id = tcm.cluster_id
            AND tcm.year = (SELECT year FROM tech_cluster_momentum GROUP BY year HAVING AVG(growth_rate) > -0.3 ORDER BY year DESC LIMIT 1)
        ORDER BY tcm.growth_rate DESC
        LIMIT 80
        """,
    ).fetchall()

    unclaimed_high = []
    # Determine weakness threshold dynamically based on firms' median scores
    all_a_scores = [s["a"] for s in all_scores.values()]
    all_b_scores = [s["b"] for s in all_scores.values()]
    median_a = sorted(all_a_scores)[len(all_a_scores) // 2] if all_a_scores else 0.3
    median_b = sorted(all_b_scores)[len(all_b_scores) // 2] if all_b_scores else 0.3
    weak_threshold = max(MIN_SCORE, min(median_a, median_b))

    for r in unclaimed_rows:
        cid = r["cluster_id"]
        scores = all_scores.get(cid, {"a": 0, "b": 0})
        if scores["a"] < weak_threshold and scores["b"] < weak_threshold:
            unclaimed_high.append({
                "cluster_id": cid,
                "cpc_class": r["cpc_class"],
                "label": r["label"],
                "momentum": round(r["momentum"], 4),
                "firm_a_score": round(scores["a"], 3),
                "firm_b_score": round(scores["b"], 3),
                "first_mover_advantage": "high" if r["momentum"] > 0.3 else "moderate",
            })
            if len(unclaimed_high) >= 5:
                break

    # Tech distance and convergence (from firm_tech_vectors)
    tech_dist = _get_tech_distance(conn, fid_a, fid_b, effective_year)
    convergence = _get_convergence_trend(conn, fid_a, fid_b)

    # Negotiation power: combine exclusive territory + contested advantage
    a_exclusive_strength = sum(e["score_gap"] for e in a_strong)
    b_exclusive_strength = sum(abs(e["score_gap"]) for e in b_strong)
    # Add weighted contested area advantage (firm with higher avg score in contested)
    contested_a_avg = sum(e["firm_a_score"] for e in contested) / max(len(contested), 1)
    contested_b_avg = sum(e["firm_b_score"] for e in contested) / max(len(contested), 1)
    contested_advantage_a = max(0, (contested_a_avg - contested_b_avg) * len(contested) * 0.1)
    contested_advantage_b = max(0, (contested_b_avg - contested_a_avg) * len(contested) * 0.1)
    a_total = a_exclusive_strength + contested_advantage_a + len(a_strong) * 0.01
    b_total = b_exclusive_strength + contested_advantage_b + len(b_strong) * 0.01
    combined = a_total + b_total + 0.01
    negotiation = {
        "firm_a": round(max(0.05, min(0.95, a_total / combined)), 3),
        "firm_b": round(max(0.05, min(0.95, b_total / combined)), 3),
    }

    # Generate scenarios
    scenarios = []

    # Filter: only consider clusters in firms' core CPC areas (top 5 sections)
    def _core_sections(firm_id):
        # Get top CPC sections by startability score (using tech_clusters.cpc_class)
        rows = conn.execute(
            "SELECT SUBSTR(tc.cpc_class, 1, 1) as sec, SUM(ss.score) as total "
            "FROM startability_surface ss "
            "JOIN tech_clusters tc ON ss.cluster_id = tc.cluster_id "
            "WHERE ss.firm_id = ? AND ss.year = (SELECT MAX(year) FROM startability_surface WHERE firm_id = ?) "
            "AND ss.score > 0.9 "
            "GROUP BY sec ORDER BY total DESC LIMIT 2",
            (firm_id, firm_id)
        ).fetchall()
        return set(r[0] for r in rows) if rows else set()
    core_a = _core_sections(fid_a)
    core_b = _core_sections(fid_b)
    core_all = core_a | core_b | {"G", "H"}  # Electronics and physics sections

    # Filter all territory lists by core CPC sections
    a_strong_core = [e for e in a_strong if e.get("cpc_class", "X")[0] in core_all]
    b_strong_core = [e for e in b_strong if e.get("cpc_class", "X")[0] in core_all]
    contested_core = [e for e in contested if e.get("cpc_class", "X")[0] in core_all]
    unclaimed_core = [e for e in unclaimed_high if e.get("cpc_class", "X")[0] in core_all]

    # Attack scenario: Target B's strong area where A has best chance
    if b_strong_core and len(scenarios) < scenario_count:
        # Best attack target: B-strong cluster with high momentum and small gap
        b_attackable = sorted(
            b_strong_core,
            key=lambda x: (x["momentum"] or 0) * 10 + x["firm_a_score"],
            reverse=True,
        )
        t = b_attackable[0]
        nearest_a = a_strong[0]["cpc_class"] if a_strong else "既存"
        scenarios.append({
            "type": "attack",
            "description": f"{t['label']}({t['cpc_class']})への参入",
            "target_cluster": t["cluster_id"],
            "firm_a_current_score": t["firm_a_score"],
            "firm_b_score": t["firm_b_score"],
            "gap_to_close": abs(t["score_gap"]),
            "feasibility": "high" if t["firm_a_score"] > 0.5 else ("moderate" if t["firm_a_score"] > 0.3 else "low"),
            "rationale": f"firm_aの{nearest_a}技術基盤を活用して{t['cpc_class']}に展開可能（現在{t['score_gap']:.3f}差）",
        })

    # Defend scenario: Contested area where A's lead is thin
    if contested_core and len(scenarios) < scenario_count:
        # Most strategically important: highest combined_strength (where both firms are most invested)
        vulnerable = sorted(contested_core, key=lambda x: x.get("combined_strength", 0), reverse=True)
        d = vulnerable[0]
        scenarios.append({
            "type": "defend",
            "description": f"{d['label']}({d['cpc_class']})領域でのリード維持",
            "target_cluster": d["cluster_id"],
            "urgency": "high" if (d["momentum"] or 0) > 0.5 else "medium",
            "gap": d["score_gap"],
            "momentum": d["momentum"],
            "rationale": f"高成長領域（momentum={d['momentum']:.2f}）で{abs(d['score_gap']):.3f}差のみ。防御出願で差を維持すべき",
        })

    # Preempt scenario: Unclaimed area with highest relative momentum
    if unclaimed_core and len(scenarios) < scenario_count:
        p = unclaimed_core[0]
        mom = p["momentum"] or 0
        window = "narrow" if mom > 0.3 else ("moderate" if mom > 0 else "wide")
        scenarios.append({
            "type": "preempt",
            "description": f"{p['label']}({p['cpc_class']})への先行出願",
            "target_cluster": p["cluster_id"],
            "momentum": p["momentum"],
            "window": window,
            "rationale": f"相対的に高成長の領域（momentum={mom:.3f}）で両社不在。先行出願で将来の交渉力を確保",
        })

    # Year fallback note
    year_note = None
    if effective_year != year:
        year_note = (
            f"指定年({year})のデータが不十分なため、"
            f"直近の利用可能年({effective_year})のデータを使用しています。"
        )

    return {
        "endpoint": "adversarial_strategy",
        "analysis_year": effective_year,
        "requested_year": year,
        **({"year_note": year_note} if year_note else {}),
        "overview": {
            "firm_a": {
                "firm_id": fid_a,
                "name": res_a.entity.canonical_name,
                "strong_clusters": len(a_strong),
            },
            "firm_b": {
                "firm_id": fid_b,
                "name": res_b.entity.canonical_name,
                "strong_clusters": len(b_strong),
            },
            "contested_clusters": len(contested),
            "total_clusters_analyzed": len(all_scores),
            "tech_distance": tech_dist["distance"],
            "tech_cosine_similarity": tech_dist["cosine_similarity"],
            "convergence_trend": convergence,
            "negotiation_power": negotiation,
        },
        "territory_map": {
            "firm_a_leads": a_strong[:10],
            "firm_b_leads": b_strong[:10],
            "contested": contested[:10],
            "unclaimed_high_momentum": unclaimed_high[:5],
        },
        "scenarios": scenarios,
    }
