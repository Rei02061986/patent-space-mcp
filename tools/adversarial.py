"""Adversarial Strategy tool implementation.

Compares two firms' patent portfolios and generates game-theoretic
attack/defend/preempt scenarios based on cluster overlap analysis.
"""
from __future__ import annotations

import json
import sqlite3
import struct
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver


def _get_firm_clusters_from_patents(
    conn: sqlite3.Connection, firm_id: str
) -> dict[str, int]:
    """Get cluster_id → patent_count for a firm from patent_cluster_mapping."""
    rows = conn.execute(
        """
        SELECT pcm.cluster_id, COUNT(DISTINCT pcm.publication_number) AS cnt
        FROM patent_cluster_mapping pcm
        JOIN patent_assignees pa ON pcm.publication_number = pa.publication_number
        WHERE pa.firm_id = ?
        GROUP BY pcm.cluster_id
        """,
        (firm_id,),
    ).fetchall()
    return {r["cluster_id"]: r["cnt"] for r in rows}


def _get_cluster_metadata(
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
            AND tcm.year = (SELECT MAX(year) FROM tech_cluster_momentum)
        WHERE tc.cluster_id IN ({placeholders})
        """,
        list(cluster_ids),
    ).fetchall()
    return {r["cluster_id"]: dict(r) for r in rows}


def _get_startability_for_firm(
    conn: sqlite3.Connection, firm_id: str, cluster_ids: set[str], year: int
) -> dict[str, float]:
    """Get startability scores for a firm across specific clusters."""
    if not cluster_ids:
        return {}
    placeholders = ",".join("?" for _ in cluster_ids)
    rows = conn.execute(
        f"""
        SELECT cluster_id, score
        FROM startability_surface
        WHERE firm_id = ? AND year = ?
          AND cluster_id IN ({placeholders})
        """,
        [firm_id, year] + list(cluster_ids),
    ).fetchall()
    return {r["cluster_id"]: r["score"] for r in rows}


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

    # Simple linear trend: negative slope = converging
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
    """Compare two firms' patent portfolios and generate strategic scenarios."""

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

    # Get cluster sets from patent_cluster_mapping
    a_clusters = _get_firm_clusters_from_patents(conn, fid_a)
    b_clusters = _get_firm_clusters_from_patents(conn, fid_b)

    a_set = set(a_clusters.keys())
    b_set = set(b_clusters.keys())

    overlap = a_set & b_set
    a_only = a_set - b_set
    b_only = b_set - a_set

    all_ids = a_set | b_set
    cluster_meta = _get_cluster_metadata(conn, all_ids)

    # Get startability for cross-territory analysis
    a_start = _get_startability_for_firm(conn, fid_a, b_only, year)
    b_start = _get_startability_for_firm(conn, fid_b, a_only, year)

    # Overlap analysis
    overlap_analysis = []
    for cid in sorted(overlap, key=lambda c: cluster_meta.get(c, {}).get("latest_momentum") or 0, reverse=True):
        meta = cluster_meta.get(cid, {})
        a_start_score = _get_startability_for_firm(conn, fid_a, {cid}, year).get(cid, 0)
        b_start_score = _get_startability_for_firm(conn, fid_b, {cid}, year).get(cid, 0)
        dominant = "firm_a" if a_start_score >= b_start_score else "firm_b"
        overlap_analysis.append({
            "cluster_id": cid,
            "cpc_class": meta.get("cpc_class", ""),
            "label": meta.get("label", ""),
            "firm_a_patents": a_clusters.get(cid, 0),
            "firm_b_patents": b_clusters.get(cid, 0),
            "firm_a_score": round(a_start_score, 3),
            "firm_b_score": round(b_start_score, 3),
            "dominant": dominant,
            "momentum": meta.get("latest_momentum") or 0,
        })

    # A-exclusive (firm_a's strengths)
    a_exclusive = []
    for cid in sorted(a_only, key=lambda c: a_clusters.get(c, 0), reverse=True):
        meta = cluster_meta.get(cid, {})
        b_potential = b_start.get(cid, 0)
        vulnerability = "low" if b_potential < 0.3 else ("high" if b_potential > 0.5 else "moderate")
        a_exclusive.append({
            "cluster_id": cid,
            "cpc_class": meta.get("cpc_class", ""),
            "label": meta.get("label", ""),
            "firm_a_patents": a_clusters.get(cid, 0),
            "momentum": meta.get("latest_momentum") or 0,
            "firm_b_potential_startability": round(b_potential, 3),
            "vulnerability": vulnerability,
        })

    # B-exclusive (firm_b's strengths, potential attack targets)
    b_exclusive = []
    for cid in sorted(b_only, key=lambda c: b_clusters.get(c, 0), reverse=True):
        meta = cluster_meta.get(cid, {})
        a_potential = a_start.get(cid, 0)
        feasibility = "high" if a_potential > 0.5 else ("moderate" if a_potential > 0.3 else "low")
        b_exclusive.append({
            "cluster_id": cid,
            "cpc_class": meta.get("cpc_class", ""),
            "label": meta.get("label", ""),
            "firm_b_patents": b_clusters.get(cid, 0),
            "momentum": meta.get("latest_momentum") or 0,
            "firm_a_potential_startability": round(a_potential, 3),
            "attack_feasibility": feasibility,
        })

    # Unclaimed high-momentum clusters
    unclaimed = conn.execute(
        """
        SELECT tc.cluster_id, tc.cpc_class, tc.label,
               tcm.growth_rate AS momentum
        FROM tech_clusters tc
        JOIN tech_cluster_momentum tcm ON tc.cluster_id = tcm.cluster_id
            AND tcm.year = (SELECT MAX(year) FROM tech_cluster_momentum)
        WHERE tcm.growth_rate > 0.3
        ORDER BY tcm.growth_rate DESC
        LIMIT 20
        """,
    ).fetchall()

    unclaimed_high = []
    for r in unclaimed:
        cid = r["cluster_id"]
        if cid not in a_set and cid not in b_set:
            unclaimed_high.append({
                "cluster_id": cid,
                "cpc_class": r["cpc_class"],
                "label": r["label"],
                "momentum": r["momentum"],
                "first_mover_advantage": "high" if r["momentum"] > 0.8 else "moderate",
            })
            if len(unclaimed_high) >= 5:
                break

    # Tech distance and convergence
    tech_dist = _get_tech_distance(conn, fid_a, fid_b, year)
    convergence = _get_convergence_trend(conn, fid_a, fid_b)

    # Compute negotiation power
    a_strength = sum(
        (cluster_meta.get(c, {}).get("latest_momentum") or 0.1) for c in a_only
    )
    b_strength = sum(
        (cluster_meta.get(c, {}).get("latest_momentum") or 0.1) for c in b_only
    )
    total = a_strength + b_strength + 0.01
    negotiation = {
        "firm_a": round(a_strength / total, 3),
        "firm_b": round(b_strength / total, 3),
    }

    # Generate scenarios
    scenarios = []

    # Attack scenario
    attack_targets = [t for t in b_exclusive if t["attack_feasibility"] != "low"]
    attack_targets.sort(key=lambda t: t.get("momentum", 0), reverse=True)
    if attack_targets and len(scenarios) < scenario_count:
        t = attack_targets[0]
        # Find nearest a_exclusive cluster
        nearest_a = a_exclusive[0]["cpc_class"] if a_exclusive else "既存"
        scenarios.append({
            "type": "attack",
            "description": f"{t['label']}({t['cpc_class']})への参入",
            "target_cluster": t["cluster_id"],
            "feasibility": t["attack_feasibility"],
            "firm_a_startability": t["firm_a_potential_startability"],
            "rationale": f"firm_aの{nearest_a}技術基盤を活用して{t['cpc_class']}に展開可能",
        })

    # Defend scenario
    defend_targets = [
        o for o in overlap_analysis
        if o["dominant"] == "firm_b" or (o["firm_a_score"] - o["firm_b_score"]) < 0.1
    ]
    defend_targets.sort(key=lambda o: o["firm_a_score"] - o["firm_b_score"])
    if defend_targets and len(scenarios) < scenario_count:
        d = defend_targets[0]
        gap = round(d["firm_a_score"] - d["firm_b_score"], 3)
        urgency = "high" if abs(gap) < 0.05 else "medium"
        scenarios.append({
            "type": "defend",
            "description": f"{d['label']}領域でのリード維持",
            "target_cluster": d["cluster_id"],
            "urgency": urgency,
            "gap": gap,
            "rationale": f"firm_bが{abs(gap):.3f}差まで迫っており、防御出願で差を維持すべき",
        })

    # Preempt scenario
    if unclaimed_high and len(scenarios) < scenario_count:
        p = unclaimed_high[0]
        window = "narrow" if p["momentum"] > 0.8 else "moderate"
        scenarios.append({
            "type": "preempt",
            "description": f"{p['label']}({p['cpc_class']})への先行出願",
            "target_cluster": p["cluster_id"],
            "momentum": p["momentum"],
            "window": window,
            "rationale": "高成長領域で両社不在。先行出願で将来の交渉力を確保",
        })

    return {
        "endpoint": "adversarial_strategy",
        "overview": {
            "firm_a": {
                "firm_id": fid_a,
                "name": res_a.entity.canonical_name,
                "total_clusters": len(a_set),
            },
            "firm_b": {
                "firm_id": fid_b,
                "name": res_b.entity.canonical_name,
                "total_clusters": len(b_set),
            },
            "overlap_clusters": len(overlap),
            "tech_distance": tech_dist["distance"],
            "tech_cosine_similarity": tech_dist["cosine_similarity"],
            "convergence_trend": convergence,
            "negotiation_power": negotiation,
        },
        "territory_map": {
            "overlap": overlap_analysis[:10],
            "firm_a_exclusive": a_exclusive[:10],
            "firm_b_exclusive": b_exclusive[:10],
            "unclaimed_high_momentum": unclaimed_high[:5],
        },
        "scenarios": scenarios,
    }
