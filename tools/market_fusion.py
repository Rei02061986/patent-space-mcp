"""Patent-Market Fusion tool implementation.

Combines patent portfolio analysis with GDELT market signals to produce
a unified assessment score. Supports multiple use cases: investment screening,
M&A target identification, license partner matching, and general analysis.
"""
from __future__ import annotations

import math
import re
import struct
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver
from space.embedding_bridge import (
    _get_centroids,
    find_matching_clusters,
    text_to_proxy_embedding,
)


# Purpose-specific weight profiles
_PURPOSE_WEIGHTS = {
    "investment": {
        "tech_strength": 0.30,
        "growth_potential": 0.35,
        "diversity": 0.15,
        "market_sentiment": 0.20,
    },
    "ma_target": {
        "tech_strength": 0.40,
        "growth_potential": 0.20,
        "diversity": 0.25,
        "market_sentiment": 0.15,
    },
    "license_match": {
        "tech_strength": 0.50,
        "growth_potential": 0.15,
        "diversity": 0.20,
        "market_sentiment": 0.15,
    },
    "general": {
        "tech_strength": 0.30,
        "growth_potential": 0.30,
        "diversity": 0.20,
        "market_sentiment": 0.20,
    },
}


def _detect_query_type(query: str) -> str:
    """Auto-detect if query is a firm name, technology description, or CPC code."""
    q = query.strip()
    # Patent publication number pattern: CC-digits-kind (e.g., JP-7637366-B1)
    if re.match(r"^[A-Z]{2}-\d+-[A-Z]\d?$", q):
        return "patent"
    # CPC pattern: single letter + digits (+ optional letter)
    if re.match(r"^[A-HY]\d{2}[A-Z]?$", q, re.IGNORECASE):
        return "technology"
    # Short queries are likely firm names; long queries are technology descriptions
    if len(q.split()) <= 4:
        return "firm"
    return "text"


def _compute_tech_strength(
    conn, firm_id: str, year: int
) -> dict[str, Any]:
    """Compute tech_strength component: portfolio size, value, cluster coverage."""
    # Patent count and avg value
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT pa.publication_number) AS patent_count,
               AVG(pvi.value_score) AS avg_value
        FROM patent_assignees pa
        LEFT JOIN patent_value_index pvi ON pa.publication_number = pvi.publication_number
        WHERE pa.firm_id = ?
        """,
        (firm_id,),
    ).fetchone()

    patent_count = row["patent_count"] if row else 0
    avg_value = row["avg_value"] if row and row["avg_value"] else 0.0

    # Cluster coverage
    cluster_row = conn.execute(
        """
        SELECT COUNT(DISTINCT ss.cluster_id) AS cluster_count,
               AVG(ss.score) AS avg_startability
        FROM startability_surface ss
        WHERE ss.firm_id = ? AND ss.year = ? AND ss.gate_open = 1
        """,
        (firm_id, year),
    ).fetchone()

    cluster_count = cluster_row["cluster_count"] if cluster_row else 0
    avg_startability = cluster_row["avg_startability"] if cluster_row and cluster_row["avg_startability"] else 0.0

    return _format_tech_strength(patent_count, avg_value, cluster_count, avg_startability)


def _format_tech_strength(
    patent_count: int, avg_value: float, cluster_count: int, avg_startability: float
) -> dict[str, Any]:
    count_score = min(1.0, math.log1p(patent_count) / math.log1p(10000))
    value_score_norm = min(1.0, avg_value)
    cluster_score = min(1.0, cluster_count / 50)
    strength = count_score * 0.4 + value_score_norm * 0.3 + cluster_score * 0.3
    return {
        "score": round(strength, 3),
        "patent_count": patent_count,
        "avg_value_score": round(avg_value, 3),
        "cluster_coverage": cluster_count,
        "avg_startability": round(avg_startability, 3),
    }


def _compute_growth_potential(
    conn, firm_id: str, year: int
) -> dict[str, Any]:
    """Compute growth_potential: portfolio momentum in high-growth clusters."""
    rows = conn.execute(
        """
        SELECT ss.cluster_id, ss.score,
               COALESCE(tcm.growth_rate, 0) AS momentum,
               COALESCE(tcm.acceleration, 0) AS acceleration
        FROM startability_surface ss
        LEFT JOIN tech_cluster_momentum tcm
            ON ss.cluster_id = tcm.cluster_id
            AND tcm.year = (SELECT MAX(year) FROM tech_cluster_momentum)
        WHERE ss.firm_id = ? AND ss.year = ? AND ss.gate_open = 1
        ORDER BY COALESCE(tcm.growth_rate, 0) DESC
        """,
        (firm_id, year),
    ).fetchall()
    return _format_growth(rows)


def _format_growth(rows) -> dict[str, Any]:
    if not rows:
        return {"score": 0.0, "high_growth_clusters": 0, "avg_momentum": 0.0}
    momentums = [r["momentum"] for r in rows]
    avg_momentum = sum(momentums) / len(momentums) if momentums else 0
    high_growth = sum(1 for m in momentums if m > 0.3)
    weighted = sum(r["score"] * max(0, r["momentum"]) for r in rows)
    total_weight = sum(r["score"] for r in rows) or 1.0
    growth_score = min(1.0, weighted / total_weight * 2)
    return {
        "score": round(growth_score, 3),
        "high_growth_clusters": high_growth,
        "avg_momentum": round(avg_momentum, 3),
    }


def _compute_diversity(
    conn, firm_id: str
) -> dict[str, Any]:
    """Compute tech diversity: CPC section spread + Shannon entropy."""
    rows = conn.execute(
        """
        SELECT SUBSTR(c.cpc_code, 1, 1) AS section, COUNT(*) AS cnt
        FROM patent_cpc c
        JOIN patent_assignees a ON c.publication_number = a.publication_number
        WHERE a.firm_id = ?
        GROUP BY SUBSTR(c.cpc_code, 1, 1)
        """,
        (firm_id,),
    ).fetchall()
    return _format_diversity(rows)


def _format_diversity(rows) -> dict[str, Any]:
    if not rows:
        return {"score": 0.0, "section_count": 0, "entropy": 0.0, "sections": {}}
    total = sum(r["cnt"] for r in rows)
    sections = {r["section"]: r["cnt"] for r in rows}
    entropy = 0.0
    for cnt in sections.values():
        p = cnt / total if total > 0 else 0
        if p > 0:
            entropy -= p * math.log2(p)
    diversity_score = min(1.0, entropy / 3.0)
    return {
        "score": round(diversity_score, 3),
        "section_count": len(sections),
        "entropy": round(entropy, 3),
        "sections": sections,
    }


def _compute_market_sentiment(
    conn, firm_id: str
) -> dict[str, Any]:
    """Compute market_sentiment from GDELT features."""
    row = conn.execute(
        """
        SELECT direction_score, openness_score, investment_score,
               governance_friction_score, leadership_score,
               total_mentions
        FROM gdelt_company_features
        WHERE firm_id = ?
        ORDER BY year DESC, quarter DESC
        LIMIT 1
        """,
        (firm_id,),
    ).fetchone()

    if not row:
        return {"score": 0.5, "available": False}

    direction = row["direction_score"] or 0
    openness = row["openness_score"] or 0
    investment = row["investment_score"] or 0
    governance = row["governance_friction_score"] or 0
    leadership = row["leadership_score"] or 0
    sentiment = (
        direction * 0.25
        + openness * 0.20
        + investment * 0.25
        + (1 - governance) * 0.15
        + leadership * 0.15
    )
    sentiment = max(0.0, min(1.0, sentiment))

    return {
        "score": round(sentiment, 3),
        "available": True,
        "direction": round(direction, 3),
        "openness": round(openness, 3),
        "investment": round(investment, 3),
        "governance_friction": round(governance, 3),
        "leadership": round(leadership, 3),
        "total_mentions": row["total_mentions"],
    }


def _fast_batch_rank(
    conn, firm_ids: list[str], year: int
) -> dict[str, dict[str, Any]]:
    """Fast-path ranking using ONLY pre-computed tables.

    Uses firm_tech_vectors + startability_surface + tech_cluster_momentum
    + gdelt_company_features. NO expensive JOINs on patent_assignees or
    patent_cpc (30-44M rows).

    On HDD, this is 100x faster than _batch_compute_components because it
    only touches small tables (~30K rows each).
    """
    if not firm_ids:
        return {}

    ph = ",".join("?" * len(firm_ids))

    # 1. Tech strength from firm_tech_vectors (pre-computed patent_count + diversity)
    ftv_rows = conn.execute(
        f"""
        SELECT firm_id, patent_count, tech_diversity, tech_concentration, velocity
        FROM firm_tech_vectors
        WHERE firm_id IN ({ph}) AND year = ?
        """,
        firm_ids + [year],
    ).fetchall()
    ftv_map = {r["firm_id"]: r for r in ftv_rows}

    # 2. Cluster coverage from startability_surface
    cluster_rows = conn.execute(
        f"""
        SELECT ss.firm_id,
               COUNT(DISTINCT ss.cluster_id) AS cluster_count,
               AVG(ss.score) AS avg_startability
        FROM startability_surface ss
        WHERE ss.firm_id IN ({ph}) AND ss.year = ? AND ss.gate_open = 1
        GROUP BY ss.firm_id
        """,
        firm_ids + [year],
    ).fetchall()
    cluster_map = {
        r["firm_id"]: (r["cluster_count"], r["avg_startability"] or 0.0)
        for r in cluster_rows
    }

    # 3. Growth from startability + momentum
    growth_rows = conn.execute(
        f"""
        SELECT ss.firm_id, ss.cluster_id, ss.score,
               COALESCE(tcm.growth_rate, 0) AS momentum
        FROM startability_surface ss
        LEFT JOIN tech_cluster_momentum tcm
            ON ss.cluster_id = tcm.cluster_id
            AND tcm.year = (SELECT MAX(year) FROM tech_cluster_momentum)
        WHERE ss.firm_id IN ({ph}) AND ss.year = ? AND ss.gate_open = 1
        """,
        firm_ids + [year],
    ).fetchall()
    growth_by_firm: dict[str, list] = {}
    for r in growth_rows:
        growth_by_firm.setdefault(r["firm_id"], []).append(r)

    # 4. Market sentiment (batch)
    sent_rows = conn.execute(
        f"""
        SELECT firm_id, direction_score, openness_score, investment_score,
               governance_friction_score, leadership_score, total_mentions
        FROM gdelt_company_features
        WHERE firm_id IN ({ph})
        ORDER BY year DESC, quarter DESC
        """,
        firm_ids,
    ).fetchall()
    sent_map: dict[str, Any] = {}
    for r in sent_rows:
        if r["firm_id"] not in sent_map:
            sent_map[r["firm_id"]] = r

    # Assemble
    results: dict[str, dict[str, Any]] = {}
    for fid in firm_ids:
        ftv = ftv_map.get(fid)
        pc = ftv["patent_count"] if ftv else 0
        cc, sa = cluster_map.get(fid, (0, 0.0))
        tech = _format_tech_strength(pc, 0.0, cc, sa)

        growth = _format_growth(growth_by_firm.get(fid, []))

        td = ftv["tech_diversity"] if ftv else 0.0
        # tech_diversity is entropy (bits), typically 0-6 for CPC sub-classes.
        # Normalize to 0-1: max CPC entropy ~ log2(250 classes) ≈ 8
        td_val = td or 0.0
        div = {
            "score": round(min(1.0, td_val / 5.0), 3),
            "section_count": 0,
            "entropy": round(td_val, 3),
            "sections": {},
        }

        sent_row = sent_map.get(fid)
        if sent_row:
            direction = sent_row["direction_score"] or 0
            openness = sent_row["openness_score"] or 0
            investment = sent_row["investment_score"] or 0
            governance = sent_row["governance_friction_score"] or 0
            leadership = sent_row["leadership_score"] or 0
            s_val = (
                direction * 0.25 + openness * 0.20 + investment * 0.25
                + (1 - governance) * 0.15 + leadership * 0.15
            )
            sentiment = {"score": round(max(0.0, min(1.0, s_val)), 3), "available": True}
        else:
            sentiment = {"score": 0.5, "available": False}

        results[fid] = {
            "tech_strength": tech,
            "growth_potential": growth,
            "diversity": div,
            "market_sentiment": sentiment,
        }

    return results


def _firm_mode(
    store: PatentStore,
    resolver: EntityResolver,
    query: str,
    purpose: str,
    year: int,
) -> dict[str, Any]:
    """Handle firm query mode."""
    resolved = resolver.resolve(query, country_hint="JP")
    if resolved is None:
        return {"error": f"Could not resolve firm: '{query}'"}

    firm_id = resolved.entity.canonical_id
    firm_name = resolved.entity.canonical_name

    conn = store._conn()

    weights = _PURPOSE_WEIGHTS.get(purpose, _PURPOSE_WEIGHTS["general"])

    # Try requested year first, fall back to latest available
    actual_year = year
    components = _fast_batch_rank(conn, [firm_id], year)
    c = components.get(firm_id, {})
    # If tech_strength has no cluster coverage, try latest year
    if c.get("tech_strength", {}).get("cluster_coverage", 0) == 0:
        best_row = conn.execute(
            "SELECT MAX(year) as y FROM startability_surface WHERE firm_id = ?",
            (firm_id,),
        ).fetchone()
        best = best_row["y"] if best_row and best_row["y"] else None
        if best and best != year:
            actual_year = best
            components = _fast_batch_rank(conn, [firm_id], actual_year)
            c = components.get(firm_id, {})
    tech = c.get("tech_strength", {"score": 0})
    growth = c.get("growth_potential", {"score": 0})
    diversity = c.get("diversity", {"score": 0})
    sentiment = c.get("market_sentiment", {"score": 0.5})

    fusion_score = (
        weights["tech_strength"] * tech["score"]
        + weights["growth_potential"] * growth["score"]
        + weights["diversity"] * diversity["score"]
        + weights["market_sentiment"] * sentiment["score"]
    )

    return {
        "endpoint": "patent_market_fusion",
        "query_type": "firm",
        "firm": {
            "firm_id": firm_id,
            "name": firm_name,
        },
        "purpose": purpose,
        "fusion_score": round(fusion_score, 3),
        "components": {
            "tech_strength": tech,
            "growth_potential": growth,
            "diversity": diversity,
            "market_sentiment": sentiment,
        },
        "weights": weights,
    }


def _technology_mode(
    store: PatentStore,
    resolver: EntityResolver,
    query: str,
    purpose: str,
    year: int,
    max_results: int,
) -> dict[str, Any]:
    """Handle technology/text query mode: find and rank firms for a technology.

    v2: Falls back to direct tech_clusters CPC lookup when embedding-based
    matching fails (e.g. center_vector empty). Also auto-detects best year.
    """
    conn = store._conn()

    # Try embedding-based cluster matching first
    clusters = []
    try:
        bridge_result = text_to_proxy_embedding(store, query)
        proxy = bridge_result.get("proxy_embedding")
        clusters = find_matching_clusters(
            store,
            proxy_embedding=proxy,
            text=query if proxy is None else None,
            top_k=5,
            min_similarity=0.2,
        )
    except Exception:
        pass  # Fall through to CPC lookup

    # Fallback: direct tech_clusters CPC lookup
    if not clusters:
        q = query.strip().upper()
        # Try CPC class match (e.g., "H01L" → H01L_0)
        cpc_row = conn.execute(
            """
            SELECT cluster_id, label, cpc_class, patent_count
            FROM tech_clusters
            WHERE cpc_class = ?
            ORDER BY patent_count DESC LIMIT 1
            """,
            (q[:4],),
        ).fetchone()
        if cpc_row:
            clusters = [{
                "cluster_id": cpc_row["cluster_id"],
                "label": cpc_row["label"],
                "cpc_class": cpc_row["cpc_class"],
                "patent_count": cpc_row["patent_count"],
                "similarity": 1.0,
            }]
        else:
            # Try label LIKE match
            like = f"%{query}%"
            label_rows = conn.execute(
                """
                SELECT cluster_id, label, cpc_class, patent_count
                FROM tech_clusters
                WHERE label LIKE ? OR top_terms LIKE ?
                ORDER BY patent_count DESC LIMIT 3
                """,
                (like, like),
            ).fetchall()
            clusters = [
                {
                    "cluster_id": r["cluster_id"],
                    "label": r["label"],
                    "cpc_class": r["cpc_class"],
                    "patent_count": r["patent_count"],
                    "similarity": 0.5,
                }
                for r in label_rows
            ]

    if not clusters:
        return {
            "endpoint": "patent_market_fusion",
            "query_type": "technology",
            "error": "Could not find relevant technology clusters",
        }

    primary = clusters[0]
    primary_cluster_id = primary["cluster_id"]

    # Get top firms in this cluster (with year fallback)
    firm_rows = conn.execute(
        """
        SELECT ss.firm_id, ss.score
        FROM startability_surface ss
        WHERE ss.cluster_id = ? AND ss.year = ? AND ss.gate_open = 1
        ORDER BY ss.score DESC
        LIMIT ?
        """,
        (primary_cluster_id, year, max_results * 2),
    ).fetchall()

    # Year fallback
    actual_year = year
    if not firm_rows:
        best_row = conn.execute(
            "SELECT MAX(year) as y FROM startability_surface WHERE cluster_id = ?",
            (primary_cluster_id,),
        ).fetchone()
        best = best_row["y"] if best_row and best_row["y"] else None
        if best and best != year:
            actual_year = best
            firm_rows = conn.execute(
                """
                SELECT ss.firm_id, ss.score
                FROM startability_surface ss
                WHERE ss.cluster_id = ? AND ss.year = ? AND ss.gate_open = 1
                ORDER BY ss.score DESC
                LIMIT ?
                """,
                (primary_cluster_id, actual_year, max_results * 2),
            ).fetchall()

    weights = _PURPOSE_WEIGHTS.get(purpose, _PURPOSE_WEIGHTS["general"])
    firm_ids = [fr["firm_id"] for fr in firm_rows]
    startability_scores = {fr["firm_id"]: fr["score"] for fr in firm_rows}
    components = _fast_batch_rank(conn, firm_ids, actual_year)

    ranked_firms = []
    for fid in firm_ids:
        c = components.get(fid, {})
        ts = c.get("tech_strength", {}).get("score", 0)
        gp = c.get("growth_potential", {}).get("score", 0)
        dv = c.get("diversity", {}).get("score", 0)
        ms = c.get("market_sentiment", {}).get("score", 0.5)
        fusion = (
            weights["tech_strength"] * ts
            + weights["growth_potential"] * gp
            + weights["diversity"] * dv
            + weights["market_sentiment"] * ms
        )
        ranked_firms.append({
            "firm_id": fid,
            "fusion_score": round(fusion, 3),
            "startability_in_cluster": round(startability_scores.get(fid, 0), 3),
            "tech_strength": round(ts, 3),
            "growth_potential": round(gp, 3),
            "diversity": round(dv, 3),
            "market_sentiment": round(ms, 3),
        })

    ranked_firms.sort(key=lambda x: x["fusion_score"], reverse=True)

    return {
        "endpoint": "patent_market_fusion",
        "query_type": "technology",
        "technology_context": {
            "primary_cluster": {
                "cluster_id": primary_cluster_id,
                "label": primary.get("label", ""),
                "cpc_class": primary.get("cpc_class", ""),
                "similarity": primary.get("similarity", 0),
            },
            "related_clusters": [
                {
                    "cluster_id": c["cluster_id"],
                    "label": c.get("label", ""),
                    "similarity": c.get("similarity", 0),
                }
                for c in clusters[1:4]
            ],
        },
        "purpose": purpose,
        "year": actual_year,
        "weights": weights,
        "firms": ranked_firms[:max_results],
        "result_count": len(ranked_firms[:max_results]),
    }


def _patent_mode(
    store: PatentStore,
    resolver: EntityResolver,
    query: str,
    purpose: str,
    year: int,
    max_results: int,
) -> dict[str, Any]:
    """Handle patent query mode: map patent -> cluster -> ranked firms."""
    del resolver  # kept for signature parity with dispatch and future extensions
    patent = store.get_patent(query)
    if patent is None:
        return {
            "error": f"Patent not found: '{query}'",
            "query": query,
            "query_type": "patent",
        }

    patent_cpcs_raw = patent.get("cpc_codes", [])
    patent_cpcs = []
    primary_cpc = None
    for cpc in patent_cpcs_raw:
        if isinstance(cpc, dict):
            code = cpc.get("cpc_code")
            if code:
                patent_cpcs.append(code)
                if primary_cpc is None and cpc.get("is_first"):
                    primary_cpc = code
        else:
            code = str(cpc)
            if code:
                patent_cpcs.append(code)

    if primary_cpc is None and patent_cpcs:
        primary_cpc = patent_cpcs[0]

    assignees_raw = patent.get("assignees", [])
    assignees = []
    for a in assignees_raw:
        if isinstance(a, dict):
            name = a.get("harmonized_name") or a.get("raw_name")
            if name:
                assignees.append(name)
        else:
            name = str(a)
            if name:
                assignees.append(name)

    patent_meta = {
        "publication_number": patent.get("publication_number", query),
        "title": patent.get("title_ja") or patent.get("title_en") or "",
        "cpc_codes": patent_cpcs,
        "assignees": assignees,
        "filing_date": patent.get("filing_date"),
        "publication_date": patent.get("publication_date"),
    }

    mapping = store.get_patent_cluster(query)
    primary_cluster: dict[str, Any] | None = None
    related_clusters: list[dict[str, Any]] = []

    if mapping:
        primary_cluster = {
            "cluster_id": mapping["cluster_id"],
            "label": mapping.get("label", ""),
            "cpc_class": mapping.get("cpc_class", ""),
            "distance": mapping.get("distance"),
            "patent_count": mapping.get("patent_count"),
            "growth_rate": mapping.get("growth_rate"),
        }
        cpc_prefix = (mapping.get("cpc_class") or "")[:4]
        if cpc_prefix:
            related = find_matching_clusters(store, cpc_prefix=cpc_prefix, top_k=5)
            related_clusters = [
                c for c in related if c.get("cluster_id") != mapping["cluster_id"]
            ][:4]
    else:
        cpc_prefix = (primary_cpc or "")[:4]
        if cpc_prefix:
            clusters = find_matching_clusters(store, cpc_prefix=cpc_prefix, top_k=5)
            if clusters:
                primary_cluster = clusters[0]
                related_clusters = clusters[1:4]

    # Fallback 3: use patent's embedding to find nearest cluster
    if primary_cluster is None:
        emb_blob = store.get_patent_embedding(query)
        if emb_blob and len(emb_blob) == 512:  # 64 doubles × 8 bytes
            try:
                import numpy as np
                emb = np.array(struct.unpack("64d", emb_blob), dtype=np.float64)
                clusters = find_matching_clusters(
                    store, proxy_embedding=emb, top_k=5, min_similarity=0.1,
                )
                if clusters:
                    primary_cluster = clusters[0]
                    related_clusters = clusters[1:4]
            except (struct.error, ImportError):
                pass

    if primary_cluster is None:
        return {
            "endpoint": "patent_market_fusion",
            "query_type": "patent",
            "patent": patent_meta,
            "technology_context": {
                "primary_cluster": None,
                "related_clusters": [],
            },
            "ranked_firms": [],
            "purpose": purpose,
            "weights": _PURPOSE_WEIGHTS.get(purpose, _PURPOSE_WEIGHTS["general"]),
            "note": (
                "Cluster/startability data not yet populated. "
                "Run clustering pipeline first."
            ),
        }

    conn = store._conn()

    firm_rows = conn.execute(
        """
        SELECT ss.firm_id, ss.score
        FROM startability_surface ss
        WHERE ss.cluster_id = ? AND ss.year = ? AND ss.gate_open = 1
        ORDER BY ss.score DESC
        LIMIT ?
        """,
        (primary_cluster["cluster_id"], year, max_results * 2),
    ).fetchall()

    weights = _PURPOSE_WEIGHTS.get(purpose, _PURPOSE_WEIGHTS["general"])
    firm_ids = [fr["firm_id"] for fr in firm_rows]
    startability_scores = {fr["firm_id"]: fr["score"] for fr in firm_rows}
    components = _fast_batch_rank(conn, firm_ids, year)

    ranked_firms = []
    for fid in firm_ids:
        c = components.get(fid, {})
        ts = c.get("tech_strength", {}).get("score", 0)
        gp = c.get("growth_potential", {}).get("score", 0)
        dv = c.get("diversity", {}).get("score", 0)
        ms = c.get("market_sentiment", {}).get("score", 0.5)
        fusion = (
            weights["tech_strength"] * ts
            + weights["growth_potential"] * gp
            + weights["diversity"] * dv
            + weights["market_sentiment"] * ms
        )
        ranked_firms.append({
            "firm_id": fid,
            "fusion_score": round(fusion, 3),
            "startability_in_cluster": round(startability_scores.get(fid, 0), 3),
            "tech_strength": round(ts, 3),
            "growth_potential": round(gp, 3),
            "diversity": round(dv, 3),
            "market_sentiment": round(ms, 3),
        })

    ranked_firms.sort(key=lambda x: x["fusion_score"], reverse=True)

    response = {
        "endpoint": "patent_market_fusion",
        "query_type": "patent",
        "patent": patent_meta,
        "technology_context": {
            "primary_cluster": primary_cluster,
            "related_clusters": related_clusters,
        },
        "ranked_firms": ranked_firms[:max_results],
        "purpose": purpose,
        "weights": weights,
        "result_count": len(ranked_firms[:max_results]),
    }
    if not ranked_firms:
        response["note"] = (
            "Cluster/startability data not yet populated. "
            "Run clustering pipeline first."
        )
    return response


def patent_market_fusion(
    store: PatentStore,
    resolver: EntityResolver,
    query: str,
    query_type: str | None = None,
    purpose: str = "general",
    year: int = 2024,
    max_results: int = 10,
) -> dict[str, Any]:
    """Combine patent portfolio analysis with GDELT market signals.

    Args:
        query: Company name, technology description, CPC code, or patent publication number.
        query_type: "firm", "technology", "text", or "patent". Auto-detected if None.
        purpose: "investment", "ma_target", "license_match", or "general".
        year: Analysis year (default: 2024).
        max_results: Maximum results for technology mode.
    """
    if purpose not in _PURPOSE_WEIGHTS:
        purpose = "general"

    if query_type is None:
        query_type = _detect_query_type(query)

    if query_type == "firm":
        return _firm_mode(store, resolver, query, purpose, year)
    if query_type == "patent":
        return _patent_mode(store, resolver, query, purpose, year, max_results)
    else:
        return _technology_mode(store, resolver, query, purpose, year, max_results)
