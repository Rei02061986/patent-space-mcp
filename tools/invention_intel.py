"""Invention-Time Intelligence tool implementation.

Given a technology description, identifies the relevant patent cluster,
provides prior art analysis, FTO risk assessment, whitespace opportunities,
and strategic recommendations.

v6: Added CPC keyword fallback for cluster matching. When text contains
domain keywords (e.g., "全固体電池", "battery"), maps them directly to
CPC codes (H01M) and finds clusters via cpc_class match. This fixes
the issue where "全固体電池の固体電解質材料" returned no clusters because
LIKE '%battery%' didn't match "batteries" in cluster labels.

v5: "FTS-cold-aware" — when first FTS5 query hits hard_deadline, immediately
abandons ALL subsequent FTS5-based approaches. Goes straight to pre-warmed-
data-only fallbacks (cluster label matching). This reduces cold-page latency
from 33-48s to ~2-5s (one I/O stall + instant fallback).
"""
from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

from db.sqlite_store import PatentStore, _sanitize_fts5
from space.embedding_bridge import (
    _cosine_similarity,
    _get_centroids,
    _unpack_embedding,
    find_matching_clusters,
    text_to_proxy_embedding,
)


# Common JP→EN tech term mapping for cluster label matching
_JP_EN_MAP = {
    "半導体": "semiconductor",
    "電池": "battery",
    "バッテリー": "battery",
    "自動運転": "autonomous",
    "人工知能": "artificial intelligence",
    "機械学習": "machine learning",
    "ディープラーニング": "deep learning",
    "通信": "communication",
    "無線": "wireless",
    "光学": "optic",
    "レーザー": "laser",
    "医薬": "pharma",
    "抗体": "antibody",
    "触媒": "catalyst",
    "ロボット": "robot",
    "センサー": "sensor",
    "ディスプレイ": "display",
    "メモリ": "memory",
    "燃料電池": "fuel cell",
    "太陽電池": "solar cell",
    "有機EL": "organic light",
    "画像処理": "image process",
    "音声認識": "speech recognition",
    "自動車": "vehicle",
    "モーター": "motor",
    "圧縮機": "compressor",
    "フィルタ": "filter",
    "樹脂": "resin",
    "接着": "adhesive",
    "塗料": "coating",
    "微細": "fine pattern",
    "パターン": "pattern",
    "製造": "manufactur",
    "プロセス": "process",
}


def _is_english(text: str) -> bool:
    """Heuristic: text is mostly ASCII = English."""
    ascii_count = sum(1 for c in text if ord(c) < 128)
    return ascii_count / max(len(text), 1) > 0.8


def _extract_keywords(text: str) -> list[str]:
    """Extract meaningful keywords from text for LIKE matching.

    For Japanese text, translates common tech terms to English
    since cluster labels are English-only.
    """
    if _is_english(text):
        stop_words = {
            "a", "an", "the", "and", "or", "not", "in", "on", "at", "to", "for",
            "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
            "have", "has", "had", "do", "does", "this", "that", "it", "its",
            "using", "based", "method", "system", "apparatus", "device",
            "comprising", "wherein", "according", "invention", "present",
        }
        words = re.split(r'\W+', text.lower())
        return [w for w in words if len(w) > 2 and w not in stop_words][:6]
    else:
        # Japanese: translate known terms to English
        keywords = []
        for jp, en in _JP_EN_MAP.items():
            if jp in text:
                keywords.append(en)
        # Also add original Japanese words for any JP content in labels/terms
        jp_words = [w for w in text.split() if len(w) > 1][:3]
        keywords.extend(jp_words)
        return keywords[:6]


def _cluster_label_fallback(
    conn: sqlite3.Connection,
    text: str,
    top_k: int = 5,
) -> list[dict]:
    """Find matching clusters by LIKE-matching labels and top_terms (no FTS5).

    This is a lightweight fallback when FTS5 is unavailable due to cold pages.
    Searches the tech_clusters table (607 rows) which is always in page cache.
    """
    keywords = _extract_keywords(text)
    if not keywords:
        return []

    like_parts = []
    params = []
    for kw in keywords[:4]:
        like_parts.append("(label LIKE '%' || ? || '%' OR top_terms LIKE '%' || ? || '%')")
        params.extend([kw, kw])

    if not like_parts:
        return []

    where = " OR ".join(like_parts)

    try:
        rows = conn.execute(
            f"""
            SELECT cluster_id, label, cpc_class, patent_count, growth_rate,
                   top_applicants, top_terms
            FROM tech_clusters
            WHERE {where}
            ORDER BY patent_count DESC
            LIMIT ?
            """,
            params + [top_k],
        ).fetchall()
    except sqlite3.OperationalError:
        return []

    results = []
    for r in rows:
        label_lower = (r["label"] or "").lower()
        terms_lower = (r["top_terms"] or "").lower()
        match_count = sum(1 for kw in keywords if kw in label_lower or kw in terms_lower)
        sim = round(match_count / max(len(keywords), 1), 4)

        results.append({
            "cluster_id": r["cluster_id"],
            "label": r["label"],
            "cpc_class": r["cpc_class"],
            "similarity": sim,
            "patent_count": r["patent_count"],
            "growth_rate": r["growth_rate"],
            "match_method": "label_like_fallback",
        })

    results.sort(key=lambda x: x["similarity"], reverse=True)
    return results[:top_k]


# Keyword → CPC direct mapping (covers common Japanese/English tech terms)
_KEYWORD_CPC = {
    "電池": "H01M", "バッテリー": "H01M", "battery": "H01M",
    "全固体電池": "H01M", "solid-state battery": "H01M", "固体電解質": "H01M",
    "リチウム": "H01M", "lithium": "H01M",
    "半導体": "H01L", "semiconductor": "H01L",
    "AI": "G06N", "人工知能": "G06N", "機械学習": "G06N", "machine learning": "G06N",
    "深層学習": "G06N", "deep learning": "G06N", "ニューラル": "G06N",
    "自動運転": "B60W", "autonomous driving": "B60W", "ADAS": "B60W",
    "ロボット": "B25J", "robot": "B25J",
    "5G": "H04W", "通信": "H04W", "wireless": "H04W",
    "量子": "G06N", "quantum": "G06N",
    "水素": "C01B", "hydrogen": "C01B",
    "燃料電池": "H01M", "fuel cell": "H01M",
    "有機EL": "H10K", "OLED": "H10K",
    "太陽電池": "H02S", "solar cell": "H02S", "solar": "H02S",
    "医薬": "A61K", "pharmaceutical": "A61K", "drug": "A61K",
    "遺伝子": "C12N", "gene": "C12N", "CRISPR": "C12N",
    "ブロックチェーン": "G06Q", "blockchain": "G06Q",
    "3Dプリンタ": "B33Y", "additive manufacturing": "B33Y",
    "EV": "B60L", "電気自動車": "B60L", "electric vehicle": "B60L",
    "ドローン": "B64U", "drone": "B64U", "UAV": "B64U",
    "触媒": "B01J", "catalyst": "B01J",
    "フィルタ": "B01D", "filter": "B01D", "濾過": "B01D",
    "樹脂": "C08L", "resin": "C08L", "ポリマー": "C08L", "polymer": "C08L",
    "センサー": "G01N", "sensor": "G01N",
    "ディスプレイ": "G09G", "display": "G09G",
    "メモリ": "G11C", "memory": "G11C",
    "モーター": "H02K", "motor": "H02K",
    "カメラ": "H04N", "camera": "H04N", "画像": "G06T", "image": "G06T",
}


def _cpc_keyword_fallback(
    conn: sqlite3.Connection,
    text: str,
    top_k: int = 5,
) -> list[dict]:
    """Map text keywords directly to CPC codes, then find clusters by cpc_class.

    This is the most reliable fallback — bypasses LIKE matching entirely.
    Works because tech_clusters has cpc_class indexed.
    """
    text_lower = text.lower()
    matched_cpcs: dict[str, float] = {}  # cpc → specificity score

    for keyword, cpc in _KEYWORD_CPC.items():
        if keyword.lower() in text_lower:
            # Longer keywords are more specific → higher score
            specificity = len(keyword) / 10.0
            if cpc not in matched_cpcs or specificity > matched_cpcs[cpc]:
                matched_cpcs[cpc] = specificity

    if not matched_cpcs:
        return []

    # Query tech_clusters for each matched CPC
    results = []
    for cpc, specificity in sorted(matched_cpcs.items(), key=lambda x: -x[1]):
        rows = conn.execute(
            "SELECT cluster_id, label, cpc_class, patent_count, growth_rate "
            "FROM tech_clusters WHERE cpc_class = ? ORDER BY patent_count DESC LIMIT ?",
            (cpc, top_k),
        ).fetchall()
        for r in rows:
            results.append({
                "cluster_id": r["cluster_id"],
                "label": r["label"],
                "cpc_class": r["cpc_class"],
                "similarity": round(min(specificity, 1.0), 4),
                "patent_count": r["patent_count"],
                "growth_rate": r["growth_rate"],
                "match_method": "cpc_keyword_fallback",
            })

    # Deduplicate by cluster_id, keep highest similarity
    seen: dict[str, dict] = {}
    for r in results:
        cid = r["cluster_id"]
        if cid not in seen or r["similarity"] > seen[cid]["similarity"]:
            seen[cid] = r

    return sorted(seen.values(), key=lambda x: -x["similarity"])[:top_k]


def _get_prior_art(
    conn: sqlite3.Connection,
    cluster_id: str,
    cpc_class: str,
    proxy_embedding,
    max_results: int = 20,
) -> list[dict]:
    """Find prior art patents in the target cluster's CPC class.

    Returns empty list on timeout (cold HDD pages) instead of re-raising.
    """
    try:
        pub_rows = conn.execute(
            """
            SELECT DISTINCT c.publication_number
            FROM patent_cpc c
            WHERE c.cpc_code LIKE ? || '%'
            LIMIT ?
            """,
            (cpc_class, max_results * 3),
        ).fetchall()
    except sqlite3.OperationalError:
        # CPC table cold — return empty rather than crashing
        return []

    if not pub_rows:
        return []

    pub_numbers = [r["publication_number"] for r in pub_rows]
    ph = ",".join("?" * len(pub_numbers))
    try:
        rows = conn.execute(
            f"""
            SELECT publication_number, title_ja, title_en,
                   filing_date, entity_status
            FROM patents
            WHERE publication_number IN ({ph})
            ORDER BY publication_date DESC
            """,
            pub_numbers,
        ).fetchall()
    except sqlite3.OperationalError:
        return []

    results = []
    for r in rows:
        results.append({
            "publication_number": r["publication_number"],
            "title": r["title_ja"] or r["title_en"] or "",
            "filing_date": r["filing_date"],
            "assignees": "",
            "similarity": None,
        })

    return results[:max_results]


def _assess_fto_risk(
    conn: sqlite3.Connection,
    cluster_id: str,
    cpc_class: str,
    proxy_embedding,
) -> dict:
    """Assess freedom-to-operate risk using tech_clusters and startability_surface.

    Both tables are pre-warmed, so this should always be fast.
    """
    try:
        cluster = conn.execute(
            "SELECT patent_count, top_applicants FROM tech_clusters WHERE cluster_id = ?",
            (cluster_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        cluster = None

    patent_count = cluster["patent_count"] if cluster else 0

    try:
        top_firms = conn.execute(
            """
            SELECT ss.firm_id, ss.score
            FROM startability_surface ss
            WHERE ss.cluster_id = ? AND ss.gate_open = 1
            ORDER BY ss.score DESC
            LIMIT 10
            """,
            (cluster_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        top_firms = []

    blocking_entities = [
        {"firm_id": r["firm_id"], "startability": round(r["score"], 3)}
        for r in top_firms
    ]

    if patent_count == 0:
        risk_level = "low"
    elif patent_count < 500:
        risk_level = "moderate"
    elif patent_count < 5000:
        risk_level = "high"
    else:
        risk_level = "very_high"

    active_firms = len(blocking_entities)
    if active_firms >= 20 and risk_level == "moderate":
        risk_level = "high"

    return {
        "risk_level": risk_level,
        "total_patents_in_area": patent_count,
        "active_firms_count": active_firms,
        "top_blocking_entities": blocking_entities[:5],
    }


def _find_whitespace(
    conn: sqlite3.Connection,
    primary_cluster_id: str,
    proxy_embedding,
    centroids: dict,
    top_n: int = 5,
) -> list[dict]:
    """Find adjacent whitespace clusters using pre-warmed tech_clusters."""
    if proxy_embedding is None:
        return []

    primary_centroid = centroids.get(primary_cluster_id)
    if primary_centroid is None:
        return []

    candidates = []
    for cid, centroid in centroids.items():
        if cid == primary_cluster_id:
            continue
        sim = _cosine_similarity(primary_centroid, centroid)
        if sim < 0.3:
            continue
        candidates.append((cid, sim))

    candidates.sort(key=lambda x: x[1], reverse=True)

    candidate_ids = [cid for cid, _ in candidates[:top_n * 3]]
    if not candidate_ids:
        return []

    ph = ",".join("?" * len(candidate_ids))
    try:
        tc_rows = conn.execute(
            f"""
            SELECT cluster_id, label, cpc_class, patent_count
            FROM tech_clusters
            WHERE cluster_id IN ({ph})
            """,
            candidate_ids,
        ).fetchall()
    except sqlite3.OperationalError:
        return []

    tc_meta = {r["cluster_id"]: r for r in tc_rows}

    try:
        mom_rows = conn.execute(
            f"""
            SELECT cluster_id, growth_rate
            FROM tech_cluster_momentum
            WHERE cluster_id IN ({ph})
              AND year = (SELECT MAX(year) FROM tech_cluster_momentum)
            """,
            candidate_ids,
        ).fetchall()
    except sqlite3.OperationalError:
        mom_rows = []

    mom_map = {r["cluster_id"]: r["growth_rate"] or 0 for r in mom_rows}

    results = []
    for cid, sim in candidates[:top_n * 3]:
        row = tc_meta.get(cid)
        if row is None:
            continue

        patent_count = row["patent_count"] or 0
        momentum = mom_map.get(cid, 0)

        density_score = max(0, 1 - patent_count / 10000)
        ws_score = (
            sim * 0.5
            + density_score * 0.3
            + max(0, min(1, momentum)) * 0.2
        )

        results.append({
            "cluster_id": cid,
            "cpc_class": row["cpc_class"],
            "label": row["label"],
            "proximity_to_query": round(sim, 4),
            "patent_count": patent_count,
            "momentum": round(momentum, 3),
            "whitespace_score": round(ws_score, 3),
        })

    results.sort(key=lambda x: x["whitespace_score"], reverse=True)
    return results[:top_n]


def invention_intelligence(
    store: PatentStore,
    text: str,
    max_prior_art: int = 20,
    include_fto: bool = True,
    include_whitespace: bool = True,
) -> dict[str, Any]:
    """Analyze a technology description for prior art, FTO risk, and whitespace.

    v5: FTS-cold-aware. When first FTS5 query hits hard_deadline (indicated by
    OperationalError("interrupted")), immediately abandons ALL subsequent FTS5
    approaches. Uses ONLY pre-warmed data: tech_clusters (607 rows, always in
    page cache) for cluster matching, and startability_surface for FTO.

    On cold HDD pages this reduces latency from 33-48s to ~2-5s.
    """

    # Disable per-query timeout — let _safe_call's hard_deadline (12s) handle
    # safety. FTS5 queries benefit hugely from removing the progress handler
    # overhead (checking every 50K VM instructions).
    store._relax_timeout()

    # Track whether FTS5/large-table scans are available.
    # Once set to True, ALL subsequent FTS5 attempts are skipped.
    fts_cold = False

    # Step 1: Try proxy embedding via FTS-to-embedding bridge
    centroids = _get_centroids(store)
    bridge_result = {
        "matched_patents": 0,
        "embeddings_found": 0,
        "confidence": 0.0,
    }
    proxy = None

    if centroids:
        try:
            bridge_result = text_to_proxy_embedding(store, text)
            proxy = bridge_result.get("proxy_embedding")
        except sqlite3.OperationalError:
            # FTS5 timed out (hard_deadline) — mark cold, skip all FTS5
            fts_cold = True
            store._relax_timeout()  # Re-relax after OperationalError

    # Step 2: Find matching clusters
    clusters = None
    if not fts_cold:
        # FTS5 is available — try embedding-based cluster matching
        try:
            clusters = find_matching_clusters(
                store,
                proxy_embedding=proxy,
                text=text,
                top_k=5,
                min_similarity=0.0,
            )
        except sqlite3.OperationalError:
            # FTS5 timed out in cluster matching — mark cold
            fts_cold = True
            store._relax_timeout()

    # Fallback 1: CPC keyword mapping (most reliable — directly maps terms to CPC codes)
    if not clusters:
        conn = store._conn()
        clusters = _cpc_keyword_fallback(conn, text, top_k=5)

    # Fallback 2: pre-warmed-data-only cluster label matching (LIKE on labels)
    if not clusters:
        conn = store._conn()
        clusters = _cluster_label_fallback(conn, text, top_k=5)

    if not clusters:
        return {
            "endpoint": "invention_intelligence",
            "error": "Could not find relevant technology clusters for the given text.",
            "bridge_info": bridge_result,
            "suggestion": "Try a more specific technology description with domain-specific terms.",
        }

    primary = clusters[0]
    primary_cluster_id = primary["cluster_id"]
    primary_cpc_class = primary.get("cpc_class", primary_cluster_id.rsplit("_", 1)[0])

    conn = store._conn()

    # Step 3: Prior art (CPC-based — may timeout on cold pages, returns empty)
    # Skip if FTS is cold (CPC table is also cold — same HDD, same pages)
    if fts_cold:
        prior_art = []
    else:
        store._relax_timeout()
        prior_art = _get_prior_art(conn, primary_cluster_id, primary_cpc_class, proxy, max_prior_art)

    # Step 4: Cluster landscape (uses pre-warmed tech_clusters — always fast)
    store._relax_timeout()
    try:
        cluster_detail = conn.execute(
            """
            SELECT tc.cluster_id, tc.label, tc.cpc_class, tc.patent_count,
                   tc.growth_rate, tc.top_applicants, tc.top_terms,
                   tcm.growth_rate AS momentum, tcm.acceleration
            FROM tech_clusters tc
            LEFT JOIN tech_cluster_momentum tcm
                ON tc.cluster_id = tcm.cluster_id
                AND tcm.year = (SELECT MAX(year) FROM tech_cluster_momentum)
            WHERE tc.cluster_id = ?
            """,
            (primary_cluster_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        cluster_detail = None

    landscape = {
        "primary_cluster": {
            "cluster_id": primary_cluster_id,
            "label": cluster_detail["label"] if cluster_detail else primary.get("label", ""),
            "cpc_class": cluster_detail["cpc_class"] if cluster_detail else primary_cpc_class,
            "patent_count": cluster_detail["patent_count"] if cluster_detail else primary.get("patent_count", 0),
            "momentum": cluster_detail["momentum"] if cluster_detail else 0,
            "acceleration": cluster_detail["acceleration"] if cluster_detail else 0,
        },
        "related_clusters": [
            {
                "cluster_id": c["cluster_id"],
                "cpc_class": c.get("cpc_class", ""),
                "label": c.get("label", ""),
                "similarity": c["similarity"],
            }
            for c in clusters[1:5]
        ],
    }

    if cluster_detail and cluster_detail["top_applicants"]:
        try:
            landscape["primary_cluster"]["top_applicants"] = json.loads(
                cluster_detail["top_applicants"]
            )[:10]
        except (json.JSONDecodeError, TypeError):
            pass

    if cluster_detail and cluster_detail["top_terms"]:
        try:
            landscape["primary_cluster"]["top_terms"] = json.loads(
                cluster_detail["top_terms"]
            )[:10]
        except (json.JSONDecodeError, TypeError):
            pass

    # Step 5: FTO assessment (uses pre-warmed startability_surface — always fast)
    fto = None
    if include_fto:
        store._relax_timeout()
        fto = _assess_fto_risk(conn, primary_cluster_id, primary_cpc_class, proxy)

    # Step 6: Whitespace analysis (uses pre-warmed centroids + tech_clusters)
    whitespace = None
    if include_whitespace:
        store._relax_timeout()
        whitespace = _find_whitespace(
            conn, primary_cluster_id, proxy, centroids, top_n=5
        )

    # Step 7: Strategic summary
    patent_count = landscape["primary_cluster"]["patent_count"] or 0
    density = "high" if patent_count > 1000 else "moderate" if patent_count > 200 else "low"
    momentum_val = landscape["primary_cluster"]["momentum"] or 0

    strategy_notes = []
    if density == "high":
        strategy_notes.append("高密度領域 — 差別化ポイントの明確化が重要")
    if momentum_val and momentum_val > 0.5:
        strategy_notes.append("高成長領域 — 早期出願で先行者利益を確保")
    if fto and fto["risk_level"] in ("high", "very_high"):
        strategy_notes.append("FTOリスク高 — 回避設計または既存権利者とのライセンス交渉を検討")
    if whitespace:
        ws = whitespace[0]
        strategy_notes.append(
            f"隣接ホワイトスペース: {ws['label']}({ws['cpc_class']}) — "
            f"出願密度低く参入余地あり"
        )

    match_method = clusters[0].get("match_method", "embedding")
    prior_art_note = ""
    if not prior_art:
        prior_art_note = "Prior art search skipped (database cache warming up). Try again in a few minutes."

    result = {
        "endpoint": "invention_intelligence",
        "match_method": match_method,
        "bridge_info": bridge_result,
        "landscape": landscape,
        "prior_art": {
            "count": len(prior_art),
            "patents": prior_art,
        },
        "fto_assessment": fto,
        "whitespace_opportunities": whitespace,
        "strategy": {
            "density": density,
            "momentum": round(momentum_val, 3) if momentum_val else 0,
            "notes": strategy_notes,
        },
    }
    if prior_art_note:
        result["prior_art"]["note"] = prior_art_note

    return result
