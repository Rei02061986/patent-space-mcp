"""Patent claim/scope analysis tools.

Provides abstract-based technical scope analysis, patent comparison,
and freedom-to-operate (FTO) assessment. All analysis uses patent
abstracts and CPC classification since the database does not contain
actual claim text.

Functions:
    claim_analysis    — Analyze a patent's technical scope from abstract + CPC
    claim_comparison  — Compare technical scope of 2-10 patents
    fto_analysis      — Freedom-to-operate risk assessment
"""
from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

from db.sqlite_store import PatentStore
from space.embedding_bridge import (
    _cosine_similarity,
    _unpack_embedding,
    find_matching_clusters,
    text_to_proxy_embedding,
)

# ---------------------------------------------------------------------------
# Keyword -> CPC direct mapping (reused from invention_intel.py)
# ---------------------------------------------------------------------------
_KEYWORD_CPC: dict[str, str] = {
    "電池": "H01M", "battery": "H01M", "半導体": "H01L", "semiconductor": "H01L",
    "AI": "G06N", "機械学習": "G06N", "自動運転": "B60W", "5G": "H04W",
    "ロボット": "B25J", "EV": "B60L", "燃料電池": "H01M", "太陽電池": "H02S",
    "医薬": "A61K", "触媒": "B01J", "センサー": "G01N", "カメラ": "H04N",
}

# ---------------------------------------------------------------------------
# Common stop words for abstract parsing
# ---------------------------------------------------------------------------
_EN_STOP_WORDS = frozenset({
    "a", "an", "the", "and", "or", "not", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "being", "have", "has", "had", "do", "does", "did", "will", "would",
    "could", "should", "may", "might", "shall", "can", "this", "that",
    "these", "those", "it", "its", "as", "but", "if", "so", "than", "such",
    "using", "based", "method", "system", "apparatus", "device", "present",
    "invention", "wherein", "comprising", "according", "also", "each",
    "one", "two", "first", "second", "said", "provided", "includes",
    "included", "including", "more", "other", "which", "when", "where",
    "about", "above", "below", "between", "into", "through", "during",
    "before", "after", "further", "then", "there", "here", "all", "any",
    "both", "most", "some", "no", "only", "same", "own", "up", "out",
})

_DISCLAIMER_SCOPE = (
    "This analysis is based on patent abstract and CPC classification. "
    "Full claim analysis requires review of actual claim text."
)

_DISCLAIMER_COMPARISON = (
    "This comparison is based on patent abstracts, CPC codes, and embedding "
    "similarity. Actual claim scope overlap requires professional claim chart analysis."
)

_DISCLAIMER_FTO = (
    "This FTO analysis is preliminary and based on patent metadata. "
    "Professional patent attorney review is required for actual FTO opinions."
)


# ===================================================================
# Helper: extract technical elements from abstract text
# ===================================================================

def _is_english(text: str) -> bool:
    """Heuristic: text is mostly ASCII -> likely English."""
    ascii_count = sum(1 for c in text if ord(c) < 128)
    return ascii_count / max(len(text), 1) > 0.8


def _extract_technical_elements(abstract: str | None) -> list[str]:
    """Extract key technical phrases from an abstract.

    For English text: split by sentence boundaries and punctuation,
    remove stop words, keep meaningful multi-word phrases.
    For Japanese text: split by Japanese punctuation markers.
    """
    if not abstract or not abstract.strip():
        return []

    abstract = abstract.strip()

    if _is_english(abstract):
        return _extract_en_elements(abstract)
    else:
        return _extract_ja_elements(abstract)


def _extract_en_elements(text: str) -> list[str]:
    """Extract technical phrases from English abstract."""
    # Split into sentences
    sentences = re.split(r'[.;]', text)
    elements: list[str] = []

    for sentence in sentences:
        sentence = sentence.strip()
        if len(sentence) < 10:
            continue

        # Split sentence into phrase chunks by commas and conjunctions
        chunks = re.split(r'[,()]|\band\b|\bor\b|\bwherein\b|\bwhereby\b', sentence)

        for chunk in chunks:
            chunk = chunk.strip()
            if len(chunk) < 5:
                continue

            # Tokenize and filter stop words
            words = re.split(r'\s+', chunk.lower())
            meaningful = [w for w in words if w not in _EN_STOP_WORDS and len(w) > 2]

            if not meaningful:
                continue

            # Reconstruct phrase from meaningful words (keep first 6 words)
            phrase = " ".join(meaningful[:6])
            if len(phrase) > 3 and phrase not in elements:
                elements.append(phrase)

    # Deduplicate and limit
    return elements[:15]


def _extract_ja_elements(text: str) -> list[str]:
    """Extract technical phrases from Japanese abstract."""
    # Split by Japanese sentence/clause boundaries
    segments = re.split(r'[。、；;,.\n]', text)
    elements: list[str] = []

    for seg in segments:
        seg = seg.strip()
        if len(seg) < 3:
            continue

        # Remove common filler patterns
        seg = re.sub(r'^(本発明は|この|また|さらに|そして|前記|上記)', '', seg)
        seg = seg.strip()

        if len(seg) < 3:
            continue

        # Remove trailing particles / verb endings for cleaner phrases
        seg = re.sub(r'(する|される|した|された|して|であり|であって|であること|ものである|を提供する|に関する|を有する|が設けられ|を備え)$', '', seg)
        seg = seg.strip()

        if len(seg) >= 3 and seg not in elements:
            elements.append(seg)

    return elements[:15]


# ===================================================================
# Helper: scope assessment from CPC codes
# ===================================================================

def _assess_scope(cpc_codes: list[str]) -> dict[str, Any]:
    """Assess patent scope breadth from CPC code distribution."""
    if not cpc_codes:
        return {
            "cpc_breadth": 0,
            "cpc_sections": [],
            "primary_cpc": None,
            "all_cpcs": [],
            "scope_level": "narrow",
        }

    unique_cpcs = sorted(set(cpc_codes))
    sections = sorted(set(c[0] for c in unique_cpcs if c))
    classes_4 = sorted(set(c[:4] for c in unique_cpcs if len(c) >= 4))
    subclasses = sorted(set(c[:8] for c in unique_cpcs if len(c) >= 8))

    breadth = len(unique_cpcs)
    n_sections = len(sections)
    n_classes = len(classes_4)

    if n_sections >= 3 or n_classes >= 6 or breadth >= 10:
        scope_level = "broad"
    elif n_sections >= 2 or n_classes >= 3 or breadth >= 4:
        scope_level = "moderate"
    else:
        scope_level = "narrow"

    # Primary CPC: first one flagged as is_first, else first in list
    primary = unique_cpcs[0] if unique_cpcs else None

    return {
        "cpc_breadth": breadth,
        "cpc_sections": sections,
        "cpc_classes": classes_4,
        "primary_cpc": primary,
        "all_cpcs": unique_cpcs,
        "scope_level": scope_level,
    }


# ===================================================================
# Helper: CPC Jaccard similarity
# ===================================================================

def _jaccard(set_a: set, set_b: set) -> float:
    """Jaccard coefficient between two sets."""
    if not set_a and not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return round(intersection / union, 4) if union > 0 else 0.0


# ===================================================================
# Helper: CPC keyword fallback for FTO cluster matching
# ===================================================================

def _cpc_from_keywords(text: str) -> list[str]:
    """Extract CPC codes from text via keyword matching."""
    text_lower = text.lower()
    matched: dict[str, float] = {}
    for keyword, cpc in _KEYWORD_CPC.items():
        if keyword.lower() in text_lower:
            specificity = len(keyword) / 10.0
            if cpc not in matched or specificity > matched[cpc]:
                matched[cpc] = specificity
    return sorted(matched.keys(), key=lambda c: matched[c], reverse=True)


def _find_clusters_for_cpcs(
    conn: sqlite3.Connection,
    cpc_codes: list[str],
    limit: int = 10,
) -> list[dict]:
    """Find tech_clusters matching a list of CPC codes."""
    results: list[dict] = []
    seen: set[str] = set()

    for cpc in cpc_codes:
        try:
            rows = conn.execute(
                "SELECT cluster_id, label, cpc_class, patent_count, growth_rate "
                "FROM tech_clusters WHERE cpc_class = ? "
                "ORDER BY patent_count DESC LIMIT ?",
                (cpc, limit),
            ).fetchall()
        except sqlite3.OperationalError:
            continue

        for r in rows:
            cid = r["cluster_id"]
            if cid not in seen:
                seen.add(cid)
                results.append({
                    "cluster_id": cid,
                    "label": r["label"],
                    "cpc_class": r["cpc_class"],
                    "patent_count": r["patent_count"],
                    "growth_rate": r["growth_rate"],
                })

    return results[:limit]


# ===================================================================
# Tool 1: claim_analysis
# ===================================================================

def claim_analysis(
    store: PatentStore,
    publication_number: str | None = None,
    text: str | None = None,
) -> dict[str, Any]:
    """Analyze a patent's technical scope from its abstract and CPC codes.

    Two modes:
      - publication_number: fetch the patent and analyze its scope
      - text: find matching clusters and provide cluster-level analysis

    The database does NOT contain claim text. All analysis is derived
    from the abstract and CPC classification codes.
    """
    if publication_number:
        return _analyze_patent(store, publication_number)

    if text:
        return _analyze_text(store, text)

    return {
        "endpoint": "claim_analysis",
        "error": "Either publication_number or text must be provided.",
        "disclaimer": _DISCLAIMER_SCOPE,
    }


def _analyze_patent(store: PatentStore, pub_num: str) -> dict[str, Any]:
    """Analyze a single patent by publication number."""
    conn = store._conn()

    # ------------------------------------------------------------------
    # Fetch patent metadata
    # ------------------------------------------------------------------
    try:
        pat_row = conn.execute(
            """
            SELECT p.publication_number, p.title_ja, p.title_en,
                   p.abstract_ja, p.abstract_en,
                   p.filing_date, p.publication_date,
                   p.entity_status, p.country_code, p.kind_code
            FROM patents p
            WHERE p.publication_number = ?
            """,
            (pub_num,),
        ).fetchone()
    except sqlite3.OperationalError:
        pat_row = None

    if pat_row is None:
        return {
            "endpoint": "claim_analysis",
            "error": f"Patent {pub_num} not found in database.",
            "disclaimer": _DISCLAIMER_SCOPE,
        }

    title = pat_row["title_ja"] or pat_row["title_en"] or ""
    abstract = pat_row["abstract_ja"] or pat_row["abstract_en"] or ""
    filing_date = pat_row["filing_date"]
    status = pat_row["entity_status"] or "unknown"

    # ------------------------------------------------------------------
    # Fetch CPC codes
    # ------------------------------------------------------------------
    cpc_codes: list[str] = []
    primary_cpc: str | None = None
    try:
        cpc_rows = conn.execute(
            "SELECT cpc_code, is_first FROM patent_cpc WHERE publication_number = ?",
            (pub_num,),
        ).fetchall()
        for cr in cpc_rows:
            code = cr["cpc_code"]
            if code:
                cpc_codes.append(code)
            if cr["is_first"] and code:
                primary_cpc = code
    except sqlite3.OperationalError:
        pass

    # ------------------------------------------------------------------
    # Fetch assignees
    # ------------------------------------------------------------------
    assignees: list[str] = []
    try:
        asg_rows = conn.execute(
            "SELECT harmonized_name FROM patent_assignees WHERE publication_number = ?",
            (pub_num,),
        ).fetchall()
        assignees = [r["harmonized_name"] for r in asg_rows if r["harmonized_name"]]
    except sqlite3.OperationalError:
        pass

    # ------------------------------------------------------------------
    # Fetch forward citations
    # ------------------------------------------------------------------
    forward_citations = 0
    try:
        cit_row = conn.execute(
            "SELECT forward_citations FROM citation_counts WHERE publication_number = ?",
            (pub_num,),
        ).fetchone()
        if cit_row:
            forward_citations = cit_row["forward_citations"] or 0
    except sqlite3.OperationalError:
        pass

    # ------------------------------------------------------------------
    # Fetch legal status
    # ------------------------------------------------------------------
    legal_status = status
    try:
        ls_row = conn.execute(
            "SELECT status FROM patent_legal_status WHERE publication_number = ?",
            (pub_num,),
        ).fetchone()
        if ls_row and ls_row["status"]:
            legal_status = ls_row["status"]
    except sqlite3.OperationalError:
        pass

    # ------------------------------------------------------------------
    # Extract technical elements from abstract
    # ------------------------------------------------------------------
    technical_elements = _extract_technical_elements(abstract)

    # ------------------------------------------------------------------
    # Scope assessment from CPC codes
    # ------------------------------------------------------------------
    scope = _assess_scope(cpc_codes)
    if primary_cpc:
        scope["primary_cpc"] = primary_cpc

    # ------------------------------------------------------------------
    # Find related patents (same primary CPC class, ranked by citations)
    # ------------------------------------------------------------------
    related_patents: list[dict] = []
    lookup_cpc = primary_cpc or (cpc_codes[0] if cpc_codes else None)
    if lookup_cpc:
        cpc_class_4 = lookup_cpc[:4] if len(lookup_cpc) >= 4 else lookup_cpc
        try:
            rel_rows = conn.execute(
                """
                SELECT DISTINCT c.publication_number, p.title_ja, p.title_en,
                       COALESCE(cc.forward_citations, 0) AS fwd_cit
                FROM patent_cpc c
                JOIN patents p ON p.publication_number = c.publication_number
                LEFT JOIN citation_counts cc ON cc.publication_number = c.publication_number
                WHERE c.cpc_code LIKE ? || '%'
                  AND c.publication_number != ?
                ORDER BY fwd_cit DESC
                LIMIT 10
                """,
                (cpc_class_4, pub_num),
            ).fetchall()

            # Compute CPC overlap for each related patent
            source_cpc_set = set(cpc_codes)
            for rr in rel_rows:
                rel_pub = rr["publication_number"]
                # Count how many CPC codes overlap
                try:
                    rel_cpc_rows = conn.execute(
                        "SELECT cpc_code FROM patent_cpc WHERE publication_number = ?",
                        (rel_pub,),
                    ).fetchall()
                    rel_cpc_set = {r["cpc_code"] for r in rel_cpc_rows if r["cpc_code"]}
                except sqlite3.OperationalError:
                    rel_cpc_set = set()

                overlap = len(source_cpc_set & rel_cpc_set)

                related_patents.append({
                    "publication_number": rel_pub,
                    "title": rr["title_ja"] or rr["title_en"] or "",
                    "forward_citations": rr["fwd_cit"],
                    "cpc_overlap": overlap,
                })
        except sqlite3.OperationalError:
            pass

    return {
        "endpoint": "claim_analysis",
        "patent": {
            "publication_number": pub_num,
            "title": title,
            "abstract": abstract,
            "filing_date": filing_date,
            "status": legal_status,
            "forward_citations": forward_citations,
            "assignees": assignees,
        },
        "technical_elements": technical_elements,
        "scope_assessment": scope,
        "related_patents": related_patents,
        "disclaimer": _DISCLAIMER_SCOPE,
    }


def _analyze_text(store: PatentStore, text: str) -> dict[str, Any]:
    """Analyze a technology description via cluster matching."""
    conn = store._conn()

    # Try embedding-based cluster matching first
    clusters: list[dict] = []
    match_method = "none"
    try:
        clusters = find_matching_clusters(
            store,
            text=text,
            top_k=5,
            min_similarity=0.0,
        )
        if clusters:
            match_method = "embedding"
    except sqlite3.OperationalError:
        pass

    # Fallback: CPC keyword mapping
    if not clusters:
        kw_cpcs = _cpc_from_keywords(text)
        if kw_cpcs:
            clusters_raw = _find_clusters_for_cpcs(conn, kw_cpcs, limit=5)
            clusters = [
                {**c, "similarity": 0.8, "match_method": "cpc_keyword"}
                for c in clusters_raw
            ]
            match_method = "cpc_keyword"

    if not clusters:
        return {
            "endpoint": "claim_analysis",
            "error": "Could not find matching technology clusters for the given text.",
            "suggestion": "Try more specific technology keywords or provide a publication_number.",
            "disclaimer": _DISCLAIMER_SCOPE,
        }

    # Extract elements from the input text itself
    technical_elements = _extract_technical_elements(text)

    # Aggregate CPC info from matched clusters
    all_cpcs = []
    for c in clusters:
        cpc_class = c.get("cpc_class", "")
        if cpc_class:
            all_cpcs.append(cpc_class)

    scope = _assess_scope(all_cpcs)

    return {
        "endpoint": "claim_analysis",
        "mode": "text_analysis",
        "match_method": match_method,
        "input_text": text[:500],
        "technical_elements": technical_elements,
        "matched_clusters": [
            {
                "cluster_id": c.get("cluster_id", ""),
                "label": c.get("label", ""),
                "cpc_class": c.get("cpc_class", ""),
                "similarity": c.get("similarity", 0),
                "patent_count": c.get("patent_count", 0),
            }
            for c in clusters
        ],
        "scope_assessment": scope,
        "disclaimer": _DISCLAIMER_SCOPE,
    }


# ===================================================================
# Tool 2: claim_comparison
# ===================================================================

def claim_comparison(
    store: PatentStore,
    publication_numbers: list[str] | None = None,
) -> dict[str, Any]:
    """Compare technical scope of 2-10 patents.

    Computes pairwise CPC overlap (Jaccard) and embedding similarity.
    Identifies shared and unique CPC codes across all patents.
    """
    if not publication_numbers or len(publication_numbers) < 2:
        return {
            "endpoint": "claim_comparison",
            "error": "At least 2 publication_numbers are required (max 10).",
            "disclaimer": _DISCLAIMER_COMPARISON,
        }

    if len(publication_numbers) > 10:
        publication_numbers = publication_numbers[:10]

    conn = store._conn()
    ph = ",".join("?" * len(publication_numbers))

    # ------------------------------------------------------------------
    # Batch fetch patent metadata
    # ------------------------------------------------------------------
    patent_info: dict[str, dict] = {}
    try:
        rows = conn.execute(
            f"""
            SELECT publication_number, title_ja, title_en,
                   abstract_ja, abstract_en
            FROM patents
            WHERE publication_number IN ({ph})
            """,
            publication_numbers,
        ).fetchall()
        for r in rows:
            pub = r["publication_number"]
            patent_info[pub] = {
                "publication_number": pub,
                "title": r["title_ja"] or r["title_en"] or "",
            }
    except sqlite3.OperationalError:
        pass

    # Check for missing patents
    found_pubs = list(patent_info.keys())
    missing = [p for p in publication_numbers if p not in patent_info]
    if not found_pubs:
        return {
            "endpoint": "claim_comparison",
            "error": "None of the specified patents were found in the database.",
            "missing": missing,
            "disclaimer": _DISCLAIMER_COMPARISON,
        }

    # ------------------------------------------------------------------
    # Batch fetch CPC codes
    # ------------------------------------------------------------------
    cpc_map: dict[str, set[str]] = {pub: set() for pub in found_pubs}
    try:
        cpc_rows = conn.execute(
            f"""
            SELECT publication_number, cpc_code
            FROM patent_cpc
            WHERE publication_number IN ({",".join("?" * len(found_pubs))})
            """,
            found_pubs,
        ).fetchall()
        for cr in cpc_rows:
            pub = cr["publication_number"]
            code = cr["cpc_code"]
            if pub in cpc_map and code:
                cpc_map[pub].add(code)
    except sqlite3.OperationalError:
        pass

    # Update patent_info with CPC count
    for pub in found_pubs:
        patent_info[pub]["cpc_count"] = len(cpc_map[pub])

    # ------------------------------------------------------------------
    # Batch fetch embeddings from patent_research_data
    # ------------------------------------------------------------------
    embedding_map: dict[str, Any] = {}
    try:
        emb_rows = conn.execute(
            f"""
            SELECT publication_number, embedding_v1
            FROM patent_research_data
            WHERE publication_number IN ({",".join("?" * len(found_pubs))})
              AND embedding_v1 IS NOT NULL
            """,
            found_pubs,
        ).fetchall()
        for er in emb_rows:
            pub = er["publication_number"]
            emb = _unpack_embedding(er["embedding_v1"])
            if emb is not None:
                embedding_map[pub] = emb
    except sqlite3.OperationalError:
        pass

    # ------------------------------------------------------------------
    # Compute pairwise similarities
    # ------------------------------------------------------------------
    pairwise: list[dict] = []
    for i in range(len(found_pubs)):
        for j in range(i + 1, len(found_pubs)):
            pa = found_pubs[i]
            pb = found_pubs[j]

            # CPC Jaccard
            cpc_overlap = _jaccard(cpc_map[pa], cpc_map[pb])

            # Embedding cosine similarity
            emb_sim: float | None = None
            if pa in embedding_map and pb in embedding_map:
                emb_sim = round(_cosine_similarity(embedding_map[pa], embedding_map[pb]), 4)

            # Combined score
            if emb_sim is not None:
                combined = round(0.5 * cpc_overlap + 0.5 * emb_sim, 4)
            else:
                combined = cpc_overlap

            pairwise.append({
                "patent_a": pa,
                "patent_b": pb,
                "cpc_overlap": cpc_overlap,
                "embedding_similarity": emb_sim,
                "combined": combined,
            })

    # ------------------------------------------------------------------
    # Shared CPCs (intersection of ALL patents)
    # ------------------------------------------------------------------
    all_cpc_sets = [cpc_map[pub] for pub in found_pubs if cpc_map[pub]]
    if all_cpc_sets:
        shared_cpcs = sorted(set.intersection(*all_cpc_sets))
    else:
        shared_cpcs = []

    # ------------------------------------------------------------------
    # Unique CPCs per patent
    # ------------------------------------------------------------------
    unique_cpcs: dict[str, list[str]] = {}
    for pub in found_pubs:
        others_union = set()
        for other_pub in found_pubs:
            if other_pub != pub:
                others_union |= cpc_map[other_pub]
        unique_cpcs[pub] = sorted(cpc_map[pub] - others_union)

    # ------------------------------------------------------------------
    # Overall overlap assessment
    # ------------------------------------------------------------------
    if pairwise:
        avg_combined = sum(p["combined"] for p in pairwise) / len(pairwise)
        if avg_combined >= 0.6:
            overlap_assessment = "high overlap — patents cover substantially similar technology areas"
        elif avg_combined >= 0.3:
            overlap_assessment = "moderate overlap — patents share some technology areas but have distinct aspects"
        else:
            overlap_assessment = "distinct technologies — patents cover largely different technology areas"
    else:
        avg_combined = 0.0
        overlap_assessment = "insufficient data for assessment"

    result: dict[str, Any] = {
        "endpoint": "claim_comparison",
        "patents": [patent_info[pub] for pub in found_pubs],
        "pairwise_similarity": pairwise,
        "shared_cpcs": shared_cpcs,
        "unique_cpcs": unique_cpcs,
        "overlap_assessment": overlap_assessment,
        "disclaimer": _DISCLAIMER_COMPARISON,
    }

    if missing:
        result["missing_patents"] = missing
        result["note"] = f"{len(missing)} patent(s) not found in database: {', '.join(missing)}"

    return result


# ===================================================================
# Tool 3: fto_analysis
# ===================================================================

def fto_analysis(
    store: PatentStore,
    text: str | None = None,
    cpc_codes: list[str] | None = None,
    target_jurisdiction: str = "JP",
    max_blocking: int = 20,
) -> dict[str, Any]:
    """Freedom-to-operate analysis.

    Identifies potential blocking patents, assesses risk by assignee
    concentration, and provides an expiry timeline. Uses abstract-based
    cluster matching and CPC overlap rather than claim analysis.
    """
    if not text and not cpc_codes:
        return {
            "endpoint": "fto_analysis",
            "error": "Either text or cpc_codes must be provided.",
            "disclaimer": _DISCLAIMER_FTO,
        }

    max_blocking = min(max(int(max_blocking), 1), 100)
    conn = store._conn()

    # ------------------------------------------------------------------
    # Step 1: Identify relevant technology clusters
    # ------------------------------------------------------------------
    matched_clusters: list[dict] = []
    effective_cpcs: list[str] = []

    if cpc_codes:
        # Direct CPC code lookup
        effective_cpcs = list(cpc_codes)
        matched_clusters = _find_clusters_for_cpcs(conn, cpc_codes, limit=10)

    if text and not matched_clusters:
        # Try embedding-based cluster matching
        try:
            cluster_results = find_matching_clusters(
                store,
                text=text,
                top_k=5,
                min_similarity=0.0,
            )
            for c in cluster_results:
                cpc_class = c.get("cpc_class", "")
                if cpc_class and cpc_class not in effective_cpcs:
                    effective_cpcs.append(cpc_class)
                matched_clusters.append({
                    "cluster_id": c.get("cluster_id", ""),
                    "label": c.get("label", ""),
                    "cpc_class": cpc_class,
                    "patent_count": c.get("patent_count", 0),
                    "growth_rate": c.get("growth_rate"),
                })
        except sqlite3.OperationalError:
            pass

    if text and not effective_cpcs:
        # CPC keyword fallback
        kw_cpcs = _cpc_from_keywords(text)
        if kw_cpcs:
            effective_cpcs = kw_cpcs
            if not matched_clusters:
                matched_clusters = _find_clusters_for_cpcs(conn, kw_cpcs, limit=5)

    if not effective_cpcs and not matched_clusters:
        return {
            "endpoint": "fto_analysis",
            "error": "Could not identify relevant technology areas. Try more specific keywords or CPC codes.",
            "suggestion": "Provide CPC codes directly (e.g., ['H01M', 'G06N']) or use more specific technology terms.",
            "disclaimer": _DISCLAIMER_FTO,
        }

    # ------------------------------------------------------------------
    # Step 2: Count active firms from startability_surface
    # ------------------------------------------------------------------
    active_firms_count = 0
    cluster_ids = [c["cluster_id"] for c in matched_clusters if c.get("cluster_id")]
    active_firms_list: list[dict] = []

    if cluster_ids:
        ph_cl = ",".join("?" * len(cluster_ids))
        try:
            firm_rows = conn.execute(
                f"""
                SELECT ss.firm_id, ss.cluster_id, ss.score
                FROM startability_surface ss
                WHERE ss.cluster_id IN ({ph_cl})
                  AND ss.gate_open = 1
                  AND ss.year = (
                      SELECT MAX(year) FROM startability_surface
                      WHERE cluster_id = ss.cluster_id
                  )
                ORDER BY ss.score DESC
                LIMIT 50
                """,
                cluster_ids,
            ).fetchall()
            seen_firms: set[str] = set()
            for fr in firm_rows:
                fid = fr["firm_id"]
                if fid and fid not in seen_firms:
                    seen_firms.add(fid)
                    active_firms_list.append({
                        "firm_id": fid,
                        "cluster_id": fr["cluster_id"],
                        "startability_score": round(fr["score"], 4),
                    })
            active_firms_count = len(seen_firms)
        except sqlite3.OperationalError:
            pass

    # ------------------------------------------------------------------
    # Step 3: Find potential blocking patents
    # ------------------------------------------------------------------
    blocking_patents: list[dict] = []

    if effective_cpcs:
        # Build CPC LIKE conditions
        cpc_conditions = []
        cpc_params: list[Any] = []
        for cpc in effective_cpcs[:5]:  # Limit to 5 CPC prefixes
            cpc_conditions.append("c.cpc_code LIKE ? || '%'")
            cpc_params.append(cpc)

        cpc_where = " OR ".join(cpc_conditions)

        try:
            bp_rows = conn.execute(
                f"""
                SELECT DISTINCT c.publication_number,
                       p.title_ja, p.title_en,
                       p.filing_date, p.entity_status,
                       COALESCE(cc.forward_citations, 0) AS fwd_cit,
                       pls.status AS legal_status,
                       pls.expiry_date
                FROM patent_cpc c
                JOIN patents p ON p.publication_number = c.publication_number
                LEFT JOIN citation_counts cc ON cc.publication_number = c.publication_number
                LEFT JOIN patent_legal_status pls ON pls.publication_number = c.publication_number
                WHERE ({cpc_where})
                  AND (pls.status IS NULL OR pls.status = 'alive')
                ORDER BY fwd_cit DESC
                LIMIT ?
                """,
                cpc_params + [max_blocking],
            ).fetchall()

            # Fetch assignees for blocking patents in batch
            bp_pubs = [r["publication_number"] for r in bp_rows]
            assignee_map: dict[str, tuple[str, str | None]] = {}
            if bp_pubs:
                bp_ph = ",".join("?" * len(bp_pubs))
                try:
                    asg_rows = conn.execute(
                        f"""
                        SELECT publication_number, harmonized_name, firm_id
                        FROM patent_assignees
                        WHERE publication_number IN ({bp_ph})
                        """,
                        bp_pubs,
                    ).fetchall()
                    for ar in asg_rows:
                        pub = ar["publication_number"]
                        if pub not in assignee_map:
                            assignee_map[pub] = (
                                ar["harmonized_name"] or "Unknown",
                                ar["firm_id"],
                            )
                except sqlite3.OperationalError:
                    pass

            # Build blocking patents list
            max_fwd = max((r["fwd_cit"] for r in bp_rows), default=1) or 1
            for r in bp_rows:
                pub = r["publication_number"]
                assignee_name, firm_id = assignee_map.get(pub, ("Unknown", None))
                fwd = r["fwd_cit"] or 0
                risk_contribution = round(fwd / max_fwd, 4) if max_fwd > 0 else 0.0

                blocking_patents.append({
                    "publication_number": pub,
                    "title": r["title_ja"] or r["title_en"] or "",
                    "assignee": assignee_name,
                    "firm_id": firm_id,
                    "forward_citations": fwd,
                    "filing_date": r["filing_date"],
                    "status": r["legal_status"] or r["entity_status"] or "unknown",
                    "expiry_date": r["expiry_date"],
                    "risk_contribution": risk_contribution,
                })

        except sqlite3.OperationalError:
            pass

    # ------------------------------------------------------------------
    # Step 4: Aggregate risk by assignee
    # ------------------------------------------------------------------
    assignee_risk: dict[str, dict] = {}
    for bp in blocking_patents:
        key = bp["assignee"]
        if key not in assignee_risk:
            assignee_risk[key] = {
                "assignee": key,
                "firm_id": bp["firm_id"],
                "patent_count": 0,
                "total_citations": 0,
            }
        assignee_risk[key]["patent_count"] += 1
        assignee_risk[key]["total_citations"] += bp["forward_citations"]

    # Compute risk share
    total_bp_citations = sum(d["total_citations"] for d in assignee_risk.values()) or 1
    risk_by_assignee = sorted(
        assignee_risk.values(),
        key=lambda x: x["total_citations"],
        reverse=True,
    )
    for entry in risk_by_assignee:
        entry["risk_share"] = round(entry["total_citations"] / total_bp_citations, 4)

    # ------------------------------------------------------------------
    # Step 5: Expiry timeline
    # ------------------------------------------------------------------
    expiry_timeline: list[dict] = []
    expiry_years: dict[int, int] = {}
    total_with_expiry = 0

    for bp in blocking_patents:
        exp = bp.get("expiry_date")
        if exp and isinstance(exp, int) and exp > 0:
            # expiry_date stored as YYYYMMDD integer
            year = exp // 10000
            if 2020 <= year <= 2050:
                expiry_years[year] = expiry_years.get(year, 0) + 1
                total_with_expiry += 1

    if expiry_years and total_with_expiry > 0:
        cumulative = 0
        for year in sorted(expiry_years.keys()):
            cumulative += expiry_years[year]
            expiry_timeline.append({
                "year": year,
                "expiring_count": expiry_years[year],
                "cumulative_expired_pct": round(cumulative / total_with_expiry, 4),
            })

    # ------------------------------------------------------------------
    # Step 6: Compute overall risk score and level
    # ------------------------------------------------------------------
    total_patents_in_area = sum(
        c.get("patent_count", 0) for c in matched_clusters
    )

    # Risk score: weighted combination of density, concentration, citation intensity
    density_factor = min(total_patents_in_area / 10000, 1.0) if total_patents_in_area > 0 else 0.0
    concentration_factor = (risk_by_assignee[0]["risk_share"] if risk_by_assignee else 0.0)
    citation_factor = min(
        sum(bp["forward_citations"] for bp in blocking_patents) / 500, 1.0
    ) if blocking_patents else 0.0
    firms_factor = min(active_firms_count / 30, 1.0)

    risk_score = round(
        0.35 * density_factor
        + 0.25 * concentration_factor
        + 0.25 * citation_factor
        + 0.15 * firms_factor,
        4,
    )

    if risk_score >= 0.7:
        overall_risk = "very_high"
    elif risk_score >= 0.45:
        overall_risk = "high"
    elif risk_score >= 0.2:
        overall_risk = "moderate"
    else:
        overall_risk = "low"

    # ------------------------------------------------------------------
    # Step 7: Recommendations
    # ------------------------------------------------------------------
    recommendations = _generate_fto_recommendations(
        overall_risk=overall_risk,
        risk_score=risk_score,
        blocking_patents=blocking_patents,
        risk_by_assignee=risk_by_assignee,
        expiry_timeline=expiry_timeline,
        total_patents_in_area=total_patents_in_area,
        active_firms_count=active_firms_count,
    )

    return {
        "endpoint": "fto_analysis",
        "technology_scope": {
            "input_text": text[:500] if text else None,
            "input_cpcs": cpc_codes,
            "matched_clusters": [
                {
                    "cluster_id": c.get("cluster_id", ""),
                    "label": c.get("label", ""),
                    "patent_count": c.get("patent_count", 0),
                }
                for c in matched_clusters
            ],
        },
        "risk_assessment": {
            "overall_risk": overall_risk,
            "risk_score": risk_score,
            "total_patents_in_area": total_patents_in_area,
            "active_firms_count": active_firms_count,
        },
        "blocking_patents": blocking_patents,
        "risk_by_assignee": risk_by_assignee,
        "expiry_timeline": expiry_timeline,
        "recommendations": recommendations,
        "disclaimer": _DISCLAIMER_FTO,
    }


def _generate_fto_recommendations(
    overall_risk: str,
    risk_score: float,
    blocking_patents: list[dict],
    risk_by_assignee: list[dict],
    expiry_timeline: list[dict],
    total_patents_in_area: int,
    active_firms_count: int,
) -> list[str]:
    """Generate strategic FTO recommendations based on risk analysis."""
    recs: list[str] = []

    if overall_risk == "very_high":
        recs.append(
            "FTOリスクが非常に高い領域です。事業化前に専門の特許弁護士による"
            "詳細なクレーム分析を強く推奨します。"
        )
    elif overall_risk == "high":
        recs.append(
            "FTOリスクが高い領域です。主要な阻害特許のクレーム範囲を"
            "専門家に確認してもらうことを推奨します。"
        )
    elif overall_risk == "moderate":
        recs.append(
            "中程度のFTOリスクがあります。主要特許の動向を注視しつつ、"
            "回避設計の検討を推奨します。"
        )
    else:
        recs.append(
            "FTOリスクは比較的低い領域です。ただし、新規出願の動向には"
            "引き続き注意が必要です。"
        )

    # Concentration-based recommendation
    if risk_by_assignee and risk_by_assignee[0].get("risk_share", 0) >= 0.4:
        top_assignee = risk_by_assignee[0]["assignee"]
        recs.append(
            f"特許が{top_assignee}に集中しています。"
            "ライセンス交渉またはクロスライセンスの可能性を検討してください。"
        )

    # Expiry-based recommendation
    near_expiry = [e for e in expiry_timeline if e["year"] <= 2028]
    if near_expiry:
        total_near = sum(e["expiring_count"] for e in near_expiry)
        recs.append(
            f"2028年までに{total_near}件の関連特許が期限切れとなる見込みです。"
            "期限切れを待つ戦略も選択肢になり得ます。"
        )

    # Design-around recommendation
    if blocking_patents and total_patents_in_area > 500:
        recs.append(
            "出願密度が高い領域です。差別化された技術アプローチによる"
            "回避設計(design-around)を検討してください。"
        )

    # Multi-firm landscape
    if active_firms_count >= 10:
        recs.append(
            f"この技術領域には{active_firms_count}社以上の活発な出願人がいます。"
            "パテントプールや標準必須特許(SEP)の可能性を確認してください。"
        )

    return recs
