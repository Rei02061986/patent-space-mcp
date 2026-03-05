"""FTS-to-Embedding Bridge for Patent Space MCP.

Solves the problem of generating compatible embeddings from arbitrary text
when the underlying embeddings (Google Patents Research, 64-dim) come from
a proprietary model.

Strategy:
1. Text → FTS5 search → candidate patents
2. Candidate patents → retrieve their existing 64-dim embeddings
3. Weighted centroid of matched embeddings → proxy embedding
4. Proxy embedding → cosine similarity against cluster centroids

v2: On first FTS5 "interrupted" (hard_deadline), immediately stop all FTS5
attempts and raise. Removed LIKE fallback on patents table (13.7M rows,
equally slow on cold HDD pages). Callers should catch OperationalError
and use pre-warmed-data-only fallbacks (cluster label matching).
"""
from __future__ import annotations

import sqlite3
import struct
from functools import lru_cache
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from db.sqlite_store import PatentStore

EMBED_DIM = 64

# Stop words to remove from English FTS queries
_EN_STOP_WORDS = frozenset({
    "a", "an", "the", "and", "or", "not", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "being", "have", "has", "had", "do", "does", "did", "will", "would",
    "could", "should", "may", "might", "shall", "can", "this", "that",
    "these", "those", "it", "its", "as", "but", "if", "so", "than", "such",
    "using", "based", "method", "system", "apparatus", "device",
})


def _looks_english(text: str) -> bool:
    """Heuristic: text is mostly ASCII → likely English."""
    ascii_count = sum(1 for c in text if ord(c) < 128)
    return ascii_count / max(len(text), 1) > 0.8


def _fts_query_variants(text: str) -> list[str]:
    """Generate multiple FTS5 query variants for better matching.

    Returns a list of queries to try in order (most specific → most relaxed).
    """
    import re

    text = text.strip()
    if not text:
        return []

    variants = [text]  # original first

    # For English text, try additional strategies
    if _looks_english(text):
        # Remove stop words and short words
        words = [
            w for w in re.split(r'\W+', text.lower())
            if len(w) > 2 and w not in _EN_STOP_WORDS
        ]
        if words:
            # OR query (relaxed)
            variants.append(" OR ".join(words[:6]))
            # Individual important words
            for w in words[:3]:
                if len(w) > 4:
                    variants.append(w)
    else:
        # Japanese: try splitting into meaningful chunks
        words = [w for w in text.split() if len(w) > 1]
        if words and len(words) > 1:
            variants.append(" OR ".join(words[:5]))

    return variants


def _unpack_embedding(blob: bytes | None) -> np.ndarray | None:
    if blob is None:
        return None
    try:
        return np.array(struct.unpack("64d", blob), dtype=np.float64)
    except (struct.error, TypeError):
        return None


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    na, nb = np.linalg.norm(a), np.linalg.norm(b)
    if na == 0 or nb == 0:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


def text_to_proxy_embedding(
    store: PatentStore,
    text: str,
    max_candidates: int = 200,
    min_embeddings: int = 3,
) -> dict[str, Any]:
    """Generate a proxy 64-dim embedding from text via FTS-to-embedding bridge.

    v2: On first "interrupted" OperationalError, raises immediately (no more
    variants, no LIKE fallback). Caller should catch and use pre-warmed fallback.

    Returns:
        {
            "proxy_embedding": np.ndarray(64) or None,
            "matched_patents": int,
            "embeddings_found": int,
            "confidence": float (0-1),
        }
    """
    conn = store._conn()

    # Step 1: FTS5 search for candidate patents
    # Try multiple query strategies — but abort ALL on first "interrupted"
    fts_rows = []
    for attempt_query in _fts_query_variants(text):
        if not attempt_query:
            continue
        try:
            fts_rows = conn.execute(
                """
                SELECT f.publication_number, rank
                FROM patents_fts f
                WHERE patents_fts MATCH ?
                ORDER BY rank
                LIMIT ?
                """,
                (attempt_query, max_candidates),
            ).fetchall()
            if fts_rows:
                break
        except sqlite3.OperationalError as e:
            if "interrupted" in str(e):
                raise  # Abort immediately — FTS5 is cold
            continue

    # v2: Removed LIKE fallback on patents table (13.7M rows, equally slow
    # on cold HDD pages). When FTS5 returns nothing, we return None and let
    # the caller use pre-warmed-data-only fallbacks.

    if not fts_rows:
        return {
            "proxy_embedding": None,
            "matched_patents": 0,
            "embeddings_found": 0,
            "confidence": 0.0,
        }

    # Step 2: Retrieve embeddings for matched patents
    pub_numbers = [r[0] for r in fts_rows]
    # FTS5 rank is negative (more negative = better match)
    ranks = [abs(r[1]) if r[1] else 1.0 for r in fts_rows]

    # Batch lookup embeddings
    placeholders = ",".join("?" for _ in pub_numbers)
    emb_rows = conn.execute(
        f"""
        SELECT publication_number, embedding_v1
        FROM patent_research_data
        WHERE publication_number IN ({placeholders})
          AND embedding_v1 IS NOT NULL
        """,
        pub_numbers,
    ).fetchall()

    if len(emb_rows) < min_embeddings:
        return {
            "proxy_embedding": None,
            "matched_patents": len(pub_numbers),
            "embeddings_found": len(emb_rows),
            "confidence": 0.0,
        }

    # Step 3: Compute weighted centroid
    pub_to_rank = dict(zip(pub_numbers, ranks))
    embeddings = []
    weights = []

    for pub, blob in emb_rows:
        emb = _unpack_embedding(blob)
        if emb is not None:
            embeddings.append(emb)
            # Weight = 1/rank (higher rank → lower weight)
            rank_val = pub_to_rank.get(pub, 1.0)
            weights.append(1.0 / max(rank_val, 0.01))

    if not embeddings:
        return {
            "proxy_embedding": None,
            "matched_patents": len(pub_numbers),
            "embeddings_found": 0,
            "confidence": 0.0,
        }

    emb_matrix = np.stack(embeddings, axis=0)
    weight_arr = np.array(weights, dtype=np.float64)
    weight_arr /= weight_arr.sum()

    proxy = np.average(emb_matrix, axis=0, weights=weight_arr)

    # Confidence based on number of embeddings found
    confidence = min(1.0, len(embeddings) / 20.0)

    return {
        "proxy_embedding": proxy,
        "matched_patents": len(pub_numbers),
        "embeddings_found": len(embeddings),
        "confidence": confidence,
    }


def load_cluster_centroids(store: PatentStore) -> dict[str, np.ndarray]:
    """Load all tech_clusters center vectors into memory.

    ~607 clusters × 64 floats × 8 bytes = ~300KB. Cached after first call.
    """
    rows = store._conn().execute(
        "SELECT cluster_id, center_vector FROM tech_clusters WHERE center_vector IS NOT NULL"
    ).fetchall()

    centroids = {}
    for cluster_id, blob in rows:
        emb = _unpack_embedding(blob)
        if emb is not None:
            centroids[cluster_id] = emb

    return centroids


# Module-level cache for centroids
_CENTROID_CACHE: dict[str, np.ndarray] | None = None


def _get_centroids(store: PatentStore) -> dict[str, np.ndarray]:
    global _CENTROID_CACHE
    if _CENTROID_CACHE is None:
        _CENTROID_CACHE = load_cluster_centroids(store)
    return _CENTROID_CACHE


def _fts_cpc_fallback(
    store: PatentStore,
    proxy_embedding: np.ndarray | None,
    text: str | None,
    top_k: int,
    exclude_cpc_section: str | None,
    min_similarity: float,
) -> list[dict[str, Any]]:
    """Fallback: find clusters by CPC codes of FTS-matched patents.

    When cluster centroids are not available, we:
    1. Find patents matching the text via FTS
    2. Look up their CPC codes
    3. Map CPC codes to tech_clusters (via cpc_class column)
    4. Rank clusters by match frequency

    v2: On first "interrupted" from FTS5, raises immediately.
    """
    import re

    conn = store._conn()

    # Step 1: Get FTS-matched patent publication numbers
    fts_pubs = []
    if text:
        queries = _fts_query_variants(text)
        for q in queries:
            if not q:
                continue
            try:
                rows = conn.execute(
                    "SELECT publication_number FROM patents_fts WHERE patents_fts MATCH ? LIMIT 200",
                    (q,),
                ).fetchall()
                fts_pubs = [r["publication_number"] for r in rows]
                if fts_pubs:
                    break
            except sqlite3.OperationalError as e:
                if "interrupted" in str(e):
                    raise  # Abort — FTS5 is cold
                continue
            except Exception:
                continue

    if not fts_pubs:
        return []

    # Step 2: Batch fetch CPC codes for matched patents
    cpc_counts: dict[str, int] = {}
    for i in range(0, len(fts_pubs), 500):
        batch = fts_pubs[i:i + 500]
        ph = ",".join("?" * len(batch))
        try:
            cpc_rows = conn.execute(
                f"SELECT cpc_code FROM patent_cpc WHERE publication_number IN ({ph})",
                batch,
            ).fetchall()
            for cr in cpc_rows:
                code = cr["cpc_code"] or ""
                # Extract CPC class (first 4 chars, e.g., "H01M")
                cpc_class = code[:4] if len(code) >= 4 else code
                if cpc_class:
                    cpc_counts[cpc_class] = cpc_counts.get(cpc_class, 0) + 1
        except sqlite3.OperationalError as e:
            if "interrupted" in str(e):
                raise  # Abort — hard_deadline exceeded
            continue
        except Exception:
            continue

    if not cpc_counts:
        return []

    # Step 3: Rank CPC classes by frequency
    ranked_cpcs = sorted(cpc_counts.items(), key=lambda x: x[1], reverse=True)
    total_cpc_hits = sum(c for _, c in ranked_cpcs)

    # Step 4: Map to tech_clusters
    top_cpc_classes = [cpc for cpc, _ in ranked_cpcs[:top_k * 2]]
    ph = ",".join("?" * len(top_cpc_classes))
    try:
        cluster_rows = conn.execute(
            f"""
            SELECT cluster_id, label, cpc_class, patent_count, growth_rate,
                   top_applicants, top_terms
            FROM tech_clusters
            WHERE cpc_class IN ({ph})
            ORDER BY patent_count DESC
            """,
            top_cpc_classes,
        ).fetchall()
    except sqlite3.OperationalError as e:
        if "interrupted" in str(e):
            raise
        return []

    results = []
    for r in cluster_rows:
        cpc_class = r["cpc_class"] or ""
        if exclude_cpc_section and cpc_class and cpc_class[0] == exclude_cpc_section:
            continue

        # Similarity proxy: fraction of FTS-matched patents with this CPC
        cpc_freq = cpc_counts.get(cpc_class, 0)
        sim = round(cpc_freq / total_cpc_hits, 4) if total_cpc_hits > 0 else 0

        if sim < min_similarity:
            continue

        results.append({
            "cluster_id": r["cluster_id"],
            "label": r["label"],
            "cpc_class": cpc_class,
            "similarity": sim,
            "patent_count": r["patent_count"],
            "growth_rate": r["growth_rate"],
            "top_applicants": _safe_json(r["top_applicants"]),
            "top_terms": _safe_json(r["top_terms"]),
            "match_method": "fts_cpc_fallback",
        })

    results.sort(key=lambda x: x["similarity"], reverse=True)
    return results[:top_k]


def find_matching_clusters(
    store: PatentStore,
    proxy_embedding: np.ndarray | None = None,
    cpc_prefix: str | None = None,
    text: str | None = None,
    top_k: int = 10,
    exclude_cpc_section: str | None = None,
    min_similarity: float = 0.0,
    skip_fts: bool = False,
) -> list[dict[str, Any]]:
    """Find top-k clusters matching a proxy embedding, CPC prefix, or text.

    Priority: proxy_embedding > cpc_prefix > text (FTS fallback).

    Args:
        skip_fts: When True, skip FTS5-based approaches (text_to_proxy_embedding
            and _fts_cpc_fallback). Use this when caller knows FTS5 is cold.
    """
    centroids = _get_centroids(store)
    conn = store._conn()

    # If no proxy embedding, try to generate one from text (unless FTS is cold)
    if proxy_embedding is None and text is not None and not skip_fts:
        result = text_to_proxy_embedding(store, text)
        proxy_embedding = result.get("proxy_embedding")

    # If still no embedding, fall back to CPC prefix matching
    if proxy_embedding is None and cpc_prefix:
        rows = conn.execute(
            """
            SELECT cluster_id, label, cpc_class, patent_count, growth_rate,
                   top_applicants, top_terms
            FROM tech_clusters
            WHERE cpc_class LIKE ? || '%'
            ORDER BY patent_count DESC
            LIMIT ?
            """,
            (cpc_prefix, top_k),
        ).fetchall()

        results = []
        for r in rows:
            cpc_class = r["cpc_class"] or ""
            if exclude_cpc_section and cpc_class and cpc_class[0] == exclude_cpc_section:
                continue
            results.append({
                "cluster_id": r["cluster_id"],
                "label": r["label"],
                "cpc_class": cpc_class,
                "similarity": 1.0,  # exact CPC match
                "patent_count": r["patent_count"],
                "growth_rate": r["growth_rate"],
                "top_applicants": _safe_json(r["top_applicants"]),
                "top_terms": _safe_json(r["top_terms"]),
            })
        return results[:top_k]

    if proxy_embedding is None:
        return []

    # Compute cosine similarity against all cluster centroids
    scored = []
    for cluster_id, centroid in centroids.items():
        sim = _cosine_similarity(proxy_embedding, centroid)
        if sim >= min_similarity:
            scored.append((cluster_id, sim))

    scored.sort(key=lambda x: x[1], reverse=True)

    # Batch fetch cluster metadata (instead of N individual queries)
    candidate_ids = [cid for cid, _ in scored[: top_k * 3]]

    # Fallback: if no centroids available, use CPC-based cluster matching
    # via FTS-matched patents' CPC codes (skip if FTS is cold)
    if not candidate_ids:
        if skip_fts:
            return []
        fallback_text = text or getattr(store, '_last_fts_query', None)
        return _fts_cpc_fallback(
            store, proxy_embedding, fallback_text, top_k,
            exclude_cpc_section, min_similarity,
        )
    ph = ",".join("?" * len(candidate_ids))
    meta_rows = conn.execute(
        f"""
        SELECT cluster_id, label, cpc_class, patent_count, growth_rate,
               top_applicants, top_terms
        FROM tech_clusters
        WHERE cluster_id IN ({ph})
        """,
        candidate_ids,
    ).fetchall()
    metadata = {r["cluster_id"]: r for r in meta_rows}

    results = []
    for cluster_id, sim in scored:
        row = metadata.get(cluster_id)
        if row is None:
            continue

        cpc_class = row["cpc_class"] or ""
        if exclude_cpc_section and cpc_class and cpc_class[0] == exclude_cpc_section:
            continue

        results.append({
            "cluster_id": row["cluster_id"],
            "label": row["label"],
            "cpc_class": cpc_class,
            "similarity": round(sim, 4),
            "patent_count": row["patent_count"],
            "growth_rate": row["growth_rate"],
            "top_applicants": _safe_json(row["top_applicants"]),
            "top_terms": _safe_json(row["top_terms"]),
        })

        if len(results) >= top_k:
            break

    return results


def _safe_json(raw: str | None) -> list | dict:
    if not raw:
        return []
    try:
        import json
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []
