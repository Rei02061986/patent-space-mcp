"""Cross-Domain Discovery tool implementation.

v4: Check centroids before attempting FTS5. If center_vector is empty
for all clusters, skip FTS5/embedding and go straight to label matching.
This avoids the 45s text_to_proxy_embedding call that gets killed by
the 12s _safe_call timer.
"""
from __future__ import annotations

import re
import sqlite3
from typing import Any

from db.sqlite_store import PatentStore
from space.embedding_bridge import (
    _get_centroids,
    find_matching_clusters,
    text_to_proxy_embedding,
)

_CPC_PATTERN = re.compile(r"^[A-HY]\d{2}[A-Z]?$", re.IGNORECASE)

_JP_EN_MAP = {
    "半導体": "semiconductor", "電池": "battery", "バッテリー": "battery",
    "自動運転": "autonomous", "人工知能": "artificial intelligence",
    "機械学習": "machine learning", "ディープラーニング": "deep learning",
    "通信": "communication", "無線": "wireless", "光学": "optic",
    "レーザー": "laser", "医薬": "pharma", "抗体": "antibody",
    "触媒": "catalyst", "ロボット": "robot", "センサー": "sensor",
    "量子": "quantum", "コンピュータ": "comput", "コンピューティング": "comput",
    "ディスプレイ": "display", "メモリ": "memory", "モーター": "motor",
    "画像処理": "image process", "音声認識": "speech recognition",
}


def _detect_query_type(query: str) -> str:
    q = query.strip().upper()
    if _CPC_PATTERN.match(q):
        return "cpc"
    return "text"


def _cluster_label_search(conn, query: str, exclude_section: str | None, top_n: int) -> list[dict]:
    """Search tech_clusters by label/top_terms keywords (instant, pre-warmed)."""
    keywords = []
    ascii_count = sum(1 for c in query if ord(c) < 128)
    is_english = ascii_count / max(len(query), 1) > 0.8
    if is_english:
        keywords = [w for w in re.split(r'\W+', query.lower()) if len(w) > 2][:4]
    else:
        for jp, en in _JP_EN_MAP.items():
            if jp in query:
                keywords.append(en)
        jp_words = [w for w in query.split() if len(w) > 1][:3]
        keywords.extend(jp_words)

    if not keywords:
        keywords = [query.lower()]

    like_parts = []
    params = []
    for kw in keywords[:6]:
        like_parts.append("(label LIKE '%' || ? || '%' OR top_terms LIKE '%' || ? || '%')")
        params.extend([kw, kw])

    where = " OR ".join(like_parts)
    if exclude_section:
        where = f"({where}) AND cpc_class NOT LIKE ? || '%'"
        params.append(exclude_section)

    rows = conn.execute(
        f"""
        SELECT cluster_id, label, cpc_class, patent_count
        FROM tech_clusters WHERE {where}
        ORDER BY patent_count DESC LIMIT ?
        """,
        params + [top_n],
    ).fetchall()

    return [
        {
            "cluster_id": r["cluster_id"],
            "cpc_class": r["cpc_class"],
            "label": r["label"],
            "patent_count": r["patent_count"],
            "similarity": 0.0,
        }
        for r in rows
    ]


def _cpc_section_fallback(conn, exclude_section: str | None, top_n: int) -> list[dict]:
    """Top clusters from different CPC sections by patent_count (instant)."""
    if exclude_section:
        rows = conn.execute(
            "SELECT cluster_id, label, cpc_class, patent_count FROM tech_clusters WHERE cpc_class NOT LIKE ? || '%' ORDER BY patent_count DESC LIMIT ?",
            (exclude_section, top_n),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT cluster_id, label, cpc_class, patent_count FROM tech_clusters ORDER BY patent_count DESC LIMIT ?",
            (top_n,),
        ).fetchall()
    return [
        {"cluster_id": r["cluster_id"], "cpc_class": r["cpc_class"],
         "label": r["label"], "patent_count": r["patent_count"], "similarity": 0.0}
        for r in rows
    ]


def cross_domain_discovery(
    store: PatentStore,
    query: str,
    top_n: int = 10,
    exclude_same_domain: bool = True,
    min_similarity: float = 0.3,
) -> dict[str, Any]:
    """Discover cross-domain technology clusters related to a query.

    v4: Checks centroids availability FIRST. If center_vectors are empty,
    skips all FTS5/embedding work and goes directly to label matching.
    """
    store._relax_timeout()

    query_type = _detect_query_type(query)
    q_upper = query.strip().upper()

    exclude_section = None
    source_info: dict[str, Any] = {}
    clusters = []

    # Check if centroids are available for embedding-based matching
    centroids = _get_centroids(store)
    has_centroids = bool(centroids)

    if query_type == "cpc":
        exclude_section = q_upper[0] if exclude_same_domain else None
        source_info = {"cpc_code": q_upper, "cpc_section": q_upper[0], "query_type": "cpc"}

        conn = store._conn()
        src_row = conn.execute(
            "SELECT cluster_id, label, cpc_class, center_vector, patent_count FROM tech_clusters WHERE cpc_class = ? ORDER BY patent_count DESC LIMIT 1",
            (q_upper[:4],),
        ).fetchone()

        if src_row:
            source_info["cluster_id"] = src_row["cluster_id"]
            source_info["label"] = src_row["label"]
            source_info["patent_count"] = src_row["patent_count"]

        if src_row and src_row["center_vector"] and has_centroids:
            from space.embedding_bridge import _unpack_embedding
            proxy = _unpack_embedding(src_row["center_vector"])
            try:
                clusters = find_matching_clusters(
                    store, proxy_embedding=proxy, top_k=top_n * 3,
                    exclude_cpc_section=exclude_section, min_similarity=min_similarity,
                )
            except Exception:
                pass
        else:
            source_info["note"] = "Using CPC-section fallback (no embeddings)"

        # Fallback: CPC-section exclusion
        if not clusters:
            clusters = _cpc_section_fallback(conn, exclude_section, top_n)

    else:  # text query
        source_info = {"query_type": "text", "text": query}

        if has_centroids:
            # Try embedding-based matching (may be slow with FTS5)
            try:
                bridge_result = text_to_proxy_embedding(store, query)
                proxy = bridge_result.get("proxy_embedding")
                if proxy is not None:
                    clusters = find_matching_clusters(
                        store, proxy_embedding=proxy, top_k=top_n * 3,
                        exclude_cpc_section=exclude_section, min_similarity=min_similarity,
                    )
                    if clusters and exclude_same_domain:
                        # Infer section from top cluster
                        exclude_section = clusters[0].get("cpc_class", "X")[0]
                        clusters = [c for c in clusters if c.get("cpc_class", "X")[0] != exclude_section]
            except Exception:
                source_info["note"] = "Embedding lookup failed, using label matching"
        else:
            source_info["note"] = "No embeddings available, using label matching"

        # Fallback: keyword matching on cluster labels
        if not clusters:
            conn = store._conn()
            clusters = _cluster_label_search(conn, query, exclude_section, top_n)
        if not clusters:
            conn = store._conn()
            clusters = _cpc_section_fallback(conn, exclude_section, top_n)

    if not clusters:
        return {
            "endpoint": "cross_domain_discovery",
            "source": source_info,
            "discoveries": [],
            "result_count": 0,
        }

    # ─── Batch enrichment ───
    target_clusters = clusters[:top_n]
    cluster_ids = [c["cluster_id"] for c in target_clusters]
    conn = store._conn()
    ph = ",".join("?" * len(cluster_ids))

    momentum_map: dict[str, dict] = {}
    try:
        mom_rows = conn.execute(
            f"SELECT cluster_id, growth_rate, acceleration FROM tech_cluster_momentum WHERE cluster_id IN ({ph}) AND year = (SELECT MAX(year) FROM tech_cluster_momentum)",
            cluster_ids,
        ).fetchall()
        for r in mom_rows:
            momentum_map[r["cluster_id"]] = {"growth_rate": r["growth_rate"] or 0, "acceleration": r["acceleration"] or 0}
    except sqlite3.OperationalError as e:
        if "interrupted" in str(e):
            raise

    firms_map: dict[str, list[dict]] = {cid: [] for cid in cluster_ids}
    try:
        firm_rows = conn.execute(
            f"SELECT cluster_id, firm_id, score FROM startability_surface WHERE cluster_id IN ({ph}) AND gate_open = 1 ORDER BY cluster_id, score DESC",
            cluster_ids,
        ).fetchall()
        count_per_cluster: dict[str, int] = {}
        for r in firm_rows:
            cid = r["cluster_id"]
            cnt = count_per_cluster.get(cid, 0)
            if cnt < 5:
                firms_map[cid].append({"firm_id": r["firm_id"], "startability": round(r["score"], 3)})
                count_per_cluster[cid] = cnt + 1
    except sqlite3.OperationalError as e:
        if "interrupted" in str(e):
            raise

    discoveries = []
    for c in target_clusters:
        cluster_id = c["cluster_id"]
        momentum = momentum_map.get(cluster_id, {"growth_rate": 0, "acceleration": 0})
        top_firms = firms_map.get(cluster_id, [])
        avg_s = sum(f["startability"] for f in top_firms) / len(top_firms) if top_firms else 0.0
        entry_difficulty = "low" if avg_s > 0.6 else ("moderate" if avg_s > 0.3 else "high")

        cpc_class = c.get("cpc_class", "")
        label = c.get("label") or cpc_class
        src_label = source_info.get("label") or source_info.get("cpc_code", query)
        hypothesis = f"{src_label}の技術を{label}({cpc_class})領域に応用する可能性。"

        discoveries.append({
            "cluster_id": cluster_id, "cpc_class": cpc_class, "label": label,
            "similarity": c.get("similarity", 0.0), "patent_count": c.get("patent_count"),
            "growth_rate": momentum.get("growth_rate", 0),
            "acceleration": momentum.get("acceleration", 0),
            "top_terms": c.get("top_terms", []),
            "top_players": top_firms, "entry_difficulty": entry_difficulty,
            "connection_hypothesis": hypothesis,
        })

    return {
        "endpoint": "cross_domain_discovery",
        "source": source_info,
        "discoveries": discoveries,
        "result_count": len(discoveries),
    }
