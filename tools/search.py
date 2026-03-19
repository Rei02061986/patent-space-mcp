"""patent_search tool implementation.

v3: FTS5 timeout fallback goes directly to cluster_hint (pre-warmed tech_clusters,
607 rows) instead of title LIKE (13.7M rows, also slow on cold HDD).
Includes JP→EN tech term mapping for Japanese query support.
"""
from __future__ import annotations

import math
import re
import sqlite3
from typing import Any

from db.sqlite_store import PatentStore, _sanitize_fts5
from tools.pagination import paginate
from tools.jp_tech_cpc_map import JP_TECH_CPC_MAP as _EXTENDED_JP_CPC

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
}


def _batch_enrich(conn, results: list[dict]) -> list[dict]:
    """Batch-fetch CPC codes and assignees for a list of patent dicts."""
    if not results:
        return results
    pub_numbers = [d["publication_number"] for d in results]
    cpc_map: dict[str, list[str]] = {}
    assignee_map: dict[str, list[dict]] = {}
    for i in range(0, len(pub_numbers), 500):
        batch = pub_numbers[i : i + 500]
        ph = ",".join("?" * len(batch))
        try:
            for c in conn.execute(
                f"SELECT publication_number, cpc_code FROM patent_cpc WHERE publication_number IN ({ph})",
                batch,
            ):
                cpc_map.setdefault(c["publication_number"], []).append(c["cpc_code"])
        except sqlite3.OperationalError:
            pass
        try:
            for a in conn.execute(
                f"SELECT publication_number, harmonized_name, firm_id FROM patent_assignees WHERE publication_number IN ({ph})",
                batch,
            ):
                assignee_map.setdefault(a["publication_number"], []).append(
                    {"name": a["harmonized_name"], "firm_id": a["firm_id"]}
                )
        except sqlite3.OperationalError:
            pass
    for d in results:
        pub = d["publication_number"]
        d["cpc_codes"] = cpc_map.get(pub, [])
        d["assignees"] = assignee_map.get(pub, [])
    return results


def _build_search_summary(patents: list[dict[str, Any]]) -> dict[str, Any]:
    """Build compact summary for search result set."""
    top_applicants: dict[tuple[str, str | None], int] = {}
    cpc_dist: dict[str, int] = {}
    pub_dates: list[int] = []

    for p in patents:
        pub_date = p.get("publication_date")
        if isinstance(pub_date, int):
            pub_dates.append(pub_date)

        for cpc in p.get("cpc_codes", []):
            if not cpc:
                continue
            cpc_key = str(cpc)[:4]
            cpc_dist[cpc_key] = cpc_dist.get(cpc_key, 0) + 1

        for a in p.get("assignees", []):
            name = a.get("name")
            if not name:
                continue
            firm_id = a.get("firm_id")
            key = (name, firm_id)
            top_applicants[key] = top_applicants.get(key, 0) + 1

    applicants_sorted = sorted(
        top_applicants.items(),
        key=lambda x: x[1],
        reverse=True,
    )[:5]
    cpc_sorted = sorted(cpc_dist.items(), key=lambda x: x[1], reverse=True)[:10]

    earliest = min(pub_dates) if pub_dates else None
    latest = max(pub_dates) if pub_dates else None

    return {
        "top_applicants": [
            {"name": k[0], "firm_id": k[1], "count": v}
            for k, v in applicants_sorted
        ],
        "date_range": {
            "earliest": earliest,
            "latest": latest,
        },
        "cpc_distribution": [
            {"cpc_class": cpc, "count": cnt}
            for cpc, cnt in cpc_sorted
        ],
    }


def _search_with_multi_cpc(
    store: PatentStore,
    query: str | None,
    cpc_codes: list[str] | None,
    applicant: str | None,
    firm_id: str | None,
    date_from_int: int | None,
    date_to_int: int | None,
    limit: int,
    country_code: str | None = None,
) -> tuple[list[dict], int]:
    """Search with support for multiple CPC codes."""
    if not cpc_codes or len(cpc_codes) <= 1:
        results = store.search(
            query=query,
            cpc_prefix=cpc_codes[0] if cpc_codes else None,
            assignee=applicant,
            firm_id=firm_id,
            date_from=date_from_int,
            date_to=date_to_int,
            limit=limit,
            country_code=country_code,
        )
        total = store.count(
            query=query,
            cpc_prefix=cpc_codes[0] if cpc_codes else None,
            assignee=applicant,
            date_from=date_from_int,
            date_to=date_to_int,
            country_code=country_code,
        )
        return results, total

    # Multi-CPC: intersect results from all CPC codes
    conditions: list[str] = []
    params: list[Any] = []

    if query:
        safe_q = _sanitize_fts5(query)
        if safe_q:
            # FTS5 with DatabaseError fallback to LIKE
            try:
                _fts_pubs = [r[0] for r in conn.execute(
                    "SELECT publication_number FROM patents_fts "
                    "WHERE patents_fts MATCH ? LIMIT 5000", (safe_q,)
                ).fetchall()]
                if _fts_pubs:
                    _ph = ",".join("?" * len(_fts_pubs))
                    conditions.append(f"p.publication_number IN ({_ph})")
                    params.extend(_fts_pubs)
                else:
                    conditions.append("0")
            except (sqlite3.DatabaseError, sqlite3.OperationalError):
                _words = [w.strip('"') for w in safe_q.split() if len(w.strip('"')) >= 3]
                if _words:
                    _like_parts = []
                    for _w in _words[:4]:
                        _like_parts.append(
                            "(p.title_ja LIKE '%' || ? || '%' OR p.title_en LIKE '%' || ? || '%')"
                        )
                        params.extend([_w, _w])
                    conditions.append("(" + " AND ".join(_like_parts) + ")")

    for cpc in cpc_codes:
        conditions.append(
            "p.publication_number IN "
            "(SELECT publication_number FROM patent_cpc WHERE cpc_code LIKE ? || '%')"
        )
        params.append(cpc)

    if applicant:
        conditions.append(
            "p.publication_number IN "
            "(SELECT publication_number FROM patent_assignees "
            "WHERE harmonized_name LIKE '%' || ? || '%')"
        )
        params.append(applicant)

    if firm_id:
        conditions.append(
            "p.publication_number IN "
            "(SELECT publication_number FROM patent_assignees WHERE firm_id = ?)"
        )
        params.append(firm_id)

    if date_from_int:
        conditions.append("p.publication_date >= ?")
        params.append(date_from_int)
    if date_to_int:
        conditions.append("p.publication_date <= ?")
        params.append(date_to_int)

    if country_code:
        conditions.append("p.country_code = ?")
        params.append(country_code)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    with store._conn() as conn:
        count_row = conn.execute(
            f"SELECT COUNT(*) as cnt FROM patents p {where}", params
        ).fetchone()
        total = count_row["cnt"] if count_row else 0

        rows = conn.execute(
            f"""
            SELECT p.publication_number, p.family_id, p.country_code,
                   p.kind_code, p.title_ja, p.title_en,
                   p.abstract_ja, p.abstract_en,
                   p.filing_date, p.publication_date, p.grant_date,
                   p.entity_status
            FROM patents p
            {where}
            ORDER BY p.publication_date DESC
            LIMIT ?
            """,
            params + [limit],
        ).fetchall()

    results = [dict(row) for row in rows]
    _batch_enrich(store._conn(), results)

    return results, total


def _is_english(text: str) -> bool:
    """Heuristic: text is mostly ASCII = English."""
    ascii_count = sum(1 for c in text if ord(c) < 128)
    return ascii_count / max(len(text), 1) > 0.8


def _translate_jp_keywords(text: str) -> list[str]:
    """Translate Japanese keywords to English using the JP→EN mapping."""
    en_words = []
    for jp, en in _JP_EN_MAP.items():
        if jp in text:
            en_words.append(en)
    return en_words


def _cluster_hint(
    store: PatentStore,
    query: str | None = None,
    cpc_codes: list[str] | None = None,
) -> list[dict]:
    """Return matching tech_clusters as a hint when patent search fails.

    Uses pre-warmed tech_clusters table (607 rows, always in page cache).
    Supports both English and Japanese queries via JP→EN term mapping.
    """
    conn = store._conn()
    results = []

    # Match by CPC codes first
    if cpc_codes:
        for cpc in cpc_codes[:3]:
            try:
                rows = conn.execute(
                    """
                    SELECT cluster_id, label, cpc_class, patent_count, growth_rate
                    FROM tech_clusters
                    WHERE cpc_class LIKE ? || '%'
                    ORDER BY patent_count DESC
                    LIMIT 3
                    """,
                    (cpc,),
                ).fetchall()
                for r in rows:
                    results.append({
                        "cluster_id": r["cluster_id"],
                        "label": r["label"],
                        "cpc_class": r["cpc_class"],
                        "patent_count": r["patent_count"],
                    })
            except sqlite3.OperationalError:
                pass

    # Match by query keywords in labels/terms
    if query and len(results) < 3:
        # Get English keywords (direct or translated from Japanese)
        keywords = []
        if _is_english(query):
            keywords = [w for w in re.split(r'\W+', query.lower()) if len(w) > 2][:4]
        else:
            # Translate Japanese terms to English
            keywords = _translate_jp_keywords(query)
            # Also try the original Japanese in case labels/terms have JP content
            jp_words = [w for w in query.split() if len(w) > 1][:3]
            keywords.extend(jp_words)

        if keywords:
            like_parts = []
            params = []
            for kw in keywords[:6]:
                like_parts.append(
                    "(label LIKE '%' || ? || '%' OR top_terms LIKE '%' || ? || '%')"
                )
                params.extend([kw, kw])
            where = " OR ".join(like_parts)
            try:
                rows = conn.execute(
                    f"""
                    SELECT cluster_id, label, cpc_class, patent_count, growth_rate
                    FROM tech_clusters
                    WHERE {where}
                    ORDER BY patent_count DESC
                    LIMIT 5
                    """,
                    params,
                ).fetchall()
                seen = {r["cluster_id"] for r in results}
                for r in rows:
                    if r["cluster_id"] not in seen:
                        results.append({
                            "cluster_id": r["cluster_id"],
                            "label": r["label"],
                            "cpc_class": r["cpc_class"],
                            "patent_count": r["patent_count"],
                        })
            except sqlite3.OperationalError:
                pass

    return results[:5]


def patent_search(
    store: PatentStore,
    query: str | None = None,
    cpc_codes: list[str] | None = None,
    applicant: str | None = None,
    jurisdiction: str = "JP",
    date_from: str | None = None,
    date_to: str | None = None,
    max_results: int = 20,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """Search local patent database with FTS5 timeout fallback.

    When FTS5 or CPC/applicant scans timeout on cold HDD pages, returns
    related technology clusters from pre-warmed data instead of generic error.
    """
    date_from_int = int(date_from.replace("-", "")) if date_from else None
    date_to_int = int(date_to.replace("-", "")) if date_to else None
    page = max(int(page), 1)
    requested_page_size = min(max(int(page_size), 1), 100)
    max_results = max(1, int(max_results))
    limit = max(max_results, page * requested_page_size)

    # Relax per-query timeout — let _safe_call's hard_deadline (12s) handle
    # safety. The progress handler's per-query timeout (10s) adds massive
    # overhead for FTS5 and CPC scan queries on HDD (checking every 50K VM
    # instructions). With relax_timeout, per-query check is skipped but
    # hard_deadline from _safe_call is still active.
    store._relax_timeout()

    search_method = "fts5"
    try:
        results, total_count = _search_with_multi_cpc(
            store=store,
            query=query,
            cpc_codes=cpc_codes,
            applicant=applicant,
            firm_id=None,
            date_from_int=date_from_int,
            date_to_int=date_to_int,
            limit=limit,
            country_code=None,  # Post-filtered (SQL too slow on HDD)
        )
    except sqlite3.OperationalError as e:
        if "interrupted" in str(e):
            # hard_deadline hit — go directly to cluster hint (instant)
            store._relax_timeout()
            results, total_count = [], 0
            search_method = "timeout"
        else:
            raise

    # Determine actual search method
    if search_method == "fts5" and total_count == -1:
        search_method = "title_like"
        total_count = len(results)

    # English fallback: if FTS5 worked but returned few results, try LIKE on title_en
    # (only if pre-warm has populated the pages — fast path)
    if (
        query
        and _is_english(query)
        and len(results) < 3
        and not cpc_codes
        and search_method == "fts5"
    ):
        store._reset_timeout()
        try:
            en_conditions = ["p.title_en LIKE '%' || ? || '%'"]
            en_params: list[Any] = [query]
            if date_from_int:
                en_conditions.append("p.publication_date >= ?")
                en_params.append(date_from_int)
            if date_to_int:
                en_conditions.append("p.publication_date <= ?")
                en_params.append(date_to_int)

            en_where = "WHERE " + " AND ".join(en_conditions)
            conn = store._conn()
            en_rows = conn.execute(
                f"""
                SELECT p.publication_number, p.family_id, p.country_code,
                       p.kind_code, p.title_ja, p.title_en,
                       p.abstract_ja, p.abstract_en,
                       p.filing_date, p.publication_date, p.grant_date,
                       p.entity_status
                FROM patents p
                {en_where}
                ORDER BY p.publication_date DESC
                LIMIT ?
                """,
                en_params + [limit],
            ).fetchall()

            if len(en_rows) > len(results):
                results = [dict(row) for row in en_rows]
                total_count = len(results)
                search_method = "title_en_like"
                store._reset_timeout()
                _batch_enrich(conn, results)
        except sqlite3.OperationalError:
            pass  # Keep whatever we already have

    # When we have no results, provide cluster hints from pre-warmed data
    cluster_hint = None
    if not results and (query or cpc_codes):
        cluster_hint = _cluster_hint(store, query=query, cpc_codes=cpc_codes)

    # Post-filter by jurisdiction (SQL filter on 160M rows is too slow on HDD)
    if jurisdiction and jurisdiction != "ALL" and results:
        results = [r for r in results if r.get("country_code") == jurisdiction]
        total_count = len(results)  # Approximate (filtered count)

    paged = paginate(results, page=page, page_size=requested_page_size)
    page_size_clamped = paged["page_size"]
    pages = math.ceil(total_count / page_size_clamped) if total_count > 0 else 1
    summary = _build_search_summary(results)

    result = {
        "total": total_count,
        "page": page,
        "page_size": page_size_clamped,
        "pages": pages,
        "results": paged["results"],
        "summary": summary,
        "patents": paged["results"],
        "result_count": len(paged["results"]),
        "total_count": total_count,
        "search_method": search_method,
        "query_params": {
            "query": query,
            "cpc_codes": cpc_codes,
            "applicant": applicant,
            "jurisdiction": jurisdiction,
            "date_from": date_from,
            "date_to": date_to,
        },
    }

    if cluster_hint:
        result["related_clusters"] = cluster_hint
        if search_method == "timeout":
            result["note"] = (
                "Database cache is warming up after restart. Individual patent results "
                "will be available in a few minutes. Related technology clusters shown below. "
                "Alternative: use tech_landscape, startability_ranking, or tech_clusters_list."
            )

    return result
