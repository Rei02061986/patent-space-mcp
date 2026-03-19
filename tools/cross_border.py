"""cross_border_similarity tool — detect similar patents across jurisdictions.

Finds patents in target countries that are similar to a given firm's portfolio
or a specific patent/technology description. Uses CPC overlap and family
relationships for matching (embedding-based matching when center_vectors are
populated).
"""
from __future__ import annotations

import sqlite3

from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver


def _resolve_firm(resolver, name):
    r = resolver.resolve(name, country_hint="JP")
    if r is None:
        return None, None
    return r.entity.canonical_id, r.entity.canonical_name


def _get_firm_cpc(conn, firm_id, limit=20):
    """Get top CPC codes for a firm from startability_surface clusters."""
    rows = conn.execute(
        "SELECT cluster_id, score FROM startability_surface "
        "WHERE firm_id = ? AND year = (SELECT MAX(year) FROM startability_surface WHERE firm_id = ?) "
        "AND gate_open = 1 ORDER BY score DESC LIMIT ?",
        (firm_id, firm_id, limit),
    ).fetchall()
    return [(r["cluster_id"].split("_")[0] if "_" in r["cluster_id"] else r["cluster_id"], r["score"]) for r in rows]


def _get_patent_cpc(conn, pub_num):
    """Get CPC codes for a specific patent."""
    rows = conn.execute(
        "SELECT cpc_code FROM patent_cpc WHERE publication_number = ?",
        (pub_num,),
    ).fetchall()
    return [r["cpc_code"][:4] for r in rows]


def _get_patent_info(conn, pub_num):
    """Get basic patent info."""
    row = conn.execute(
        "SELECT publication_number, title_ja, title_en, country_code, "
        "filing_date, family_id FROM patents WHERE publication_number = ?",
        (pub_num,),
    ).fetchone()
    if row is None:
        return None
    return dict(row)


def _search_similar_by_cpc(conn, cpc_codes, target_jurisdictions, exclude_pub=None,
                           time_ref=None, time_window="all", top_n=20):
    """Find patents in target jurisdictions matching CPC codes."""
    if not cpc_codes:
        return []

    # Build CPC filter — use top 5 CPC codes
    cpc_list = cpc_codes[:5]
    placeholders = ",".join("?" * len(cpc_list))
    like_parts = " OR ".join(
        f"c.cpc_code LIKE ? || '%'" for _ in cpc_list
    )

    jurisdiction_ph = ",".join("?" * len(target_jurisdictions))

    params = list(cpc_list)  # for LIKE
    params.extend(target_jurisdictions)

    time_clause = ""
    if time_ref and time_window == "after":
        time_clause = "AND p.filing_date > ?"
        params.append(time_ref)
    elif time_ref and time_window == "before":
        time_clause = "AND p.filing_date < ?"
        params.append(time_ref)

    if exclude_pub:
        exclude_clause = "AND p.publication_number != ?"
        params.append(exclude_pub)
    else:
        exclude_clause = ""

    params.append(top_n * 3)  # fetch more for de-dup

    sql = f"""
        SELECT DISTINCT p.publication_number, p.title_ja, p.title_en,
               p.country_code, p.filing_date, p.family_id,
               COUNT(DISTINCT c.cpc_code) as cpc_match_count
        FROM patent_cpc c
        JOIN patents p ON p.publication_number = c.publication_number
        WHERE ({like_parts})
          AND p.country_code IN ({jurisdiction_ph})
          {time_clause}
          {exclude_clause}
        GROUP BY p.publication_number
        ORDER BY cpc_match_count DESC, p.filing_date DESC
        LIMIT ?
    """

    try:
        rows = conn.execute(sql, params).fetchall()
    except Exception:
        # Fallback: simpler query without JOIN complexity
        rows = []
        for cpc in cpc_list[:3]:
            try:
                simple_rows = conn.execute(
                    "SELECT DISTINCT c.publication_number "
                    "FROM patent_cpc c "
                    "WHERE c.cpc_code LIKE ? || '%' "
                    "LIMIT ?",
                    (cpc, top_n),
                ).fetchall()
                for sr in simple_rows:
                    pinfo = _get_patent_info(conn, sr["publication_number"])
                    if pinfo and pinfo.get("country_code") in target_jurisdictions:
                        rows.append(pinfo)
            except Exception:
                continue

    return [dict(r) for r in rows][:top_n]


def cross_border_similarity(
    store: PatentStore,
    resolver: EntityResolver,
    query: str,
    query_type: str = "firm",
    target_jurisdictions: list[str] | None = None,
    min_similarity: float = 0.7,
    time_window: str = "all",
    top_n: int = 20,
) -> dict[str, Any]:
    """Find similar patent filings across jurisdictions."""
    import time as _time
    _cb_deadline = _time.monotonic() + 50  # 50s time budget
    store._relax_timeout()

    if target_jurisdictions is None:
        target_jurisdictions = ["CN", "KR", "US", "EP"]

    conn = store._conn()
    source_info: dict[str, Any] = {}
    cpc_codes: list[str] = []
    time_ref = None
    exclude_pub = None
    source_family_id = None

    if query_type == "firm":
        fid, fname = _resolve_firm(resolver, query)
        if fid is None:
            return {"error": f"Could not resolve firm: '{query}'",
                    "suggestion": "Try the exact company name, Japanese name, or stock ticker"}

        firm_cpc = _get_firm_cpc(conn, fid)
        cpc_codes = [c[0] for c in firm_cpc]
        source_info = {
            "type": "firm",
            "firm_id": fid,
            "firm_name": fname,
            "top_cpc_areas": [{"cpc": c[0], "score": round(c[1], 3)} for c in firm_cpc[:10]],
        }

    elif query_type == "patent":
        pinfo = _get_patent_info(conn, query)
        if pinfo is None:
            return {"error": f"Patent not found: '{query}'"}

        cpc_codes = _get_patent_cpc(conn, query)
        time_ref = pinfo.get("filing_date")
        exclude_pub = query
        source_family_id = pinfo.get("family_id")
        source_info = {
            "type": "patent",
            "publication_number": query,
            "title": pinfo.get("title_en") or pinfo.get("title_ja"),
            "country_code": pinfo.get("country_code"),
            "filing_date": time_ref,
            "family_id": source_family_id,
            "cpc_codes": cpc_codes,
        }

    elif query_type == "text":
        # Text query: map to cluster CPC via tech_clusters label matching
        keywords = query.lower().split()[:5]
        like_parts = " OR ".join(
            "(label LIKE '%' || ? || '%' OR top_terms LIKE '%' || ? || '%')"
            for _ in keywords
        )
        params = []
        for kw in keywords:
            params.extend([kw, kw])

        cluster_rows = conn.execute(
            f"SELECT cluster_id, cpc_class, label FROM tech_clusters "
            f"WHERE {like_parts} ORDER BY patent_count DESC LIMIT 5",
            params,
        ).fetchall()

        cpc_codes = list({r["cpc_class"] for r in cluster_rows if r["cpc_class"]})
        source_info = {
            "type": "text",
            "query": query,
            "matched_clusters": [
                {"cluster_id": r["cluster_id"], "label": r["label"]}
                for r in cluster_rows
            ],
            "derived_cpc_codes": cpc_codes,
        }
    else:
        return {"error": f"Invalid query_type: '{query_type}'. Use 'firm', 'patent', or 'text'."}

    if not cpc_codes:
        return {
            "endpoint": "cross_border_similarity",
            "source": source_info,
            "similar_filings": [],
            "summary": {"total_found": 0},
            "note": "CPC情報が取得できませんでした。",
        }

    # Search for similar patents
    try:
        raw_results = _search_similar_by_cpc(
            conn, cpc_codes, target_jurisdictions,
            exclude_pub=exclude_pub,
            time_ref=time_ref,
            time_window=time_window,
            top_n=top_n * 2,
        )
    except sqlite3.OperationalError as e:
        if "interrupt" in str(e).lower():
            return {
                "endpoint": "cross_border_similarity",
                "source": source_info,
                "similar_filings": [],
                "summary": {"total_found": 0},
                "note": "Query timed out during cross-border patent search. Try fewer CPC codes or jurisdictions.",
            }
        raise

    # Enrich results
    similar_filings = []
    by_jurisdiction: dict[str, int] = {}

    for _ri, r in enumerate(raw_results):
        if _time.monotonic() > _cb_deadline:
            break  # time budget exceeded
        pub_num = r.get("publication_number", "")
        country = r.get("country_code", "")
        filing_date = r.get("filing_date")
        family_id = r.get("family_id")

        # CPC overlap calculation
        try:
            result_cpc = _get_patent_cpc(conn, pub_num)
        except sqlite3.OperationalError:
            result_cpc = []
        source_cpc_set = set(cpc_codes)
        result_cpc_set = set(result_cpc)
        cpc_overlap = (
            len(source_cpc_set & result_cpc_set) / max(len(source_cpc_set | result_cpc_set), 1)
        )

        if cpc_overlap < min_similarity * 0.5:
            continue

        # Time lag
        time_lag_days = None
        if time_ref and filing_date:
            try:
                from datetime import datetime
                d1 = datetime.strptime(str(time_ref)[:8], "%Y%m%d")
                d2 = datetime.strptime(str(filing_date)[:8], "%Y%m%d")
                time_lag_days = (d2 - d1).days
            except (ValueError, TypeError):
                pass

        # Family relationship
        family_related = bool(
            source_family_id and family_id and source_family_id == family_id
        )

        # Get assignee
        try:
            assignee_row = conn.execute(
                "SELECT harmonized_name FROM patent_assignees "
                "WHERE publication_number = ? LIMIT 1",
                (pub_num,),
            ).fetchone()
            applicant = assignee_row["harmonized_name"] if assignee_row else None
        except sqlite3.OperationalError:
            applicant = None

        # Similarity score (CPC-based)
        similarity_score = round(cpc_overlap, 3)

        similar_filings.append({
            "publication_number": pub_num,
            "title": r.get("title_en") or r.get("title_ja"),
            "applicant": applicant,
            "jurisdiction": country,
            "filing_date": filing_date,
            "similarity_score": similarity_score,
            "time_lag_days": time_lag_days,
            "cpc_overlap": round(cpc_overlap, 3),
            "family_related": family_related,
        })

        by_jurisdiction[country] = by_jurisdiction.get(country, 0) + 1

    # Sort by similarity
    similar_filings.sort(key=lambda x: x["similarity_score"], reverse=True)
    similar_filings = similar_filings[:top_n]

    # Recalculate summary
    if similar_filings:
        avg_sim = sum(f["similarity_score"] for f in similar_filings) / len(similar_filings)
        lag_values = [f["time_lag_days"] for f in similar_filings if f["time_lag_days"] is not None]
        avg_lag = sum(lag_values) / len(lag_values) if lag_values else None
    else:
        avg_sim = 0.0
        avg_lag = None

    return {
        "endpoint": "cross_border_similarity",
        "source": source_info,
        "similar_filings": similar_filings,
        "summary": {
            "total_found": len(similar_filings),
            "by_jurisdiction": by_jurisdiction,
            "avg_similarity": round(avg_sim, 3),
            "avg_time_lag_days": round(avg_lag) if avg_lag is not None else None,
        },
        "note": (
            "本ツールは類似性の高い出願を検出します。権利侵害の判断は含みません。"
            "法的判断には専門家への相談を推奨します。"
        ),
        "visualization_hint": {
            "recommended_chart": "sankey",
            "title": "国際類似特許フロー",
            "axes": {"source": "source_jurisdiction", "target": "jurisdiction", "value": "similarity_score"},
        },
    }
