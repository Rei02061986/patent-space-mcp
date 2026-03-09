"""patent_summary tool — LLM-free patent summarization.

Provides structured, rule-based/extractive summaries of individual
patents and technology area briefs. No LLM API calls are used;
all summarization is done via SQL aggregation and text extraction.

Two main functions:
  - patent_summary: Structured summary of a single patent at three detail levels
  - technology_brief: Overview of a technology area by CPC/keyword
"""
from __future__ import annotations

import json
import logging
import math
import re
import sqlite3
from typing import Any

from db.sqlite_store import PatentStore
from tools.pagination import paginate

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CPC label helper — graceful import
# ---------------------------------------------------------------------------

_CPC_LABELS: dict[str, str] = {}

try:
    from tools.cpc_labels_ja import CPC_CLASS_JA

    _CPC_LABELS = CPC_CLASS_JA
except ImportError:
    _log.debug("cpc_labels_ja not available; CPC labels will be raw codes")


def _cpc_label(code: str) -> str:
    """Return a human-readable label for a CPC code.

    Looks up the 4-char subclass (e.g. 'H01L') in CPC_CLASS_JA.
    Falls back to the raw code if no label exists.
    """
    if not code:
        return code
    subclass = code[:4]
    label = _CPC_LABELS.get(subclass)
    if label:
        return f"{subclass} ({label})"
    return code


def _format_date(d: int | None) -> str | None:
    """Convert an integer date YYYYMMDD to ISO string YYYY-MM-DD."""
    if d is None:
        return None
    s = str(d)
    if len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def _truncate(text: str | None, max_len: int = 300) -> str | None:
    """Truncate text to max_len characters, appending '...' if trimmed."""
    if text is None:
        return None
    if len(text) <= max_len:
        return text
    return text[: max_len - 3].rstrip() + "..."


def _extract_key_phrases(abstract: str | None, max_phrases: int = 5) -> list[str]:
    """Extract rudimentary key phrases from an abstract.

    This is a lightweight heuristic: split on sentence boundaries, take
    the first N non-trivial sentences. No NLP library required.
    """
    if not abstract:
        return []
    # Split on period, exclamation, question mark, or CJK period
    sentences = re.split(r"[.!?\u3002]+", abstract)
    phrases: list[str] = []
    for sent in sentences:
        sent = sent.strip()
        if len(sent) < 10:
            continue
        phrases.append(sent)
        if len(phrases) >= max_phrases:
            break
    return phrases


# ===========================================================================
# 1. patent_summary
# ===========================================================================


def patent_summary(
    store: PatentStore,
    publication_number: str,
    level: str = "simple",
) -> dict[str, Any]:
    """Generate a structured summary of a patent.

    Args:
        store: PatentStore instance.
        publication_number: Patent publication number (e.g. 'JP-6543210-B1').
        level: Detail level — 'simple', 'detailed', or 'expert'.

    Returns:
        Dict with 'endpoint' key and structured summary data.

    Levels:
        simple   — Title + abstract excerpt + primary CPC + assignee + dates.
                   Under 500 characters total for the synopsis.
        detailed — All CPC codes with labels, all assignees, citation counts,
                   family info, legal status, value index.
        expert   — Above + related patents (by CPC overlap) + technology
                   positioning via cluster + competitive context.
    """
    if level not in ("simple", "detailed", "expert"):
        level = "simple"

    conn = store._conn()

    # ------------------------------------------------------------------
    # Fetch core patent data
    # ------------------------------------------------------------------
    pat = conn.execute(
        "SELECT * FROM patents WHERE publication_number = ?",
        (publication_number,),
    ).fetchone()

    if not pat:
        return {
            "endpoint": "patent_summary",
            "error": f"Patent not found: {publication_number}",
        }

    pat = dict(pat)

    # CPC codes
    cpcs = conn.execute(
        "SELECT cpc_code, is_inventive, is_first "
        "FROM patent_cpc WHERE publication_number = ?",
        (publication_number,),
    ).fetchall()
    cpc_list = [dict(c) for c in cpcs]

    # Assignees
    assignees = conn.execute(
        "SELECT raw_name, harmonized_name, country_code, firm_id "
        "FROM patent_assignees WHERE publication_number = ?",
        (publication_number,),
    ).fetchall()
    assignee_list = [dict(a) for a in assignees]

    # Primary CPC (is_first=1, or first inventive, or just first)
    primary_cpc = None
    for c in cpc_list:
        if c.get("is_first"):
            primary_cpc = c["cpc_code"]
            break
    if primary_cpc is None:
        for c in cpc_list:
            if c.get("is_inventive"):
                primary_cpc = c["cpc_code"]
                break
    if primary_cpc is None and cpc_list:
        primary_cpc = cpc_list[0]["cpc_code"]

    # Primary assignee
    primary_assignee = None
    if assignee_list:
        primary_assignee = (
            assignee_list[0].get("harmonized_name")
            or assignee_list[0].get("raw_name")
        )

    # Title — prefer JA, fall back to EN
    title = pat.get("title_ja") or pat.get("title_en") or "(no title)"

    # Abstract — prefer JA, fall back to EN
    abstract = pat.get("abstract_ja") or pat.get("abstract_en")

    # ------------------------------------------------------------------
    # Build synopsis (under 500 chars for simple)
    # ------------------------------------------------------------------
    synopsis_parts: list[str] = []
    synopsis_parts.append(title)
    if primary_assignee:
        synopsis_parts.append(f"[{primary_assignee}]")
    if primary_cpc:
        synopsis_parts.append(_cpc_label(primary_cpc))
    filing = _format_date(pat.get("filing_date"))
    if filing:
        synopsis_parts.append(f"Filed: {filing}")
    if abstract:
        remaining = 500 - sum(len(p) + 2 for p in synopsis_parts)
        if remaining > 50:
            synopsis_parts.append(_truncate(abstract, max_len=remaining))

    synopsis = " | ".join(synopsis_parts)

    # ------------------------------------------------------------------
    # simple level result
    # ------------------------------------------------------------------
    result: dict[str, Any] = {
        "endpoint": "patent_summary",
        "publication_number": publication_number,
        "level": level,
        "title": title,
        "title_en": pat.get("title_en"),
        "title_ja": pat.get("title_ja"),
        "primary_cpc": _cpc_label(primary_cpc) if primary_cpc else None,
        "primary_assignee": primary_assignee,
        "country_code": pat.get("country_code"),
        "kind_code": pat.get("kind_code"),
        "filing_date": _format_date(pat.get("filing_date")),
        "publication_date": _format_date(pat.get("publication_date")),
        "grant_date": _format_date(pat.get("grant_date")),
        "abstract_excerpt": _truncate(abstract, max_len=300),
        "synopsis": synopsis[:500],
    }

    if level == "simple":
        return result

    # ------------------------------------------------------------------
    # detailed level additions
    # ------------------------------------------------------------------

    # All CPC codes with labels
    result["cpc_codes"] = [
        {
            "code": c["cpc_code"],
            "label": _cpc_label(c["cpc_code"]),
            "is_inventive": bool(c.get("is_inventive")),
            "is_first": bool(c.get("is_first")),
        }
        for c in cpc_list
    ]

    # All assignees
    result["assignees"] = [
        {
            "name": a.get("harmonized_name") or a.get("raw_name"),
            "raw_name": a.get("raw_name"),
            "country_code": a.get("country_code"),
            "firm_id": a.get("firm_id"),
        }
        for a in assignee_list
    ]

    # Inventors
    inventors = conn.execute(
        "SELECT name, country_code FROM patent_inventors WHERE publication_number = ?",
        (publication_number,),
    ).fetchall()
    result["inventors"] = [dict(i) for i in inventors]

    # Citation counts
    forward_count = pat.get("citation_count_forward") or 0
    # Check citation_counts table as well
    cc_row = conn.execute(
        "SELECT forward_citations FROM citation_counts WHERE publication_number = ?",
        (publication_number,),
    ).fetchone()
    if cc_row and cc_row["forward_citations"]:
        forward_count = max(forward_count, cc_row["forward_citations"])

    backward_citations = conn.execute(
        "SELECT cited_publication FROM patent_citations WHERE citing_publication = ?",
        (publication_number,),
    ).fetchall()
    result["citations"] = {
        "forward_count": forward_count,
        "backward_count": len(backward_citations),
        "backward_refs": [c["cited_publication"] for c in backward_citations[:20]],
    }

    # Family info
    family_id = pat.get("family_id")
    result["family"] = {"family_id": family_id, "family_size": None}
    if family_id:
        fam_row = conn.execute(
            "SELECT family_size FROM patent_family WHERE publication_number = ?",
            (publication_number,),
        ).fetchone()
        if fam_row:
            result["family"]["family_size"] = fam_row["family_size"]
        else:
            # Count siblings in patents table
            sib_count = conn.execute(
                "SELECT COUNT(*) AS cnt FROM patents WHERE family_id = ?",
                (family_id,),
            ).fetchone()
            if sib_count:
                result["family"]["family_size"] = sib_count["cnt"]

    # Legal status
    ls_row = conn.execute(
        "SELECT status, expiry_date FROM patent_legal_status WHERE publication_number = ?",
        (publication_number,),
    ).fetchone()
    if ls_row:
        result["legal_status"] = {
            "status": ls_row["status"],
            "expiry_date": _format_date(ls_row["expiry_date"]),
        }
    else:
        result["legal_status"] = {
            "status": pat.get("entity_status"),
            "expiry_date": None,
        }

    # Value index
    vi_row = conn.execute(
        "SELECT value_score, citation_component, family_component, "
        "recency_component, cluster_momentum_component "
        "FROM patent_value_index WHERE publication_number = ?",
        (publication_number,),
    ).fetchone()
    if vi_row:
        result["value_index"] = dict(vi_row)
    else:
        result["value_index"] = None

    # Research data top terms
    rd_row = conn.execute(
        "SELECT top_terms FROM patent_research_data WHERE publication_number = ?",
        (publication_number,),
    ).fetchone()
    if rd_row and rd_row["top_terms"]:
        try:
            result["top_terms"] = json.loads(rd_row["top_terms"])
        except (json.JSONDecodeError, TypeError):
            result["top_terms"] = None
    else:
        result["top_terms"] = None

    # Full abstract (not truncated)
    result["abstract_full"] = abstract

    # Key phrases extracted from abstract
    result["key_phrases"] = _extract_key_phrases(abstract)

    if level == "detailed":
        return result

    # ------------------------------------------------------------------
    # expert level additions
    # ------------------------------------------------------------------

    # Related patents by CPC overlap (same primary subclass)
    if primary_cpc:
        primary_subclass = primary_cpc[:4]
        try:
            related_rows = conn.execute(
                """
                SELECT pc.publication_number, p.title_ja, p.title_en,
                       p.filing_date, p.citation_count_forward
                FROM patent_cpc pc
                JOIN patents p ON pc.publication_number = p.publication_number
                WHERE pc.cpc_code LIKE ? || '%'
                  AND pc.publication_number != ?
                ORDER BY p.citation_count_forward DESC
                LIMIT 10
                """,
                (primary_subclass, publication_number),
            ).fetchall()
            result["related_patents"] = [
                {
                    "publication_number": r["publication_number"],
                    "title": r["title_ja"] or r["title_en"],
                    "filing_date": _format_date(r["filing_date"]),
                    "forward_citations": r["citation_count_forward"] or 0,
                }
                for r in related_rows
            ]
        except sqlite3.OperationalError as e:
            _log.warning(f"Related patents query failed: {e}")
            result["related_patents"] = []
    else:
        result["related_patents"] = []

    # Technology positioning via cluster
    cluster_row = conn.execute(
        """
        SELECT pcm.cluster_id, pcm.distance,
               tc.label, tc.cpc_class, tc.patent_count, tc.growth_rate,
               tc.top_applicants, tc.top_terms
        FROM patent_cluster_mapping pcm
        JOIN tech_clusters tc ON pcm.cluster_id = tc.cluster_id
        WHERE pcm.publication_number = ?
        """,
        (publication_number,),
    ).fetchone()

    if cluster_row:
        cluster_info: dict[str, Any] = {
            "cluster_id": cluster_row["cluster_id"],
            "cluster_label": cluster_row["label"],
            "cpc_class": cluster_row["cpc_class"],
            "distance_from_center": (
                round(cluster_row["distance"], 4)
                if cluster_row["distance"] is not None
                else None
            ),
            "cluster_patent_count": cluster_row["patent_count"],
            "cluster_growth_rate": cluster_row["growth_rate"],
        }
        # Parse top_applicants / top_terms JSON
        for field in ("top_applicants", "top_terms"):
            raw = cluster_row[field]
            if raw:
                try:
                    cluster_info[f"cluster_{field}"] = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    cluster_info[f"cluster_{field}"] = None
            else:
                cluster_info[f"cluster_{field}"] = None

        result["technology_positioning"] = cluster_info
    else:
        result["technology_positioning"] = None

    # Competitive context: other assignees in the same cluster
    if cluster_row:
        cluster_id = cluster_row["cluster_id"]
        try:
            competitors = conn.execute(
                """
                SELECT pa.harmonized_name AS name, pa.firm_id,
                       COUNT(DISTINCT pa.publication_number) AS patent_count
                FROM patent_cluster_mapping pcm
                JOIN patent_assignees pa ON pcm.publication_number = pa.publication_number
                WHERE pcm.cluster_id = ?
                  AND pa.harmonized_name IS NOT NULL
                GROUP BY pa.harmonized_name
                ORDER BY patent_count DESC
                LIMIT 10
                """,
                (cluster_id,),
            ).fetchall()
            result["competitive_context"] = {
                "cluster_id": cluster_id,
                "top_players": [
                    {
                        "name": r["name"],
                        "firm_id": r["firm_id"],
                        "patent_count": r["patent_count"],
                    }
                    for r in competitors
                ],
            }
        except sqlite3.OperationalError as e:
            _log.warning(f"Competitive context query failed: {e}")
            result["competitive_context"] = None
    else:
        result["competitive_context"] = None

    # Startability surface entry (if assignee has firm_id)
    if assignee_list and cluster_row:
        firm_ids = [a.get("firm_id") for a in assignee_list if a.get("firm_id")]
        if firm_ids:
            cluster_id = cluster_row["cluster_id"]
            ph = ",".join("?" * len(firm_ids))
            try:
                ss_rows = conn.execute(
                    f"""
                    SELECT firm_id, year, score, gate_open,
                           phi_tech_cos, phi_tech_dist, phi_tech_cpc,
                           phi_tech_cite, phi_org, phi_dyn
                    FROM startability_surface
                    WHERE cluster_id = ? AND firm_id IN ({ph})
                    ORDER BY year DESC
                    LIMIT 5
                    """,
                    [cluster_id] + firm_ids,
                ).fetchall()
                if ss_rows:
                    result["startability_context"] = [dict(r) for r in ss_rows]
                else:
                    result["startability_context"] = None
            except sqlite3.OperationalError as e:
                _log.warning(f"Startability query failed: {e}")
                result["startability_context"] = None
        else:
            result["startability_context"] = None
    else:
        result["startability_context"] = None

    return result


# ===========================================================================
# 2. technology_brief
# ===========================================================================


def technology_brief(
    store: PatentStore,
    query: str | None = None,
    cpc_prefix: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    top_n_applicants: int = 10,
    trend_years: int = 10,
) -> dict[str, Any]:
    """Generate a technology area brief/overview.

    Uses CPC-based search (indexed) or LIKE-based title search.
    Aggregates patent counts, top applicants, filing trends, and
    sub-CPC distribution to produce a structured overview.

    Args:
        store: PatentStore instance.
        query: Natural language or keyword query (LIKE-based, no FTS5).
        cpc_prefix: CPC prefix filter (e.g. 'G06N', 'H01M10').
        date_from: Start date as 'YYYY-MM-DD' string.
        date_to: End date as 'YYYY-MM-DD' string.
        top_n_applicants: Number of top applicants to return. Default: 10.
        trend_years: Number of years for the filing trend. Default: 10.

    Returns:
        Dict with 'endpoint' key and structured technology brief.
    """
    if not query and not cpc_prefix:
        return {
            "endpoint": "technology_brief",
            "error": "At least one of 'query' or 'cpc_prefix' is required.",
        }

    conn = store._conn()

    date_from_int = int(date_from.replace("-", "")) if date_from else None
    date_to_int = int(date_to.replace("-", "")) if date_to else None

    # ------------------------------------------------------------------
    # Build the base filter conditions
    # ------------------------------------------------------------------
    # We build two styles of queries depending on whether CPC or keyword is used.
    # CPC-based queries are fast (indexed); keyword queries use LIKE on titles.

    # Common date conditions applied to patents table
    date_conditions: list[str] = []
    date_params: list[Any] = []
    if date_from_int:
        date_conditions.append("p.publication_date >= ?")
        date_params.append(date_from_int)
    if date_to_int:
        date_conditions.append("p.publication_date <= ?")
        date_params.append(date_to_int)

    date_where = (" AND " + " AND ".join(date_conditions)) if date_conditions else ""

    # ------------------------------------------------------------------
    # Strategy: CPC-first, keyword as additional filter
    # ------------------------------------------------------------------
    use_cpc = cpc_prefix is not None
    use_keyword = query is not None and len(query.strip()) > 0

    # Determine the target patent set
    if use_cpc and use_keyword:
        # CPC filter + keyword LIKE filter
        keyword_like = f"%{query.strip()}%"
        count_sql = f"""
            SELECT COUNT(DISTINCT p.publication_number) AS cnt
            FROM patent_cpc pc
            JOIN patents p ON pc.publication_number = p.publication_number
            WHERE pc.cpc_code LIKE ? || '%'
              AND (p.title_ja LIKE ? OR p.title_en LIKE ?)
              {date_where}
        """
        count_params = [cpc_prefix, keyword_like, keyword_like] + date_params
    elif use_cpc:
        count_sql = f"""
            SELECT COUNT(DISTINCT pc.publication_number) AS cnt
            FROM patent_cpc pc
            JOIN patents p ON pc.publication_number = p.publication_number
            WHERE pc.cpc_code LIKE ? || '%'
              {date_where}
        """
        count_params = [cpc_prefix] + date_params
    else:
        # Keyword-only: LIKE on title (can be slow, but necessary)
        keyword_like = f"%{query.strip()}%"
        count_sql = f"""
            SELECT COUNT(*) AS cnt
            FROM patents p
            WHERE (p.title_ja LIKE ? OR p.title_en LIKE ?)
              {date_where}
        """
        count_params = [keyword_like, keyword_like] + date_params

    # Execute count
    try:
        total_patents = conn.execute(count_sql, count_params).fetchone()["cnt"]
    except sqlite3.OperationalError as e:
        _log.warning(f"Count query failed: {e}")
        total_patents = -1  # unknown

    # ------------------------------------------------------------------
    # Top applicants
    # ------------------------------------------------------------------
    if use_cpc and use_keyword:
        keyword_like = f"%{query.strip()}%"
        applicant_sql = f"""
            SELECT pa.harmonized_name AS name, pa.firm_id,
                   COUNT(DISTINCT pa.publication_number) AS cnt
            FROM patent_cpc pc
            JOIN patent_assignees pa ON pc.publication_number = pa.publication_number
            JOIN patents p ON pc.publication_number = p.publication_number
            WHERE pc.cpc_code LIKE ? || '%'
              AND (p.title_ja LIKE ? OR p.title_en LIKE ?)
              AND pa.harmonized_name IS NOT NULL
              {date_where}
            GROUP BY pa.harmonized_name
            ORDER BY cnt DESC
            LIMIT ?
        """
        applicant_params: list[Any] = [
            cpc_prefix, keyword_like, keyword_like,
        ] + date_params + [top_n_applicants]
    elif use_cpc:
        applicant_sql = f"""
            SELECT pa.harmonized_name AS name, pa.firm_id,
                   COUNT(DISTINCT pa.publication_number) AS cnt
            FROM patent_cpc pc
            JOIN patent_assignees pa ON pc.publication_number = pa.publication_number
            JOIN patents p ON pc.publication_number = p.publication_number
            WHERE pc.cpc_code LIKE ? || '%'
              AND pa.harmonized_name IS NOT NULL
              {date_where}
            GROUP BY pa.harmonized_name
            ORDER BY cnt DESC
            LIMIT ?
        """
        applicant_params = [cpc_prefix] + date_params + [top_n_applicants]
    else:
        keyword_like = f"%{query.strip()}%"
        applicant_sql = f"""
            SELECT pa.harmonized_name AS name, pa.firm_id,
                   COUNT(DISTINCT pa.publication_number) AS cnt
            FROM patents p
            JOIN patent_assignees pa ON p.publication_number = pa.publication_number
            WHERE (p.title_ja LIKE ? OR p.title_en LIKE ?)
              AND pa.harmonized_name IS NOT NULL
              {date_where}
            GROUP BY pa.harmonized_name
            ORDER BY cnt DESC
            LIMIT ?
        """
        applicant_params = [keyword_like, keyword_like] + date_params + [top_n_applicants]

    try:
        applicant_rows = conn.execute(applicant_sql, applicant_params).fetchall()
        top_applicants = [
            {
                "name": r["name"],
                "firm_id": r["firm_id"],
                "count": r["cnt"],
                "share_pct": (
                    round((r["cnt"] * 100.0) / total_patents, 2)
                    if total_patents and total_patents > 0
                    else None
                ),
            }
            for r in applicant_rows
        ]
    except sqlite3.OperationalError as e:
        _log.warning(f"Applicant query failed: {e}")
        top_applicants = []

    # ------------------------------------------------------------------
    # Filing trend (by year)
    # ------------------------------------------------------------------
    if use_cpc:
        trend_sql = f"""
            SELECT CAST(p.publication_date / 10000 AS INTEGER) AS year,
                   COUNT(DISTINCT p.publication_number) AS cnt
            FROM patent_cpc pc
            JOIN patents p ON pc.publication_number = p.publication_number
            WHERE pc.cpc_code LIKE ? || '%'
              AND p.publication_date IS NOT NULL
              {date_where}
            GROUP BY year
            ORDER BY year
        """
        trend_params: list[Any] = [cpc_prefix] + date_params
    else:
        keyword_like = f"%{query.strip()}%"
        trend_sql = f"""
            SELECT CAST(p.publication_date / 10000 AS INTEGER) AS year,
                   COUNT(*) AS cnt
            FROM patents p
            WHERE (p.title_ja LIKE ? OR p.title_en LIKE ?)
              AND p.publication_date IS NOT NULL
              {date_where}
            GROUP BY year
            ORDER BY year
        """
        trend_params = [keyword_like, keyword_like] + date_params

    try:
        trend_rows = conn.execute(trend_sql, trend_params).fetchall()
        filing_trend = [{"year": r["year"], "count": r["cnt"]} for r in trend_rows]
    except sqlite3.OperationalError as e:
        _log.warning(f"Filing trend query failed: {e}")
        filing_trend = []

    # Trim to recent N years
    if filing_trend and trend_years:
        filing_trend = filing_trend[-trend_years:]

    # ------------------------------------------------------------------
    # CPC sub-distribution (subclass level, top 15)
    # ------------------------------------------------------------------
    if use_cpc:
        # Get sub-distribution within the prefix
        sub_sql = f"""
            SELECT SUBSTR(pc.cpc_code, 1, 4) AS subclass,
                   COUNT(DISTINCT pc.publication_number) AS cnt
            FROM patent_cpc pc
            JOIN patents p ON pc.publication_number = p.publication_number
            WHERE pc.cpc_code LIKE ? || '%'
              {date_where}
            GROUP BY subclass
            ORDER BY cnt DESC
            LIMIT 15
        """
        sub_params: list[Any] = [cpc_prefix] + date_params
    else:
        keyword_like = f"%{query.strip()}%"
        sub_sql = f"""
            SELECT SUBSTR(pc.cpc_code, 1, 4) AS subclass,
                   COUNT(DISTINCT pc.publication_number) AS cnt
            FROM patents p
            JOIN patent_cpc pc ON p.publication_number = pc.publication_number
            WHERE (p.title_ja LIKE ? OR p.title_en LIKE ?)
              {date_where}
            GROUP BY subclass
            ORDER BY cnt DESC
            LIMIT 15
        """
        sub_params = [keyword_like, keyword_like] + date_params

    try:
        sub_rows = conn.execute(sub_sql, sub_params).fetchall()
        cpc_distribution = [
            {
                "cpc_class": r["subclass"],
                "label": _cpc_label(r["subclass"]),
                "count": r["cnt"],
            }
            for r in sub_rows
        ]
    except sqlite3.OperationalError as e:
        _log.warning(f"CPC distribution query failed: {e}")
        cpc_distribution = []

    # ------------------------------------------------------------------
    # Growth assessment
    # ------------------------------------------------------------------
    growth_rate: float | None = None
    growth_assessment = "unknown"

    if len(filing_trend) >= 2:
        # Compare last 3 years average vs previous 3 years average
        recent_years = filing_trend[-3:]
        if len(filing_trend) >= 6:
            previous_years = filing_trend[-6:-3]
        else:
            previous_years = filing_trend[: len(filing_trend) - 3]

        recent_avg = sum(y["count"] for y in recent_years) / len(recent_years)
        previous_avg = (
            sum(y["count"] for y in previous_years) / len(previous_years)
            if previous_years
            else 0
        )

        if previous_avg > 0:
            growth_rate = round((recent_avg - previous_avg) / previous_avg, 4)
            if growth_rate > 0.10:
                growth_assessment = "growing"
            elif growth_rate < -0.10:
                growth_assessment = "declining"
            else:
                growth_assessment = "stable"
        elif recent_avg > 0:
            growth_assessment = "growing"
            growth_rate = None  # cannot compute from zero base
        else:
            growth_assessment = "inactive"
            growth_rate = 0.0

    # ------------------------------------------------------------------
    # Determine effective date range from the trend data
    # ------------------------------------------------------------------
    effective_date_from = date_from
    effective_date_to = date_to
    if filing_trend:
        if not effective_date_from:
            effective_date_from = str(filing_trend[0]["year"])
        if not effective_date_to:
            effective_date_to = str(filing_trend[-1]["year"])

    # ------------------------------------------------------------------
    # Key findings (auto-generated bullet points)
    # ------------------------------------------------------------------
    key_findings: list[str] = []

    if total_patents and total_patents > 0:
        key_findings.append(
            f"Total patents matching: {total_patents:,}"
        )

    if growth_rate is not None and growth_rate != 0.0:
        pct = round(growth_rate * 100, 1)
        direction = "increased" if pct > 0 else "decreased"
        key_findings.append(
            f"Filing activity {direction} {abs(pct)}% (3-year average comparison)"
        )

    if top_applicants:
        leader = top_applicants[0]
        key_findings.append(
            f"{leader['name']} leads with {leader['count']:,} patents"
            + (f" ({leader['share_pct']}% share)" if leader.get("share_pct") else "")
        )

    if cpc_distribution:
        top_cpc = cpc_distribution[0]
        key_findings.append(
            f"Dominant sub-area: {top_cpc['label']} ({top_cpc['count']:,} patents)"
        )

    if len(cpc_distribution) >= 3:
        # Technology breadth indicator
        top3_total = sum(c["count"] for c in cpc_distribution[:3])
        all_total = sum(c["count"] for c in cpc_distribution)
        if all_total > 0:
            concentration = round(top3_total / all_total * 100, 1)
            if concentration > 80:
                key_findings.append(
                    f"Highly concentrated: top 3 CPC subclasses cover {concentration}%"
                )
            elif concentration < 50:
                key_findings.append(
                    f"Broad technology spread: top 3 CPC subclasses cover only {concentration}%"
                )

    # Check for tech cluster info
    if use_cpc and cpc_prefix:
        try:
            cluster_rows = conn.execute(
                """
                SELECT cluster_id, label, patent_count, growth_rate
                FROM tech_clusters
                WHERE cpc_class = ?
                ORDER BY patent_count DESC
                LIMIT 3
                """,
                (cpc_prefix[:4],),
            ).fetchall()
            if cluster_rows:
                for cr in cluster_rows:
                    gr = cr["growth_rate"]
                    if gr is not None:
                        gr_str = f"{gr:+.1%}" if abs(gr) < 10 else f"{gr:+.1f}"
                        key_findings.append(
                            f"Cluster '{cr['label']}': {cr['patent_count']:,} patents, "
                            f"growth {gr_str}"
                        )
        except sqlite3.OperationalError:
            pass

    # ------------------------------------------------------------------
    # Assemble result
    # ------------------------------------------------------------------
    technology_label = cpc_prefix or query or "unknown"
    if use_cpc and cpc_prefix:
        cpc_label_full = _cpc_label(cpc_prefix)
        if cpc_label_full != cpc_prefix:
            technology_label = cpc_label_full

    return {
        "endpoint": "technology_brief",
        "technology": technology_label,
        "query": query,
        "cpc_prefix": cpc_prefix,
        "total_patents": total_patents,
        "date_range": {
            "from": effective_date_from,
            "to": effective_date_to,
        },
        "growth_assessment": growth_assessment,
        "growth_rate": growth_rate,
        "top_applicants": top_applicants,
        "cpc_distribution": cpc_distribution,
        "filing_trend": filing_trend,
        "key_findings": key_findings,
    }
