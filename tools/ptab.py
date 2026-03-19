"""PTAB and litigation search / risk analysis tools.

Provides four functions called from server.py via
``_safe_call(fn, store=_store, resolver=_resolver, ...)``.

Tools:
  1. ptab_search      — Search PTAB trials by various filters
  2. ptab_risk        — Compute IPR/PTAB risk score for a patent, tech area, or firm
  3. litigation_search — Search litigation cases
  4. litigation_risk   — Compute litigation risk for a firm or tech area
"""
from __future__ import annotations

import sqlite3
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver
from tools.pagination import paginate

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_PTAB_STATUS_OUTCOMES = {
    # Statuses that indicate the patent survived challenge
    "survived": {
        "FWD Entered",        # Final Written Decision — claims survived (partially or fully)
    },
    # Statuses that indicate claims were cancelled
    "cancelled": {
        "FWD Entered",        # Also FWD — need to check claim outcome separately
    },
    # Statuses indicating settlement (ambiguous outcome)
    "settled": {
        "Terminated-Settled",
        "Terminated-Settled After Institution",
    },
    # Statuses indicating petition was denied (patent not challenged)
    "denied": {
        "Terminated-Denied",
    },
}

# For risk scoring: statuses where the patent was actually challenged (instituted)
_INSTITUTED_STATUSES = {
    "FWD Entered",
    "Terminated-Settled",
    "Terminated-Settled After Institution",
}

# Statuses where the challenge did not proceed
_DENIED_STATUSES = {
    "Terminated-Denied",
    "Terminated-Dismissed",
}


def _resolve_firm(resolver: EntityResolver | None, name: str) -> dict[str, Any]:
    """Attempt entity resolution.  Returns dict with firm_id and canonical name."""
    if not resolver or not name:
        return {"firm_id": None, "canonical_name": name}
    result = resolver.resolve(name)
    if result:
        return {
            "firm_id": result.entity.canonical_id,
            "canonical_name": result.entity.canonical_name,
            "confidence": result.confidence,
        }
    return {"firm_id": None, "canonical_name": name}


def _safe_count(conn: sqlite3.Connection, sql: str, params: list) -> int:
    """Execute a COUNT query safely, returning 0 on error."""
    try:
        row = conn.execute(sql, params).fetchone()
        return row[0] if row else 0
    except sqlite3.OperationalError:
        return 0


# ---------------------------------------------------------------------------
# Tool 1: ptab_search
# ---------------------------------------------------------------------------

def ptab_search(
    store: PatentStore,
    patent_number: str | None = None,
    petitioner: str | None = None,
    patent_owner: str | None = None,
    trial_type: str | None = None,
    status: str | None = None,
    max_results: int = 20,
    page: int = 1,
    page_size: int = 20,
    resolver: EntityResolver | None = None,
) -> dict[str, Any]:
    """Search PTAB trials by various filters (AND logic).

    Parameters
    ----------
    patent_number : str, optional
        US patent number to search (LIKE match).
    petitioner : str, optional
        Petitioner party name (LIKE match).
    patent_owner : str, optional
        Patent owner name (LIKE match).
    trial_type : str, optional
        Trial type prefix filter, e.g. "IPR", "PGR", "CBM".
    status : str, optional
        Prosecution status filter (LIKE match).
    max_results : int
        Maximum total results to fetch from DB before pagination.
    page, page_size : int
        Pagination controls.
    resolver : EntityResolver, optional
        For entity name resolution on petitioner/owner names.
    """
    page = max(int(page), 1)
    page_size = min(max(int(page_size), 1), 100)
    max_results = max(1, int(max_results))

    conditions: list[str] = []
    params: list[Any] = []

    if patent_number:
        conditions.append("patent_number LIKE '%' || ? || '%'")
        params.append(patent_number.strip().upper())

    if petitioner:
        conditions.append("petitioner LIKE '%' || ? || '%'")
        params.append(petitioner.strip())

    if patent_owner:
        conditions.append("patent_owner LIKE '%' || ? || '%'")
        params.append(patent_owner.strip())

    if trial_type:
        # trial_number starts with type: "IPR2017-01234", "PGR2020-00123"
        conditions.append("trial_number LIKE ? || '%'")
        params.append(trial_type.strip().upper())

    if status:
        conditions.append("prosecution_status LIKE '%' || ? || '%'")
        params.append(status.strip())

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    filters_applied = {
        "patent_number": patent_number,
        "petitioner": petitioner,
        "patent_owner": patent_owner,
        "trial_type": trial_type,
        "status": status,
    }

    conn = store._conn()

    try:
        count_row = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM ptab_trials {where}",
            params,
        ).fetchone()
        total = count_row["cnt"] if count_row else 0

        limit = max(max_results, page * page_size)
        rows = conn.execute(
            f"SELECT trial_number, patent_number, publication_number, "
            f"filing_date, institution_decision_date, prosecution_status, "
            f"accorded_filing_date, petitioner, patent_owner, "
            f"inventor_name, application_number "
            f"FROM ptab_trials {where} "
            f"ORDER BY filing_date DESC, trial_number DESC "
            f"LIMIT ?",
            params + [limit],
        ).fetchall()
    except sqlite3.OperationalError:
        return {
            "endpoint": "ptab_search",
            "error": "Query timed out. Try narrower filters.",
            "filters_applied": filters_applied,
        }

    all_results = [dict(r) for r in rows]

    paged = paginate(all_results, page=page, page_size=page_size)

    return {
        "endpoint": "ptab_search",
        "trials": paged["results"],
        "total": total,
        "page": paged["page"],
        "page_size": paged["page_size"],
        "pages": paged["pages"],
        "filters_applied": filters_applied,
    }


# ---------------------------------------------------------------------------
# Tool 2: ptab_risk
# ---------------------------------------------------------------------------

def ptab_risk(
    store: PatentStore,
    patent_number: str | None = None,
    cpc_prefix: str | None = None,
    applicant: str | None = None,
    resolver: EntityResolver | None = None,
) -> dict[str, Any]:
    """Compute IPR/PTAB risk score for a patent, technology area, or applicant.

    Exactly one of ``patent_number``, ``cpc_prefix``, or ``applicant``
    should be provided.  The function returns a risk score (0.0 to 1.0)
    plus a breakdown of the underlying statistics.

    Scoring methodology:
      - Single patent: binary (challenged or not) + status outcome weighting.
      - CPC area: ratio of patents with PTAB challenges to total patents.
      - Applicant: ratio of their patents that have been challenged.
    """
    conn = store._conn()

    # ── Single patent risk ──
    if patent_number:
        pn = patent_number.strip().upper()
        try:
            trials = conn.execute(
                "SELECT trial_number, prosecution_status, petitioner, "
                "filing_date, institution_decision_date "
                "FROM ptab_trials WHERE patent_number = ?",
                (pn,),
            ).fetchall()
        except sqlite3.OperationalError:
            return {
                "endpoint": "ptab_risk",
                "error": "Query timed out.",
                "patent_number": pn,
            }

        trial_list = [dict(t) for t in trials]
        trial_count = len(trial_list)

        if trial_count == 0:
            return {
                "endpoint": "ptab_risk",
                "patent_number": pn,
                "risk_score": 0.0,
                "risk_label": "no_challenge",
                "trial_count": 0,
                "trials": [],
                "explanation": (
                    f"Patent {pn} has no PTAB trial history. "
                    "This does not guarantee safety, but no IPR/PGR/CBM "
                    "challenges are on record."
                ),
            }

        # Score based on outcomes
        instituted = sum(
            1 for t in trial_list
            if t.get("prosecution_status") in _INSTITUTED_STATUSES
        )
        denied = sum(
            1 for t in trial_list
            if t.get("prosecution_status") in _DENIED_STATUSES
        )
        # Higher risk if challenges were instituted
        if trial_count > 0:
            institution_rate = instituted / trial_count
        else:
            institution_rate = 0.0

        # Risk score: base 0.3 for having any challenge, +0.5 scaled by institution rate
        risk_score = min(1.0, 0.3 + 0.5 * institution_rate + 0.05 * min(trial_count, 4))

        if risk_score >= 0.7:
            risk_label = "high"
        elif risk_score >= 0.4:
            risk_label = "moderate"
        else:
            risk_label = "low"

        # Unique petitioners
        petitioners: dict[str, int] = {}
        for t in trial_list:
            pet = t.get("petitioner") or "Unknown"
            petitioners[pet] = petitioners.get(pet, 0) + 1
        top_petitioners = sorted(
            petitioners.items(), key=lambda x: x[1], reverse=True
        )[:5]

        return {
            "endpoint": "ptab_risk",
            "patent_number": pn,
            "risk_score": round(risk_score, 3),
            "risk_label": risk_label,
            "trial_count": trial_count,
            "instituted": instituted,
            "denied": denied,
            "institution_rate": round(institution_rate, 3),
            "top_petitioners": [
                {"name": name, "challenges": cnt}
                for name, cnt in top_petitioners
            ],
            "trials": trial_list[:10],  # Cap at 10 for response size
        }

    # ── CPC area risk ──
    if cpc_prefix:
        prefix = cpc_prefix.strip().upper()
        try:
            # Total patents — fast path via tech_clusters (pre-computed)
            tc_row = conn.execute(
                "SELECT SUM(patent_count) as total FROM tech_clusters "
                "WHERE cpc_class = ?",
                (prefix[:4],),
            ).fetchone()
            if tc_row and tc_row["total"] and tc_row["total"] > 0:
                total_patents = tc_row["total"]
            else:
                total_patents = _safe_count(
                    conn,
                    "SELECT COUNT(DISTINCT publication_number) FROM patent_cpc "
                    "WHERE cpc_code LIKE ? || '%'",
                    [prefix],
                )

            # PTAB trials are US patents with bare patent_number (e.g. "7790869").
            # Use a join approach with LIMIT for speed
            try:
                challenged = conn.execute(
                    "SELECT COUNT(DISTINCT pt.patent_number) AS cnt "
                    "FROM ptab_trials pt "
                    "JOIN patent_cpc pc ON ("
                    "  pc.publication_number = 'US-' || pt.patent_number || '-B1' "
                    "  OR pc.publication_number = 'US-' || pt.patent_number || '-B2'"
                    ") WHERE pc.cpc_code LIKE ? || '%'",
                    (prefix,),
                ).fetchone()
                challenged_count = challenged["cnt"] if challenged else 0
            except sqlite3.OperationalError:
                challenged_count = 0

            # If no matches via publication_number format, fall back to
            # counting ALL ptab_trials as a universe estimate
            if challenged_count == 0 and total_patents > 0:
                # Estimate: total ptab_trials / total US patents * patents in CPC area
                total_ptab = _safe_count(conn, "SELECT COUNT(*) FROM ptab_trials", [])
                total_us = _safe_count(
                    conn,
                    "SELECT COUNT(*) FROM patents WHERE country_code = 'US'",
                    [],
                )
                if total_us > 0:
                    # Use tech_clusters estimate for US patents in CPC
                    us_tc_row = conn.execute(
                        "SELECT SUM(patent_count) as total FROM tech_clusters "
                        "WHERE cpc_class = ?",
                        (prefix[:4],),
                    ).fetchone()
                    us_in_cpc = (us_tc_row["total"] // 3 if us_tc_row and us_tc_row["total"]
                                 else _safe_count(
                        conn,
                        "SELECT COUNT(DISTINCT pc.publication_number) FROM patent_cpc pc "
                        "JOIN patents p ON pc.publication_number = p.publication_number "
                        "WHERE pc.cpc_code LIKE ? || '%' AND p.country_code = 'US' "
                        "LIMIT 1",
                        [prefix],
                    ))
                    # Use proportional estimate
                    challenged_count = int(total_ptab * us_in_cpc / total_us) if total_us > 0 else 0

            # Status breakdown
            status_rows = conn.execute(
                "SELECT prosecution_status, COUNT(*) AS cnt "
                "FROM ptab_trials "
                "GROUP BY prosecution_status "
                "ORDER BY cnt DESC",
            ).fetchall()
            status_breakdown = [
                {"status": r["prosecution_status"], "count": r["cnt"]}
                for r in status_rows
            ]
        except sqlite3.OperationalError:
            return {
                "endpoint": "ptab_risk",
                "error": "Query timed out. Try a more specific CPC prefix.",
                "cpc_prefix": prefix,
            }

        trial_rate = (
            challenged_count / total_patents if total_patents > 0 else 0.0
        )
        # Normalise: typical IPR rate is ~0.5-2% of patents
        # Score: scale so 2%+ = high risk
        risk_score = min(1.0, trial_rate / 0.02) if trial_rate > 0 else 0.0

        if risk_score >= 0.7:
            risk_label = "high"
        elif risk_score >= 0.3:
            risk_label = "moderate"
        else:
            risk_label = "low"

        return {
            "endpoint": "ptab_risk",
            "cpc_prefix": prefix,
            "risk_score": round(risk_score, 3),
            "risk_label": risk_label,
            "total_patents_in_area": total_patents,
            "challenged_patents": challenged_count,
            "trial_rate": round(trial_rate, 5),
            "status_breakdown": status_breakdown,
        }

    # ── Applicant risk ──
    if applicant:
        resolved = _resolve_firm(resolver, applicant)
        firm_id = resolved.get("firm_id")
        canonical = resolved.get("canonical_name", applicant)

        try:
            # Search ptab_trials where patent_owner matches
            # Try multiple search terms for broader coverage
            search_terms = {canonical}
            if applicant.strip() != canonical:
                search_terms.add(applicant.strip())

            trial_list: list[dict] = []
            seen_trials: set[str] = set()
            for term in search_terms:
                rows = conn.execute(
                    "SELECT trial_number, patent_number, prosecution_status, "
                    "petitioner, filing_date "
                    "FROM ptab_trials "
                    "WHERE patent_owner LIKE '%' || ? || '%' "
                    "ORDER BY filing_date DESC "
                    "LIMIT 200",
                    (term,),
                ).fetchall()
                for r in rows:
                    d = dict(r)
                    tn = d.get("trial_number", "")
                    if tn not in seen_trials:
                        seen_trials.add(tn)
                        trial_list.append(d)

            # Get total patent count from firm_tech_vectors (fast pre-computed)
            # instead of scanning patent_assignees (30M rows)
            total_owned = 0
            if firm_id:
                row = conn.execute(
                    "SELECT patent_count FROM firm_tech_vectors "
                    "WHERE firm_id = ? ORDER BY year DESC LIMIT 1",
                    (firm_id,),
                ).fetchone()
                if row:
                    total_owned = row["patent_count"] or 0

            # Fallback: estimate from ptab_trials patent_owner count
            if total_owned == 0:
                total_owned = max(len(trial_list) * 50, 1000)  # Conservative estimate

        except sqlite3.OperationalError:
            return {
                "endpoint": "ptab_risk",
                "error": "Query timed out.",
                "applicant": canonical,
            }

        trial_count = len(trial_list)
        challenged_rate = (
            trial_count / total_owned if total_owned > 0 else 0.0
        )

        # Risk score: scale so 5%+ challenged = high
        risk_score = min(1.0, challenged_rate / 0.05) if challenged_rate > 0 else 0.0
        # Bonus risk for high absolute count
        if trial_count >= 20:
            risk_score = min(1.0, risk_score + 0.1)
        elif trial_count >= 50:
            risk_score = min(1.0, risk_score + 0.2)

        if risk_score >= 0.7:
            risk_label = "high"
        elif risk_score >= 0.3:
            risk_label = "moderate"
        else:
            risk_label = "low"

        # Status summary
        status_counts: dict[str, int] = {}
        for t in trial_list:
            s = t.get("prosecution_status") or "Unknown"
            status_counts[s] = status_counts.get(s, 0) + 1
        status_breakdown = [
            {"status": s, "count": c}
            for s, c in sorted(status_counts.items(), key=lambda x: x[1], reverse=True)
        ]

        # Top challengers
        petitioner_counts: dict[str, int] = {}
        for t in trial_list:
            pet = t.get("petitioner") or "Unknown"
            petitioner_counts[pet] = petitioner_counts.get(pet, 0) + 1
        top_petitioners = [
            {"name": name, "challenges": cnt}
            for name, cnt in sorted(
                petitioner_counts.items(), key=lambda x: x[1], reverse=True
            )[:10]
        ]

        return {
            "endpoint": "ptab_risk",
            "applicant": canonical,
            "firm_id": firm_id,
            "risk_score": round(risk_score, 3),
            "risk_label": risk_label,
            "total_patents_owned": total_owned,
            "trials_as_owner": trial_count,
            "challenged_rate": round(challenged_rate, 5),
            "status_breakdown": status_breakdown,
            "top_challengers": top_petitioners,
            "recent_trials": trial_list[:10],
        }

    return {
        "endpoint": "ptab_risk",
        "error": "Provide one of: patent_number, cpc_prefix, or applicant.",
    }


# ---------------------------------------------------------------------------
# Tool 3: litigation_search
# ---------------------------------------------------------------------------

def litigation_search(
    store: PatentStore,
    plaintiff: str | None = None,
    defendant: str | None = None,
    patent_number: str | None = None,
    court: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    page: int = 1,
    page_size: int = 20,
    resolver: EntityResolver | None = None,
) -> dict[str, Any]:
    """Search patent litigation cases with optional filters (AND logic).

    Parameters
    ----------
    plaintiff, defendant : str, optional
        Party name filter (LIKE match).
    patent_number : str, optional
        Search via the litigation_patents link table.
    court : str, optional
        Court name filter (LIKE match).
    date_from, date_to : str, optional
        Date range filter on date_filed (ISO format YYYY-MM-DD).
    page, page_size : int
        Pagination controls.
    resolver : EntityResolver, optional
        Not currently used for search, reserved for future name resolution.
    """
    page = max(int(page), 1)
    page_size = min(max(int(page_size), 1), 100)

    conditions: list[str] = []
    params: list[Any] = []
    join_clause = ""

    if patent_number:
        # Join with litigation_patents to filter by patent
        join_clause = (
            "JOIN litigation_patents lp ON lc.case_id = lp.case_id "
        )
        conditions.append("lp.patent_number LIKE '%' || ? || '%'")
        params.append(patent_number.strip().upper())

    if plaintiff:
        conditions.append("lc.plaintiff LIKE '%' || ? || '%'")
        params.append(plaintiff.strip())

    if defendant:
        conditions.append("lc.defendant LIKE '%' || ? || '%'")
        params.append(defendant.strip())

    if court:
        conditions.append("lc.court LIKE '%' || ? || '%'")
        params.append(court.strip())

    if date_from:
        conditions.append("lc.date_filed >= ?")
        params.append(date_from.strip())

    if date_to:
        conditions.append("lc.date_filed <= ?")
        params.append(date_to.strip())

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    filters_applied = {
        "plaintiff": plaintiff,
        "defendant": defendant,
        "patent_number": patent_number,
        "court": court,
        "date_from": date_from,
        "date_to": date_to,
    }

    conn = store._conn()

    try:
        # Count total matching rows
        count_sql = (
            f"SELECT COUNT(DISTINCT lc.case_id) AS cnt "
            f"FROM litigation_cases lc {join_clause} {where}"
        )
        count_row = conn.execute(count_sql, params).fetchone()
        total = count_row["cnt"] if count_row else 0

        # Fetch results with pagination via LIMIT/OFFSET
        offset = (page - 1) * page_size
        rows = conn.execute(
            f"SELECT DISTINCT lc.case_id, lc.case_number, lc.court, "
            f"lc.judge, lc.date_filed, lc.date_terminated, "
            f"lc.plaintiff, lc.defendant, lc.nature_of_suit, lc.outcome "
            f"FROM litigation_cases lc {join_clause} {where} "
            f"ORDER BY lc.date_filed DESC, lc.case_id DESC "
            f"LIMIT ? OFFSET ?",
            params + [page_size, offset],
        ).fetchall()
    except sqlite3.OperationalError:
        return {
            "endpoint": "litigation_search",
            "error": "Query timed out. Try narrower filters.",
            "filters_applied": filters_applied,
        }

    results = [dict(r) for r in rows]

    # Enrich each case with its associated patent numbers
    if results:
        case_ids = [r["case_id"] for r in results]
        ph = ",".join("?" * len(case_ids))
        try:
            patent_rows = conn.execute(
                f"SELECT case_id, patent_number FROM litigation_patents "
                f"WHERE case_id IN ({ph})",
                case_ids,
            ).fetchall()
            patent_map: dict[int, list[str]] = {}
            for pr in patent_rows:
                patent_map.setdefault(pr["case_id"], []).append(
                    pr["patent_number"]
                )
            for r in results:
                r["patent_numbers"] = patent_map.get(r["case_id"], [])
        except sqlite3.OperationalError:
            for r in results:
                r["patent_numbers"] = []

    import math
    pages = math.ceil(total / page_size) if total > 0 else 1

    return {
        "endpoint": "litigation_search",
        "cases": results,
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": pages,
        "filters_applied": filters_applied,
    }


# ---------------------------------------------------------------------------
# Tool 4: litigation_risk
# ---------------------------------------------------------------------------

def litigation_risk(
    store: PatentStore,
    firm_query: str | None = None,
    cpc_prefix: str | None = None,
    resolver: EntityResolver | None = None,
) -> dict[str, Any]:
    """Compute litigation risk for a firm or technology area.

    Parameters
    ----------
    firm_query : str, optional
        Firm name or identifier.  Searches both plaintiff and defendant fields.
    cpc_prefix : str, optional
        CPC classification prefix to assess technology-area litigation density.
    resolver : EntityResolver, optional
        For entity name resolution.

    Returns a risk score (0.0 to 1.0) with breakdown.
    """
    conn = store._conn()

    # ── Firm-level litigation risk ──
    if firm_query:
        resolved = _resolve_firm(resolver, firm_query)
        firm_id = resolved.get("firm_id")
        canonical = resolved.get("canonical_name", firm_query)

        # Build search terms: canonical name + original query
        search_terms = {canonical}
        if firm_query.strip() != canonical:
            search_terms.add(firm_query.strip())

        try:
            # Cases as plaintiff
            plaintiff_cases: list[dict] = []
            defendant_cases: list[dict] = []

            for term in search_terms:
                p_rows = conn.execute(
                    "SELECT case_id, case_number, court, date_filed, "
                    "defendant, nature_of_suit, outcome "
                    "FROM litigation_cases "
                    "WHERE plaintiff LIKE '%' || ? || '%' "
                    "ORDER BY date_filed DESC LIMIT 100",
                    (term,),
                ).fetchall()
                plaintiff_cases.extend(dict(r) for r in p_rows)

                d_rows = conn.execute(
                    "SELECT case_id, case_number, court, date_filed, "
                    "plaintiff, nature_of_suit, outcome "
                    "FROM litigation_cases "
                    "WHERE defendant LIKE '%' || ? || '%' "
                    "ORDER BY date_filed DESC LIMIT 100",
                    (term,),
                ).fetchall()
                defendant_cases.extend(dict(r) for r in d_rows)

            # Deduplicate by case_id
            seen_p: set[int] = set()
            unique_plaintiff: list[dict] = []
            for c in plaintiff_cases:
                cid = c["case_id"]
                if cid not in seen_p:
                    seen_p.add(cid)
                    unique_plaintiff.append(c)

            seen_d: set[int] = set()
            unique_defendant: list[dict] = []
            for c in defendant_cases:
                cid = c["case_id"]
                if cid not in seen_d:
                    seen_d.add(cid)
                    unique_defendant.append(c)

            plaintiff_count = len(unique_plaintiff)
            defendant_count = len(unique_defendant)
            total_cases = plaintiff_count + defendant_count

        except sqlite3.OperationalError:
            return {
                "endpoint": "litigation_risk",
                "error": "Query timed out.",
                "firm_query": canonical,
            }

        # Risk score: being a defendant is riskier than being a plaintiff
        # Scale: 10+ defendant cases = high risk
        defendant_risk = min(1.0, defendant_count / 10.0)
        plaintiff_factor = min(0.3, plaintiff_count / 30.0)  # Plaintiffs show litigiousness
        risk_score = min(1.0, defendant_risk * 0.7 + plaintiff_factor * 0.3)

        if risk_score >= 0.7:
            risk_label = "high"
        elif risk_score >= 0.3:
            risk_label = "moderate"
        else:
            risk_label = "low"

        # Top opponents (as defendant)
        opponent_counts: dict[str, int] = {}
        for c in unique_defendant:
            opp = c.get("plaintiff") or "Unknown"
            opponent_counts[opp] = opponent_counts.get(opp, 0) + 1
        top_opponents = [
            {"name": name, "cases": cnt}
            for name, cnt in sorted(
                opponent_counts.items(), key=lambda x: x[1], reverse=True
            )[:10]
        ]

        # Court distribution
        court_counts: dict[str, int] = {}
        for c in unique_plaintiff + unique_defendant:
            ct = c.get("court") or "Unknown"
            court_counts[ct] = court_counts.get(ct, 0) + 1
        top_courts = [
            {"court": ct, "cases": cnt}
            for ct, cnt in sorted(
                court_counts.items(), key=lambda x: x[1], reverse=True
            )[:5]
        ]

        # Nature of suit distribution
        nos_counts: dict[str, int] = {}
        for c in unique_plaintiff + unique_defendant:
            nos = c.get("nature_of_suit") or "Unknown"
            nos_counts[nos] = nos_counts.get(nos, 0) + 1
        nos_breakdown = [
            {"nature_of_suit": nos, "cases": cnt}
            for nos, cnt in sorted(
                nos_counts.items(), key=lambda x: x[1], reverse=True
            )[:5]
        ]

        return {
            "endpoint": "litigation_risk",
            "firm_query": canonical,
            "firm_id": firm_id,
            "risk_score": round(risk_score, 3),
            "risk_label": risk_label,
            "cases_as_plaintiff": plaintiff_count,
            "cases_as_defendant": defendant_count,
            "total_cases": total_cases,
            "top_opponents": top_opponents,
            "top_courts": top_courts,
            "nature_of_suit_breakdown": nos_breakdown,
            "recent_as_defendant": unique_defendant[:5],
            "recent_as_plaintiff": unique_plaintiff[:5],
        }

    # ── CPC area litigation risk ──
    if cpc_prefix:
        prefix = cpc_prefix.strip().upper()
        try:
            total_in_area = _safe_count(
                conn,
                "SELECT COUNT(DISTINCT publication_number) FROM patent_cpc "
                "WHERE cpc_code LIKE ? || '%'",
                [prefix],
            )

            # litigation_patents may be empty (link table not always populated).
            # Primary approach: count litigation_patents entries that match
            # patents in this CPC area via US publication number format.
            litigated_count = 0
            lit_patents_total = _safe_count(
                conn, "SELECT COUNT(*) FROM litigation_patents", []
            )

            if lit_patents_total > 0:
                # Try matching via publication_number format
                row = conn.execute(
                    "SELECT COUNT(DISTINCT lp.patent_number) AS cnt "
                    "FROM litigation_patents lp "
                    "WHERE EXISTS ("
                    "  SELECT 1 FROM patent_cpc pc "
                    "  WHERE pc.cpc_code LIKE ? || '%' "
                    "  AND (pc.publication_number = 'US-' || lp.patent_number || '-B1' "
                    "   OR  pc.publication_number = 'US-' || lp.patent_number || '-B2')"
                    ")",
                    (prefix,),
                ).fetchone()
                litigated_count = row["cnt"] if row else 0

            # Fallback: estimate from litigation_cases keyword matching
            # on nature_of_suit (patent infringement cases in general)
            if litigated_count == 0:
                # Use CPC section name as keyword heuristic for top plaintiffs
                total_lit_cases = _safe_count(
                    conn,
                    "SELECT COUNT(*) FROM litigation_cases "
                    "WHERE nature_of_suit LIKE '%Patent%'",
                    [],
                )
                total_us_patents = _safe_count(
                    conn,
                    "SELECT COUNT(*) FROM patents WHERE country_code = 'US'",
                    [],
                )
                us_in_cpc = _safe_count(
                    conn,
                    "SELECT COUNT(DISTINCT pc.publication_number) FROM patent_cpc pc "
                    "JOIN patents p ON pc.publication_number = p.publication_number "
                    "WHERE pc.cpc_code LIKE ? || '%' AND p.country_code = 'US'",
                    [prefix],
                )
                # Proportional estimate
                if total_us_patents > 0:
                    litigated_count = int(
                        total_lit_cases * us_in_cpc / total_us_patents
                    )

            # Top plaintiffs from all patent litigation cases
            top_plaintiff_rows = conn.execute(
                "SELECT plaintiff, COUNT(DISTINCT case_id) AS cnt "
                "FROM litigation_cases "
                "WHERE nature_of_suit LIKE '%Patent%' "
                "AND plaintiff IS NOT NULL "
                "GROUP BY plaintiff "
                "ORDER BY cnt DESC LIMIT 10",
            ).fetchall()
            top_plaintiffs = [
                {"name": r["plaintiff"], "cases": r["cnt"]}
                for r in top_plaintiff_rows
            ]

        except sqlite3.OperationalError:
            return {
                "endpoint": "litigation_risk",
                "error": "Query timed out. Try a more specific CPC prefix.",
                "cpc_prefix": prefix,
            }

        litigation_rate = (
            litigated_count / total_in_area if total_in_area > 0 else 0.0
        )
        # Scale: 1%+ litigated = high risk for a tech area
        risk_score = min(1.0, litigation_rate / 0.01) if litigation_rate > 0 else 0.0

        if risk_score >= 0.7:
            risk_label = "high"
        elif risk_score >= 0.3:
            risk_label = "moderate"
        else:
            risk_label = "low"

        return {
            "endpoint": "litigation_risk",
            "cpc_prefix": prefix,
            "risk_score": round(risk_score, 3),
            "risk_label": risk_label,
            "total_patents_in_area": total_in_area,
            "litigated_patents": litigated_count,
            "litigation_rate": round(litigation_rate, 5),
            "top_plaintiffs_in_area": top_plaintiffs,
        }

    return {
        "endpoint": "litigation_risk",
        "error": "Provide one of: firm_query or cpc_prefix.",
    }
