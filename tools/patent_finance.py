"""Patent finance tools -- Black-Scholes option valuation, volatility,
VaR (5-factor + Monte Carlo), and CAPM-style tech beta.

Uses pre-computed tables (tech_cluster_momentum, firm_tech_vectors,
startability_surface, citation_counts) to avoid full patents table scans.
"""
from __future__ import annotations

import math
from typing import Any

import numpy as np
from scipy.stats import norm

from db.sqlite_store import PatentStore


def _resolve_firm_id_db(resolved, conn, table="firm_tech_vectors"):
    """Resolve entity to DB firm_id with company_XXXX fallback."""
    firm_id = resolved.entity.canonical_id if hasattr(resolved, 'entity') else str(resolved)
    row = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE firm_id = ?", (firm_id,)).fetchone()
    if row and row[0] > 0:
        return firm_id
    ticker = getattr(resolved.entity, 'ticker', None) if hasattr(resolved, 'entity') else None
    if ticker:
        alt_id = f"company_{ticker}"
        row2 = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE firm_id = ?", (alt_id,)).fetchone()
        if row2 and row2[0] > 0:
            return alt_id
    like_row = conn.execute(f"SELECT DISTINCT firm_id FROM {table} WHERE firm_id LIKE ? LIMIT 1", (f"{firm_id}%",)).fetchone()
    if like_row:
        return like_row[0]
    return firm_id
from entity.resolver import EntityResolver
from tools.cpc_labels_ja import CPC_CLASS_JA
from tools.royalty_benchmarks import get_royalty_rate, get_wacc, get_tax_rate, get_sector


# ─── helpers ─────────────────────────────────────────────────────────

def _resolve_firm(resolver: EntityResolver, name: str) -> str | None:
    res = resolver.resolve(name, country_hint="JP")
    return res.entity.canonical_id if res else None


def _cpc4(code: str) -> str:
    """Extract 4-char CPC class from a code like 'H01M10/052'."""
    return code[:4] if code else ""


def _get_yearly_counts_from_momentum(conn, cluster_ids: list[str],
                                      year_from: int, year_to: int) -> dict[int, int]:
    """Aggregate yearly patent counts from tech_cluster_momentum."""
    if not cluster_ids:
        return {}
    ph = ",".join("?" for _ in cluster_ids)
    rows = conn.execute(
        f"SELECT year, SUM(patent_count) as total "
        f"FROM tech_cluster_momentum "
        f"WHERE cluster_id IN ({ph}) AND year BETWEEN ? AND ? "
        f"GROUP BY year ORDER BY year",
        (*cluster_ids, year_from, year_to),
    ).fetchall()
    return {r["year"]: r["total"] for r in rows}


def _get_total_market_counts(conn, year_from: int, year_to: int) -> dict[int, int]:
    """Total patent filings across all clusters per year."""
    rows = conn.execute(
        "SELECT year, SUM(patent_count) as total "
        "FROM tech_cluster_momentum "
        "WHERE year BETWEEN ? AND ? "
        "GROUP BY year ORDER BY year",
        (year_from, year_to),
    ).fetchall()
    return {r["year"]: r["total"] for r in rows}


def _log_returns(counts: dict[int, int]) -> list[float]:
    """Compute log returns from year-to-year counts."""
    years = sorted(counts.keys())
    returns = []
    for i in range(1, len(years)):
        c0 = counts[years[i - 1]]
        c1 = counts[years[i]]
        if c0 > 0 and c1 > 0:
            returns.append(math.log(c1 / c0))
    return returns


def _find_clusters_for_cpc(conn, cpc_prefix: str, limit: int = 20) -> list[str]:
    """Find cluster_ids matching a CPC prefix."""
    rows = conn.execute(
        "SELECT cluster_id FROM tech_clusters "
        "WHERE cpc_class LIKE ? ORDER BY patent_count DESC LIMIT ?",
        (f"{cpc_prefix}%", limit),
    ).fetchall()
    return [r["cluster_id"] for r in rows]


def _find_best_year(conn, firm_id: str) -> int:
    """Find the latest year with substantial data for a firm.

    Picks the latest year whose row count is >= 90% of the max count.
    This avoids picking ancient years when counts are equal/similar.
    """
    rows = conn.execute(
        "SELECT year, COUNT(*) as cnt FROM startability_surface "
        "WHERE firm_id = ? GROUP BY year ORDER BY cnt DESC",
        (firm_id,),
    ).fetchall()
    if not rows:
        return 2023
    max_cnt = rows[0]["cnt"]
    threshold = max(1, int(max_cnt * 0.9))
    # Among years with >= 90% of max count, pick the latest
    best = max(
        (r["year"] for r in rows if r["cnt"] >= threshold),
        default=rows[0]["year"],
    )
    return best


def _keyword_to_cpc(query: str) -> str | None:
    """Map common keywords to CPC prefixes."""
    _KW_CPC = {
        "電池": "H01M", "バッテリー": "H01M", "battery": "H01M",
        "半導体": "H01L", "semiconductor": "H01L",
        "AI": "G06N", "人工知能": "G06N", "機械学習": "G06N",
        "自動運転": "B60W", "autonomous": "B60W",
        "EV": "B60L", "電気自動車": "B60L",
        "ロボット": "B25J", "robot": "B25J",
        "水素": "C01B", "hydrogen": "C01B",
        "医薬": "A61K", "pharmaceutical": "A61K",
        "5G": "H04W", "通信": "H04W",
    }
    ql = query.lower()
    for kw, cpc in _KW_CPC.items():
        if kw.lower() in ql:
            return cpc
    return None


def _cluster_growth_rate(conn, cluster_id: str) -> float | None:
    """Get most recent growth_rate from tech_cluster_momentum."""
    row = conn.execute(
        "SELECT growth_rate FROM tech_cluster_momentum "
        "WHERE cluster_id = ? AND year = (SELECT year FROM tech_cluster_momentum GROUP BY year HAVING AVG(growth_rate) > -0.3 ORDER BY year DESC LIMIT 1) LIMIT 1",
        (cluster_id,),
    ).fetchone()
    return float(row["growth_rate"]) if row and row["growth_rate"] is not None else None


# ─── Black-Scholes helpers ──────────────────────────────────────────

def _black_scholes_call(S: float, K: float, T: float, r: float, sigma: float) -> dict:
    """Compute Black-Scholes call option value and full Greeks.

    Returns value, delta, theta, vega, gamma, rho, d1, d2.
    """
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return {
            "value": 0, "delta": 0, "theta": 0, "vega": 0,
            "gamma": 0, "rho": 0, "d1": 0, "d2": 0,
        }

    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r + sigma**2 / 2) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T

    C = S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    delta = norm.cdf(d1)
    theta = (-(S * norm.pdf(d1) * sigma) / (2 * sqrt_T)
             - r * K * math.exp(-r * T) * norm.cdf(d2))
    vega = S * sqrt_T * norm.pdf(d1)
    gamma = norm.pdf(d1) / (S * sigma * sqrt_T)
    rho = K * T * math.exp(-r * T) * norm.cdf(d2)

    return {
        "value": round(C, 4),
        "delta": round(delta, 4),
        "theta": round(theta, 4),
        "vega": round(vega, 4),
        "gamma": round(gamma, 6),
        "rho": round(rho, 4),
        "d1": round(d1, 4),
        "d2": round(d2, 4),
    }


# ─── Shared estimation helpers ──────────────────────────────────────

def _get_cpc_volatility(conn, cpc_prefix: str) -> float:
    """Get volatility of a CPC area from tech_cluster_momentum."""
    cluster_ids = _find_clusters_for_cpc(conn, cpc_prefix)
    if not cluster_ids:
        return 0.3  # default
    counts = _get_yearly_counts_from_momentum(conn, cluster_ids, 2016, 2024)
    if len(counts) < 3:
        return 0.3
    returns = _log_returns(counts)
    if not returns:
        return 0.3
    return max(0.05, float(np.std(returns)))


def _estimate_S_K(conn, cpc_prefix: str, S_user: float | None,
                  K_user: float | None) -> tuple[float, float, str]:
    """Estimate or use user-provided S, K.

    Returns (S, K, confidence) where confidence is 'high', 'medium', or 'low'.
    """
    if S_user is not None and K_user is not None:
        return S_user, K_user, "high"

    # Estimate from CPC filing density and royalty benchmarks
    cluster_ids = _find_clusters_for_cpc(conn, cpc_prefix)
    total_patents = 0
    if cluster_ids:
        ph = ",".join("?" for _ in cluster_ids)
        row = conn.execute(
            f"SELECT SUM(patent_count) as total FROM tech_cluster_momentum "
            f"WHERE cluster_id IN ({ph}) AND year = "
            f"(SELECT year FROM tech_cluster_momentum GROUP BY year HAVING AVG(growth_rate) > -0.3 ORDER BY year DESC LIMIT 1)",
            cluster_ids,
        ).fetchone()
        total_patents = (row["total"] or 0) if row else 0

    # Use royalty_benchmarks for the rate
    _, typical_rate, _, _ = get_royalty_rate(cpc_prefix)

    S_est = max(10.0, total_patents * typical_rate * 100 / max(total_patents, 1) * 100)
    K_est = S_est * 0.7

    confidence = "medium" if total_patents > 100 else "low"

    return (
        S_user if S_user is not None else round(S_est, 2),
        K_user if K_user is not None else round(K_est, 2),
        confidence if not (S_user is not None or K_user is not None) else "medium",
    )


def _tech_risk_premium(conn, cpc_prefix: str) -> float:
    """Technology risk premium based on cluster momentum.

    Returns a multiplier:
      - <-0.1 growth_rate (declining tech) => 0.7 discount
      - >0.2 growth_rate (hot tech) => 1.2 premium
      - otherwise => 1.0 neutral
    """
    cluster_ids = _find_clusters_for_cpc(conn, cpc_prefix, limit=5)
    if not cluster_ids:
        return 1.0
    rates = []
    for cid in cluster_ids:
        gr = _cluster_growth_rate(conn, cid)
        if gr is not None:
            rates.append(gr)
    if not rates:
        return 1.0
    avg_rate = float(np.mean(rates))
    if avg_rate < -0.1:
        return 0.7
    elif avg_rate > 0.2:
        return 1.2
    else:
        return 1.0


# ─── Tool 1: patent_option_value ────────────────────────────────────

def patent_option_value(
    store: PatentStore,
    resolver: EntityResolver,
    query: str,
    query_type: str | None = None,
    S: float | None = None,
    K: float | None = None,
    risk_free_rate: float = 0.02,
    year: int = 2024,
) -> dict[str, Any]:
    """Black-Scholes real option valuation for patents.

    Includes full Greeks (delta, theta, vega, gamma, rho), citation decay
    adjustment, and technology risk premium.
    """
    store._relax_timeout()
    conn = store._conn()

    # Auto-detect query_type
    if query_type is None:
        q = query.strip().upper()
        if q.startswith("JP-") or q.startswith("US-") or q.startswith("EP-"):
            query_type = "patent"
        elif len(q) <= 8 and q[:1].isalpha() and q[1:3].isdigit():
            query_type = "technology"
        else:
            query_type = "firm"

    if query_type == "patent":
        return _option_value_patent(conn, query, S, K, risk_free_rate, year)
    elif query_type == "technology":
        return _option_value_technology(conn, query, S, K, risk_free_rate, year)
    elif query_type == "firm":
        firm_id = _resolve_firm(resolver, query)
        if not firm_id:
            return {"error": f"Could not resolve firm: '{query}'"}
        return _option_value_firm(conn, firm_id, S, K, risk_free_rate, year)
    else:
        return {"error": f"Invalid query_type: '{query_type}'"}


def _citation_decay_factor(year: int, filing_year: int) -> float:
    """Exponential citation decay: older patents' citation value decays.

    decay = exp(-0.05 * age).  A 20-year-old patent retains ~37% of
    its citation premium compared to a brand-new one.
    """
    age = max(0, year - filing_year)
    return math.exp(-0.05 * age)


def _option_value_patent(conn, pub_num: str, S_user, K_user, r, year) -> dict:
    """Option value for a single patent with full Greeks and risk adjustments."""
    pat = conn.execute(
        "SELECT publication_number, filing_date, title_ja, title_en, "
        "citation_count_forward FROM patents WHERE publication_number = ?",
        (pub_num,),
    ).fetchone()
    if not pat:
        return {"error": f"Patent not found: '{pub_num}'"}

    filing_date = pat["filing_date"]
    filing_year = int(str(filing_date)[:4]) if filing_date else year - 10
    T = max(0, 20 - (year - filing_year))
    if T <= 0:
        return {
            "endpoint": "patent_option_value",
            "patent": pub_num,
            "status": "expired",
            "remaining_years": 0,
            "option_value": 0,
            "interpretation": "この特許は既に満了しています。",
        }

    # Get primary CPC
    cpc_row = conn.execute(
        "SELECT cpc_code FROM patent_cpc WHERE publication_number = ? LIMIT 1",
        (pub_num,),
    ).fetchone()
    cpc = cpc_row["cpc_code"] if cpc_row else "H01M"
    cpc4 = _cpc4(cpc)

    sigma = _get_cpc_volatility(conn, cpc4)
    S, K, valuation_confidence = _estimate_S_K(conn, cpc4, S_user, K_user)

    bs = _black_scholes_call(S, K, T, r, sigma)

    # Citation multiplier with decay adjustment
    cited_by = pat["citation_count_forward"] or 0
    raw_citation_mult = 1 + math.log(1 + cited_by) / 5
    decay = _citation_decay_factor(year, filing_year)
    adjusted_citation_mult = 1 + (raw_citation_mult - 1) * decay

    # Technology risk premium
    risk_premium = _tech_risk_premium(conn, cpc4)

    adjusted_value = round(bs["value"] * adjusted_citation_mult * risk_premium, 4)

    title = pat["title_ja"] or pat["title_en"] or ""

    result: dict[str, Any] = {
        "endpoint": "patent_option_value",
        "query_type": "patent",
        "patent": pub_num,
        "title": title,
        "filing_year": filing_year,
        "remaining_years": T,
        "cpc": cpc,
        "cpc_label": CPC_CLASS_JA.get(cpc4, ""),
        "parameters": {
            "S": S, "K": K, "T": T, "r": r, "sigma": round(sigma, 4),
            "user_specified_S_K": valuation_confidence == "high",
        },
        "valuation_confidence": valuation_confidence,
        "black_scholes": bs,
        "citation_adjustment": {
            "cited_by_count": cited_by,
            "raw_citation_mult": round(raw_citation_mult, 4),
            "decay_factor": round(decay, 4),
            "adjusted_citation_mult": round(adjusted_citation_mult, 4),
        },
        "tech_risk_premium": risk_premium,
        "adjusted_option_value": adjusted_value,
        "greeks": {
            "delta": bs["delta"],
            "gamma": bs["gamma"],
            "theta": bs["theta"],
            "vega": bs["vega"],
            "rho": bs["rho"],
        },
    }
    if valuation_confidence != "high":
        result["note"] = (
            "S, Kは推定値を使用。実際の市場データがある場合はS, Kパラメータで指定可能。"
        )
    return result


def _option_value_firm(conn, firm_id: str, S_user, K_user, r, year) -> dict:
    """Portfolio option value for a firm with citation decay and risk premium."""
    actual_year = _find_best_year(conn, firm_id)

    # Get firm's top-cited patents (limit 1000 via citation_counts + patent_assignees)
    # Use GROUP BY to eliminate duplicates from multiple assignees
    rows = conn.execute(
        "SELECT pa.publication_number, MAX(cc.forward_citations) as forward_citations, "
        "p.filing_date "
        "FROM patent_assignees pa "
        "JOIN citation_counts cc ON pa.publication_number = cc.publication_number "
        "JOIN patents p ON pa.publication_number = p.publication_number "
        "WHERE pa.firm_id = ? "
        "GROUP BY pa.publication_number "
        "ORDER BY forward_citations DESC LIMIT 1000",
        (firm_id,),
    ).fetchall()

    if not rows:
        # Fallback: try without citation_counts
        rows = conn.execute(
            "SELECT pa.publication_number, MAX(p.citation_count_forward) as forward_citations, "
            "p.filing_date "
            "FROM patent_assignees pa "
            "JOIN patents p ON pa.publication_number = p.publication_number "
            "WHERE pa.firm_id = ? "
            "GROUP BY pa.publication_number "
            "ORDER BY forward_citations DESC LIMIT 500",
            (firm_id,),
        ).fetchall()

    if not rows:
        return {"error": f"No patents found for firm: '{firm_id}'"}

    # Get firm's dominant CPC from firm_tech_vectors
    ftv = conn.execute(
        "SELECT dominant_cpc, patent_count, tech_diversity FROM firm_tech_vectors "
        "WHERE firm_id = ? AND year = ?",
        (firm_id, actual_year),
    ).fetchone()
    dominant_cpc = (ftv["dominant_cpc"] or "H01M") if ftv else "H01M"
    total_patents = (ftv["patent_count"] or len(rows)) if ftv else len(rows)
    cpc4 = _cpc4(dominant_cpc)

    sigma = _get_cpc_volatility(conn, cpc4)
    S, K, valuation_confidence = _estimate_S_K(conn, cpc4, S_user, K_user)
    risk_premium = _tech_risk_premium(conn, cpc4)

    # Calculate option value for each patent
    values = []
    remaining_dist: dict[str, int] = {}
    top_patents = []
    seen_pubs: set[str] = set()
    greeks_agg = {"delta": [], "gamma": [], "theta": [], "vega": [], "rho": []}

    for row in rows:
        fd = row["filing_date"]
        fy = int(str(fd)[:4]) if fd else year - 10
        T = max(0, 20 - (year - fy))
        cited_by = row["forward_citations"] or 0

        if T > 0:
            bs = _black_scholes_call(S, K, T, r, sigma)
            raw_mult = 1 + math.log(1 + cited_by) / 5
            decay = _citation_decay_factor(year, fy)
            adj_mult = 1 + (raw_mult - 1) * decay
            adj_val = bs["value"] * adj_mult * risk_premium
            for g in greeks_agg:
                greeks_agg[g].append(bs[g])
        else:
            adj_val = 0
            bs = {"value": 0, "delta": 0, "gamma": 0, "theta": 0, "vega": 0, "rho": 0}

        values.append(adj_val)

        # Track remaining years distribution
        bucket = f"{max(0, T)}"
        remaining_dist[bucket] = remaining_dist.get(bucket, 0) + 1

        pub = row["publication_number"]
        if len(top_patents) < 10 and adj_val > 0 and pub not in seen_pubs:
            seen_pubs.add(pub)
            top_patents.append({
                "patent": pub,
                "option_value": round(adj_val, 2),
                "cited_by": cited_by,
                "remaining_years": T,
                "delta": bs["delta"],
            })

    # Extrapolate for remaining patents not in sample
    sample_avg = sum(values) / len(values) if values else 0
    extrapolated_total = sample_avg * total_patents

    # Get CPC distribution from startability_surface
    cpc_data = conn.execute(
        "SELECT cluster_id, score FROM startability_surface "
        "WHERE firm_id = ? AND year = ? ORDER BY score DESC LIMIT 20",
        (firm_id, actual_year),
    ).fetchall()
    cpc_dist = []
    for c in cpc_data:
        cid = c["cluster_id"]
        cp = _cpc4(cid)
        cpc_dist.append({
            "cpc": cp, "label": CPC_CLASS_JA.get(cp, ""),
            "score": round(c["score"], 3),
        })

    top_patents.sort(key=lambda x: x["option_value"], reverse=True)

    result: dict[str, Any] = {
        "endpoint": "patent_option_value",
        "query_type": "firm",
        "firm_id": firm_id,
        "year": actual_year,
        "valuation_confidence": valuation_confidence,
        "parameters": {
            "S": S, "K": K, "r": r, "sigma": round(sigma, 4),
            "user_specified_S_K": valuation_confidence == "high",
            "tech_risk_premium": risk_premium,
        },
        "portfolio_summary": {
            "total_patents": total_patents,
            "sample_size": len(rows),
            "sample_avg_option_value": round(sample_avg, 2),
            "portfolio_option_value": round(extrapolated_total, 2),
            "active_patents_in_sample": sum(1 for v in values if v > 0),
        },
        "top_value_patents": top_patents,
        "cpc_distribution": cpc_dist[:10],
        "remaining_years_distribution": {
            k: remaining_dist[k]
            for k in sorted(remaining_dist.keys(), key=lambda x: int(x))
        },
        "greeks_portfolio": {
            "avg_delta": round(float(np.mean(greeks_agg["delta"])) if greeks_agg["delta"] else 0, 4),
            "avg_gamma": round(float(np.mean(greeks_agg["gamma"])) if greeks_agg["gamma"] else 0, 6),
            "avg_theta": round(float(np.mean(greeks_agg["theta"])) if greeks_agg["theta"] else 0, 4),
            "avg_vega": round(float(np.mean(greeks_agg["vega"])) if greeks_agg["vega"] else 0, 4),
            "avg_rho": round(float(np.mean(greeks_agg["rho"])) if greeks_agg["rho"] else 0, 4),
        },
    }
    if valuation_confidence != "high":
        result["note"] = (
            "S, Kは推定値を使用。実際の市場データがある場合はS, Kパラメータで指定可能。"
        )
    return result


def _option_value_technology(conn, cpc_prefix: str, S_user, K_user, r, year) -> dict:
    """Option value for a technology area with risk premium."""
    cpc_prefix = cpc_prefix.strip().upper()
    cluster_ids = _find_clusters_for_cpc(conn, cpc_prefix)
    cpc4 = _cpc4(cpc_prefix)

    sigma = _get_cpc_volatility(conn, cpc_prefix)
    S, K, valuation_confidence = _estimate_S_K(conn, cpc_prefix, S_user, K_user)
    risk_premium = _tech_risk_premium(conn, cpc_prefix)

    # Average T from filing trend
    counts = _get_yearly_counts_from_momentum(conn, cluster_ids, 2016, year)
    if counts:
        weighted_year = sum(y * c for y, c in counts.items()) / max(sum(counts.values()), 1)
        avg_T = max(1, 20 - (year - int(weighted_year)))
    else:
        avg_T = 10

    bs = _black_scholes_call(S, K, avg_T, r, sigma)

    # Apply risk premium to the base value
    adjusted_value = round(bs["value"] * risk_premium, 4)

    # Top players
    top_firms = []
    if cluster_ids:
        for cid in cluster_ids[:3]:
            frows = conn.execute(
                "SELECT firm_id, score FROM startability_surface "
                "WHERE cluster_id = ? AND year = "
                "(SELECT MAX(year) FROM startability_surface WHERE cluster_id = ?) "
                "ORDER BY score DESC LIMIT 5",
                (cid, cid),
            ).fetchall()
            for fr in frows:
                top_firms.append({
                    "firm_id": fr["firm_id"],
                    "score": round(fr["score"], 3),
                })

    # Deduplicate
    seen: set[str] = set()
    unique_firms = []
    for f in top_firms:
        if f["firm_id"] not in seen:
            seen.add(f["firm_id"])
            unique_firms.append(f)
    top_firms = unique_firms[:10]

    result: dict[str, Any] = {
        "endpoint": "patent_option_value",
        "query_type": "technology",
        "cpc_prefix": cpc_prefix,
        "cpc_label": CPC_CLASS_JA.get(cpc4, ""),
        "valuation_confidence": valuation_confidence,
        "parameters": {
            "S": S, "K": K, "T_avg": avg_T, "r": r, "sigma": round(sigma, 4),
            "user_specified_S_K": valuation_confidence == "high",
            "tech_risk_premium": risk_premium,
        },
        "technology_option_value": bs,
        "risk_adjusted_value": adjusted_value,
        "volatility": round(sigma, 4),
        "greeks": {
            "delta": bs["delta"],
            "gamma": bs["gamma"],
            "theta": bs["theta"],
            "vega": bs["vega"],
            "rho": bs["rho"],
        },
        "top_players": top_firms,
        "matched_clusters": len(cluster_ids),
    }
    if valuation_confidence != "high":
        result["note"] = (
            "S, Kは推定値を使用。実際の市場データがある場合はS, Kパラメータで指定可能。"
        )
    return result


# ─── Tool 2: tech_volatility ────────────────────────────────────────

def tech_volatility(
    store: PatentStore,
    resolver: EntityResolver | None = None,
    query: str = "",
    query_type: str | None = None,
    date_from: str = "2015-01-01",
    date_to: str = "2024-12-31",
) -> dict[str, Any]:
    """Technology volatility + decay curve + half-life.

    Returns sigma, drift, tech Sharpe, regime classification, annualized
    volatility, citation decay curve, half-life, and percentile ranking.
    """
    store._relax_timeout()
    conn = store._conn()

    year_from = int(date_from[:4])
    year_to = int(date_to[:4])
    q = query.strip()

    # Resolve query to CPC prefix
    if query_type == "firm" and resolver:
        firm_id = _resolve_firm(resolver, q)
        if not firm_id:
            return {"error": f"Could not resolve firm: '{q}'"}
        ftv = conn.execute(
            "SELECT dominant_cpc FROM firm_tech_vectors "
            "WHERE firm_id = ? ORDER BY year DESC LIMIT 1",
            (firm_id,),
        ).fetchone()
        cpc_prefix = (ftv["dominant_cpc"] or "H01M") if ftv else "H01M"
    else:
        cpc_prefix = q.upper().replace("-", "").replace(" ", "")
        if not cpc_prefix:
            return {"error": "query is required (CPC code or keyword)"}

    # Map keywords to CPC
    kw_match = _keyword_to_cpc(q)
    if kw_match:
        cpc_prefix = kw_match

    cpc4 = _cpc4(cpc_prefix)
    cluster_ids = _find_clusters_for_cpc(conn, cpc_prefix)

    # Get yearly counts
    counts = _get_yearly_counts_from_momentum(
        conn, cluster_ids if cluster_ids else [f"{cpc_prefix}_0"], year_from, year_to
    )
    if len(counts) < 3:
        return {
            "error": f"Insufficient data for '{cpc_prefix}' (need >=3 years)",
            "years_found": len(counts),
        }

    # Detect and flag incomplete final year data
    # 2024+ is known incomplete; also flag if count < 50% of previous year
    incomplete_year = None
    sorted_years = sorted(counts.keys())
    if len(sorted_years) >= 2:
        last_yr = sorted_years[-1]
        prev_yr = sorted_years[-2]
        if last_yr >= 2024 or counts[last_yr] < counts[prev_yr] * 0.5:
            incomplete_year = last_yr
            # Exclude incomplete year from volatility calculation
            counts_for_calc = {y: c for y, c in counts.items() if y != last_yr}
        else:
            counts_for_calc = counts
    else:
        counts_for_calc = counts

    # Log returns (using clean data without incomplete year)
    returns = _log_returns(counts_for_calc)
    sigma = float(np.std(returns)) if returns else 0
    drift = float(np.mean(returns)) if returns else 0
    tech_sharpe = drift / sigma if sigma > 1e-6 else 0

    # Annualized volatility (trading days equivalent for comparability)
    annualized_vol = sigma * math.sqrt(252)

    if drift > 0.05:
        regime = "growth"
    elif drift > -0.05:
        regime = "mature"
    else:
        regime = "declining"

    # Decay curve from citation_impact + patent filing dates (fast, no heavy JOINs)
    decay_curve = []
    half_life = None
    try:
        # Get a sample of cited patents in this CPC area with their citation counts
        # and filing years, then compute age-based decay
        # 2-step: get publication_numbers from patent_cpc, then lookup
        cpc_pubs = conn.execute(
            """SELECT publication_number FROM patent_cpc
            WHERE substr(cpc_code, 1, 4) = ? AND is_first = 1
            LIMIT 20000""",
            (cpc4,),
        ).fetchall()
        pub_ids = [r["publication_number"] for r in cpc_pubs]

        sample_rows = []
        if pub_ids:
            # Batch lookup in citation_impact + patents
            BATCH = 2000
            for i in range(0, min(len(pub_ids), 10000), BATCH):
                batch = pub_ids[i:i+BATCH]
                ph = ",".join("?" for _ in batch)
                rows = conn.execute(
                    f"""SELECT ci.citation_count,
                              CAST(p.filing_date / 10000 AS INTEGER) as filing_year
                    FROM citation_impact ci
                    JOIN patents p ON ci.publication_number = p.publication_number
                    WHERE ci.publication_number IN ({ph})
                      AND p.filing_date > 0
                      AND ci.citation_count > 0""",
                    batch,
                ).fetchall()
                sample_rows.extend(rows)

        if sample_rows:
            from collections import Counter
            # Compute age distribution of citations weighted by count
            current_yr = max(counts.keys()) if counts else 2023
            age_counts = Counter()
            for r in sample_rows:
                age = current_yr - r["filing_year"]
                if 0 <= age <= 30:
                    age_counts[age] += r["citation_count"]

            if age_counts:
                max_age = max(age_counts.keys())
                total_cit = sum(age_counts.values())
                cumulative = 0
                for age_yr in range(0, min(max_age + 1, 25)):
                    c = age_counts.get(age_yr, 0)
                    cumulative += c
                    decay_curve.append({
                        "lag_years": age_yr,
                        "citation_count": c,
                        "cumulative_pct": round(cumulative / total_cit, 4),
                    })
                    if half_life is None and cumulative >= total_cit * 0.5:
                        half_life = age_yr
    except Exception:
        half_life = 5  # Industry average

    # Timeline (includes all years, incomplete year flagged)
    timeline = []
    years = sorted(counts.keys())
    for i, y in enumerate(years):
        entry: dict[str, Any] = {"year": y, "patent_count": counts[y]}
        if i > 0 and counts[years[i - 1]] > 0:
            entry["log_return"] = round(
                math.log(counts[y] / counts[years[i - 1]]), 4
            )
        if incomplete_year and y == incomplete_year:
            entry["incomplete"] = True
            entry["note"] = "データ取り込み未完了のため件数が過少"
        timeline.append(entry)

    # Percentile vs all tech clusters (filtered: avg >= 100 patents/yr, >= 3 years)
    # Use same year range as our sigma (exclude incomplete years)
    percentile = 50
    try:
        compare_year_to = (year_to - 1) if incomplete_year else year_to
        all_momentum = conn.execute(
            "SELECT cluster_id, year, patent_count "
            "FROM tech_cluster_momentum "
            "WHERE year BETWEEN ? AND ? "
            "ORDER BY cluster_id, year",
            (year_from, compare_year_to),
        ).fetchall()
        cluster_data: dict[str, dict[int, int]] = {}
        for r in all_momentum:
            cid = r["cluster_id"]
            cluster_data.setdefault(cid, {})[r["year"]] = r["patent_count"]

        comparison_sigmas = []
        for cid, yearly in cluster_data.items():
            if len(yearly) < 3:
                continue
            avg_cnt = sum(yearly.values()) / len(yearly)
            if avg_cnt < 100:
                continue  # Skip tiny clusters (noisy log returns)
            rets = _log_returns(yearly)
            if len(rets) >= 2:
                comparison_sigmas.append(float(np.std(rets)))

        if comparison_sigmas and sigma > 0:
            percentile = round(
                sum(1 for s in comparison_sigmas if s < sigma)
                / len(comparison_sigmas) * 100
            )
    except Exception:
        percentile = 50  # Fallback

    # Build richer interpretation
    label = CPC_CLASS_JA.get(cpc4, cpc_prefix)
    vol_context = (
        "非常に高い（上位10%）" if percentile >= 90
        else "高い（上位25%）" if percentile >= 75
        else "平均的" if percentile >= 25
        else "低い（安定的）"
    )
    interp_parts = [
        f"{label}のボラティリティσ={sigma:.3f}（年率換算{annualized_vol:.1f}%）",
        f"ドリフト={drift:.3f}",
        f"{'成長' if regime == 'growth' else '成熟' if regime == 'mature' else '衰退'}段階",
        f"変動性は全技術中{vol_context}（{percentile}パーセンタイル）",
    ]
    if half_life is not None:
        interp_parts.append(f"被引用半減期{half_life}年")

    result = {
        "endpoint": "tech_volatility",
        "cpc_prefix": cpc_prefix,
        "cpc_label": label,
        "date_range": {"from": year_from, "to": year_to},
        "volatility": {
            "sigma": round(sigma, 4),
            "annualized_volatility": round(annualized_vol, 4),
            "drift": round(drift, 4),
            "tech_sharpe": round(tech_sharpe, 4),
            "regime": regime,
            "percentile_vs_all": percentile,
        },
        "timeline": timeline,
        "decay_curve": decay_curve[:20],
        "half_life_years": half_life,
        "interpretation": "。".join(interp_parts) + "。",
        "visualization_hint": {
            "recommended_chart": "dual_axis",
            "title": f"技術ボラティリティ: {cpc_prefix}",
            "axes": {
                "x": "timeline[].year",
                "y_left": "timeline[].patent_count",
                "y_right": "timeline[].log_return",
            },
        },
    }
    if incomplete_year:
        result["incomplete_year_note"] = (
            f"{incomplete_year}年のデータは取り込み未完了のため、"
            f"ボラティリティ計算から除外しています。"
        )
    return result


# ─── Tool 3: portfolio_var ──────────────────────────────────────────

def portfolio_var(
    store: PatentStore,
    resolver: EntityResolver,
    firm: str,
    horizon_years: int = 5,
    confidence: float = 0.95,
    year: int = 2024,
) -> dict[str, Any]:
    """Portfolio Value-at-Risk with 5 risk factors, Monte Carlo, and stress scenarios.

    Risk factors:
      1. Expiration (patents reaching 20-year limit)
      2. Obsolescence (technology area declining)
      3. Invalidation (crowded technology, challenge probability)
      4. Design-around (competitors with high startability)
      5. Concentration (portfolio HHI / low diversity)

    Uses ONLY fast-path tables: firm_tech_vectors, startability_surface,
    tech_cluster_momentum, tech_clusters.  Patent-level sampling is capped
    at 500 rows.
    """
    store._relax_timeout()
    conn = store._conn()

    firm_id = _resolve_firm(resolver, firm)
    if not firm_id:
        return {"error": f"Could not resolve firm: '{firm}'"}
    # DB fallback: check if firm_id exists, try company_XXXX
    _resolved = resolver.resolve(firm, country_hint="JP") if resolver else None
    if _resolved:
        firm_id = _resolve_firm_id_db(_resolved, conn, "firm_tech_vectors")

    actual_year = _find_best_year(conn, firm_id)

    # ── Firm-level data from fast-path tables ──────────────────────

    ftv = conn.execute(
        "SELECT patent_count, dominant_cpc, tech_diversity, tech_vector "
        "FROM firm_tech_vectors WHERE firm_id = ? AND year = ?",
        (firm_id, actual_year),
    ).fetchone()
    total_patents = (ftv["patent_count"] or 0) if ftv else 0
    tech_diversity = (ftv["tech_diversity"] or 0) if ftv else 0
    dominant_cpc = _cpc4((ftv["dominant_cpc"] or "H01M") if ftv else "H01M")

    if total_patents == 0:
        # Fallback: try LIKE match for parent/subsidiary
        like_row = conn.execute(
            "SELECT firm_id, patent_count, dominant_cpc, tech_diversity, tech_vector "
            "FROM firm_tech_vectors WHERE firm_id LIKE ? AND year = ? "
            "ORDER BY patent_count DESC LIMIT 1",
            (f"{firm_id}%", actual_year),
        ).fetchone()
        if like_row:
            firm_id = like_row["firm_id"]
            total_patents = like_row["patent_count"] or 0
            tech_diversity = like_row["tech_diversity"] or 0
            dominant_cpc = _cpc4((like_row["dominant_cpc"] or "H01M"))
            ftv = like_row
        if total_patents == 0:
            return {"error": f"No patent data in firm_tech_vectors for: '{firm_id}'"}

    # ── Startability surface data for this firm ────────────────────

    ss_rows = conn.execute(
        "SELECT cluster_id, score, gate_open FROM startability_surface "
        "WHERE firm_id = ? AND year = ? ORDER BY score DESC",
        (firm_id, actual_year),
    ).fetchall()

    firm_clusters = [r["cluster_id"] for r in ss_rows]
    firm_cluster_scores = {r["cluster_id"]: r["score"] for r in ss_rows}

    # ── Sample patents for expiration analysis (LIMIT 500) ─────────

    pat_rows = conn.execute(
        "SELECT pa.publication_number, p.filing_date, p.citation_count_forward, "
        "  (SELECT cpc_code FROM patent_cpc "
        "   WHERE publication_number = pa.publication_number LIMIT 1) as cpc "
        "FROM patent_assignees pa "
        "JOIN patents p ON pa.publication_number = p.publication_number "
        "WHERE pa.firm_id = ? AND p.filing_date IS NOT NULL AND p.filing_date > 0 "
        "ORDER BY p.citation_count_forward DESC "
        "LIMIT 500",
        (firm_id,),
    ).fetchall()

    sample_size = len(pat_rows)

    # ── FACTOR 1: Expiration Risk ──────────────────────────────────

    expiring_in_horizon = []
    active_count = 0
    remaining_dist: dict[int, int] = {}
    total_citation_weight = 0
    expiring_citation_weight = 0
    cpc_expiring: dict[str, int] = {}
    cpc_total: dict[str, int] = {}

    for row in pat_rows:
        fd = row["filing_date"]
        fy = int(str(fd)[:4]) if fd else year - 10
        remaining = max(0, 20 - (year - fy))
        cpc4 = _cpc4(row["cpc"] or "")
        citations = row["citation_count_forward"] or 0
        cite_weight = 1 + math.log(1 + citations)

        bucket = min(remaining, 20)
        remaining_dist[bucket] = remaining_dist.get(bucket, 0) + 1

        if cpc4:
            cpc_total[cpc4] = cpc_total.get(cpc4, 0) + 1

        if remaining > 0:
            active_count += 1
            total_citation_weight += cite_weight
            if remaining <= horizon_years:
                expiring_citation_weight += cite_weight
                expiring_in_horizon.append({
                    "patent": row["publication_number"],
                    "remaining_years": remaining,
                    "cpc": cpc4,
                    "citations": citations,
                })
                if cpc4:
                    cpc_expiring[cpc4] = cpc_expiring.get(cpc4, 0) + 1

    expiration_exposure = (
        expiring_citation_weight / total_citation_weight
        if total_citation_weight > 0 else 0
    )

    # Top expiring CPC areas
    top_expiring_cpc = []
    for cpc4, exp_cnt in sorted(cpc_expiring.items(), key=lambda x: x[1], reverse=True)[:5]:
        t = cpc_total.get(cpc4, 1)
        top_expiring_cpc.append({
            "cpc": cpc4,
            "label": CPC_CLASS_JA.get(cpc4, ""),
            "expiring": exp_cnt,
            "total": t,
            "loss_rate": round(exp_cnt / t, 3),
        })

    # ── FACTOR 2: Obsolescence Risk ────────────────────────────────

    declining_clusters = []
    obsolescence_weighted = 0.0
    total_cluster_weight = 0.0

    for cid in firm_clusters:
        gr = _cluster_growth_rate(conn, cid)
        score = firm_cluster_scores.get(cid, 0)
        total_cluster_weight += score
        if gr is not None and gr < -0.1:
            declining_clusters.append({
                "cluster_id": cid,
                "cpc": _cpc4(cid),
                "label": CPC_CLASS_JA.get(_cpc4(cid), ""),
                "growth_rate": round(gr, 4),
                "firm_score": round(score, 3),
            })
            obsolescence_weighted += score

    obsolescence_exposure = (
        obsolescence_weighted / total_cluster_weight
        if total_cluster_weight > 0 else 0
    )

    # ── FACTOR 3: Invalidation Risk ────────────────────────────────

    dense_areas = []
    invalidation_weighted = 0.0

    for cid in firm_clusters[:20]:
        cpc4_c = _cpc4(cid)
        tc_row = conn.execute(
            "SELECT patent_count FROM tech_clusters WHERE cluster_id = ?",
            (cid,),
        ).fetchone()
        if tc_row and tc_row["patent_count"]:
            density = tc_row["patent_count"]
            # Calibrate invalidation probability: denser clusters have higher risk
            # 0-1000 patents: ~2%, 1000-5000: ~5%, 5000-20000: ~8%, 20000+: ~12%
            if density > 20000:
                inv_prob = 0.12
            elif density > 5000:
                inv_prob = 0.08
            elif density > 1000:
                inv_prob = 0.05
            else:
                inv_prob = 0.02

            score = firm_cluster_scores.get(cid, 0)
            invalidation_weighted += score * inv_prob
            if density > 5000:
                dense_areas.append({
                    "cluster_id": cid,
                    "cpc": cpc4_c,
                    "label": CPC_CLASS_JA.get(cpc4_c, ""),
                    "patent_count": density,
                    "invalidation_probability": inv_prob,
                })

    invalidation_exposure = (
        invalidation_weighted / total_cluster_weight
        if total_cluster_weight > 0 else 0
    )
    dense_areas.sort(key=lambda x: x["patent_count"], reverse=True)

    # ── FACTOR 4: Design-Around Risk ───────────────────────────────

    design_around_competitors: dict[str, float] = {}
    design_around_weighted = 0.0

    for cid in firm_clusters[:15]:
        comp_rows = conn.execute(
            "SELECT firm_id, score FROM startability_surface "
            "WHERE cluster_id = ? AND year = ? AND firm_id != ? AND gate_open = 1 "
            "ORDER BY score DESC LIMIT 5",
            (cid, actual_year, firm_id),
        ).fetchall()
        n_competitors = len(comp_rows)
        firm_score = firm_cluster_scores.get(cid, 0)
        if n_competitors > 0:
            # More competitors with gate_open = higher risk
            risk_mult = min(1.0, n_competitors * 0.15)
            design_around_weighted += firm_score * risk_mult
            for cr in comp_rows:
                cf = cr["firm_id"]
                if cf not in design_around_competitors:
                    design_around_competitors[cf] = 0
                design_around_competitors[cf] += cr["score"]

    design_around_exposure = (
        design_around_weighted / total_cluster_weight
        if total_cluster_weight > 0 else 0
    )

    top_competitors = sorted(
        [{"firm_id": k, "aggregate_score": round(v, 3)}
         for k, v in design_around_competitors.items()],
        key=lambda x: x["aggregate_score"], reverse=True,
    )[:5]

    # ── FACTOR 5: Concentration Risk ───────────────────────────────

    # HHI from CPC distribution
    cpc_shares = {}
    for cpc4_key, cnt in cpc_total.items():
        cpc_shares[cpc4_key] = cnt
    total_cpc_cnt = sum(cpc_shares.values()) or 1
    hhi = sum((cnt / total_cpc_cnt) ** 2 for cnt in cpc_shares.values())

    # Also use tech_diversity (entropy) from firm_tech_vectors
    # Entropy 0-6 range; normalize by dividing by 5.0 (as per MEMORY.md)
    normalized_diversity = min(1.0, tech_diversity / 5.0)
    concentration_exposure = max(0, 1.0 - normalized_diversity) * 0.5 + hhi * 0.5

    if hhi > 0.25:
        concentration_severity = "high"
    elif hhi > 0.15:
        concentration_severity = "medium"
    else:
        concentration_severity = "low"

    # ── Monte Carlo Simulation ─────────────────────────────────────

    n_simulations = 1000
    rng = np.random.default_rng(42)  # reproducible
    mc_results = np.empty(n_simulations)

    for i in range(n_simulations):
        exp_loss = rng.beta(2, 8) * expiration_exposure
        obs_loss = rng.beta(2, 10) * obsolescence_exposure
        inv_loss = rng.beta(1, 20) * invalidation_exposure
        da_loss = rng.beta(2, 15) * design_around_exposure
        conc_loss = rng.beta(2, 10) * concentration_exposure
        mc_results[i] = exp_loss + obs_loss + inv_loss + da_loss + conc_loss

    var_95 = float(np.percentile(mc_results, 95))
    var_99 = float(np.percentile(mc_results, 99))
    cvar_95 = float(np.mean(mc_results[mc_results >= var_95])) if np.any(mc_results >= var_95) else var_95

    loss_dist = {
        f"p{p}": round(float(np.percentile(mc_results, p)), 4)
        for p in [10, 25, 50, 75, 90]
    }

    # ── Stress Scenarios ───────────────────────────────────────────

    def _run_stress(multipliers: dict[str, float], label: str, desc: str) -> dict:
        """Run a single stress scenario with factor multipliers."""
        stressed = np.empty(n_simulations)
        for i in range(n_simulations):
            exp_l = rng.beta(2, 8) * expiration_exposure * multipliers.get("expiration", 1)
            obs_l = rng.beta(2, 10) * obsolescence_exposure * multipliers.get("obsolescence", 1)
            inv_l = rng.beta(1, 20) * invalidation_exposure * multipliers.get("invalidation", 1)
            da_l = rng.beta(2, 15) * design_around_exposure * multipliers.get("design_around", 1)
            conc_l = rng.beta(2, 10) * concentration_exposure * multipliers.get("concentration", 1)
            stressed[i] = exp_l + obs_l + inv_l + da_l + conc_l
        s_var = float(np.percentile(stressed, 95))
        s_cvar = float(np.mean(stressed[stressed >= s_var])) if np.any(stressed >= s_var) else s_var
        return {
            "scenario": label,
            "description": desc,
            "multipliers": multipliers,
            "var_95": round(s_var, 4),
            "cvar_95": round(s_cvar, 4),
            "increase_vs_base": round(s_var / max(var_95, 1e-6) - 1, 3) if var_95 > 0 else 0,
        }

    stress_scenarios = [
        _run_stress(
            {"obsolescence": 3.0, "design_around": 2.0},
            "tech_disruption",
            "技術的破壊（代替技術の急速な台頭により陳腐化リスクが3倍、回避設計リスクが2倍に拡大）",
        ),
        _run_stress(
            {"expiration": 2.0, "concentration": 1.5},
            "patent_cliff",
            "特許の崖（主要特許の集中満了により満了リスクが2倍、集中リスクが1.5倍に拡大）",
        ),
        _run_stress(
            {"design_around": 3.0, "invalidation": 2.0},
            "aggressive_competitor",
            "攻撃的競合（競合の参入加速により回避設計リスクが3倍、無効化リスクが2倍に拡大）",
        ),
    ]

    # ── Option-value-weighted VaR ──────────────────────────────────

    sigma = _get_cpc_volatility(conn, dominant_cpc)
    option_val_at_risk = 0.0
    for exp in expiring_in_horizon[:200]:
        T = exp["remaining_years"]
        S, K_val, _ = _estimate_S_K(conn, exp["cpc"] or dominant_cpc, None, None)
        bs = _black_scholes_call(S, K_val, T, 0.02, sigma)
        mult = 1 + math.log(1 + exp["citations"]) / 5
        option_val_at_risk += bs["value"] * mult

    # ── High-value expiring patents ────────────────────────────────

    high_value_expiring = sorted(
        [e for e in expiring_in_horizon if e["citations"] > 0],
        key=lambda x: x["citations"], reverse=True,
    )[:10]

    # ── Interpretation ─────────────────────────────────────────────

    risk_factors_summary = sorted(
        [
            ("満了リスク", expiration_exposure),
            ("陳腐化リスク", obsolescence_exposure),
            ("無効化リスク", invalidation_exposure),
            ("回避設計リスク", design_around_exposure),
            ("集中リスク", concentration_exposure),
        ],
        key=lambda x: x[1], reverse=True,
    )
    top_risk_name = risk_factors_summary[0][0]
    interp = (
        f"{firm_id}の特許ポートフォリオVaR分析: "
        f"5リスク因子のMonte Carlo推定によるVaR(95%)={var_95:.3f}, CVaR={cvar_95:.3f}。"
        f"最大リスク因子は{top_risk_name}（エクスポージャ{risk_factors_summary[0][1]:.1%}）。"
        f"{horizon_years}年以内にサンプル{active_count}件中{len(expiring_in_horizon)}件が満了予定。"
    )

    return {
        "endpoint": "portfolio_var",
        "firm_id": firm_id,
        "year": actual_year,
        "horizon_years": horizon_years,
        "confidence": confidence,
        "portfolio_stats": {
            "total_patents_estimated": total_patents,
            "sample_analyzed": sample_size,
            "active_in_sample": active_count,
            "dominant_cpc": dominant_cpc,
            "tech_diversity_entropy": round(tech_diversity, 3),
        },
        "risk_factors": {
            "expiration": {
                "exposure_pct": round(expiration_exposure, 4),
                "expiring_count": len(expiring_in_horizon),
                "severity": (
                    "high" if expiration_exposure > 0.3
                    else "medium" if expiration_exposure > 0.15
                    else "low"
                ),
                "top_expiring_cpc": top_expiring_cpc,
            },
            "obsolescence": {
                "exposure_pct": round(obsolescence_exposure, 4),
                "severity": (
                    "high" if obsolescence_exposure > 0.2
                    else "medium" if obsolescence_exposure > 0.1
                    else "low"
                ),
                "top_declining_clusters": declining_clusters[:5],
            },
            "invalidation": {
                "exposure_pct": round(invalidation_exposure, 4),
                "severity": (
                    "high" if invalidation_exposure > 0.08
                    else "medium" if invalidation_exposure > 0.04
                    else "low"
                ),
                "dense_areas": dense_areas[:5],
            },
            "design_around": {
                "exposure_pct": round(design_around_exposure, 4),
                "severity": (
                    "high" if design_around_exposure > 0.25
                    else "medium" if design_around_exposure > 0.12
                    else "low"
                ),
                "top_competitors": top_competitors,
            },
            "concentration": {
                "hhi": round(hhi, 4),
                "tech_diversity_normalized": round(normalized_diversity, 4),
                "exposure_pct": round(concentration_exposure, 4),
                "severity": concentration_severity,
            },
        },
        "monte_carlo": {
            "n_simulations": n_simulations,
            "var_95": round(var_95, 4),
            "cvar_95": round(cvar_95, 4),
            "var_99": round(var_99, 4),
            "loss_distribution": loss_dist,
            "mean_loss": round(float(np.mean(mc_results)), 4),
        },
        "stress_scenarios": stress_scenarios,
        "option_value_at_risk": round(option_val_at_risk, 2),
        "remaining_years_distribution": {
            str(k): remaining_dist.get(k, 0)
            for k in range(0, 21)
        },
        "high_value_expiring": high_value_expiring,
        "interpretation": interp,
        "disclaimer": (
            "VaRは過去データに基づく統計的推定であり、将来のリスクを保証するものではありません。"
            "Monte Carloシミュレーションのパラメータはヒューリスティックに設定されています。"
        ),
    }


# ─── Tool 4: tech_beta ──────────────────────────────────────────────

def tech_beta(
    store: PatentStore,
    resolver: EntityResolver | None = None,
    query: str = "",
    query_type: str | None = None,
    benchmark: str = "all",
    date_from: str = "2015-01-01",
    date_to: str = "2024-12-31",
) -> dict[str, Any]:
    """CAPM-style technology beta with rolling window and stability assessment.

    Returns beta, alpha, R-squared, rolling beta series, stability
    assessment, expected return (CAPM), classification, and peer comparison.
    """
    store._relax_timeout()
    conn = store._conn()

    year_from = int(date_from[:4])
    year_to = int(date_to[:4])
    q = query.strip()

    # Resolve to CPC prefix(es)
    if query_type == "firm" or (resolver and q and (not q[:1].isascii() or not q[:1].isalpha())):
        firm_id = _resolve_firm(resolver, q) if resolver else None
        if firm_id:
            ftv = conn.execute(
                "SELECT dominant_cpc FROM firm_tech_vectors "
                "WHERE firm_id = ? ORDER BY year DESC LIMIT 1",
                (firm_id,),
            ).fetchone()
            cpc_prefix = (ftv["dominant_cpc"] or "H01M") if ftv else "H01M"
            cpc_prefix = _cpc4(cpc_prefix)
            query_type = "firm"
        else:
            cpc_prefix = q.upper()[:4]
            query_type = "technology"
    else:
        cpc_prefix = q.upper().replace("-", "").replace(" ", "")[:4]
        query_type = query_type or "technology"

    cpc4 = _cpc4(cpc_prefix) if cpc_prefix else ""
    cluster_ids = _find_clusters_for_cpc(conn, cpc_prefix)

    # Get tech-specific yearly counts
    tech_counts = _get_yearly_counts_from_momentum(
        conn, cluster_ids if cluster_ids else [f"{cpc_prefix}_0"], year_from, year_to
    )

    # Get market benchmark counts
    if benchmark == "section":
        section = cpc_prefix[0] if cpc_prefix else ""
        section_clusters = conn.execute(
            "SELECT cluster_id FROM tech_clusters WHERE cpc_class LIKE ? LIMIT 200",
            (f"{section}%",),
        ).fetchall()
        section_ids = [r["cluster_id"] for r in section_clusters]
        market_counts = _get_yearly_counts_from_momentum(
            conn, section_ids, year_from, year_to
        )
        benchmark_label = f"CPC Section {section}"
    else:
        market_counts = _get_total_market_counts(conn, year_from, year_to)
        benchmark_label = "全技術市場"

    # Compute returns
    tech_returns = _log_returns(tech_counts)
    market_returns = _log_returns(market_counts)

    # Align lengths
    min_len = min(len(tech_returns), len(market_returns))
    if min_len < 3:
        return {
            "error": (
                f"Insufficient data for beta calculation "
                f"(need >=3 years of returns, got {min_len})"
            ),
        }

    tr = np.array(tech_returns[:min_len])
    mr = np.array(market_returns[:min_len])

    # Full-period beta calculation
    cov_matrix = np.cov(tr, mr)
    beta = (
        float(cov_matrix[0, 1] / cov_matrix[1, 1])
        if cov_matrix[1, 1] > 1e-12 else 1.0
    )
    alpha = float(np.mean(tr) - beta * np.mean(mr))
    r_squared = (
        float(np.corrcoef(tr, mr)[0, 1] ** 2) if len(tr) > 1 else 0
    )

    # ── Rolling Beta (3-year window) ───────────────────────────────

    all_years = sorted(set(tech_counts.keys()) & set(market_counts.keys()))
    rolling_window = 3
    rolling_betas_list = []

    if len(all_years) >= rolling_window + 1:
        for start_idx in range(len(all_years) - rolling_window):
            window_years = all_years[start_idx: start_idx + rolling_window + 1]
            w_tech = {y: tech_counts[y] for y in window_years if y in tech_counts}
            w_mkt = {y: market_counts[y] for y in window_years if y in market_counts}

            w_tr = _log_returns(w_tech)
            w_mr = _log_returns(w_mkt)
            w_min = min(len(w_tr), len(w_mr))

            if w_min >= 2:
                w_tr_arr = np.array(w_tr[:w_min])
                w_mr_arr = np.array(w_mr[:w_min])
                w_cov = np.cov(w_tr_arr, w_mr_arr)
                w_beta = (
                    float(w_cov[0, 1] / w_cov[1, 1])
                    if w_cov[1, 1] > 1e-12 else 1.0
                )
                w_alpha = float(np.mean(w_tr_arr) - w_beta * np.mean(w_mr_arr))
                rolling_betas_list.append({
                    "period": f"{window_years[0]}-{window_years[-1]}",
                    "beta": round(w_beta, 4),
                    "alpha": round(w_alpha, 4),
                })

    # Stability assessment from rolling betas
    if len(rolling_betas_list) >= 2:
        rb_values = [rb["beta"] for rb in rolling_betas_list]
        rb_std = float(np.std(rb_values))
        if rb_std < 0.3:
            stability = "stable"
            stability_label = "安定（時期による変動が小さい）"
        elif rb_std < 0.5:
            stability = "moderate"
            stability_label = "やや変動あり"
        else:
            stability = "volatile"
            stability_label = "不安定（時期による変動が大きい）"
    else:
        rb_std = 0.0
        stability = "insufficient_data"
        stability_label = "ローリングベータ算出に十分なデータなし"

    # ── CAPM Expected Return ───────────────────────────────────────

    risk_free = 0.02
    market_premium = 0.05
    expected_return = risk_free + beta * market_premium

    # ── Classification ─────────────────────────────────────────────

    if beta > 1.2 and alpha > 0.02:
        classification = "high_beta_high_alpha"
        class_label = "成長技術（市場超過リターン）"
    elif beta > 1.2:
        classification = "high_beta_low_alpha"
        class_label = "景気敏感技術"
    elif alpha > 0.02:
        classification = "low_beta_high_alpha"
        class_label = "独自成長技術（ニッチ）"
    else:
        classification = "low_beta_low_alpha"
        class_label = "成熟技術"

    # ── Peer comparison (same section) ─────────────────────────────

    section = cpc_prefix[0] if cpc_prefix else ""
    peer_clusters = conn.execute(
        "SELECT DISTINCT cpc_class FROM tech_clusters "
        "WHERE cpc_class LIKE ? AND cpc_class != ? LIMIT 10",
        (f"{section}%", cpc4),
    ).fetchall()
    peers = []
    for pc in peer_clusters[:5]:
        peer_cpc = pc["cpc_class"]
        p_clusters = _find_clusters_for_cpc(conn, peer_cpc)
        p_counts = _get_yearly_counts_from_momentum(conn, p_clusters, year_from, year_to)
        p_returns = _log_returns(p_counts)
        if len(p_returns) >= min_len:
            pr = np.array(p_returns[:min_len])
            p_cov = np.cov(pr, mr)
            p_beta = (
                float(p_cov[0, 1] / p_cov[1, 1])
                if p_cov[1, 1] > 1e-12 else 1.0
            )
            p_alpha = float(np.mean(pr) - p_beta * np.mean(mr))
            # Peer classification
            if p_beta > 1.2 and p_alpha > 0.02:
                p_class = "成長"
            elif p_beta > 1.2:
                p_class = "景気敏感"
            elif p_alpha > 0.02:
                p_class = "ニッチ"
            else:
                p_class = "成熟"
            peers.append({
                "cpc": peer_cpc,
                "label": CPC_CLASS_JA.get(peer_cpc, ""),
                "beta": round(p_beta, 4),
                "alpha": round(p_alpha, 4),
                "classification": p_class,
            })

    # Sort peers by beta for easy comparison
    peers.sort(key=lambda x: x["beta"], reverse=True)

    # ── Interpretation ─────────────────────────────────────────────

    label = CPC_CLASS_JA.get(cpc4, cpc_prefix)
    interp = (
        f"{label}のβ={beta:.3f}, α={alpha:.3f}, R²={r_squared:.3f}。"
        f"分類: {class_label}。"
        f"CAPM期待リターン={expected_return:.1%}。"
        f"{'市場平均より高いボラティリティ。' if beta > 1 else '市場平均より安定的。'}"
        f"ローリングベータ安定性: {stability_label}。"
    )

    return {
        "endpoint": "tech_beta",
        "query": q,
        "query_type": query_type,
        "cpc_prefix": cpc_prefix,
        "cpc_label": label,
        "benchmark": benchmark_label,
        "date_range": {"from": year_from, "to": year_to},
        "beta": round(beta, 4),
        "alpha": round(alpha, 4),
        "r_squared": round(r_squared, 4),
        "classification": classification,
        "classification_label": class_label,
        "capm": {
            "risk_free_rate": risk_free,
            "market_premium": market_premium,
            "expected_return": round(expected_return, 4),
        },
        "rolling_beta": {
            "window_years": rolling_window,
            "series": rolling_betas_list,
            "std_of_betas": round(rb_std, 4),
            "stability": stability,
            "stability_label": stability_label,
        },
        "tech_stats": {
            "mean_return": round(float(np.mean(tr)), 4),
            "std_return": round(float(np.std(tr)), 4),
            "num_years": min_len + 1,
        },
        "market_stats": {
            "mean_return": round(float(np.mean(mr)), 4),
            "std_return": round(float(np.std(mr)), 4),
        },
        "peer_comparison": peers,
        "interpretation": interp,
        "visualization_hint": {
            "recommended_chart": "scatter",
            "title": f"技術ベータ: {cpc_prefix} vs {benchmark_label}",
            "axes": {
                "x": "market_return",
                "y": "tech_return",
                "annotation": "beta regression line",
            },
        },
    }
