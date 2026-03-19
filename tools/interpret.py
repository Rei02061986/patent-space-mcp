"""Interpretation generator for MCP tool responses.

Adds human-readable interpretation text to tool results,
transforming raw numbers into actionable business insights.
"""
from __future__ import annotations
from typing import Any


def _startability_interpret(r: dict) -> str:
    score = r.get("score", 0)
    firm = r.get("firm_id", "the firm")
    cluster = r.get("cluster_id", "this technology")
    if score >= 0.8:
        return f"{firm} has very high readiness (score={score:.3f}) for {cluster}. The firm already possesses strong technological foundations in this area and could compete effectively with minimal additional investment."
    elif score >= 0.6:
        return f"{firm} has moderate readiness (score={score:.3f}) for {cluster}. The firm has relevant adjacent capabilities but would need targeted R&D investment to become competitive."
    elif score >= 0.3:
        return f"{firm} has limited readiness (score={score:.3f}) for {cluster}. Significant capability gaps exist. Consider partnerships, acquisitions, or licensing rather than organic entry."
    else:
        return f"{firm} has low readiness (score={score:.3f}) for {cluster}. This technology area is distant from the firm's current portfolio. Entry would require substantial strategic commitment."


def _adversarial_interpret(r: dict) -> str:
    ov = r.get("overview", {})
    fa = ov.get("firm_a", {}).get("name", "Firm A")
    fb = ov.get("firm_b", {}).get("name", "Firm B")
    sim = ov.get("tech_cosine_similarity", 0)
    contested = ov.get("contested_clusters", 0)
    total = ov.get("total_clusters_analyzed", 1)
    pct = contested * 100 // max(total, 1)
    np_a = ov.get("negotiation_power", {}).get("firm_a", 0.5)
    if sim > 0.9:
        overlap_text = f"extremely high portfolio overlap (cosine similarity={sim:.3f})"
    elif sim > 0.7:
        overlap_text = f"significant portfolio overlap (cosine similarity={sim:.3f})"
    else:
        overlap_text = f"moderate portfolio differentiation (cosine similarity={sim:.3f})"
    power_text = f"{fa} holds {'stronger' if np_a > 0.5 else 'weaker'} negotiation leverage ({np_a:.1%})"
    return f"{fa} and {fb} show {overlap_text}, contesting {contested}/{total} clusters ({pct}%). {power_text}. Consider the strategic scenarios for attack, defend, or preempt opportunities."


def _tech_gap_interpret(r: dict) -> str:
    fa = r.get("firm_a", {}).get("name", "Firm A")
    fb = r.get("firm_b", {}).get("name", "Firm B")
    fit = r.get("acquisition_fit", "unknown")
    synergy = r.get("synergy_score", 0)
    overlap = r.get("overlap_score", 0)
    if fit == "high_synergy":
        return f"{fa} and {fb} are highly complementary (synergy={synergy:.2f}). An M&A or partnership would fill mutual technology gaps effectively."
    elif fit == "high_overlap":
        return f"{fa} and {fb} have high overlap (overlap={overlap:.2f}). Consolidation could strengthen market position but offers limited diversification."
    else:
        return f"{fa} and {fb} show mixed complementarity (synergy={synergy:.2f}, overlap={overlap:.2f}). Selective technology licensing may be more effective than full acquisition."


def _patent_valuation_interpret(r: dict) -> str:
    tier = r.get("value_tier", "unknown")
    score = r.get("overall_score", 0)
    firm = r.get("firm_id", r.get("query", ""))
    if tier in ("premium", "high"):
        return f"Patent portfolio rated '{tier}' (score={score:.2f}). High citation impact and broad technology coverage indicate strong IP position suitable for licensing or defensive use."
    elif tier in ("medium", "standard"):
        return f"Patent portfolio rated '{tier}' (score={score:.2f}). Moderate IP strength. Consider targeted prosecution in high-value technology areas to improve portfolio quality."
    else:
        return f"Patent portfolio rated '{tier}' (score={score:.2f}). Limited IP strength relative to peers. Recommend reviewing prosecution strategy and considering strategic acquisitions."


def _tech_beta_interpret(r: dict) -> str:
    beta = r.get("beta", 1.0)
    cls = r.get("classification", "")
    query = r.get("query", "")
    if beta > 1.5:
        return f"Technology area '{query}' has high beta ({beta:.2f}), indicating it is more volatile than the overall patent market. Growth potential is high but with corresponding risk."
    elif beta > 1.0:
        return f"Technology area '{query}' has moderate-high beta ({beta:.2f}), growing slightly faster than the market. A balanced risk-return profile suitable for core portfolio allocation."
    elif beta > 0.5:
        return f"Technology area '{query}' has low beta ({beta:.2f}), indicating stable, mature technology with limited growth but lower risk."
    else:
        return f"Technology area '{query}' has very low beta ({beta:.2f}), suggesting a niche or declining area. Consider reallocation unless the technology serves a specific strategic purpose."


def _portfolio_var_interpret(r: dict) -> str:
    var_val = r.get("var_95_pct", r.get("var_pct", 0))
    firm = r.get("firm_id", "the firm")
    horizon = r.get("horizon_years", 5)
    if var_val and var_val > 30:
        return f"{firm} faces significant patent expiration risk: {var_val:.1f}% of portfolio value at risk over {horizon} years at 95% confidence. Prioritize renewal filings and defensive prosecution in expiring technology areas."
    elif var_val and var_val > 15:
        return f"{firm} has moderate patent expiration risk: {var_val:.1f}% of portfolio value at risk over {horizon} years. Standard portfolio maintenance recommended."
    else:
        return f"{firm} has low patent expiration risk ({var_val or 0:.1f}% VaR over {horizon} years). Portfolio is well-distributed across expiration dates."


def _fto_interpret(r: dict) -> str:
    risk = r.get("risk_assessment", {})
    level = risk.get("overall_risk", "unknown")
    blocking = len(r.get("blocking_patents", []))
    if level == "high":
        return f"FTO risk is HIGH with {blocking} potential blocking patents identified. Professional patent attorney review is strongly recommended before proceeding. Consider design-around strategies or licensing."
    elif level == "medium":
        return f"FTO risk is MEDIUM with {blocking} potential blocking patents. Some freedom-to-operate concerns exist. Monitor closely and consider obtaining formal FTO opinion."
    else:
        return f"FTO risk is LOW ({blocking} blocking patents found). The technology space appears relatively clear, but this assessment is based on metadata analysis. Formal patent search is recommended for critical decisions."


def _similar_firms_interpret(r: dict) -> str:
    firms = r.get("similar_firms", r.get("results", []))
    query = r.get("query_firm", r.get("firm_id", ""))
    if firms:
        top = firms[0] if isinstance(firms[0], dict) else {}
        top_name = top.get("firm_name", top.get("name", "unknown"))
        sim = top.get("similarity", top.get("cosine_similarity", 0))
        return f"Most similar firm to {query} is {top_name} (similarity={sim:.3f}). {len(firms)} peer firms identified based on patent portfolio technology vector cosine similarity."
    return f"No similar firms found for {query} in the database."


def _tech_landscape_interpret(r: dict) -> str:
    total = r.get("total_patents", 0)
    top_apps = r.get("top_applicants", [])
    leader = top_apps[0].get("name", "unknown") if top_apps else "N/A"
    return f"Technology landscape: {total:,} patents filed. Market leader: {leader}. See top applicants and CPC sub-distribution for detailed competitive positioning."


def _invention_intel_interpret(r: dict) -> str:
    risk = r.get("fto_risk", r.get("risk_level", "unknown"))
    prior = len(r.get("prior_art", r.get("similar_patents", [])))
    ws = len(r.get("whitespace", r.get("whitespace_opportunities", [])))
    return f"Invention analysis: {prior} prior art references found, FTO risk={risk}, {ws} whitespace opportunities identified. Review prior art carefully before filing."




def _bayesian_interpret(r: dict) -> str:
    mode = r.get("mode", "init")
    if mode == "init":
        params = r.get("parameters", r.get("priors", {}))
        return f"Bayesian scenario initialized with data-driven priors. {len(params) if isinstance(params, (list, dict)) else 0} parameters estimated from patent/market data. Use 'update' mode to incorporate your private information, then 'simulate' for Monte Carlo NPV analysis."
    elif mode == "simulate":
        npv = r.get("expected_npv", r.get("mean_npv", 0))
        prob = r.get("probability_positive", 0)
        return f"Simulation complete. Expected NPV: {npv:,.0f}万円, probability of positive return: {prob:.1%}. Review sensitivity analysis to identify key value drivers."
    return "Bayesian scenario analysis ready. Use init → update → simulate workflow."


def _ma_target_interpret(r: dict) -> str:
    targets = r.get("targets", r.get("recommendations", []))
    strategy = r.get("strategy", "tech_gap")
    n = len(targets) if isinstance(targets, list) else 0
    return f"M&A target analysis ({strategy} strategy): {n} candidates identified, ranked by technology complementarity and acquisition fit. Review technology overlap and synergy scores for each target."


def _portfolio_evolution_interpret(r: dict) -> str:
    firm = r.get("firm_id", "the firm")
    shifts = r.get("strategic_shifts", r.get("shifts", []))
    n_shifts = len(shifts) if isinstance(shifts, list) else 0
    return f"Portfolio evolution for {firm}: {n_shifts} strategic shifts detected over the analysis period. Review year-by-year CPC distribution changes and diversity trends."


def _ip_due_diligence_interpret(r: dict) -> str:
    firm = r.get("target_firm", r.get("firm_id", ""))
    score = r.get("overall_ip_score", r.get("ip_score", 0))
    recommendation = r.get("recommendation", "")
    if score and score > 0.7:
        return f"IP due diligence for {firm}: Strong IP position (score={score:.2f}). {recommendation or 'Technology moat appears robust for investment consideration.'}"
    elif score and score > 0.4:
        return f"IP due diligence for {firm}: Moderate IP position (score={score:.2f}). {recommendation or 'Some IP gaps to address. Consider IP strengthening as post-investment priority.'}"
    return f"IP due diligence for {firm}: {recommendation or 'Review detailed IP metrics and competitive positioning.'}"


def _cross_domain_interpret(r: dict) -> str:
    discoveries = r.get("discoveries", r.get("cross_domain_clusters", []))
    n = len(discoveries) if isinstance(discoveries, list) else 0
    source = r.get("source", {})
    src_cpc = source.get("cpc_code", "") if isinstance(source, dict) else ""
    return f"Cross-domain discovery from {src_cpc}: {n} related technology clusters found in different CPC sections. These represent potential technology transfer or convergence opportunities."


def _tech_trend_interpret(r: dict) -> str:
    growth = r.get("growth_rate", r.get("cagr", 0))
    total = r.get("total_patents", 0)
    query = r.get("query", "")
    if growth and growth > 0.2:
        return f"Technology trend for '{query}': Strong growth ({growth:.1%} CAGR, {total:,} total patents). This is an actively expanding area with increasing competitive intensity."
    elif growth and growth > 0:
        return f"Technology trend for '{query}': Moderate growth ({growth:.1%} CAGR, {total:,} total patents). Stable area with steady innovation."
    else:
        return f"Technology trend for '{query}': Declining or flat ({(f'{growth:.1%}' if growth else 'N/A')} CAGR, {total:,} patents). Consider whether this represents market maturity or technology obsolescence."


def _sales_prospect_interpret(r: dict) -> str:
    prospects = r.get("prospects", r.get("targets", []))
    n = len(prospects) if isinstance(prospects, list) else 0
    return f"{n} licensing prospects identified, ranked by technology need and approach feasibility. Each includes a 'why they need it' narrative and recommended deal structure."


def _tech_volatility_interpret(r: dict) -> str:
    vol = r.get("annualized_volatility", r.get("volatility", 0))
    query = r.get("query", r.get("cpc_prefix", ""))
    if vol and vol > 0.3:
        return f"Technology area '{query}' shows high filing volatility ({vol:.1%} annualized). This indicates rapid shifts in R&D investment — either a hot emerging area or a volatile niche."
    elif vol and vol > 0.1:
        return f"Technology area '{query}' shows moderate volatility ({vol:.1%}). Typical for established growth technologies."
    return f"Technology area '{query}' shows low volatility ({(f'{vol:.1%}' if vol else 'N/A')}). Stable, mature technology area."


def _patent_option_value_interpret(r: dict) -> str:
    option_val = r.get("option_value", r.get("portfolio_option_value", 0))
    firm = r.get("firm_id", r.get("query", ""))
    return f"Real option value for {firm}: {option_val:,.0f} (arbitrary units based on citation impact × remaining life × technology relevance). Higher values indicate greater commercialization potential."


# Master dispatcher
_INTERPRETERS = {
    "startability": _startability_interpret,
    "adversarial_strategy": _adversarial_interpret,
    "tech_gap": _tech_gap_interpret,
    "patent_valuation": _patent_valuation_interpret,
    "tech_beta": _tech_beta_interpret,
    "portfolio_var": _portfolio_var_interpret,
    "fto_analysis": _fto_interpret,
    "similar_firms": _similar_firms_interpret,
    "tech_landscape": _tech_landscape_interpret,
    "invention_intelligence": _invention_intel_interpret,
    "bayesian_scenario": _bayesian_interpret,
    "ma_target": _ma_target_interpret,
    "portfolio_evolution": _portfolio_evolution_interpret,
    "ip_due_diligence": _ip_due_diligence_interpret,
    "cross_domain_discovery": _cross_domain_interpret,
    "tech_trend": _tech_trend_interpret,
    "sales_prospect": _sales_prospect_interpret,
    "tech_volatility": _tech_volatility_interpret,
    "patent_option_value": _patent_option_value_interpret,
    "tech_entropy": lambda r: f"Technology entropy analysis: Shannon entropy={r.get('current_entropy', 'N/A')}, lifecycle stage={r.get('lifecycle_stage', 'unknown')}. Higher entropy indicates more diverse applicant base (competitive market).",
}


def add_interpretation(result: dict, tool_name: str) -> dict:
    """Add interpretation field to a tool result if applicable."""
    if not isinstance(result, dict) or "error" in result:
        return result
    endpoint = result.get("endpoint", tool_name)
    fn = _INTERPRETERS.get(endpoint) or _INTERPRETERS.get(tool_name)
    if fn:
        try:
            result["interpretation"] = fn(result)
        except Exception:
            pass  # Don't break tool response if interpretation fails
    return result
