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
        return f"Simulation complete. Expected NPV: {npv:,.0f}, probability of positive return: {prob:.1%}. Review sensitivity analysis to identify key value drivers."
    return "Bayesian scenario analysis ready. Use init -> update -> simulate workflow."


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
        return f"Technology area '{query}' shows high filing volatility ({vol:.1%} annualized). This indicates rapid shifts in R&D investment -- either a hot emerging area or a volatile niche."
    elif vol and vol > 0.1:
        return f"Technology area '{query}' shows moderate volatility ({vol:.1%}). Typical for established growth technologies."
    return f"Technology area '{query}' shows low volatility ({(f'{vol:.1%}' if vol else 'N/A')}). Stable, mature technology area."


def _patent_option_value_interpret(r: dict) -> str:
    option_val = r.get("option_value", r.get("portfolio_option_value", 0))
    firm = r.get("firm_id", r.get("query", ""))
    return f"Real option value for {firm}: {option_val:,.0f} (arbitrary units based on citation impact x remaining life x technology relevance). Higher values indicate greater commercialization potential."


# ---- NEW INTERPRETATION FUNCTIONS ----

def _citation_network_interpret(r: dict) -> str:
    nodes = r.get("nodes", [])
    edges = r.get("edges", [])
    hubs = r.get("hub_patents", r.get("metrics", {}).get("hub_patents", []))
    n_nodes = len(nodes) if isinstance(nodes, list) else 0
    n_edges = len(edges) if isinstance(edges, list) else 0
    n_hubs = len(hubs) if isinstance(hubs, list) else 0
    return f"Citation network: {n_nodes} patents connected by {n_edges} citation links. {n_hubs} hub patent(s) identified with high centrality. Hub patents are strategically important as they represent foundational IP in this technology area."


def _network_topology_interpret(r: dict) -> str:
    firm = r.get("firm", r.get("firm_id", "the firm"))
    density = r.get("density", r.get("network_density", 0))
    components = r.get("components", r.get("connected_components", 0))
    avg_degree = r.get("avg_degree", r.get("average_degree", 0))
    if density and density > 0.3:
        return f"{firm}'s citation network is highly interconnected (density={density:.3f}, avg degree={avg_degree:.1f}). This indicates a focused, coherent technology strategy with strong internal knowledge flows."
    elif density and density > 0.1:
        return f"{firm}'s citation network has moderate connectivity (density={density:.3f}, avg degree={avg_degree:.1f}). Technology portfolio spans multiple but related domains."
    return f"{firm}'s citation network is sparse (density={density or 0:.3f}, {components} components). This suggests a diversified portfolio with limited cross-pollination between technology areas."


def _knowledge_flow_interpret(r: dict) -> str:
    source = r.get("source_cpc", r.get("source", ""))
    target = r.get("target_cpc", r.get("target", ""))
    flow_strength = r.get("flow_strength", r.get("citation_flow", 0))
    n_paths = len(r.get("paths", r.get("flow_paths", [])))
    return f"Knowledge flow from {source} to {target}: strength={flow_strength:.3f} across {n_paths} citation paths. This indicates {'significant' if flow_strength > 0.3 else 'limited'} technology transfer between these domains."


def _tech_fusion_detector_interpret(r: dict) -> str:
    cpc_a = r.get("cpc_a", "")
    cpc_b = r.get("cpc_b", "")
    fusion_score = r.get("fusion_score", r.get("convergence_score", 0))
    n_bridge = len(r.get("bridge_patents", r.get("fusion_patents", [])))
    if fusion_score and fusion_score > 0.5:
        return f"Strong technology fusion detected between {cpc_a} and {cpc_b} (score={fusion_score:.3f}). {n_bridge} bridge patents span both domains. This convergence represents a potential new market opportunity."
    return f"Technology fusion between {cpc_a} and {cpc_b}: score={fusion_score:.3f}, {n_bridge} bridge patents. {'Emerging convergence worth monitoring.' if fusion_score and fusion_score > 0.2 else 'Limited convergence currently.'}"


def _patent_market_fusion_interpret(r: dict) -> str:
    firm = r.get("firm", r.get("firm_id", ""))
    fusion_score = r.get("fusion_score", r.get("overall_score", 0))
    tech_strength = r.get("tech_strength", 0)
    market_sentiment = r.get("market_sentiment", 0)
    return f"Patent-market fusion for {firm}: overall score={fusion_score:.2f} (tech strength={tech_strength:.2f}, market sentiment={market_sentiment:.2f}). {'Strong alignment between IP position and market outlook.' if fusion_score and fusion_score > 0.6 else 'Gap between IP assets and market positioning to address.'}"


def _claim_analysis_interpret(r: dict) -> str:
    n_claims = r.get("total_claims", r.get("claim_count", 0))
    ind_claims = r.get("independent_claims", 0)
    avg_breadth = r.get("average_breadth", r.get("avg_claim_breadth", 0))
    return f"Claim analysis: {n_claims} total claims ({ind_claims} independent). Average claim breadth={avg_breadth:.2f}. {'Broad claims suggest strong defensive coverage.' if avg_breadth and avg_breadth > 0.6 else 'Narrow claims may limit enforcement scope.'}"


def _sep_landscape_interpret(r: dict) -> str:
    standard = r.get("standard", "")
    total_seps = r.get("total_seps", r.get("total_patents", 0))
    top_holders = r.get("top_holders", r.get("top_declarants", []))
    leader = top_holders[0].get("name", "unknown") if top_holders and isinstance(top_holders, list) and top_holders else "N/A"
    return f"SEP landscape for {standard}: {total_seps:,} standard-essential patents declared. Leading holder: {leader}. Understanding SEP ownership is critical for FRAND licensing negotiations and standards participation strategy."


def _sep_portfolio_interpret(r: dict) -> str:
    firm = r.get("firm", r.get("declarant", ""))
    total = r.get("total_seps", r.get("total", 0))
    standards = r.get("standards", [])
    n_standards = len(standards) if isinstance(standards, list) else 0
    return f"{firm} holds {total:,} declared SEPs across {n_standards} standard(s). SEP portfolio strength determines FRAND licensing revenue potential and negotiation leverage in standards-based markets."


def _frand_analysis_interpret(r: dict) -> str:
    firm = r.get("firm", r.get("licensor", ""))
    rate = r.get("suggested_rate", r.get("royalty_rate", 0))
    basis = r.get("rate_basis", r.get("methodology", ""))
    return f"FRAND analysis for {firm}: suggested royalty rate={rate:.2%} based on {basis or 'comparable license methodology'}. This rate reflects SEP portfolio strength relative to the standard's total essential patents."


def _corporate_hierarchy_interpret(r: dict) -> str:
    firm = r.get("firm", r.get("firm_id", ""))
    subs = r.get("subsidiaries", r.get("entities", []))
    n_subs = len(subs) if isinstance(subs, list) else 0
    total_patents = r.get("total_group_patents", r.get("total_patents", 0))
    return f"Corporate hierarchy for {firm}: {n_subs} entities identified in the group, holding {total_patents:,} patents collectively. Group-level analysis captures the full IP footprint including subsidiary filings."


def _group_portfolio_interpret(r: dict) -> str:
    firm = r.get("firm", r.get("group_name", ""))
    total = r.get("total_patents", 0)
    entities = r.get("entities", [])
    n_entities = len(entities) if isinstance(entities, list) else 0
    return f"Group portfolio for {firm}: {total:,} patents across {n_entities} entities. Consolidated view captures subsidiary and affiliate filings that single-entity analysis would miss."


def _ptab_search_interpret(r: dict) -> str:
    results = r.get("proceedings", r.get("results", []))
    n = len(results) if isinstance(results, list) else 0
    return f"{n} PTAB proceedings found. Inter partes review (IPR) and other post-grant proceedings can invalidate patent claims. Monitor these proceedings for both offensive opportunities and defensive risk."


def _ptab_risk_interpret(r: dict) -> str:
    risk_level = r.get("risk_level", r.get("overall_risk", "unknown"))
    vulnerable = r.get("vulnerable_patents", r.get("at_risk_count", 0))
    cpc = r.get("cpc_prefix", "")
    return f"PTAB risk for {cpc}: {risk_level} level, {vulnerable} patents potentially vulnerable to challenge. {'High-risk patents should be reviewed for claim strengthening or design-around strategies.' if risk_level in ('high', 'elevated') else 'Standard monitoring recommended.'}"


def _litigation_search_interpret(r: dict) -> str:
    cases = r.get("cases", r.get("results", []))
    n = len(cases) if isinstance(cases, list) else 0
    return f"{n} patent litigation cases found. Review case details for assertion patterns, venue preferences, and settlement trends to inform defensive strategy."


def _litigation_risk_interpret(r: dict) -> str:
    firm = r.get("firm", r.get("firm_id", ""))
    risk_level = r.get("risk_level", r.get("overall_risk", "unknown"))
    exposure = r.get("exposure_score", r.get("litigation_exposure", 0))
    return f"Litigation risk for {firm}: {risk_level} (exposure score={exposure:.2f}). {'Active litigation monitoring and patent insurance recommended.' if risk_level in ('high', 'elevated') else 'Standard IP risk management practices are sufficient.'}"


def _cross_border_interpret(r: dict) -> str:
    firm = r.get("firm", r.get("query", ""))
    jurisdictions = r.get("jurisdictions", r.get("countries", []))
    n_juris = len(jurisdictions) if isinstance(jurisdictions, list) else 0
    similarity = r.get("cross_border_similarity", r.get("consistency_score", 0))
    return f"Cross-border analysis for {firm}: filings in {n_juris} jurisdictions, consistency score={similarity:.2f}. {'Consistent global filing strategy.' if similarity and similarity > 0.7 else 'Significant jurisdiction-specific variations detected -- review regional strategy.'}"


def _gdelt_company_events_interpret(r: dict) -> str:
    firm = r.get("firm", r.get("company", ""))
    events = r.get("events", r.get("articles", []))
    n = len(events) if isinstance(events, list) else 0
    tone = r.get("average_tone", r.get("sentiment", 0))
    return f"GDELT events for {firm}: {n} recent news events tracked. Average sentiment={tone:.2f}. {'Positive market signals align with IP investment.' if tone and tone > 0 else 'Negative sentiment may indicate risk factors -- correlate with patent activity trends.'}"


def _firm_patent_portfolio_interpret(r: dict) -> str:
    firm = r.get("firm", r.get("firm_id", ""))
    total = r.get("total_patents", r.get("patent_count", 0))
    top_cpc = r.get("top_cpc", r.get("technology_focus", []))
    focus = top_cpc[0].get("cpc", top_cpc[0].get("code", "")) if top_cpc and isinstance(top_cpc, list) and top_cpc else "N/A"
    return f"{firm} portfolio: {total:,} patents. Primary technology focus: {focus}. Portfolio size and technology concentration indicate the firm's IP strategic priorities."


def _tech_clusters_list_interpret(r: dict) -> str:
    clusters = r.get("clusters", r.get("results", []))
    n = len(clusters) if isinstance(clusters, list) else 0
    cpc_filter = r.get("cpc_filter", "")
    return f"{n} technology clusters identified{(' in ' + cpc_filter) if cpc_filter else ''}. Clusters represent coherent technology groupings useful for startability analysis and competitive benchmarking."


def _applicant_network_interpret(r: dict) -> str:
    firm = r.get("applicant", r.get("firm", ""))
    co_applicants = r.get("co_applicants", r.get("collaborators", []))
    n = len(co_applicants) if isinstance(co_applicants, list) else 0
    return f"{firm} co-files with {n} other entities. Strong co-applicant networks indicate active R&D partnerships and joint venture activity. Key partners may be M&A targets or licensing counterparts."


def _patent_compare_interpret(r: dict) -> str:
    firms = r.get("firms", [])
    n = len(firms) if isinstance(firms, list) else 0
    if n >= 2 and isinstance(firms[0], dict):
        f1 = firms[0].get("name", "Firm 1")
        f2 = firms[1].get("name", "Firm 2")
        p1 = firms[0].get("total_patents", firms[0].get("patent_count", 0))
        p2 = firms[1].get("total_patents", firms[1].get("patent_count", 0))
        return f"Portfolio comparison: {f1} ({p1:,} patents) vs {f2} ({p2:,} patents). Review technology overlap and differentiation areas for competitive strategy insights."
    return f"Portfolio comparison across {n} firms. Review technology distribution, citation metrics, and growth trends for relative positioning."


def _startability_ranking_interpret(r: dict) -> str:
    rankings = r.get("rankings", r.get("results", []))
    n = len(rankings) if isinstance(rankings, list) else 0
    mode = r.get("mode", "")
    query = r.get("query", "")
    return f"Startability ranking ({mode}) for {query}: {n} entries ranked by technology entry readiness. Top-ranked items represent the most accessible opportunities for strategic expansion."


def _startability_delta_interpret(r: dict) -> str:
    deltas = r.get("deltas", r.get("results", []))
    n = len(deltas) if isinstance(deltas, list) else 0
    mode = r.get("mode", "")
    query = r.get("query", "")
    improving = sum(1 for d in (deltas if isinstance(deltas, list) else []) if isinstance(d, dict) and d.get("delta", 0) > 0)
    return f"Startability delta ({mode}) for {query}: {n} entries tracked. {improving} showing improvement over time. Positive deltas indicate the firm is building capabilities for market entry."


def _startability_heatmap_interpret(r: dict) -> str:
    firms = r.get("firms", [])
    clusters = r.get("clusters", [])
    n_firms = len(firms) if isinstance(firms, list) else 0
    n_clusters = len(clusters) if isinstance(clusters, list) else 0
    return f"Startability heatmap: {n_firms} firm(s) x {n_clusters} technology clusters. Visual matrix shows entry readiness across the technology landscape. Hot spots indicate natural expansion opportunities."


def _tech_trend_alert_interpret(r: dict) -> str:
    hot = r.get("hot_trends", r.get("rising", []))
    cooling = r.get("cooling_trends", r.get("declining", []))
    n_hot = len(hot) if isinstance(hot, list) else 0
    n_cool = len(cooling) if isinstance(cooling, list) else 0
    return f"Trend alerts: {n_hot} hot (accelerating) and {n_cool} cooling (decelerating) technology areas detected. Hot trends represent emerging investment opportunities; cooling trends may signal market saturation."


def _tech_map_interpret(r: dict) -> str:
    nodes = r.get("nodes", r.get("technologies", []))
    edges = r.get("edges", r.get("links", []))
    n_nodes = len(nodes) if isinstance(nodes, list) else 0
    n_edges = len(edges) if isinstance(edges, list) else 0
    return f"Technology map: {n_nodes} technology nodes connected by {n_edges} links. The map reveals technology adjacencies and clusters. Use this to identify strategic technology pathways and white space opportunities."


def _sep_search_interpret(r: dict) -> str:
    results = r.get("results", r.get("patents", []))
    n = len(results) if isinstance(results, list) else 0
    query = r.get("query", "")
    return f"SEP search for '{query}': {n} standard-essential patents found. Review declarations and essentiality claims to assess licensing obligations and FRAND commitment scope."


def _firm_tech_vector_interpret(r: dict) -> str:
    firm = r.get("firm", r.get("firm_id", ""))
    dimensions = r.get("vector_dimensions", r.get("dimensions", 0))
    top_areas = r.get("top_technology_areas", r.get("top_cpc", []))
    n_top = len(top_areas) if isinstance(top_areas, list) else 0
    return f"Technology vector for {firm}: {dimensions or 'N/A'}-dimensional representation across {n_top} primary technology areas. Use for cosine similarity comparisons with peers or acquisition targets."


def _tech_fit_interpret(r: dict) -> str:
    score = r.get("fit_score", r.get("score", 0))
    firm = r.get("firm_id", r.get("firm", ""))
    cluster = r.get("cluster_id", r.get("technology", ""))
    if score and score >= 0.7:
        return f"{firm} has excellent technology fit ({score:.3f}) for {cluster}. Existing capabilities strongly align with this technology area."
    elif score and score >= 0.4:
        return f"{firm} has moderate technology fit ({score:.3f}) for {cluster}. Some relevant capabilities exist but gaps remain."
    return f"{firm} has limited technology fit ({score or 0:.3f}) for {cluster}. Significant capability building needed."


def _group_startability_interpret(r: dict) -> str:
    score = r.get("group_score", r.get("score", 0))
    firm = r.get("firm", r.get("group_name", ""))
    cluster = r.get("cluster_id", r.get("technology", ""))
    return f"Group startability for {firm} in {cluster}: score={score:.3f}. Group-level analysis includes subsidiary capabilities, providing a more complete picture than single-entity startability."


def _patent_search_interpret(r: dict) -> str:
    results = r.get("results", r.get("patents", []))
    n = len(results) if isinstance(results, list) else 0
    total = r.get("total_hits", r.get("total", n))
    return f"Patent search: {n} results returned (total matches: {total:,}). Review titles, abstracts, and CPC codes to identify relevant prior art or competitive filings."


def _patent_summary_interpret(r: dict) -> str:
    title = r.get("title", "")
    applicant = r.get("applicant", r.get("assignee", ""))
    return f"Patent summary for '{title}' by {applicant}. Review claims and technology classification for detailed technical scope assessment."


def _technology_brief_interpret(r: dict) -> str:
    cpc = r.get("cpc_prefix", r.get("cpc", ""))
    total = r.get("total_patents", 0)
    trend = r.get("trend", r.get("growth_trend", ""))
    return f"Technology brief for {cpc}: {total:,} patents. Growth trend: {trend or 'see details'}. Use this overview to understand the technology lifecycle stage and competitive density."


def _citation_graph_viz_interpret(r: dict) -> str:
    nodes = r.get("nodes", [])
    edges = r.get("edges", [])
    n_nodes = len(nodes) if isinstance(nodes, list) else 0
    n_edges = len(edges) if isinstance(edges, list) else 0
    return f"Citation graph visualization: {n_nodes} nodes, {n_edges} edges. Visual representation of citation relationships helps identify key patents and technology lineages."


def _firm_landscape_interpret(r: dict) -> str:
    firm = r.get("firm", r.get("firm_id", ""))
    total = r.get("total_patents", 0)
    rank = r.get("market_rank", r.get("rank", "N/A"))
    return f"Firm landscape for {firm}: {total:,} patents, market rank #{rank}. Comprehensive competitive positioning view including technology distribution and filing trends."


def _category_landscape_interpret(r: dict) -> str:
    category = r.get("category", r.get("category_name", ""))
    total = r.get("total_patents", 0)
    top_firms = r.get("top_firms", [])
    n_firms = len(top_firms) if isinstance(top_firms, list) else 0
    return f"Category landscape for '{category}': {total:,} patents across {n_firms} firms. Custom category analysis enables focused competitive intelligence on user-defined technology areas."


def _portfolio_benchmark_interpret(r: dict) -> str:
    firm = r.get("firm", r.get("firm_id", ""))
    percentile = r.get("percentile", r.get("rank_percentile", 0))
    score = r.get("benchmark_score", r.get("overall_score", 0))
    return f"Portfolio benchmark for {firm}: {percentile:.0f}th percentile (score={score:.2f}). {'Above-average IP position relative to peer group.' if percentile and percentile > 50 else 'Below-average IP position -- consider strengthening key technology areas.'}"


def _classify_patents_interpret(r: dict) -> str:
    classified = r.get("classified_count", r.get("total", 0))
    categories = r.get("categories", [])
    n_cats = len(categories) if isinstance(categories, list) else 0
    return f"{classified} patents classified across {n_cats} categories. Patent classification enables custom portfolio segmentation beyond standard CPC taxonomy."


def _claim_comparison_interpret(r: dict) -> str:
    similarity = r.get("similarity_score", r.get("overlap", 0))
    patent_a = r.get("patent_a", "")
    patent_b = r.get("patent_b", "")
    return f"Claim comparison between {patent_a} and {patent_b}: similarity={similarity:.2f}. {'High overlap suggests potential infringement risk or prior art relevance.' if similarity and similarity > 0.7 else 'Differentiated claim scope.'}"


def _create_category_interpret(r: dict) -> str:
    name = r.get("category_name", r.get("name", ""))
    return f"Custom category '{name}' created successfully. Use classify_patents to assign patents and category_landscape to analyze the competitive landscape."


def _create_watch_interpret(r: dict) -> str:
    watch_id = r.get("watch_id", r.get("id", ""))
    return f"Watch '{watch_id}' created. The monitoring system will track new filings and changes matching your criteria. Use check_alerts to review notifications."


def _list_watches_interpret(r: dict) -> str:
    watches = r.get("watches", r.get("results", []))
    n = len(watches) if isinstance(watches, list) else 0
    return f"{n} active watches configured. Each watch monitors specific technology areas or firms for new patent filings and competitive changes."


def _check_alerts_interpret(r: dict) -> str:
    alerts = r.get("alerts", r.get("results", []))
    n = len(alerts) if isinstance(alerts, list) else 0
    return f"{n} alerts triggered. Review each alert for competitive filings, technology shifts, or portfolio changes requiring attention."


def _run_monitoring_interpret(r: dict) -> str:
    processed = r.get("processed", r.get("checks_run", 0))
    alerts = r.get("alerts_generated", r.get("new_alerts", 0))
    return f"Monitoring cycle complete: {processed} watches checked, {alerts} new alerts generated. Review alerts for actionable intelligence."


def _patent_detail_interpret(r: dict) -> str:
    title = r.get("title", "")
    applicant = r.get("applicant", r.get("assignee", ""))
    status = r.get("status", r.get("legal_status", ""))
    return f"Patent detail: '{title}' by {applicant}. Status: {status or 'see details'}. Review claims, citations, and family members for complete IP picture."


# Master dispatcher
_INTERPRETERS = {
    # Original 20
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
    # New additions
    "citation_network": _citation_network_interpret,
    "network_topology": _network_topology_interpret,
    "knowledge_flow": _knowledge_flow_interpret,
    "tech_fusion_detector": _tech_fusion_detector_interpret,
    "patent_market_fusion": _patent_market_fusion_interpret,
    "claim_analysis": _claim_analysis_interpret,
    "sep_landscape": _sep_landscape_interpret,
    "sep_portfolio": _sep_portfolio_interpret,
    "sep_search": _sep_search_interpret,
    "frand_analysis": _frand_analysis_interpret,
    "corporate_hierarchy": _corporate_hierarchy_interpret,
    "group_portfolio": _group_portfolio_interpret,
    "ptab_search": _ptab_search_interpret,
    "ptab_risk": _ptab_risk_interpret,
    "litigation_search": _litigation_search_interpret,
    "litigation_risk": _litigation_risk_interpret,
    "cross_border_similarity": _cross_border_interpret,
    "gdelt_company_events": _gdelt_company_events_interpret,
    "firm_patent_portfolio": _firm_patent_portfolio_interpret,
    "tech_clusters_list": _tech_clusters_list_interpret,
    "applicant_network": _applicant_network_interpret,
    "patent_compare": _patent_compare_interpret,
    "startability_ranking": _startability_ranking_interpret,
    "startability_delta": _startability_delta_interpret,
    "startability_heatmap": _startability_heatmap_interpret,
    "tech_trend_alert": _tech_trend_alert_interpret,
    "tech_map": _tech_map_interpret,
    "firm_tech_vector": _firm_tech_vector_interpret,
    "tech_fit": _tech_fit_interpret,
    "group_startability": _group_startability_interpret,
    "patent_search": _patent_search_interpret,
    "patent_summary": _patent_summary_interpret,
    "technology_brief": _technology_brief_interpret,
    "citation_graph_viz": _citation_graph_viz_interpret,
    "firm_landscape": _firm_landscape_interpret,
    "category_landscape": _category_landscape_interpret,
    "portfolio_benchmark": _portfolio_benchmark_interpret,
    "classify_patents": _classify_patents_interpret,
    "claim_comparison": _claim_comparison_interpret,
    "create_category": _create_category_interpret,
    "create_watch": _create_watch_interpret,
    "list_watches": _list_watches_interpret,
    "check_alerts": _check_alerts_interpret,
    "run_monitoring": _run_monitoring_interpret,
    "patent_detail": _patent_detail_interpret,
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
