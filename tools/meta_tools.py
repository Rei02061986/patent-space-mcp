"""Meta-tools: tool_help and tool_suggest for Patent Space MCP."""
from __future__ import annotations
from typing import Any


# Tool catalog with descriptions, params, and examples
_TOOL_CATALOG = {
    "patent_search": {
        "category": "Search",
        "description": "Search 159M+ global patents by keyword, CPC, applicant, date",
        "key_params": ["query", "cpc_codes", "applicant", "jurisdiction", "date_from", "date_to"],
        "example": {"query": "全固体電池", "cpc_codes": ["H01M10"], "jurisdiction": "JP"},
        "speed": "slow (HDD scan)",
    },
    "startability": {
        "category": "Strategy",
        "description": "Firm readiness score (0-1) for a technology area",
        "key_params": ["firm_query", "tech_query_or_cluster_id", "year"],
        "example": {"firm_query": "トヨタ", "tech_query_or_cluster_id": "H01M_0"},
        "speed": "fast (pre-computed)",
    },
    "startability_ranking": {
        "category": "Strategy",
        "description": "Rank firms by tech readiness, or rank techs for a firm",
        "key_params": ["mode (by_firm|by_tech)", "query", "top_n"],
        "example": {"mode": "by_firm", "query": "ソニー", "top_n": 10},
        "speed": "fast",
    },
    "adversarial_strategy": {
        "category": "Competitive",
        "description": "Game-theoretic patent strategy: attack/defend/preempt",
        "key_params": ["firm_a", "firm_b"],
        "example": {"firm_a": "トヨタ", "firm_b": "ホンダ"},
        "speed": "fast",
    },
    "tech_gap": {
        "category": "Competitive",
        "description": "Technology gap and synergy between two firms",
        "key_params": ["firm_a", "firm_b"],
        "example": {"firm_a": "ソニー", "firm_b": "パナソニック"},
        "speed": "fast",
    },
    "similar_firms": {
        "category": "Competitive",
        "description": "Find firms with similar patent portfolios",
        "key_params": ["firm_query", "top_n"],
        "example": {"firm_query": "NVIDIA", "top_n": 5},
        "speed": "moderate",
    },
    "patent_valuation": {
        "category": "Finance",
        "description": "Patent portfolio valuation with royalty benchmarks",
        "key_params": ["query", "query_type (firm|technology)", "purpose"],
        "example": {"query": "トヨタ", "query_type": "firm"},
        "speed": "moderate",
    },
    "patent_option_value": {
        "category": "Finance",
        "description": "Black-Scholes real option value for patents",
        "key_params": ["query", "query_type"],
        "example": {"query": "H01M", "query_type": "technology"},
        "speed": "moderate",
    },
    "portfolio_var": {
        "category": "Finance",
        "description": "Value-at-Risk from patent expiration",
        "key_params": ["firm", "confidence", "horizon_years"],
        "example": {"firm": "パナソニック", "confidence": 0.95},
        "speed": "moderate",
    },
    "tech_beta": {
        "category": "Finance",
        "description": "CAPM-style technology market sensitivity",
        "key_params": ["query", "query_type"],
        "example": {"query": "G06N", "query_type": "technology"},
        "speed": "fast",
    },
    "bayesian_scenario": {
        "category": "Finance",
        "description": "Bayesian investment scenario with Monte Carlo",
        "key_params": ["mode (init|update|simulate)", "technology", "firm_query"],
        "example": {"mode": "init", "technology": "H01M_0", "firm_query": "トヨタ"},
        "speed": "fast",
    },
    "fto_analysis": {
        "category": "Legal",
        "description": "Freedom-to-operate risk assessment",
        "key_params": ["text", "cpc_codes", "target_jurisdiction"],
        "example": {"text": "全固体電池用硫化物固体電解質", "cpc_codes": ["H01M10"]},
        "speed": "variable",
    },
    "ip_due_diligence": {
        "category": "Finance",
        "description": "Integrated IP due diligence for investment analysis",
        "key_params": ["target_firm", "investment_type"],
        "example": {"target_firm": "NVIDIA", "investment_type": "growth"},
        "speed": "moderate",
    },
    "tech_landscape": {
        "category": "Analysis",
        "description": "Filing trends and top applicants in a technology area",
        "key_params": ["cpc_prefix", "date_from", "date_to"],
        "example": {"cpc_prefix": "G06N", "date_from": "2019-01-01"},
        "speed": "slow (full scan)",
    },
    "tech_trend": {
        "category": "Analysis",
        "description": "Year-by-year filing trends with growth rates",
        "key_params": ["query"],
        "example": {"query": "H01M"},
        "speed": "fast",
    },
    "cross_domain_discovery": {
        "category": "Analysis",
        "description": "Find related technologies in different CPC sections",
        "key_params": ["query", "top_n"],
        "example": {"query": "H01M", "top_n": 5},
        "speed": "fast",
    },
    "invention_intelligence": {
        "category": "Analysis",
        "description": "Prior art search + FTO + whitespace from text description",
        "key_params": ["text", "max_prior_art"],
        "example": {"text": "AI-based drug discovery"},
        "speed": "moderate",
    },
    "ma_target": {
        "category": "Strategy",
        "description": "M&A acquisition target recommendations",
        "key_params": ["acquirer", "strategy", "top_n"],
        "example": {"acquirer": "トヨタ", "strategy": "tech_gap", "top_n": 5},
        "speed": "fast",
    },
    "sep_search": {
        "category": "Legal",
        "description": "Search standard-essential patent declarations",
        "key_params": ["query", "standard"],
        "example": {"standard": "5G"},
        "speed": "fast",
    },
    "tech_clusters_list": {
        "category": "Reference",
        "description": "Browse 607 technology clusters",
        "key_params": ["cpc_filter", "top_n"],
        "example": {"cpc_filter": "H01"},
        "speed": "instant",
    },
    "entity_resolve": {
        "category": "Reference",
        "description": "Resolve company name to canonical firm_id",
        "key_params": ["query"],
        "example": {"query": "トヨタ"},
        "speed": "instant",
    },
}

# Context → suggested tools mapping
_CONTEXT_SUGGESTIONS = {
    "競合": ["adversarial_strategy", "tech_gap", "similar_firms", "patent_compare"],
    "competitor": ["adversarial_strategy", "tech_gap", "similar_firms", "patent_compare"],
    "投資": ["ip_due_diligence", "patent_valuation", "portfolio_var", "bayesian_scenario"],
    "investment": ["ip_due_diligence", "patent_valuation", "portfolio_var", "bayesian_scenario"],
    "M&A": ["ma_target", "tech_gap", "ip_due_diligence"],
    "買収": ["ma_target", "tech_gap", "ip_due_diligence"],
    "FTO": ["fto_analysis", "invention_intelligence", "patent_search"],
    "侵害": ["fto_analysis", "litigation_risk", "claim_analysis"],
    "トレンド": ["tech_trend", "tech_landscape", "tech_trend_alert", "portfolio_evolution"],
    "trend": ["tech_trend", "tech_landscape", "tech_trend_alert", "portfolio_evolution"],
    "ライセンス": ["sales_prospect", "patent_valuation", "sep_search", "frand_analysis"],
    "license": ["sales_prospect", "patent_valuation", "sep_search", "frand_analysis"],
    "特許検索": ["patent_search", "patent_detail", "claim_analysis"],
    "search": ["patent_search", "patent_detail", "claim_analysis"],
    "ポートフォリオ": ["firm_patent_portfolio", "portfolio_evolution", "portfolio_var", "startability_ranking"],
    "portfolio": ["firm_patent_portfolio", "portfolio_evolution", "portfolio_var", "startability_ranking"],
    "参入": ["startability", "startability_ranking", "tech_fit", "cross_domain_discovery"],
    "entry": ["startability", "startability_ranking", "tech_fit", "cross_domain_discovery"],
    "AI": ["tech_trend", "startability", "tech_landscape"],
    "電池": ["tech_trend", "startability", "fto_analysis"],
    "半導体": ["tech_trend", "startability", "tech_landscape"],
    "5G": ["sep_search", "sep_landscape", "frand_analysis"],
}


def tool_help(tool_name: str | None = None) -> dict[str, Any]:
    """Get help for MCP tools. None = list all, name = specific tool."""
    if tool_name is None:
        # List all tools by category
        by_category: dict[str, list] = {}
        for name, info in _TOOL_CATALOG.items():
            cat = info["category"]
            by_category.setdefault(cat, []).append({
                "tool": name,
                "description": info["description"],
                "speed": info["speed"],
            })
        return {
            "endpoint": "tool_help",
            "total_tools": len(_TOOL_CATALOG),
            "categories": by_category,
            "tip": "Call tool_help with a specific tool_name for detailed usage.",
        }

    info = _TOOL_CATALOG.get(tool_name)
    if not info:
        # Fuzzy match
        matches = [n for n in _TOOL_CATALOG if tool_name.lower() in n.lower()]
        return {
            "endpoint": "tool_help",
            "error": f"Tool '{tool_name}' not found.",
            "did_you_mean": matches[:5] if matches else [],
            "available_tools": sorted(_TOOL_CATALOG.keys()),
        }

    return {
        "endpoint": "tool_help",
        "tool": tool_name,
        "category": info["category"],
        "description": info["description"],
        "parameters": info["key_params"],
        "example": info["example"],
        "speed": info["speed"],
    }


def tool_suggest(context: str) -> dict[str, Any]:
    """Suggest tools based on user context/intent."""
    ctx_lower = context.lower()
    suggestions = set()
    matched_contexts = []

    for keyword, tools in _CONTEXT_SUGGESTIONS.items():
        if keyword.lower() in ctx_lower:
            suggestions.update(tools)
            matched_contexts.append(keyword)

    if not suggestions:
        # Default suggestions
        suggestions = {"patent_search", "startability", "tech_landscape", "entity_resolve"}

    ranked = []
    for tool_name in suggestions:
        info = _TOOL_CATALOG.get(tool_name, {})
        ranked.append({
            "tool": tool_name,
            "description": info.get("description", ""),
            "example": info.get("example", {}),
            "speed": info.get("speed", ""),
        })

    return {
        "endpoint": "tool_suggest",
        "context": context,
        "matched_keywords": matched_contexts,
        "suggestions": ranked[:8],
        "tip": "Use tool_help('<tool_name>') for detailed usage of any suggested tool.",
    }
