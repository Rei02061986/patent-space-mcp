"""Patent Space MCP server entry point."""
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import unicodedata
from pathlib import Path

from dotenv import load_dotenv
from fastmcp import FastMCP

from db.sqlite_store import PatentStore
from entity.data.tse_auto_seed import TSE_AUTO_ENTITIES
from entity.data.tse_expanded_seed import TSE_EXPANDED_ENTITIES
from entity.data.tse_prime_seed import TSE_PRIME_ENTITIES

try:
    from entity.data.sp500_seed import SP500_ENTITIES
except ImportError:
    SP500_ENTITIES = []
try:
    from entity.data.global_seed import GLOBAL_ENTITIES
except ImportError:
    GLOBAL_ENTITIES = []
try:
    from entity.data.global_seed import GLOBAL_ENTITIES
except ImportError:
    GLOBAL_ENTITIES = []
from entity.registry import EntityRegistry
from entity.resolver import EntityResolver
from tools.compare import patent_compare
from tools.clusters import tech_clusters_list
from tools.gdelt_tool import gdelt_company_events
from tools.landscape import tech_landscape
from tools.network import applicant_network
from tools.portfolio import firm_patent_portfolio
from tools.search import patent_search
from tools.startability_tool import startability, startability_ranking
from tools.startability_delta import startability_delta
from tools.adversarial import adversarial_strategy
from tools.cross_domain import cross_domain_discovery
from tools.invention_intel import invention_intelligence
from tools.market_fusion import patent_market_fusion
from tools.tech_fit import tech_fit
from tools.vectors import firm_tech_vector
from tools.similar_firms import similar_firms
from tools.tech_gap import tech_gap
from tools.cross_border import cross_border_similarity
from tools.patent_valuation import patent_valuation
from tools.portfolio_evolution import portfolio_evolution
from tools.tech_trend_alert import tech_trend_alert
from tools.sales_prospect import sales_prospect
from tools.bayesian_scenario import bayesian_scenario

load_dotenv()

mcp = FastMCP("patent-space-mcp")

# Initialize shared state
_entity_registry = EntityRegistry()
for _e in TSE_PRIME_ENTITIES:
    _entity_registry.register(_e)
for _e in TSE_EXPANDED_ENTITIES:
    _entity_registry.register(_e)
for _e in TSE_AUTO_ENTITIES:
    _entity_registry.register(_e)
for _e in SP500_ENTITIES:
    _entity_registry.register(_e)
for _e in GLOBAL_ENTITIES:
    _entity_registry.register(_e)
_resolver = EntityResolver(_entity_registry)

# Load company display names from companies_master.csv
_FIRM_DISPLAY_NAMES: dict[str, str] = {}
_COMPANIES_CSV = os.getenv(
    "COMPANIES_MASTER_CSV",
    str(Path(__file__).resolve().parent / "data" / "companies_master.csv"),
)
# Try multiple known paths
for _csv_candidate in [
    _COMPANIES_CSV,
    os.path.expanduser(
        "~/Library/CloudStorage/GoogleDrive-teddykmk@gmail.com/"
        "マイドライブ/activation_space_jp_tse_5y/00_universe/companies_master.csv"
    ),
]:
    if os.path.exists(_csv_candidate):
        try:
            with open(_csv_candidate, encoding="utf-8-sig") as _f:
                for _row in csv.DictReader(_f):
                    _cid = (_row.get("company_id") or "").strip()
                    _name = (_row.get("name_ja") or "").strip()
                    if _cid and _name:
                        _FIRM_DISPLAY_NAMES[f"company_{_cid}"] = unicodedata.normalize("NFKC", _name)
        except Exception:
            pass
        break

# Also populate from entity registry (named firm_ids like 'toyota')
for _e in _entity_registry.all_entities():
    if _e.canonical_id not in _FIRM_DISPLAY_NAMES:
        _FIRM_DISPLAY_NAMES[_e.canonical_id] = _e.canonical_name

def _display_name(firm_id: str) -> str:
    """Resolve firm_id to human-readable display name."""
    return _FIRM_DISPLAY_NAMES.get(firm_id, firm_id)

def _enrich_firm_ids(result: dict | list) -> dict | list:
    """Walk a result structure and add display_name alongside firm_id fields."""
    if isinstance(result, dict):
        out = {}
        for k, v in result.items():
            if k == "firm_id" and isinstance(v, str):
                out[k] = unicodedata.normalize("NFKC", v) if isinstance(v, str) else v
                out["firm_name"] = _display_name(v)
            elif isinstance(v, (dict, list)):
                out[k] = _enrich_firm_ids(v)
            else:
                out[k] = unicodedata.normalize("NFKC", v) if isinstance(v, str) else v
        return out
    elif isinstance(result, list):
        return [_enrich_firm_ids(item) if isinstance(item, (dict, list)) else item for item in result]
    return _nfkc_normalize_response(result) if isinstance(result, str) else result

_db_path = os.getenv("PATENT_DB_PATH", "data/patents.db")
_store = PatentStore(_db_path)

def _safe_call(fn, *args, _tool_name=None, _timeout=120, **kwargs):
    """Call a tool function with graceful timeout handling.

    Sets a hard deadline on the store connection AND starts a timer thread
    that calls connection.interrupt() after the deadline. The progress handler
    fires every 50K VM instructions, but single instructions can block on HDD
    I/O for 2-35 seconds. connection.interrupt() IS checked during I/O waits
    (in the pager/B-tree layer), so it provides a much tighter time bound.
    """
    import time as _time
    import threading as _threading
    _CALL_DEADLINE = _timeout  # max seconds for any tool call
    hard_deadline = _time.monotonic() + _CALL_DEADLINE

    # Set hard deadline on the store's thread-local
    interrupt_timer = None
    if hasattr(_store, '_local'):
        _store._local.hard_deadline = hard_deadline
        # Start timer to call conn.interrupt() — works even during I/O stalls
        try:
            conn = _store._conn()
            interrupt_timer = _threading.Timer(_CALL_DEADLINE, conn.interrupt)
            interrupt_timer.daemon = True
            interrupt_timer.start()
        except Exception:
            pass  # Fallback to progress handler only

    try:
        result = fn(*args, **kwargs)
        if _tool_name:
            result = _inject_vis_hint(result, _tool_name)
        return _nfkc_normalize_response(result)
    except sqlite3.OperationalError as e:
        if "interrupted" in str(e):
            return {
                "error": "Query timed out — database under heavy I/O load.",
                "suggestion": (
                    "This tool needs to scan large tables which is slow during "
                    "data ingestion. Try the fast-path tools instead: "
                    "startability, tech_fit, firm_patent_portfolio, patent_market_fusion, "
                    "startability_ranking, firm_tech_vector, tech_landscape, "
                    "patent_compare, adversarial_strategy."
                ),
            }
        raise
    finally:
        if interrupt_timer is not None:
            interrupt_timer.cancel()
        if hasattr(_store, '_local'):
            _store._local.hard_deadline = None




# ─── Visualization hints for all tools ───────────────────────────────

def _nfkc_normalize_response(obj):
    """Recursively NFKC-normalize all strings in a response dict."""
    if isinstance(obj, str):
        return unicodedata.normalize("NFKC", obj)
    elif isinstance(obj, dict):
        return {k: _nfkc_normalize_response(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_nfkc_normalize_response(v) for v in obj]
    return obj


_VIS_HINTS = {
    "patent_search": {
        "recommended_chart": "table",
        "title": "特許検索結果",
        "data_mapping": {"columns": ["publication_number", "title_ja", "assignees", "filing_date", "cpc_codes"]},
        "suggested_options": {"sortable": True, "page_size": 20},
    },
    "firm_patent_portfolio": {
        "recommended_chart": "pie_and_line",
        "title": "特許ポートフォリオ分析",
        "data_mapping": {
            "pie_labels": "cpc_distribution[].code",
            "pie_values": "cpc_distribution[].count",
            "line_x": "filing_trend[].year",
            "line_y": "filing_trend[].count",
        },
        "suggested_options": {"show_top_n": 10, "color_scheme": "category20"},
    },
    "patent_detail": {
        "recommended_chart": "card",
        "title": "特許詳細",
        "data_mapping": {"title": "title_ja", "subtitle": "abstract_ja", "metadata": ["filing_date", "cpc_codes", "assignees"]},
    },
    "entity_resolve": {
        "recommended_chart": "list",
        "title": "企業名解決結果",
        "data_mapping": {"items": "results[].canonical_name", "scores": "results[].score"},
    },
    "tech_landscape": {
        "recommended_chart": "treemap",
        "title": "技術ランドスケープ",
        "data_mapping": {
            "labels": "cpc_trend[].cpc_prefix",
            "values": "cpc_trend[].count",
            "color": "cpc_trend[].growth_rate",
        },
        "suggested_options": {"color_scheme": ["#dc2626", "#f97316", "#22c55e"], "color_label": "成長率"},
    },
    "applicant_network": {
        "recommended_chart": "network",
        "title": "出願人ネットワーク",
        "data_mapping": {
            "node_id": "nodes[].id",
            "node_label": "nodes[].name",
            "node_size": "nodes[].patent_count",
            "edge_source": "edges[].from",
            "edge_target": "edges[].to",
            "edge_weight": "edges[].co_patents",
        },
    },
    "patent_compare": {
        "recommended_chart": "grouped_bar",
        "title": "企業特許ポートフォリオ比較",
        "data_mapping": {
            "groups": "firms[].name",
            "values": "firms[].patent_count",
            "detail_labels": "firms[].cpc_distribution[].code",
            "detail_values": "firms[].cpc_distribution[].max_score",
        },
        "suggested_options": {"sort": "descending", "show_values": True},
    },
    "firm_tech_vector": {
        "recommended_chart": "radar",
        "title": "技術ベクトル分析",
        "data_mapping": {"dimensions": "top_cpc[].code", "values": "top_cpc[].weight"},
    },
    "tech_clusters_list": {
        "recommended_chart": "scatter",
        "title": "技術クラスタ一覧",
        "data_mapping": {
            "x": "clusters[].patent_count",
            "y": "clusters[].growth_rate",
            "label": "clusters[].label",
            "size": "clusters[].patent_count",
        },
        "suggested_options": {"x_label": "特許件数", "y_label": "成長率", "log_scale_x": True},
    },
    "tech_fit": {
        "recommended_chart": "gauge",
        "title": "技術適合度",
        "data_mapping": {
            "value": "phi_tech_cosine",
            "components": ["phi_tech_cosine", "phi_tech_distance", "phi_tech_cpc_jaccard"],
        },
    },
    "startability": {
        "recommended_chart": "gauge",
        "title": "Startabilityスコア",
        "data_mapping": {
            "value": "score",
            "components": ["phi_tech_cosine", "phi_tech_distance", "phi_tech_cpc_jaccard"],
            "gate": "gate_open",
        },
    },
    "startability_ranking": {
        "recommended_chart": "horizontal_bar",
        "title": "Startabilityランキング",
        "data_mapping": {
            "labels": "results[].firm_name or results[].firm_id",
            "values": "results[].score",
            "color_by": "results[].gate_open",
        },
        "suggested_options": {"sort": "descending", "show_values": True, "color_scheme": ["#2563eb", "#94a3b8"]},
    },
    "startability_delta": {
        "recommended_chart": "waterfall",
        "title": "Startability変化分析",
        "data_mapping": {
            "labels": "results[].cluster_id",
            "values": "results[].delta",
            "start_values": "results[].score_start",
            "end_values": "results[].score_end",
        },
        "suggested_options": {"positive_color": "#22c55e", "negative_color": "#dc2626"},
    },
    "gdelt_company_events": {
        "recommended_chart": "multi_line",
        "title": "GDELT企業シグナル推移",
        "data_mapping": {
            "x": "quarterly_trend[].period",
            "series": {
                "direction": "quarterly_trend[].direction",
                "openness": "quarterly_trend[].openness",
                "investment": "quarterly_trend[].investment",
                "leadership": "quarterly_trend[].leadership",
            },
        },
        "suggested_options": {"y_range": [0, 1], "show_composite": True},
    },
    "cross_domain_discovery": {
        "recommended_chart": "network",
        "title": "クロスドメイン技術発見",
        "data_mapping": {
            "center_node": "source.cluster_id",
            "discovery_nodes": "discoveries[].cluster_id",
            "discovery_labels": "discoveries[].label",
            "edge_weight": "discoveries[].similarity",
            "node_color": "discoveries[].cpc_section",
        },
        "suggested_options": {"layout": "force_directed", "color_by_section": True},
    },
    "adversarial_strategy": {
        "recommended_chart": "territory_map",
        "title": "特許戦略分析",
        "data_mapping": {
            "overlap": "territory_map.overlap_clusters",
            "firm_a_exclusive": "territory_map.firm_a_exclusive",
            "firm_b_exclusive": "territory_map.firm_b_exclusive",
            "scenarios": "scenarios[].name",
        },
        "suggested_options": {"show_venn": True, "color_a": "#2563eb", "color_b": "#dc2626"},
    },
    "invention_intelligence": {
        "recommended_chart": "dashboard",
        "title": "発明インテリジェンス",
        "data_mapping": {
            "cluster": "landscape.primary_cluster",
            "prior_art_count": "prior_art.count",
            "prior_art_list": "prior_art.patents[]",
            "fto_risk": "fto_assessment.risk_level",
            "whitespace": "whitespace_opportunities[]",
        },
        "suggested_options": {"layout": "grid_2x2"},
    },
    "patent_market_fusion": {
        "recommended_chart": "radar",
        "title": "特許×市場融合分析",
        "data_mapping": {
            "dimensions": ["tech_strength", "growth_potential", "diversity", "market_sentiment"],
            "values": "components",
            "center_value": "fusion_score",
        },
        "suggested_options": {"show_benchmark": True, "fill_opacity": 0.3},
    },
}


def _inject_vis_hint(result, tool_name):
    """Add visualization_hint to result if missing and not an error."""
    if isinstance(result, dict) and "error" not in result and "visualization_hint" not in result:
        hint = _VIS_HINTS.get(tool_name)
        if hint:
            result["visualization_hint"] = hint
    return result


# =====================================================================
# Tool 1: patent_search
# =====================================================================
@mcp.tool()
def tool_patent_search(
    query: str | None = None,
    cpc_codes: list[str] | None = None,
    applicant: str | None = None,
    jurisdiction: str = "JP",
    date_from: str | None = None,
    date_to: str | None = None,
    max_results: int = 20,
    page: int = 1,
    page_size: int = 20,
) -> dict:
    """Search patents by keyword, CPC code, applicant, or date range.

    Scans the full patent table (13M+ JP patents). May be slow under heavy
    I/O load. For technology-area analysis, prefer tech_landscape (by CPC)
    or startability_ranking (by firm or cluster) which use pre-computed data.

    Args:
        query: Free-text search (Japanese or English). Searches title and abstract.
        cpc_codes: Filter by CPC classification codes (e.g., ["G06N3"]).
        applicant: Filter by applicant/assignee name (partial match).
        jurisdiction: Country code filter (default: "JP").
        date_from: Start date in YYYY-MM-DD format.
        date_to: End date in YYYY-MM-DD format.
        max_results: Maximum number of results (default: 20).

    Returns:
        Dict with patents list, result count, and total matching count.
    """
    return _safe_call(
        patent_search,
        store=_store,
        query=query,
        cpc_codes=cpc_codes,
        applicant=applicant,
        jurisdiction=jurisdiction,
        date_from=date_from,
        date_to=date_to,
        max_results=max_results,
        page=page,
        page_size=page_size,
        _tool_name="patent_search")

# =====================================================================
# Tool 2: firm_patent_portfolio
# =====================================================================
@mcp.tool()
def tool_firm_patent_portfolio(
    firm: str,
    date: str | None = None,
    include_expired: bool = False,
    detail_patents: bool = False,
) -> dict:
    """Get a firm's patent portfolio analysis.

    Accepts firm name in any language (Japanese/English) or stock ticker.
    Returns patent counts, technology distribution (CPC), filing trends,
    and co-applicant relationships.

    Args:
        firm: Company name (any language) or stock ticker (e.g., "7203").
        date: Cut-off date in YYYY-MM-DD format (default: all time).
        include_expired: Include expired patents in analysis.

    Returns:
        Dict with entity info, patent count, CPC distribution,
        filing trends, and co-applicants.
    """
    return _safe_call(firm_patent_portfolio,
        store=_store,
        resolver=_resolver,
        firm=firm,
        date=date,
        include_expired=include_expired,
        detail_patents=detail_patents,
        _tool_name="firm_patent_portfolio")

# =====================================================================
# Tool 3: patent_detail
# =====================================================================
@mcp.tool()
def tool_patent_detail(
    publication_number: str,
    include_full_text: bool = False,
    include_claims: bool = False,
) -> dict:
    """Get detailed information for a specific patent.

    Args:
        publication_number: Patent publication number (e.g., "JP-2020123456-A").
            JP granted patents use kind code B1: "JP-7637366-B1".
            JP applications use kind code A: "JP-2020-123456-A".

    Returns:
        Full patent record including CPC codes, assignees, inventors, and citations.
    """
    try:
        result = _store.get_patent(publication_number)
    except sqlite3.OperationalError as e:
        if "interrupted" in str(e):
            return {
                "error": "Query timed out — database under heavy I/O load.",
                "suggestion": "Try again later when ingestion completes.",
            }
        raise
    if result is None:
        return {"error": f"Patent not found: {publication_number}"}
    out = {
        "publication_number": result.get("publication_number"),
        "application_number": result.get("application_number"),
        "family_id": result.get("family_id"),
        "country_code": result.get("country_code"),
        "kind_code": result.get("kind_code"),
        "title_ja": result.get("title_ja"),
        "title_en": result.get("title_en"),
        "abstract_ja": result.get("abstract_ja"),
        "abstract_en": result.get("abstract_en"),
        "filing_date": result.get("filing_date"),
        "publication_date": result.get("publication_date"),
        "grant_date": result.get("grant_date"),
        "entity_status": result.get("entity_status"),
        "cpc_codes": result.get("cpc_codes", []),
        "assignees": result.get("assignees", []),
        "inventors": result.get("inventors", []),
        "citations_backward": result.get("citations_backward", []),
        "summary": {
            "top_applicants": [
                {
                    "name": a.get("harmonized_name") or a.get("raw_name"),
                    "firm_id": a.get("firm_id"),
                    "count": 1,
                }
                for a in result.get("assignees", [])[:5]
            ],
            "date_range": {
                "earliest": result.get("filing_date"),
                "latest": result.get("publication_date"),
            },
            "cpc_distribution": [
                {"cpc_class": c.get("cpc_code", "")[:4], "count": 1}
                for c in result.get("cpc_codes", [])[:10]
            ],
        },
    }

    if include_full_text:
        out["full_text"] = result.get("full_text")
    if include_claims:
        out["claims_text"] = result.get("claims_text")
    return _nfkc_normalize_response(_inject_vis_hint(out, "patent_detail"))

# =====================================================================
# Tool 4: entity_resolve
# =====================================================================
@mcp.tool()
def tool_entity_resolve(query: str, limit: int = 5) -> dict:
    """Resolve a company name to its canonical form.

    Accepts names in any language/format. Uses 3-level matching:
    exact, normalized, and fuzzy.

    Args:
        query: Company name to resolve (any language).
        limit: Maximum results to return.

    Returns:
        List of matching entities with confidence scores.
    """
    result = _resolver.resolve(query, country_hint="JP")
    results = []

    if result:
        e = result.entity
        results.append(
            {
                "canonical_id": e.canonical_id,
                "canonical_name": e.canonical_name,
                "country_code": e.country_code,
                "industry": e.industry,
                "ticker": e.ticker,
                "tse_section": e.tse_section,
                "confidence": round(result.confidence, 3),
                "match_level": result.match_level,
            }
        )

    # Also search for similar
    search_hits = _entity_registry.search(query, limit=limit)
    seen_ids = {result.entity.canonical_id} if result else set()
    for e in search_hits:
        if e.canonical_id not in seen_ids:
            seen_ids.add(e.canonical_id)
            results.append(
                {
                    "canonical_id": e.canonical_id,
                    "canonical_name": e.canonical_name,
                    "country_code": e.country_code,
                    "industry": e.industry,
                    "ticker": e.ticker,
                    "tse_section": e.tse_section,
                    "confidence": None,
                    "match_level": None,
                }
            )

    return _nfkc_normalize_response(_inject_vis_hint({"query": query, "results": results[:limit]}, "entity_resolve"))

# =====================================================================
# Tool 5: tech_landscape
# =====================================================================
@mcp.tool()
def tool_tech_landscape(
    cpc_prefix: str | None = None,
    query: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    granularity: str = "year",
    page: int = 1,
    page_size: int = 20,
) -> dict:
    """Analyze filing trends and top applicants in a technology area.

    Args:
        cpc_prefix: CPC prefix filter (e.g., "G06N").
        query: Optional free-text filter on title/abstract.
        date_from: Start date in YYYY-MM-DD format.
        date_to: End date in YYYY-MM-DD format.
        granularity: Time granularity, "year" or "quarter".

    Returns:
        CPC trend, top applicants, growth areas, and total patent count.
    """
    return _safe_call(
        tech_landscape,
        store=_store,
        cpc_prefix=cpc_prefix,
        query=query,
        date_from=date_from,
        date_to=date_to,
        granularity=granularity,
        page=page,
        page_size=page_size,
        _tool_name="tech_landscape")

# =====================================================================
# Tool 6: applicant_network
# =====================================================================
@mcp.tool()
def tool_applicant_network(
    applicant: str,
    depth: int = 1,
    min_co_patents: int = 5,
    page: int = 1,
    page_size: int = 20,
) -> dict:
    """Build co-applicant network for a target applicant.

    Args:
        applicant: Company name (any language) or stock ticker.
        depth: Traversal depth from center node.
        min_co_patents: Minimum shared patents to keep an edge.

    Returns:
        Center entity and graph-style nodes/edges of co-filing links.
    """
    return _safe_call(
        applicant_network,
        store=_store,
        resolver=_resolver,
        applicant=applicant,
        depth=depth,
        min_co_patents=min_co_patents,
        page=page,
        page_size=page_size,
        _tool_name="applicant_network")

# =====================================================================
# Tool 7: patent_compare
# =====================================================================
@mcp.tool()
def tool_patent_compare(
    firms: list[str],
    cpc_prefix: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict:
    """Compare multiple firms by patent volume, CPC mix, and trend.

    Args:
        firms: List of company names or tickers.
        cpc_prefix: Optional CPC prefix filter.
        date_from: Start date in YYYY-MM-DD format.
        date_to: End date in YYYY-MM-DD format.

    Returns:
        Per-firm portfolio metrics plus shared and unique CPC classes.
    """
    return _safe_call(
        patent_compare,
        store=_store,
        resolver=_resolver,
        firms=firms,
        cpc_prefix=cpc_prefix,
        date_from=date_from,
        date_to=date_to,
        _tool_name="patent_compare")

# =====================================================================
# Tool 8: firm_tech_vector
# =====================================================================
@mcp.tool()
def tool_firm_tech_vector(
    firm_query: str,
    year: int = 2024,
) -> dict:
    """Get a firm's precomputed technology vector and diversity metadata.

    Fast lookup from pre-computed table. Returns patent_count, dominant_cpc,
    tech_diversity (entropy), tech_concentration, and the full tech_vector.
    Data available for ~4,300 firms, years 2015-2024.

    Args:
        firm_query: Company name (any language), stock ticker, or firm_id.
        year: Analysis year (default: 2024). Falls back to latest available year.

    Returns:
        Dict with firm_id, year, patent_count, dominant_cpc, tech_diversity,
        tech_concentration, and tech_vector (float array).
    """
    return _safe_call(firm_tech_vector,
        store=_store,
        resolver=_resolver,
        firm_query=firm_query,
        year=year,
        _tool_name="firm_tech_vector")

# =====================================================================
# Tool 9: tech_clusters_list
# =====================================================================
@mcp.tool()
def tool_tech_clusters_list(
    sort_by: str = "patent_count",
    top_n: int = 200,
    cpc_filter: str | None = None,
    page: int = 1,
    page_size: int = 20,
) -> dict:
    """List technology clusters with optional CPC filtering.

    Returns 607 technology clusters derived from CPC classification.
    Each cluster has a label, patent_count, top_applicants (top firms),
    and top_terms (keywords). Use cpc_filter to narrow by CPC section
    (e.g., "H01" for electrical elements, "G06" for computing).

    Args:
        sort_by: Sort field — "patent_count" (default) or "cluster_id".
        top_n: Maximum clusters to return (default: 200, max: 2000).
        cpc_filter: CPC prefix filter (e.g., "H01", "G06N"). Partial match on cluster_id.
        page: Page number for pagination (default: 1).
        page_size: Results per page (default: 20).

    Returns:
        Paginated list of clusters with cluster_id (e.g., "H01M_0"),
        label, patent_count, top_applicants, and top_terms.
    """
    raw = _safe_call(tech_clusters_list,
        store=_store,
        sort_by=sort_by,
        top_n=top_n,
        cpc_filter=cpc_filter,
        page=page,
        page_size=page_size,
        _tool_name="tech_clusters_list")
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 10: tech_fit
# =====================================================================
@mcp.tool()
def tool_tech_fit(
    firm_query: str,
    tech_query_or_cluster_id: str,
    year: int = 2024,
) -> dict:
    """Compute phi_tech fit components for a firm and technology cluster.

    Measures how well a firm's patent portfolio aligns with a technology
    cluster. Returns CPC overlap ratio, citation proximity, co-inventor
    score, and combined phi_tech score.

    Args:
        firm_query: Company name (any language), stock ticker, or firm_id.
        tech_query_or_cluster_id: Cluster ID (e.g., "H01M_0", "G06N_0")
            or CPC code (e.g., "H01M"). Use tech_clusters_list to browse available clusters.
        year: Analysis year (default: 2024). Available: 2016-2024.

    Returns:
        Dict with phi_tech score and component breakdown (cpc_overlap,
        citation_proximity, co_inventor, combined).
    """
    return _inject_vis_hint(_enrich_firm_ids(tech_fit(
        store=_store,
        resolver=_resolver,
        firm_query=firm_query,
        tech_query_or_cluster_id=tech_query_or_cluster_id,
        year=year,
    )), "tech_fit")

# =====================================================================
# Tool 11: startability
# =====================================================================
@mcp.tool()
def tool_startability(
    firm_query: str,
    tech_query_or_cluster_id: str,
    year: int = 2024,
) -> dict:
    """Compute startability score for a firm-technology pair.

    Startability measures a firm's readiness to enter or compete in a
    technology area, combining tech_fit (phi_tech), organizational
    capability, and dynamic trajectory. Uses pre-computed surface data
    for fast lookup.

    Args:
        firm_query: Company name (any language), stock ticker, or firm_id.
        tech_query_or_cluster_id: Cluster ID (e.g., "H01M_0", "G06N_0")
            or CPC code. Use tech_clusters_list to browse available clusters.
        year: Analysis year (default: 2024). Available: 2016-2024.

    Returns:
        Dict with overall startability score (0-1), component breakdown,
        and interpretation.
    """
    return _inject_vis_hint(_enrich_firm_ids(startability(
        store=_store,
        resolver=_resolver,
        firm_query=firm_query,
        tech_query_or_cluster_id=tech_query_or_cluster_id,
        year=year,
    )), "startability")

# =====================================================================
# Tool 12: startability_ranking
# =====================================================================
@mcp.tool()
def tool_startability_ranking(
    mode: str,
    query: str,
    year: int = 2024,
    top_n: int = 20,
    page: int = 1,
    page_size: int = 20,
) -> dict:
    """Rank tech clusters or firms using precomputed startability surface.

    Two modes: "by_firm" ranks technology clusters by startability for a
    given firm (what tech areas can this firm enter?). "by_tech" ranks
    firms by startability for a given cluster (who can enter this tech?).

    Args:
        mode: "by_firm" (rank clusters for one firm) or "by_tech" (rank firms for one cluster).
        query: Firm name/ticker (by_firm) or cluster_id like "H01M_0" (by_tech).
        year: Analysis year (default: 2024). Available: 2016-2024.
        top_n: Number of results (default: 20).
        page: Page number for pagination (default: 1).
        page_size: Results per page (default: 20).

    Returns:
        Ranked list with firm_id/cluster_id, score, and metadata.
    """
    return _inject_vis_hint(_enrich_firm_ids(startability_ranking(
        store=_store,
        resolver=_resolver,
        mode=mode,
        query=query,
        year=year,
        top_n=top_n,
        page=page,
        page_size=page_size,
    )), "startability_ranking")

# =====================================================================
# Tool 13: startability_delta
# =====================================================================
@mcp.tool()
def tool_startability_delta(
    mode: str,
    query: str,
    year_from: int = 2020,
    year_to: int = 2024,
    top_n: int = 20,
    direction: str = "gainers",
    page: int = 1,
    page_size: int = 20,
) -> dict:
    """Compute change in startability scores over time.

    Shows which firm-technology pairs gained or lost the most startability.
    Requires multi-year startability surface data.

    Args:
        mode: "by_firm" (cluster deltas for one firm) or "by_tech" (firm deltas for one cluster).
        query: Firm name/ticker (by_firm) or cluster_id/CPC code (by_tech).
        year_from: Start year (default: 2020).
        year_to: End year (default: 2024).
        top_n: Number of results (default: 20).
        direction: "gainers" (highest delta), "losers" (lowest delta), or "both".

    Returns:
        Ranked list of pairs by startability delta with score_start, score_end, delta.
    """
    raw = _safe_call(
        startability_delta,
        store=_store,
        resolver=_resolver,
        mode=mode,
        query=query,
        year_from=year_from,
        year_to=year_to,
        top_n=top_n,
        direction=direction,
        page=page,
        page_size=page_size,
        _tool_name="startability_delta")
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 14: gdelt_company_events
# =====================================================================
@mcp.tool()
def tool_gdelt_company_events(
    firm_query: str,
    date_from: int | None = None,
    date_to: int | None = None,
) -> dict:
    """Fetch GDELT events/GKG and cached five-axis features for a firm.

    Returns pre-cached GDELT media signals: tone, event_count,
    theme_diversity, geographic_spread, and source_diversity.
    Data covers 2020Q1-2024Q4 for ~46 major firms.

    Args:
        firm_query: Company name (any language) or stock ticker.
        date_from: Start date as YYYYMMDD integer (e.g., 20200101).
        date_to: End date as YYYYMMDD integer (e.g., 20241231).

    Returns:
        Dict with five-axis features, event timeline, and summary.
    """
    return _safe_call(gdelt_company_events,
        store=_store,
        resolver=_resolver,
        firm_query=firm_query,
        date_from=date_from,
        date_to=date_to,
        _tool_name="gdelt_company_events")

# =====================================================================
# Tool 15: cross_domain_discovery
# =====================================================================
@mcp.tool()
def tool_cross_domain_discovery(
    query: str,
    top_n: int = 10,
    exclude_same_domain: bool = True,
    min_similarity: float = 0.3,
) -> dict:
    """Discover cross-domain technology clusters related to a query.

    Finds technology clusters in different CPC sections that share
    embedding-space proximity with the source domain.

    Args:
        query: CPC code (e.g. "H01M") or free-text technology description.
        top_n: Maximum number of results (default: 10).
        exclude_same_domain: Exclude clusters from the same CPC section (default: True).
        min_similarity: Minimum cosine similarity threshold (default: 0.3).

    Returns:
        Source info, list of cross-domain cluster discoveries with
        momentum, top players, bridging patents, and connection hypotheses.
    """
    raw = _safe_call(
        cross_domain_discovery,
        store=_store,
        query=query,
        top_n=top_n,
        exclude_same_domain=exclude_same_domain,
        min_similarity=min_similarity,
        _tool_name="cross_domain_discovery")
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 16: adversarial_strategy
# =====================================================================
@mcp.tool()
def tool_adversarial_strategy(
    firm_a: str,
    firm_b: str,
    year: int = 2024,
    scenario_count: int = 3,
) -> dict:
    """Compare two firms' patent portfolios and generate strategic scenarios.

    Performs game-theoretic analysis of two firms' technology territories,
    identifying attack targets, defense priorities, and preemption opportunities.

    Args:
        firm_a: First company name (any language) or ticker.
        firm_b: Second company name (any language) or ticker.
        year: Analysis year (default: 2024).
        scenario_count: Number of strategic scenarios to generate (default: 3).

    Returns:
        Overview (overlap, tech distance, negotiation power), territory map
        (overlap/exclusive/unclaimed clusters), and strategic scenarios.
    """
    raw = _safe_call(adversarial_strategy,
        store=_store,
        resolver=_resolver,
        firm_a=firm_a,
        firm_b=firm_b,
        year=year,
        scenario_count=scenario_count,
        _tool_name="adversarial_strategy")
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 17: invention_intelligence
# =====================================================================
@mcp.tool()
def tool_invention_intelligence(
    text: str,
    max_prior_art: int = 20,
    include_fto: bool = True,
    include_whitespace: bool = True,
) -> dict:
    """Analyze a technology description for prior art, FTO risk, and whitespace.

    Given a natural language description of an invention or technology,
    identifies the relevant patent cluster, finds prior art, assesses
    freedom-to-operate risk, and discovers whitespace opportunities.

    Args:
        text: Technology description in natural language (Japanese or English).
        max_prior_art: Maximum number of prior art results (default: 20).
        include_fto: Include FTO risk assessment (default: True).
        include_whitespace: Include whitespace opportunity analysis (default: True).

    Returns:
        Cluster landscape, prior art list, FTO assessment,
        whitespace opportunities, and strategic recommendations.
    """
    return _safe_call(
        invention_intelligence,
        store=_store,
        text=text,
        max_prior_art=max_prior_art,
        include_fto=include_fto,
        include_whitespace=include_whitespace,
        _tool_name="invention_intelligence", _timeout=60)

# =====================================================================
# Tool 18: patent_market_fusion
# =====================================================================
@mcp.tool()
def tool_patent_market_fusion(
    query: str,
    query_type: str | None = None,
    purpose: str = "general",
    year: int = 2024,
    max_results: int = 10,
) -> dict:
    """Combine patent portfolio strength with market signals for strategic analysis.

    Produces a fusion score combining tech_strength, growth_potential,
    diversity, and market_sentiment (GDELT). Supports investment screening,
    M&A target identification, and license partner matching.

    Args:
        query: Company name, technology description, CPC code, or patent publication number (e.g., JP-7637366-B1).
        query_type: "firm", "technology", "text", or "patent". Auto-detected if None.
        purpose: Analysis purpose — "investment", "ma_target", "license_match",
                 or "general" (default: "general"). Changes component weights.
        year: Analysis year (default: 2024).
        max_results: Max firms to return in technology mode (default: 10).

    Returns:
        Fusion score with component breakdown. In firm mode: single firm analysis.
        In technology mode: ranked list of firms for the technology area.
    """
    raw = _safe_call(patent_market_fusion,
        store=_store,
        resolver=_resolver,
        query=query,
        query_type=query_type,
        purpose=purpose,
        year=year,
        max_results=max_results,
        _tool_name="patent_market_fusion")
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 19: similar_firms
# =====================================================================
@mcp.tool()
def tool_similar_firms(
    firm_query: str,
    top_n: int = 10,
    year: int = 2024,
) -> dict:
    """Discover firms with similar patent portfolios.

    Computes cosine similarity between firm technology vectors across ~4,300
    companies. Useful for M&A candidate discovery, partnership identification,
    and competitive benchmarking.

    Args:
        firm_query: Company name (any language) or stock ticker.
        top_n: Number of similar firms to return (default: 10).
        year: Analysis year (default: 2024).

    Returns:
        Ranked list of similar firms with similarity scores, shared clusters,
        and unique technology strengths.
    """
    raw = _safe_call(similar_firms,
        store=_store, resolver=_resolver,
        firm_query=firm_query, top_n=top_n, year=year)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 20: tech_gap
# =====================================================================
@mcp.tool()
def tool_tech_gap(
    firm_a: str,
    firm_b: str,
    year: int = 2024,
) -> dict:
    """Analyze technology gap and synergy between two firms.

    Quantifies complementarity, overlap, and acquisition fit using
    startability surface data. Useful for M&A due diligence, partnership
    evaluation, and competitive positioning.

    Args:
        firm_a: First company name or ticker.
        firm_b: Second company name or ticker.
        year: Analysis year (default: 2024).

    Returns:
        Overlap areas, each firm's strengths, synergy score,
        overlap score, and acquisition fit classification.
    """
    raw = _safe_call(tech_gap,
        store=_store, resolver=_resolver,
        firm_a=firm_a, firm_b=firm_b, year=year)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 21: cross_border_similarity
# =====================================================================
@mcp.tool()
def tool_cross_border_similarity(
    query: str,
    query_type: str = "firm",
    target_jurisdictions: list[str] | None = None,
    min_similarity: float = 0.7,
    time_window: str = "all",
    top_n: int = 20,
) -> dict:
    """Detect similar patent filings across international jurisdictions.

    Finds patents in target countries that match a firm's portfolio, a specific
    patent, or a technology description. Useful for FTO monitoring, licensing
    opportunities, and portfolio surveillance. Always includes legal disclaimer.

    Args:
        query: Firm name, publication number, or technology description.
        query_type: "firm", "patent", or "text" (default: "firm").
        target_jurisdictions: Country codes to search (default: ["CN","KR","US","EP"]).
        min_similarity: Minimum CPC overlap threshold (default: 0.7).
        time_window: "after", "before", or "all" relative to source filing.
        top_n: Maximum results (default: 20).

    Returns:
        Similar filings with similarity scores, time lags, CPC overlap,
        family relationships, and legal disclaimer note.
    """
    raw = _safe_call(cross_border_similarity,
        store=_store, resolver=_resolver,
        query=query, query_type=query_type,
        target_jurisdictions=target_jurisdictions,
        min_similarity=min_similarity,
        time_window=time_window, top_n=top_n)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 22: patent_valuation
# =====================================================================
@mcp.tool()
def tool_patent_valuation(
    query: str,
    query_type: str = "firm",
    purpose: str = "portfolio_ranking",
) -> dict:
    """Score patent or portfolio value with royalty rate reference.

    Evaluates patents on citation impact, family breadth, technology relevance,
    remaining life, and market size. Provides industry-specific royalty rate
    benchmarks from public statistics.

    Args:
        query: Company name or patent publication number.
        query_type: "firm" (portfolio analysis) or "patent" (single patent).
        purpose: "licensing", "portfolio_ranking", or "divestiture".

    Returns:
        Patent/portfolio score with component breakdown, value tier,
        and industry royalty rate reference with disclaimer.
    """
    raw = _safe_call(patent_valuation,
        store=_store, resolver=_resolver,
        query=query, query_type=query_type, purpose=purpose)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 23: portfolio_evolution
# =====================================================================
@mcp.tool()
def tool_portfolio_evolution(
    firm_query: str,
    year_from: int = 2016,
    year_to: int = 2024,
) -> dict:
    """Track how a firm's technology portfolio evolved over time.

    Shows year-by-year changes in dominant technology areas, diversity,
    and concentration. Identifies emerging and declining technology
    investments. Summarizes strategic shifts.

    Args:
        firm_query: Company name (any language) or stock ticker.
        year_from: Start year (default: 2016).
        year_to: End year (default: 2024).

    Returns:
        Timeline with yearly metrics, emerging/declining clusters,
        and strategic shift summary.
    """
    raw = _safe_call(portfolio_evolution,
        store=_store, resolver=_resolver,
        firm_query=firm_query, year_from=year_from, year_to=year_to)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 24: tech_trend_alert
# =====================================================================
@mcp.tool()
def tool_tech_trend_alert(
    year_from: int = 2020,
    year_to: int = 2024,
    min_growth: float = 0.3,
    top_n: int = 20,
) -> dict:
    """Detect hot and cooling technology trends with market signals.

    Automatically identifies rapidly growing technology clusters, firms
    entering multiple hot areas, and declining clusters. Integrates GDELT
    media signals where available. All signals are expressed probabilistically.

    Args:
        year_from: Start year for trend analysis (default: 2020).
        year_to: End year (default: 2024).
        min_growth: Minimum growth rate to flag as "hot" (default: 0.3).
        top_n: Maximum results per category (default: 20).

    Returns:
        Hot clusters with top entrants, rising firms, cooling clusters,
        and evidence-backed signals.
    """
    raw = _safe_call(tech_trend_alert,
        store=_store,
        year_from=year_from, year_to=year_to,
        min_growth=min_growth, top_n=top_n)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 25: sales_prospect
# =====================================================================
@mcp.tool()
def tool_sales_prospect(
    firm_query: str,
    patent_or_tech: str,
    query_type: str = "cluster",
    target_count: int = 10,
) -> dict:
    """Identify and rank patent licensing sales targets.

    Finds firms that need your technology, generates "why they need it"
    narratives, and provides approach guides with deal structure
    recommendations. No contact information is included.

    Args:
        firm_query: Licensor company name (your firm).
        patent_or_tech: Patent number, technology description, or cluster_id.
        query_type: "patent", "text", or "cluster" (default: "cluster").
        target_count: Number of prospects to return (default: 10).

    Returns:
        Ranked prospects with fit scores, urgency, why_they_need_it
        narratives, evidence, and approach guides.
    """
    raw = _safe_call(sales_prospect,
        store=_store, resolver=_resolver,
        firm_query=firm_query, patent_or_tech=patent_or_tech,
        query_type=query_type, target_count=target_count)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 26: bayesian_scenario
# =====================================================================
@mcp.tool()
def tool_bayesian_scenario(
    mode: str = "init",
    technology: str | None = None,
    firm_query: str | None = None,
    investment_cost: float = 10000,
    time_horizon_years: int = 10,
    session_id: str | None = None,
    parameter: str | None = None,
    user_value: float | None = None,
    user_confidence: float = 0.5,
) -> dict:
    """Bayesian patent investment simulation with data-driven priors.

    Three modes: "init" builds priors from patent data, "update" incorporates
    user's private information via Bayesian updating, "simulate" runs Monte
    Carlo simulation. All prior parameters are evidenced by patent/GDELT data.

    Args:
        mode: "init" (start scenario), "update" (adjust parameter), "simulate" (run).
        technology: Cluster ID or tech description (required for init).
        firm_query: Optional firm name for context.
        investment_cost: Investment amount in 万円 (default: 10000).
        time_horizon_years: Analysis period (default: 10).
        session_id: Session ID from init (required for update/simulate).
        parameter: Parameter name to update (for update mode).
        user_value: User's estimated value (for update mode).
        user_confidence: 0-1 confidence in user value (default: 0.5).

    Returns:
        Init: priors with evidence. Update: posterior distribution.
        Simulate: NPV distribution, cashflow, breakeven, sensitivity.
    """
    return _safe_call(bayesian_scenario,
        store=_store, resolver=_resolver,
        mode=mode, technology=technology,
        firm_query=firm_query, investment_cost=investment_cost,
        time_horizon_years=time_horizon_years,
        session_id=session_id, parameter=parameter,
        user_value=user_value, user_confidence=user_confidence)
# =====================================================================
# Infrastructure: argument parsing, cache warm-up, HTTP app
# =====================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Patent Space MCP server")
    parser.add_argument(
        "--transport",
        choices=("stdio", "http"),
        default="stdio",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    return parser.parse_args()

def _warm_cache() -> dict[str, int]:
    """Pre-warm SQLite page cache by reading all rows of small hot tables.

    On a 310GB HDD-backed DB, cold queries are extremely slow because
    table pages are scattered across the file. By reading entire hot
    tables at startup, the OS page cache holds them for all subsequent
    queries, bringing response times from minutes to seconds.

    Uses a raw SQLite connection (no progress handler) to avoid timeout
    interrupts — pre-warm queries can take minutes on HDD.
    """
    import logging
    import time as _time

    log = logging.getLogger("patent-space-mcp")
    t0 = _time.monotonic()
    counts: dict[str, int] = {}

    # Use a raw connection WITHOUT progress handler (no timeout)
    raw_conn = sqlite3.connect(str(_store.db_path), timeout=300)
    raw_conn.execute("PRAGMA journal_mode=WAL")
    raw_conn.execute("PRAGMA cache_size=-2000000")
    raw_conn.execute("PRAGMA mmap_size=8589934592")
    raw_conn.execute("PRAGMA read_uncommitted=ON")

    # Read rows via cursor iteration to populate OS page cache.
    # startability_surface (10M rows) is included because its data pages
    # are needed for by_tech queries. Takes ~90s on HDD but makes all
    # subsequent queries instant.
    hot_tables = [
        ("tech_clusters", "SELECT * FROM tech_clusters"),
        ("tech_cluster_momentum", "SELECT * FROM tech_cluster_momentum"),
        ("firm_tech_vectors", "SELECT * FROM firm_tech_vectors"),
        ("gdelt_company_features", "SELECT * FROM gdelt_company_features"),
        ("startability_surface",
         "SELECT firm_id, cluster_id, year, score FROM startability_surface"),
    ]

    for table, query in hot_tables:
        try:
            t_start = _time.monotonic()
            cursor = raw_conn.execute(query)
            row_count = 0
            while cursor.fetchone() is not None:
                row_count += 1
            counts[table] = row_count
            elapsed_t = _time.monotonic() - t_start
            log.warning("Warmed %s: %d rows in %.1fs", table, row_count, elapsed_t)
        except Exception as exc:
            counts[table] = -1
            log.warning("Failed to warm %s: %s", table, exc)

    # Touch FTS5 index pages with representative queries across languages
    fts5_queries = [
        '\"test\"',
        '\"電池\"',
        '\"battery\"',
        '\"半導体\"',
        '\"semiconductor\"',
        '\"人工知能\"',
        '\"machine learning\"',
        '\"自動運転\"',
        '\"autonomous\"',
        '\"医薬\"',
    ]
    fts5_warmed = 0
    t_start = _time.monotonic()
    for fq in fts5_queries:
        try:
            raw_conn.execute(
                f"SELECT COUNT(*) FROM patents_fts WHERE patents_fts MATCH {fq!r}"
            ).fetchone()
            fts5_warmed += 1
        except Exception:
            pass
    elapsed_t = _time.monotonic() - t_start
    counts["fts5"] = fts5_warmed
    log.warning("FTS5 warmed %d/%d queries in %.1fs", fts5_warmed, len(fts5_queries), elapsed_t)

    raw_conn.close()
    elapsed = _time.monotonic() - t0
    log.info("Cache warm-up completed in %.1fs: %s", elapsed, counts)
    return counts

def _custom_http_app():
    """Wrap MCP ASGI app with a /health endpoint."""
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Mount, Route

    async def health(request):
        try:
            conn = _store._conn()
            # Use fast approximate count to avoid full table scan during I/O contention
            try:
                # sqlite_stat1 is populated by ANALYZE and gives fast row estimates
                row = conn.execute(
                    "SELECT stat FROM sqlite_stat1 WHERE tbl='patents' LIMIT 1"
                ).fetchone()
                if row and row[0]:
                    patent_count = int(str(row[0]).split()[0])
                else:
                    # Fall back to exact count only if stat1 not available
                    row = conn.execute("SELECT COUNT(*) FROM patents").fetchone()
                    patent_count = row[0] if row else 0
            except Exception:
                # Last resort: use max(rowid) as estimate
                try:
                    row = conn.execute("SELECT MAX(rowid) FROM patents").fetchone()
                    patent_count = row[0] if row and row[0] else 0
                except Exception:
                    patent_count = -1

            tc = conn.execute("SELECT COUNT(*) FROM tech_clusters").fetchone()
            cluster_count = tc[0] if tc else 0
            db_ok = True
        except Exception:
            patent_count = 0
            cluster_count = 0
            db_ok = False
        # Collect registered tool names
        _ALL_TOOL_NAMES = sorted(set(list(_VIS_HINTS.keys()) + [
            "similar_firms", "tech_gap", "cross_border_similarity",
            "patent_valuation", "portfolio_evolution", "tech_trend_alert",
            "sales_prospect", "bayesian_scenario",
        ]))
        return JSONResponse({
            "status": "ok" if db_ok else "degraded",
            "tools": len(_ALL_TOOL_NAMES),
            "tool_names": _ALL_TOOL_NAMES,
            "db_path": _db_path,
            "patent_count": patent_count,
            "cluster_count": cluster_count,
            "display_names_loaded": len(_FIRM_DISPLAY_NAMES),
        })

    mcp_app = mcp.http_app(path="/mcp")
    return Starlette(
        routes=[
            Route("/health", health),
            Mount("/", app=mcp_app),
        ],
        lifespan=mcp_app.lifespan,
    )


def _fix_cluster_labels():
    """One-time fix for tech_cluster labels with bad translations."""
    import logging
    log = logging.getLogger("patent-space-mcp")
    updates = [
        ("B60Y_0", "B60Y: \u8eca\u4e21\u5206\u985e (Vehicle classification index)"),
        ("C10N_0", "C10N: \u6f64\u6ed1\u5264\u5206\u985e (Lubricant classification index)"),
        ("C12R_0", "C12R: \u5fae\u751f\u7269\u5206\u985e (Microorganism classification index)"),
        ("C12Y_0", "C12Y: \u9175\u7d20\u5206\u985e (Enzyme classification index)"),
        ("F05B_0", "F05B: \u98a8\u529b\u30fb\u6c34\u529b\u6a5f\u95a2 (Wind/water power machines)"),
        ("F05C_0", "F05C: \u6a5f\u95a2\u7528\u6750\u6599 (Engine materials)"),
        ("F05D_0", "F05D: \u975e\u5bb9\u7a4d\u578b\u6a5f\u95a2 (Non-positive displacement engines)"),
        ("F21W_0", "F21W: \u7167\u660e\u5fdc\u7528\u5206\u985e (Lighting application index)"),
        ("F21Y_0", "F21Y: \u7167\u660e\u5149\u6e90\u5206\u985e (Lighting source index)"),
        ("Y10S_0", "Y10S: \u65e7\u7c73\u56fd\u5206\u985e (Legacy US classification)"),
        ("Y10T_0", "Y10T: \u65e7\u7c73\u56fd\u5206\u985e\u30fb\u7d9a (Legacy US classification continued)"),
    ]
    try:
        conn = _store._conn()
        updated = 0
        for cid, label in updates:
            row = conn.execute("SELECT label FROM tech_clusters WHERE cluster_id = ?", (cid,)).fetchone()
            if row and "indexing" in (row[0] or "").lower() or (row and "classification" in (row[0] or "").lower()):
                conn.execute("UPDATE tech_clusters SET label = ? WHERE cluster_id = ?", (label, cid))
                updated += 1
        if updated > 0:
            conn.commit()
            log.warning("Fixed %d tech_cluster labels", updated)
    except Exception as e:
        log.warning("Label migration skipped: %s", e)

def main() -> None:
    args = parse_args()
    if args.transport == "stdio":
        mcp.run(transport="stdio")
    else:
        import threading
        import uvicorn

        _fix_cluster_labels()
        # Pre-warm cache in background thread (non-blocking startup)
        threading.Thread(target=_warm_cache, daemon=True).start()
        app = _custom_http_app()
        uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main()
