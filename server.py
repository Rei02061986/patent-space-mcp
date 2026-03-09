"""Patent Space MCP server entry point."""
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import unicodedata
from pathlib import Path
from typing import Annotated

from pydantic import Field

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
from tools.citation_network import citation_network
from tools.tech_trend import tech_trend
from tools.ma_target import ma_target
from tools.patent_finance import patent_option_value, tech_volatility, portfolio_var, tech_beta
from tools.ip_due_diligence import ip_due_diligence
from tools.network_analysis import network_topology, knowledge_flow, network_resilience, tech_fusion_detector, tech_entropy
from tools.sep_analysis import sep_search, sep_landscape, sep_portfolio, frand_analysis
from tools.corporate_hierarchy import corporate_hierarchy, group_portfolio, group_startability
from tools.claim_analysis import claim_analysis, claim_comparison, fto_analysis
from tools.ai_classifier import create_category, classify_patents, category_landscape, portfolio_benchmark

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

# ── Speed optimizations for read-heavy workload ──
def _apply_read_pragmas():
    """Apply SQLite PRAGMAs for read performance on NVMe."""
    try:
        conn = _store._conn()
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA cache_size=-131072")   # 128MB page cache (was default ~2MB)
        conn.execute("PRAGMA mmap_size=4294967296")  # 4GB mmap for fast random reads
        conn.execute("PRAGMA read_uncommitted=ON")   # Don't wait for WAL writer
        conn.execute("PRAGMA temp_store=MEMORY")     # temp tables in RAM
        conn.execute("PRAGMA busy_timeout=10000")    # 10s retry on locks
        print(f"Applied read-optimized PRAGMAs (cache=128MB, mmap=4GB)")
    except Exception as e:
        print(f"Warning: Could not apply PRAGMAs: {e}")

_apply_read_pragmas()

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
    "citation_network": {
        "recommended_chart": "network",
        "title": "引用ネットワーク",
        "data_mapping": {"nodes": "nodes[].id", "edges": "edges[]", "node_size": "nodes[].forward_citations"},
    },
    "tech_trend": {
        "recommended_chart": "line_with_bar",
        "title": "技術トレンド",
        "data_mapping": {"x": "timeline[].year", "y_line": "timeline[].avg_growth_rate", "y_bar": "timeline[].total_patent_count"},
    },
    "ma_target": {
        "recommended_chart": "scatter",
        "title": "M&Aターゲット",
        "data_mapping": {"x": "results[].tech_similarity", "y": "results[].synergy_score", "size": "results[].patent_count"},
    },
    "patent_option_value": {
        "recommended_chart": "bar",
        "title": "特許オプション価値",
        "data_mapping": {"labels": "top_value_patents[].patent", "values": "top_value_patents[].option_value"},
    },
    "tech_volatility": {
        "recommended_chart": "dual_axis",
        "title": "技術ボラティリティ",
        "data_mapping": {"x": "timeline[].year", "y_left": "timeline[].patent_count", "y_right": "timeline[].log_return"},
    },
    "portfolio_var": {
        "recommended_chart": "stacked_bar",
        "title": "ポートフォリオVaR",
        "data_mapping": {"labels": "var_at_risk_cpc[].cpc", "values": "var_at_risk_cpc[].loss_rate"},
    },
    "tech_beta": {
        "recommended_chart": "scatter",
        "title": "技術ベータ",
        "data_mapping": {"x": "market_return", "y": "tech_return", "label": "cpc_prefix"},
    },
    "network_topology": {
        "recommended_chart": "network",
        "title": "引用ネットワークトポロジー",
        "data_mapping": {"nodes": "hub_patents[].patent", "node_size": "hub_patents[].cited_by"},
    },
    "knowledge_flow": {
        "recommended_chart": "sankey",
        "title": "知識フロー",
        "data_mapping": {"source": "flow_pairs[].from", "target": "flow_pairs[].to", "value": "flow_pairs[].count"},
    },
    "network_resilience": {
        "recommended_chart": "line",
        "title": "ネットワークレジリエンス",
        "data_mapping": {"x": "removal_pct", "y": "largest_component_pct", "series": ["targeted", "random"]},
    },
    "tech_fusion_detector": {
        "recommended_chart": "line",
        "title": "技術融合検出",
        "data_mapping": {"x": "fusion_timeline.years", "y": "fusion_timeline.co_citation_count"},
    },
    "tech_entropy": {
        "recommended_chart": "dual_axis",
        "title": "技術エントロピー",
        "data_mapping": {"x": "entropy_timeline[].year", "y_left": "entropy_timeline[].entropy", "y_right": "entropy_timeline[].total_filings"},
    },
    # ── SEP Analysis ──
    "sep_search": {
        "recommended_chart": "table",
        "title": "SEP宣言検索",
        "data_mapping": {"columns": ["patent_number", "standard_name", "declarant", "declaration_date"]},
        "suggested_options": {"sortable": True, "page_size": 20},
    },
    "sep_landscape": {
        "recommended_chart": "treemap",
        "title": "標準必須特許ランドスケープ",
        "data_mapping": {"labels": "top_declarants[].declarant", "values": "top_declarants[].count", "color": "top_declarants[].share"},
    },
    "sep_portfolio": {
        "recommended_chart": "pie_and_bar",
        "title": "SEPポートフォリオ",
        "data_mapping": {"pie_labels": "standards_covered[].standard", "pie_values": "standards_covered[].count", "bar_labels": "yearly_trend[].year", "bar_values": "yearly_trend[].count"},
    },
    "frand_analysis": {
        "recommended_chart": "stacked_bar",
        "title": "FRAND分析",
        "data_mapping": {"labels": "top_holders[].declarant", "values": "top_holders[].count", "share": "top_holders[].share"},
    },
    # ── Corporate Hierarchy ──
    "corporate_hierarchy": {
        "recommended_chart": "tree",
        "title": "企業グループ構造",
        "data_mapping": {"root": "root.firm_name", "children": "root.children[].firm_name"},
    },
    "group_portfolio": {
        "recommended_chart": "stacked_bar",
        "title": "グループ特許ポートフォリオ",
        "data_mapping": {"labels": "members[].firm_name", "values": "members[].patent_count"},
    },
    "group_startability": {
        "recommended_chart": "horizontal_bar",
        "title": "グループStartability",
        "data_mapping": {"labels": "member_scores[].firm_name", "values": "member_scores[].score"},
        "suggested_options": {"sort": "descending", "show_values": True},
    },
    # ── Claim Analysis ──
    "claim_analysis": {
        "recommended_chart": "card",
        "title": "特許スコープ分析",
        "data_mapping": {"title": "patent.title", "scope": "scope_assessment.scope_level", "cpcs": "scope_assessment.all_cpcs"},
    },
    "claim_comparison": {
        "recommended_chart": "heatmap",
        "title": "特許比較分析",
        "data_mapping": {"x": "patents[].publication_number", "y": "patents[].publication_number", "values": "pairwise_similarity[].combined"},
    },
    "fto_analysis": {
        "recommended_chart": "dashboard",
        "title": "FTO分析",
        "data_mapping": {"risk_level": "risk_assessment.overall_risk", "blocking": "blocking_patents[]", "timeline": "expiry_timeline[]"},
        "suggested_options": {"layout": "grid_2x2"},
    },
    # ── AI Classifier ──
    "create_category": {
        "recommended_chart": "card",
        "title": "カテゴリ作成",
        "data_mapping": {"title": "category_name", "count": "initial_patent_count"},
    },
    "classify_patents": {
        "recommended_chart": "table",
        "title": "特許分類結果",
        "data_mapping": {"columns": ["publication_number", "title", "confidence", "method"]},
    },
    "category_landscape": {
        "recommended_chart": "line_with_bar",
        "title": "カテゴリランドスケープ",
        "data_mapping": {"x": "timeline[].year", "y_bar": "timeline[].patent_count", "y_line": "timeline[].growth_rate"},
    },
    "portfolio_benchmark": {
        "recommended_chart": "horizontal_bar",
        "title": "ポートフォリオベンチマーク",
        "data_mapping": {"labels": "peer_ranking[].firm_name", "values": "peer_ranking[].patent_count"},
        "suggested_options": {"highlight_firm": True, "sort": "descending"},
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
# Tool 27: citation_network
# =====================================================================
@mcp.tool()
def tool_citation_network(
    publication_number: Annotated[str | None, Field(description="Seed patent publication number (e.g., 'JP-7637366-B1'). Required for patent mode.")] = None,
    firm_query: Annotated[str | None, Field(description="Company name or ticker for firm mode. Finds top-cited patents as seeds.")] = None,
    depth: Annotated[int, Field(description="BFS traversal depth (1 or 2). Default: 1.")] = 1,
    direction: Annotated[str, Field(description='"forward" (who cites this), "backward" (what this cites), or "both" (default).')] = "both",
    max_nodes: Annotated[int, Field(description="Maximum nodes in the network (default: 50, max: 200).")] = 50,
) -> dict:
    """Build a patent citation network around a patent or firm's top patents.

    Two modes: patent mode (BFS from a single patent) or firm mode
    (finds firm's most-cited patents, builds network from those).
    Returns graph structure with nodes, edges, and hub patent identification.
    """
    raw = _safe_call(citation_network, _timeout=120,
        store=_store, resolver=_resolver,
        publication_number=publication_number, firm_query=firm_query,
        depth=depth, direction=direction, max_nodes=max_nodes)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 28: tech_trend
# =====================================================================
@mcp.tool()
def tool_tech_trend(
    query: Annotated[str, Field(description="Technology keyword (e.g., '全固体電池'), CPC code (e.g., 'H01M'), or cluster_id.")] = "",
    cpc_prefix: Annotated[str | None, Field(description="Optional CPC prefix filter (e.g., 'H01M').")] = None,
    year_from: Annotated[int, Field(description="Start year (default: 2016).")] = 2016,
    year_to: Annotated[int, Field(description="End year (default: 2024).")] = 2024,
    top_n: Annotated[int, Field(description="Maximum results per category (default: 20).")] = 20,
) -> dict:
    """Analyze time-series technology trends with growth rates and new entrants.

    Given a technology query, returns year-by-year filing trends, growth rates,
    acceleration, new entrant detection, and sub-area breakdown. Supports
    Japanese/English keywords, CPC codes, and cluster IDs.
    """
    raw = _safe_call(tech_trend, _timeout=120,
        store=_store, resolver=_resolver,
        query=query, cpc_prefix=cpc_prefix,
        year_from=year_from, year_to=year_to, top_n=top_n)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 29: ma_target
# =====================================================================
@mcp.tool()
def tool_ma_target(
    acquirer: Annotated[str, Field(description="Acquiring company name (any language) or stock ticker.")],
    strategy: Annotated[str, Field(description='"tech_gap" (complementary), "consolidation" (overlap), or "diversification" (new markets).')] = "tech_gap",
    top_n: Annotated[int, Field(description="Number of target candidates (default: 10).")] = 10,
    year: Annotated[int, Field(description="Analysis year (default: 2024).")] = 2024,
) -> dict:
    """Recommend M&A acquisition targets based on patent portfolio analysis.

    Three strategies: "tech_gap" finds firms with complementary technology
    (fills acquirer's weak areas), "consolidation" finds firms with high
    overlap (strengthens market position), "diversification" finds firms
    in unrelated CPC sections (opens new markets).
    """
    raw = _safe_call(ma_target, _timeout=120,
        store=_store, resolver=_resolver,
        acquirer=acquirer, strategy=strategy, top_n=top_n, year=year)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 30: patent_option_value (Black-Scholes real option valuation)
# =====================================================================
@mcp.tool()
def tool_patent_option_value(
    query: Annotated[str, Field(description="Patent number (JP-XXXXX-B1), company name, or CPC code.")],
    query_type: Annotated[str | None, Field(description='"patent", "firm", or "technology" (auto-detected if omitted).')] = None,
    S: Annotated[float | None, Field(description="Underlying asset value (auto-estimated if omitted).")] = None,
    K: Annotated[float | None, Field(description="Strike price (auto-estimated if omitted).")] = None,
    risk_free_rate: Annotated[float, Field(description="Risk-free rate (default: 0.02).")] = 0.02,
    year: Annotated[int, Field(description="Analysis year (default: 2024).")] = 2024,
) -> dict:
    """Black-Scholes real option valuation for patents.

    Treats patents as real options: the right to commercialize technology.
    Computes option value, Greeks (delta, theta, vega), and citation-adjusted value.
    For firms: portfolio option value with top-10 most valuable patents.
    For technology: area-average option value with top players.

    S and K can be user-specified or auto-estimated from filing density and royalty benchmarks.
    """
    raw = _safe_call(patent_option_value, _timeout=120,
        store=_store, resolver=_resolver,
        query=query, query_type=query_type,
        S=S, K=K, risk_free_rate=risk_free_rate, year=year)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 31: tech_volatility
# =====================================================================
@mcp.tool()
def tool_tech_volatility(
    query: Annotated[str, Field(description="CPC code (e.g., 'H01M'), keyword, or company name.")],
    query_type: Annotated[str | None, Field(description='"technology", "text", or "firm".')] = None,
    date_from: Annotated[str, Field(description="Start date YYYY-MM-DD (default: 2015-01-01).")] = "2015-01-01",
    date_to: Annotated[str, Field(description="End date YYYY-MM-DD (default: 2024-12-31).")] = "2024-12-31",
) -> dict:
    """Technology volatility analysis with decay curve and half-life.

    Computes log-return volatility (sigma), drift, tech Sharpe ratio,
    and regime classification. Includes citation decay curve with half-life
    and percentile ranking vs all technologies.
    """
    raw = _safe_call(tech_volatility, _timeout=120,
        store=_store, resolver=_resolver,
        query=query, query_type=query_type,
        date_from=date_from, date_to=date_to)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 32: portfolio_var
# =====================================================================
@mcp.tool()
def tool_portfolio_var(
    firm: Annotated[str, Field(description="Company name (any language) or stock ticker.")],
    horizon_years: Annotated[int, Field(description="Risk horizon in years (default: 5).")] = 5,
    confidence: Annotated[float, Field(description="VaR confidence level (default: 0.95).")] = 0.95,
    year: Annotated[int, Field(description="Analysis year (default: 2024).")] = 2024,
) -> dict:
    """Portfolio Value-at-Risk for patent expiration risk.

    Analyzes which patents expire within the horizon, calculates CPC-level
    defense loss rates, identifies competitor threats from startability_surface,
    and estimates option-value-weighted VaR.
    """
    raw = _safe_call(portfolio_var, _timeout=120,
        store=_store, resolver=_resolver,
        firm=firm, horizon_years=horizon_years,
        confidence=confidence, year=year)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 33: tech_beta
# =====================================================================
@mcp.tool()
def tool_tech_beta(
    query: Annotated[str, Field(description="CPC code (e.g., 'H01M') or company name.")],
    query_type: Annotated[str | None, Field(description='"technology" or "firm".')] = None,
    benchmark: Annotated[str, Field(description='"all" (full market) or "section" (same CPC section).')] = "all",
    date_from: Annotated[str, Field(description="Start date YYYY-MM-DD (default: 2015-01-01).")] = "2015-01-01",
    date_to: Annotated[str, Field(description="End date YYYY-MM-DD (default: 2024-12-31).")] = "2024-12-31",
) -> dict:
    """CAPM-style technology beta: market sensitivity analysis.

    Computes beta (market co-movement), alpha (excess return), R-squared,
    and classifies technology as growth/cyclical/niche/mature.
    Includes peer comparison with same-section technologies.
    """
    raw = _safe_call(tech_beta, _timeout=120,
        store=_store, resolver=_resolver,
        query=query, query_type=query_type,
        benchmark=benchmark, date_from=date_from, date_to=date_to)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 34: network_topology
# =====================================================================
@mcp.tool()
def tool_network_topology(
    cpc_prefix: Annotated[str | None, Field(description="CPC prefix for technology area (e.g., 'H01M').")] = None,
    firm: Annotated[str | None, Field(description="Company name to analyze citation network.")] = None,
    max_patents: Annotated[int, Field(description="Max patents in network (default: 500, max: 1000).")] = 500,
    year: Annotated[int, Field(description="Analysis year (default: 2024).")] = 2024,
) -> dict:
    """Citation network topology analysis.

    Analyzes scale-free properties (power law gamma), small-world index,
    clustering coefficient, hub patents, and technology communities.
    All graph algorithms are self-contained (no networkx).
    """
    raw = _safe_call(network_topology, _timeout=120,
        store=_store, resolver=_resolver,
        cpc_prefix=cpc_prefix, firm=firm,
        max_patents=max_patents, year=year)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 35: knowledge_flow
# =====================================================================
@mcp.tool()
def tool_knowledge_flow(
    source_cpc: Annotated[str | None, Field(description="Knowledge source CPC (e.g., 'G06N' for AI).")] = None,
    target_cpc: Annotated[str | None, Field(description="Knowledge target CPC.")] = None,
    firm: Annotated[str | None, Field(description="Filter by company name.")] = None,
    date_from: Annotated[str, Field(description="Start date YYYY-MM-DD (default: 2018-01-01).")] = "2018-01-01",
    date_to: Annotated[str, Field(description="End date YYYY-MM-DD (default: 2024-12-31).")] = "2024-12-31",
    top_n: Annotated[int, Field(description="Max results per category (default: 20).")] = 20,
) -> dict:
    """Cross-CPC knowledge flow analysis.

    Maps knowledge transfer between technology domains via citation patterns.
    Identifies knowledge exporters (foundational tech) and importers (applied tech),
    spillover rates, and top flow pairs.
    """
    raw = _safe_call(knowledge_flow, _timeout=120,
        store=_store, resolver=_resolver,
        source_cpc=source_cpc, target_cpc=target_cpc,
        firm=firm, date_from=date_from, date_to=date_to, top_n=top_n)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 36: network_resilience
# =====================================================================
@mcp.tool()
def tool_network_resilience(
    firm: Annotated[str | None, Field(description="Company name to analyze.")] = None,
    cpc_prefix: Annotated[str | None, Field(description="CPC prefix for technology area.")] = None,
    attack_mode: Annotated[str, Field(description='"targeted" (hub removal) or "random".')] = "targeted",
    removal_steps: Annotated[int, Field(description="Number of removal steps (default: 10).")] = 10,
    max_patents: Annotated[int, Field(description="Max patents in network (default: 500).")] = 500,
) -> dict:
    """Patent network resilience (percolation theory).

    Simulates targeted (hub) and random node removal to measure fragility.
    Scale-free networks are robust to random failure but vulnerable to
    targeted hub attacks. Returns collapse thresholds, vulnerability index,
    and critical patents whose expiration would fragment the network.
    """
    raw = _safe_call(network_resilience, _timeout=120,
        store=_store, resolver=_resolver,
        firm=firm, cpc_prefix=cpc_prefix,
        attack_mode=attack_mode, removal_steps=removal_steps,
        max_patents=max_patents)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 37: tech_fusion_detector
# =====================================================================
@mcp.tool()
def tool_tech_fusion_detector(
    cpc_a: Annotated[str | None, Field(description="First CPC area (e.g., 'G06N').")] = None,
    cpc_b: Annotated[str | None, Field(description="Second CPC area (e.g., 'A61K').")] = None,
    firm: Annotated[str | None, Field(description="Filter by company name.")] = None,
    date_from: Annotated[str, Field(description="Start date YYYY-MM-DD (default: 2015-01-01).")] = "2015-01-01",
    date_to: Annotated[str, Field(description="End date YYYY-MM-DD (default: 2024-12-31).")] = "2024-12-31",
    min_co_citation: Annotated[int, Field(description="Min co-citations to detect fusion (default: 5).")] = 5,
) -> dict:
    """Technology fusion detector via co-citation analysis.

    Detects convergence between technology domains. In pair mode, tracks
    year-by-year co-citation growth. In auto-detect mode, scans for
    emerging fusions across all domains. Returns fusion stage, bridge
    patents, and key players.
    """
    raw = _safe_call(tech_fusion_detector, _timeout=120,
        store=_store, resolver=_resolver,
        cpc_a=cpc_a, cpc_b=cpc_b,
        firm=firm, date_from=date_from, date_to=date_to,
        min_co_citation=min_co_citation)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 38: tech_entropy
# =====================================================================
@mcp.tool()
def tool_tech_entropy(
    cpc_prefix: Annotated[str | None, Field(description="CPC prefix (e.g., 'H01M').")] = None,
    query: Annotated[str | None, Field(description="Technology keyword (e.g., '電池').")] = None,
    date_from: Annotated[str, Field(description="Start date YYYY-MM-DD (default: 2015-01-01).")] = "2015-01-01",
    date_to: Annotated[str, Field(description="End date YYYY-MM-DD (default: 2024-12-31).")] = "2024-12-31",
    granularity: Annotated[str, Field(description='"year" or "quarter" (default: year).')] = "year",
) -> dict:
    """Technology maturity via Shannon entropy of applicant diversity.

    Measures applicant concentration (Shannon entropy, HHI) over time to
    classify lifecycle stage: introduction, growth, mature, or declining.
    Identifies dominant players, new entrants, and consolidation trends.
    """
    raw = _safe_call(tech_entropy, _timeout=120,
        store=_store, resolver=_resolver,
        cpc_prefix=cpc_prefix, query=query,
        date_from=date_from, date_to=date_to,
        granularity=granularity)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)


# =====================================================================

# =====================================================================
# Tool 39: ip_due_diligence
# =====================================================================
@mcp.tool()
def tool_ip_due_diligence(
    target_firm: Annotated[str, Field(description="Target company name (any language) or stock ticker for IP due diligence.")],
    investment_type: Annotated[str, Field(description='"venture" (early-stage), "growth" (scale-up), "buyout" (M&A), or "licensing" (IP licensing). Default: venture.')] = "venture",
    benchmark_firms: Annotated[list[str] | None, Field(description="Optional list of competitor/peer firms to compare against.")] = None,
) -> dict:
    """Integrated IP due diligence for VC/PE investment analysis.

    Combines patent portfolio analysis with market signals to generate
    investment memo-style output. Evaluates technology moat, IP quality,
    geographic coverage, competitive position, and market signals.
    """
    raw = _safe_call(ip_due_diligence, _tool_name="ip_due_diligence", _timeout=120,
        store=_store, resolver=_resolver,
        target_firm=target_firm, investment_type=investment_type,
        benchmark_firms=benchmark_firms)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)

# =====================================================================
# Tool 40: sep_search
# =====================================================================
@mcp.tool()
def tool_sep_search(
    query: Annotated[str | None, Field(description="Free-text search across standard names, declarants, and patent numbers.")] = None,
    standard: Annotated[str | None, Field(description="Filter by standard name (e.g., '5G NR', 'LTE', 'Wi-Fi 6', 'HEVC').")] = None,
    declarant: Annotated[str | None, Field(description="Filter by declarant/company name (partial match).")] = None,
    patent_number: Annotated[str | None, Field(description="Filter by patent number.")] = None,
    max_results: Annotated[int, Field(description="Maximum number of results (default: 20).")] = 20,
    page: Annotated[int, Field(description="Page number for pagination (default: 1).")] = 1,
    page_size: Annotated[int, Field(description="Results per page (default: 20, max: 100).")] = 20,
) -> dict:
    """Search SEP declarations by standard, patent, or company.

    Searches the ETSI ISLD database of standard essential patent declarations.
    Filter by standard name, declarant company, or patent number.

    Args:
        query: Free-text search (Japanese or English). Searches title and abstract.
        standard: Filter by standard name (e.g., "5G NR", "LTE").
        declarant: Filter by declarant/company name (partial match).
        patent_number: Filter by patent number.
        max_results: Maximum number of results (default: 20).

    Returns:
        Dict with declarations list, result count, and total matching count."""
    return _safe_call(sep_search, _tool_name="sep_search", _timeout=30,
        store=_store, query=query, standard=standard, declarant=declarant,
        patent_number=patent_number, max_results=max_results,
        page=page, page_size=page_size)


# =====================================================================
# Tool 41: sep_landscape
# =====================================================================
@mcp.tool()
def tool_sep_landscape(
    standard: Annotated[str | None, Field(description="Standard name to analyze (e.g., 'LTE', '5G NR'). If omitted, shows overview of all standards.")] = None,
    standard_org: Annotated[str | None, Field(description="Filter by standards organization (e.g., 'ETSI', 'IEEE', 'ITU').")] = None,
    date_from: Annotated[str | None, Field(description="Start date in YYYY-MM-DD format.")] = None,
    date_to: Annotated[str | None, Field(description="End date in YYYY-MM-DD format.")] = None,
    page: Annotated[int, Field(description="Page number for pagination (default: 1).")] = 1,
    page_size: Annotated[int, Field(description="Results per page (default: 20, max: 100).")] = 20,
) -> dict:
    """Technology standard patent landscape for SEP declarations.

    Shows top declarants, declaration trends, and concentration metrics
    for a specific standard or overview of all standards.

    Args:
        standard: Standard name to analyze (e.g., "LTE", "5G NR").
        standard_org: Filter by standards organization.
        date_from: Start date filter.
        date_to: End date filter.

    Returns:
        Standard summary, top declarants, declaration trends, and concentration metrics."""
    return _safe_call(sep_landscape, _tool_name="sep_landscape", _timeout=30,
        store=_store, standard=standard, standard_org=standard_org,
        date_from=date_from, date_to=date_to,
        page=page, page_size=page_size)


# =====================================================================
# Tool 42: sep_portfolio
# =====================================================================
@mcp.tool()
def tool_sep_portfolio(
    firm_query: Annotated[str, Field(description="Company name (any language) or stock ticker.")],
    page: Annotated[int, Field(description="Page number for pagination (default: 1).")] = 1,
    page_size: Annotated[int, Field(description="Results per page (default: 20, max: 100).")] = 20,
) -> dict:
    """Get a firm's SEP (Standard Essential Patent) portfolio analysis.

    Shows which technology standards a company has declared essential patents for,
    declaration trends over time, and comparison to peer declarants.

    Args:
        firm_query: Company name (any language) or stock ticker.

    Returns:
        Dict with total declarations, standards covered, yearly trend, and peer comparison."""
    raw = _safe_call(sep_portfolio, _tool_name="sep_portfolio", _timeout=30,
        store=_store, firm_query=firm_query, resolver=_resolver,
        page=page, page_size=page_size)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)


# =====================================================================
# Tool 43: frand_analysis
# =====================================================================
@mcp.tool()
def tool_frand_analysis(
    standard: Annotated[str, Field(description="Standard name to analyze (e.g., 'LTE', '5G NR', 'Wi-Fi 6', 'HEVC').")],
    page: Annotated[int, Field(description="Page number for pagination (default: 1).")] = 1,
    page_size: Annotated[int, Field(description="Results per page (default: 20, max: 100).")] = 20,
) -> dict:
    """FRAND licensing analysis for a technology standard.

    Computes declaration concentration (HHI), identifies top patent holders,
    and estimates royalty stack dynamics. Includes legal disclaimer.

    Args:
        standard: Standard name to analyze.

    Returns:
        Concentration metrics, top holders, licensing landscape, and royalty stack estimate."""
    return _safe_call(frand_analysis, _tool_name="frand_analysis", _timeout=30,
        store=_store, standard=standard,
        page=page, page_size=page_size)


# =====================================================================
# Tool 44: corporate_hierarchy
# =====================================================================
@mcp.tool()
def tool_corporate_hierarchy(
    firm_query: Annotated[str, Field(description="Company name (any language) or stock ticker.")],
    depth: Annotated[int, Field(description="Traversal depth from target node (default: 2).")] = 2,
    include_patents: Annotated[bool, Field(description="Include patent counts per member (default: False).")] = False,
) -> dict:
    """Get corporate group structure (parent-subsidiary relationships).

    Explores parent and subsidiary relationships for a company,
    building a tree of the corporate group.

    Args:
        firm_query: Company name (any language) or stock ticker.
        depth: Traversal depth (default: 2).
        include_patents: Include patent counts per member.

    Returns:
        Group tree structure with members and relationship types."""
    raw = _safe_call(corporate_hierarchy, _tool_name="corporate_hierarchy", _timeout=30,
        store=_store, firm_query=firm_query, resolver=_resolver,
        depth=depth, include_patents=include_patents)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)


# =====================================================================
# Tool 45: group_portfolio
# =====================================================================
@mcp.tool()
def tool_group_portfolio(
    firm_query: Annotated[str, Field(description="Company name (any language) or stock ticker. Will find the corporate group.")],
    year: Annotated[int, Field(description="Analysis year (default: 2024).")] = 2024,
) -> dict:
    """Aggregate patent portfolio across a corporate group.

    Combines patent data from all group members (parent + subsidiaries)
    to show the group's total technology footprint.

    Args:
        firm_query: Company name or ticker (finds entire group).
        year: Analysis year (default: 2024).

    Returns:
        Combined patent count, member breakdown, and dominant CPCs."""
    raw = _safe_call(group_portfolio, _tool_name="group_portfolio", _timeout=60,
        store=_store, firm_query=firm_query, resolver=_resolver, year=year)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)


# =====================================================================
# Tool 46: group_startability
# =====================================================================
@mcp.tool()
def tool_group_startability(
    firm_query: Annotated[str, Field(description="Company name (any language) or stock ticker.")],
    tech_query_or_cluster_id: Annotated[str, Field(description='Cluster ID (e.g., "H01M_0") or technology keyword.')],
    year: Annotated[int, Field(description="Analysis year (default: 2024).")] = 2024,
) -> dict:
    """Group-level startability analysis across corporate group members.

    Evaluates each group member's readiness for a technology area and
    identifies the strongest entity within the group.

    Args:
        firm_query: Company name or ticker.
        tech_query_or_cluster_id: Cluster ID or technology keyword.
        year: Analysis year (default: 2024).

    Returns:
        Group score, member scores, recommended entity, and synergy analysis."""
    raw = _safe_call(group_startability, _tool_name="group_startability", _timeout=60,
        store=_store, firm_query=firm_query,
        tech_query_or_cluster_id=tech_query_or_cluster_id,
        resolver=_resolver, year=year)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)


# =====================================================================
# Tool 47: claim_analysis
# =====================================================================
@mcp.tool()
def tool_claim_analysis(
    publication_number: Annotated[str | None, Field(description='Patent publication number (e.g., "JP-7637366-B1").')] = None,
    text: Annotated[str | None, Field(description="Technology description to analyze scope for.")] = None,
) -> dict:
    """Analyze patent's technical scope from abstract and CPC classification.

    Extracts key technical elements, assesses scope breadth, and finds
    related patents. Based on abstract analysis (not claim text).

    Args:
        publication_number: Patent to analyze.
        text: Alternative: technology description text.

    Returns:
        Technical elements, scope assessment, CPC coverage, and related patents."""
    return _safe_call(claim_analysis, _tool_name="claim_analysis", _timeout=60,
        store=_store, publication_number=publication_number, text=text)


# =====================================================================
# Tool 48: claim_comparison
# =====================================================================
@mcp.tool()
def tool_claim_comparison(
    publication_numbers: Annotated[list[str], Field(description='List of patent publication numbers to compare (2-10 patents).')],
) -> dict:
    """Compare technical scope of multiple patents.

    Computes pairwise CPC overlap and embedding similarity between patents.
    Identifies shared and unique technology elements.

    Args:
        publication_numbers: List of patent numbers to compare.

    Returns:
        Pairwise similarity matrix, shared/unique CPCs, and overlap assessment."""
    return _safe_call(claim_comparison, _tool_name="claim_comparison", _timeout=60,
        store=_store, publication_numbers=publication_numbers)


# =====================================================================
# Tool 49: fto_analysis
# =====================================================================
@mcp.tool()
def tool_fto_analysis(
    text: Annotated[str | None, Field(description="Technology description for FTO analysis.")] = None,
    cpc_codes: Annotated[list[str] | None, Field(description='CPC codes to check (e.g., ["H01M10", "H01M4"]).')] = None,
    target_jurisdiction: Annotated[str, Field(description='Target jurisdiction (default: "JP").')] = "JP",
    max_blocking: Annotated[int, Field(description="Maximum blocking patents to return (default: 20).")] = 20,
) -> dict:
    """Freedom-to-operate analysis for a technology area.

    Identifies potential blocking patents, assesses risk by assignee,
    and provides expiry timeline. Preliminary analysis — professional
    patent attorney review required for actual FTO opinions.

    Args:
        text: Technology description.
        cpc_codes: CPC codes to check.
        target_jurisdiction: Target country (default: JP).
        max_blocking: Max blocking patents to return.

    Returns:
        Risk assessment, blocking patents, risk by assignee, and expiry timeline."""
    raw = _safe_call(fto_analysis, _tool_name="fto_analysis", _timeout=120,
        store=_store, text=text, cpc_codes=cpc_codes,
        target_jurisdiction=target_jurisdiction, max_blocking=max_blocking)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)


# =====================================================================
# Tool 50: create_category
# =====================================================================
@mcp.tool()
def tool_create_category(
    category_name: Annotated[str, Field(description='Category name (e.g., "EV Battery", "Autonomous Driving Sensors").')],
    description: Annotated[str | None, Field(description="Category description.")] = None,
    cpc_patterns: Annotated[list[str] | None, Field(description='CPC prefix patterns (e.g., ["H01M10", "H01M4"]).')] = None,
    keywords: Annotated[list[str] | None, Field(description='Keywords for matching (e.g., ["battery", "電池", "lithium"]).')] = None,
) -> dict:
    """Define a custom technology category and auto-classify initial patents.

    Creates a category with CPC and keyword rules, then automatically
    classifies matching patents. Use classify_patents to expand.

    Args:
        category_name: Human-readable category name.
        description: Optional description.
        cpc_patterns: CPC prefix patterns for rule-based matching.
        keywords: Keywords for text-based matching.

    Returns:
        Category ID, initial patent count, and sample classified patents."""
    return _safe_call(create_category, _tool_name="create_category", _timeout=120,
        store=_store, category_name=category_name, description=description,
        cpc_patterns=cpc_patterns, keywords=keywords)


# =====================================================================
# Tool 51: classify_patents
# =====================================================================
@mcp.tool()
def tool_classify_patents(
    category_id: Annotated[str, Field(description="Category ID to classify into (from create_category).")],
    query: Annotated[str | None, Field(description="Optional query to find and classify new patents. If omitted, lists existing classifications.")] = None,
    max_results: Annotated[int, Field(description="Maximum results (default: 100).")] = 100,
    page: Annotated[int, Field(description="Page number (default: 1).")] = 1,
    page_size: Annotated[int, Field(description="Results per page (default: 20).")] = 20,
) -> dict:
    """Classify patents into a custom category, or list existing classifications.

    If query is provided: finds matching patents and classifies them.
    If query is omitted: lists already-classified patents for the category.

    Args:
        category_id: Target category ID.
        query: Optional search query for new classification.
        max_results: Maximum new patents to classify.

    Returns:
        Classification results with confidence scores."""
    return _safe_call(classify_patents, _tool_name="classify_patents", _timeout=120,
        store=_store, category_id=category_id, query=query,
        max_results=max_results, page=page, page_size=page_size)


# =====================================================================
# Tool 52: category_landscape
# =====================================================================
@mcp.tool()
def tool_category_landscape(
    category_id: Annotated[str, Field(description="Category ID to analyze (from create_category).")],
) -> dict:
    """Landscape analysis for a custom technology category.

    Shows filing trends, top applicants, sub-technology areas,
    and top-cited patents within the category.

    Args:
        category_id: Category to analyze.

    Returns:
        Timeline, top applicants, sub-areas, and growth assessment."""
    raw = _safe_call(category_landscape, _tool_name="category_landscape", _timeout=120,
        store=_store, category_id=category_id)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)


# =====================================================================
# Tool 53: portfolio_benchmark
# =====================================================================
@mcp.tool()
def tool_portfolio_benchmark(
    firm_query: Annotated[str, Field(description="Company name (any language) or stock ticker.")],
    category_id: Annotated[str, Field(description="Category ID to benchmark against (from create_category).")],
) -> dict:
    """Benchmark a firm's patent position within a custom category vs peers.

    Computes market share, ranking, and gap analysis compared to
    category leaders and peers.

    Args:
        firm_query: Company name or ticker.
        category_id: Category to benchmark in.

    Returns:
        Firm metrics, peer ranking, gap analysis, and recommendations."""
    raw = _safe_call(portfolio_benchmark, _tool_name="portfolio_benchmark", _timeout=120,
        store=_store, firm_query=firm_query, category_id=category_id,
        resolver=_resolver)
    if isinstance(raw, dict) and "error" in raw:
        return raw
    return _enrich_firm_ids(raw)


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
            "citation_network", "tech_trend", "ma_target",
            "patent_option_value", "tech_volatility", "portfolio_var", "tech_beta",
            "network_topology", "knowledge_flow", "network_resilience",
            "tech_fusion_detector", "tech_entropy", "ip_due_diligence",
            "sep_search", "sep_landscape", "sep_portfolio", "frand_analysis",
            "corporate_hierarchy", "group_portfolio", "group_startability",
            "claim_analysis", "claim_comparison", "fto_analysis",
            "create_category", "classify_patents", "category_landscape", "portfolio_benchmark",
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

    mcp_app = mcp.http_app(path="/mcp", stateless_http=True)
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

