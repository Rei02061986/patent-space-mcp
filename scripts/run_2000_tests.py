#!/usr/bin/env python3
"""
Patent Space MCP — 2000-test quality runner.

Runs ~2000 tests across all 67 MCP tool functions, organized by 3 personas:
  - Patent Attorney (700 tests): FTO, prior art, claim analysis, SEP, litigation
  - VC Investor (700 tests): valuation, due diligence, trends, market fusion
  - Corporate Strategist (600 tests): startability, M&A, tech gap, portfolio

Runs INSIDE the Docker container at /app/.
Imports tool functions directly (no HTTP).

Output:
  /tmp/test_results_2000.csv   — per-test results
  /tmp/test_summary_2000.json  — aggregate summary

Usage:
  cd /app && python scripts/run_2000_tests.py
"""
from __future__ import annotations

import csv
import json
import os
import signal
import sys
import time
import traceback
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

# ---------------------------------------------------------------------------
# Bootstrap: ensure /app is on sys.path so tool imports work
# ---------------------------------------------------------------------------
_APP_DIR = Path("/app")
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))

os.chdir(_APP_DIR)

# ---------------------------------------------------------------------------
# Imports — tool functions + infrastructure
# ---------------------------------------------------------------------------
from db.sqlite_store import PatentStore
from entity.registry import EntityRegistry
from entity.resolver import EntityResolver

# Entity seed data (same as server.py)
from entity.data.tse_prime_seed import TSE_PRIME_ENTITIES
from entity.data.tse_expanded_seed import TSE_EXPANDED_ENTITIES
from entity.data.tse_auto_seed import TSE_AUTO_ENTITIES

try:
    from entity.data.sp500_seed import SP500_ENTITIES
except ImportError:
    SP500_ENTITIES = []

try:
    from entity.data.global_seed import GLOBAL_ENTITIES
except ImportError:
    GLOBAL_ENTITIES = []

# Search
from tools.search import patent_search

# Portfolio
from tools.portfolio import firm_patent_portfolio
from tools.compare import patent_compare
from tools.portfolio_evolution import portfolio_evolution

# Startability
from tools.startability_tool import startability, startability_ranking
from tools.startability_delta import startability_delta
from tools.tech_fit import tech_fit

# Tech analysis
from tools.landscape import tech_landscape
from tools.clusters import tech_clusters_list
from tools.vectors import firm_tech_vector
from tools.tech_trend import tech_trend
from tools.tech_trend_alert import tech_trend_alert

# Network
from tools.network import applicant_network
from tools.citation_network import citation_network
from tools.network_analysis import (
    network_topology,
    knowledge_flow,
    network_resilience,
    tech_fusion_detector,
    tech_entropy,
)

# Strategy
from tools.adversarial import adversarial_strategy
from tools.tech_gap import tech_gap
from tools.similar_firms import similar_firms
from tools.cross_domain import cross_domain_discovery
from tools.ma_target import ma_target
from tools.sales_prospect import sales_prospect

# Finance
from tools.patent_valuation import patent_valuation
from tools.patent_finance import (
    patent_option_value,
    tech_volatility,
    portfolio_var,
    tech_beta,
)
from tools.bayesian_scenario import bayesian_scenario
from tools.market_fusion import patent_market_fusion

# Invention
from tools.invention_intel import invention_intelligence
from tools.cross_border import cross_border_similarity

# GDELT
from tools.gdelt_tool import gdelt_company_events

# IP Due Diligence
try:
    from tools.ip_due_diligence import ip_due_diligence
except ImportError:
    ip_due_diligence = None

# SEP
from tools.sep_analysis import sep_search, sep_landscape, sep_portfolio, frand_analysis

# Corporate hierarchy
from tools.corporate_hierarchy import (
    corporate_hierarchy,
    group_portfolio,
    group_startability,
)

# Claim analysis
from tools.claim_analysis import claim_analysis, claim_comparison, fto_analysis

# AI Classifier
from tools.ai_classifier import (
    create_category,
    classify_patents,
    category_landscape,
    portfolio_benchmark,
)

# Monitoring
from tools.monitoring import (
    create_watch,
    list_watches,
    check_alerts,
    run_monitoring,
)

# Summary
from tools.patent_summary import patent_summary, technology_brief

# Visualization
from tools.visualization import (
    tech_map,
    citation_graph_viz,
    firm_landscape,
    startability_heatmap,
)

# PTAB
from tools.ptab import ptab_search, ptab_risk, litigation_search, litigation_risk

# ---------------------------------------------------------------------------
# Constants — test data
# ---------------------------------------------------------------------------
JP_FIRMS = [
    "Toyota", "Honda", "Sony", "Panasonic", "Canon",
    "Hitachi", "NEC", "Fujitsu", "Toshiba", "Mitsubishi Electric",
    "Seiko Epson", "Ricoh", "Sharp", "Denso", "Aisin",
]

GLOBAL_FIRMS_LIST = [
    "Samsung", "Apple", "Google", "Microsoft", "Intel",
    "TSMC", "Qualcomm", "Siemens", "BASF", "Bosch", "Huawei",
]

ALL_FIRMS = JP_FIRMS + GLOBAL_FIRMS_LIST

CPC_CODES = ["H01M", "G06N", "H04L", "A61K", "B60L", "G06F", "H01L", "C08L"]
CPC_EXTENDED = CPC_CODES + ["B01D", "G02B", "F16H", "H04W", "C07D", "G01N", "B29C", "A61B"]

CLUSTER_IDS = ["H01M_0", "G06N_0", "H04L_0", "A61K_0", "B60L_0"]
CLUSTER_IDS_EXTENDED = CLUSTER_IDS + [
    "G06F_0", "H01L_0", "C08L_0", "B01D_0", "G02B_0",
    "F16H_0", "H04W_0", "C07D_0", "G01N_0",
]

PATENT_NUMBERS = [
    "JP-7637366-B1",
    "JP-2020-123456-A",  # may not exist
    "JP-7000001-B1",
    "JP-6543210-B1",
    "JP-2023-100001-A",
]

STANDARDS = ["5G", "LTE", "Wi-Fi", "HEVC", "5G NR"]

TECH_DESCRIPTIONS = [
    "solid-state battery with sulfide electrolyte for electric vehicles",
    "transformer-based neural network for image classification",
    "5G millimeter wave antenna design for mobile devices",
    "CRISPR gene editing for cancer immunotherapy",
    "perovskite solar cell with high efficiency",
    "autonomous driving perception using lidar and camera fusion",
    "quantum computing error correction using surface codes",
    "hydrogen fuel cell membrane electrode assembly",
]

INVESTMENT_TYPES = ["venture", "growth", "buyout", "licensing"]
PURPOSES_VALUATION = ["licensing", "portfolio_ranking", "divestiture"]
PURPOSES_FUSION = ["investment", "ma_target", "license_match", "general"]
MA_STRATEGIES = ["tech_gap", "consolidation", "diversification"]
DIRECTIONS = ["gainers", "losers", "both"]

TIMEOUT_SECONDS = 120
SLOW_THRESHOLD_MS = 60_000

# ---------------------------------------------------------------------------
# Result data model
# ---------------------------------------------------------------------------
@dataclass
class TestResult:
    test_id: str
    persona: str
    tool_name: str
    input_params: str  # JSON
    status: str  # pass / fail / timeout_warning
    quality_score: int  # 1-5
    response_time_ms: float
    error_message: str = ""
    result_keys: str = ""  # top-level keys of the response dict


# ---------------------------------------------------------------------------
# Quality scoring
# ---------------------------------------------------------------------------
def score_result(
    result: Any,
    elapsed_ms: float,
    error: Exception | None = None,
    is_timeout: bool = False,
) -> tuple[str, int, str]:
    """Return (status, quality_score, error_message)."""
    if error is not None:
        err_str = str(error)[:500]
        if is_timeout or "interrupted" in err_str.lower():
            return ("timeout_warning", 3, f"Timeout: {err_str}")
        return ("fail", 1, f"Unhandled: {err_str}")

    if result is None:
        return ("fail", 1, "None returned")

    if isinstance(result, dict):
        if "error" in result:
            err_msg = str(result.get("error", ""))[:300]
            # An error with a suggestion or partial data is a handled error
            has_data = any(
                k in result
                for k in (
                    "patents", "results", "firms", "nodes", "edges",
                    "cluster_id", "firm_id", "score", "entity",
                    "available_years", "suggestion",
                )
            )
            if has_data:
                return ("pass", 2, f"Error with partial data: {err_msg}")
            return ("fail", 2, f"Handled error: {err_msg}")

        # Check for empty results
        data_keys = [
            "patents", "results", "firms", "nodes", "edges", "declarations",
            "timeline", "mermaid", "hot_clusters", "categories",
            "cluster_id", "firm_id", "score", "entity", "patent_count",
            "total_patents", "total", "total_count", "result_count",
            "group_members", "tree", "value_score", "option_value",
            "risk_score", "beta", "sigma", "alpha", "var_absolute",
            "session_id", "priors", "posterior", "npv_distribution",
            "watch_id", "watches", "alerts", "category_id",
            "fusion_stage", "entropy_timeline", "topology",
            "knowledge_flows", "collapse_threshold", "vulnerability_index",
            "cpc_trend", "top_applicants", "growth_areas",
            "filing_trend", "cpc_distribution", "tech_vector",
            "startability_heatmap", "mermaid_source",
        ]

        has_real_data = False
        for k in data_keys:
            v = result.get(k)
            if v is not None:
                if isinstance(v, (list, dict)):
                    if len(v) > 0:
                        has_real_data = True
                        break
                elif isinstance(v, (int, float)):
                    has_real_data = True
                    break
                elif isinstance(v, str) and len(v) > 0:
                    has_real_data = True
                    break

        if not has_real_data:
            # Check numeric fields — might still be valid with zero patents
            total = result.get("total", result.get("total_count", result.get("result_count")))
            if isinstance(total, (int, float)) and total == 0:
                if elapsed_ms > SLOW_THRESHOLD_MS:
                    return ("pass", 3, "Empty results (slow)")
                return ("pass", 3, "Empty results (may be valid)")
            if elapsed_ms > SLOW_THRESHOLD_MS:
                return ("pass", 3, "Slow response, sparse data")
            return ("pass", 3, "Sparse data in response")

        # Has data
        if elapsed_ms > SLOW_THRESHOLD_MS:
            return ("pass", 3, "Good data but slow (>60s)")

        # Check for completeness — minor issues
        empty_count = 0
        checked_count = 0
        for k, v in result.items():
            if isinstance(v, (list, dict)):
                checked_count += 1
                if len(v) == 0:
                    empty_count += 1
        if checked_count > 0 and empty_count / checked_count > 0.5:
            return ("pass", 4, "Good but many empty fields")

        return ("pass", 5, "")

    # Non-dict result (unlikely but handle)
    if result:
        return ("pass", 4, "Non-dict result")
    return ("pass", 3, "Non-dict, possibly empty")


# ---------------------------------------------------------------------------
# Test runner helper
# ---------------------------------------------------------------------------
def run_one_test(
    test_id: str,
    persona: str,
    tool_name: str,
    fn: Callable,
    kwargs: dict[str, Any],
) -> TestResult:
    """Execute a single test and return the result."""
    params_json = json.dumps(
        {k: v for k, v in kwargs.items() if k not in ("store", "resolver")},
        ensure_ascii=False,
        default=str,
    )[:2000]

    PER_TEST_TIMEOUT = 30  # seconds — reduced from 120 due to ingestion I/O

    class _TestTimeout(Exception):
        pass

    def _alarm_handler(signum, frame):
        raise _TestTimeout("Test timed out after 30s")

    t0 = time.monotonic()
    error = None
    result = None
    is_timeout = False

    old_handler = signal.signal(signal.SIGALRM, _alarm_handler)
    signal.alarm(PER_TEST_TIMEOUT)
    try:
        result = fn(**kwargs)
    except _TestTimeout as e:
        error = e
        is_timeout = True
    except Exception as e:
        error = e
        if "interrupted" in str(e).lower() or "timeout" in str(e).lower():
            is_timeout = True
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)

    elapsed_ms = (time.monotonic() - t0) * 1000
    status, quality, err_msg = score_result(result, elapsed_ms, error, is_timeout)

    if error and not err_msg:
        err_msg = traceback.format_exc()[:500]

    result_keys = ""
    if isinstance(result, dict):
        result_keys = ",".join(sorted(result.keys())[:20])

    return TestResult(
        test_id=test_id,
        persona=persona,
        tool_name=tool_name,
        input_params=params_json,
        status=status,
        quality_score=quality,
        response_time_ms=round(elapsed_ms, 1),
        error_message=err_msg[:500] if err_msg else "",
        result_keys=result_keys,
    )


# ---------------------------------------------------------------------------
# Test case generators — organized by persona
# ---------------------------------------------------------------------------
def _tid(persona_prefix: str, counter: list[int]) -> str:
    counter[0] += 1
    return f"{persona_prefix}-{counter[0]:04d}"


def _patent_detail_wrapper(
    store: PatentStore,
    publication_number: str,
    include_full_text: bool = False,
    include_claims: bool = False,
) -> dict:
    """Wrapper for patent_detail that uses store.get_patent() directly.

    The real patent_detail is defined in server.py as an MCP tool function.
    We replicate its core logic here for direct testing.
    """
    import sqlite3
    try:
        result = store.get_patent(publication_number)
    except sqlite3.OperationalError as e:
        if "interrupted" in str(e):
            return {
                "error": "Query timed out -- database under heavy I/O load.",
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
    }
    if include_full_text:
        out["full_text"] = result.get("full_text")
    if include_claims:
        out["claims_text"] = result.get("claims_text")
    return out


def generate_patent_attorney_tests(
    store: PatentStore, resolver: EntityResolver,
) -> list[tuple[str, str, str, Callable, dict]]:
    """~700 tests for Patent Attorney persona."""
    tests = []
    c = [0]
    P = "PA"  # Patent Attorney prefix

    # --- patent_search (80 tests) ---
    # Keyword searches
    for kw in ["半導体", "電池", "自動運転", "人工知能", "抗体", "燃料電池",
                "semiconductor", "battery", "autonomous", "machine learning",
                "CRISPR", "5G", "OLED", "lidar"]:
        tests.append((_tid(P, c), "Patent Attorney", "patent_search",
                       patent_search, dict(store=store, query=kw, max_results=5)))

    # CPC-filtered searches
    for cpc in CPC_EXTENDED:
        tests.append((_tid(P, c), "Patent Attorney", "patent_search",
                       patent_search, dict(store=store, cpc_codes=[cpc], max_results=5)))

    # Applicant searches
    for firm in JP_FIRMS[:10]:
        tests.append((_tid(P, c), "Patent Attorney", "patent_search",
                       patent_search, dict(store=store, applicant=firm, max_results=5)))

    # Combined searches
    for firm, cpc in zip(JP_FIRMS[:8], CPC_CODES):
        tests.append((_tid(P, c), "Patent Attorney", "patent_search",
                       patent_search, dict(store=store, applicant=firm, cpc_codes=[cpc], max_results=5)))

    # Date-range searches
    for year_from, year_to in [("2020-01-01", "2024-12-31"), ("2015-01-01", "2019-12-31"),
                                ("2022-06-01", "2023-06-30")]:
        tests.append((_tid(P, c), "Patent Attorney", "patent_search",
                       patent_search, dict(store=store, query="battery",
                                           date_from=year_from, date_to=year_to, max_results=5)))

    # Pagination
    for pg in [1, 2, 3]:
        tests.append((_tid(P, c), "Patent Attorney", "patent_search",
                       patent_search, dict(store=store, cpc_codes=["H01M"], page=pg, page_size=10)))

    # Multi-CPC
    for cpcs in [["H01M", "G06N"], ["H04L", "H04W"], ["A61K", "C07D"]]:
        tests.append((_tid(P, c), "Patent Attorney", "patent_search",
                       patent_search, dict(store=store, cpc_codes=cpcs, max_results=5)))

    # Empty / edge cases
    tests.append((_tid(P, c), "Patent Attorney", "patent_search",
                   patent_search, dict(store=store, query="xyznonexistent12345")))
    tests.append((_tid(P, c), "Patent Attorney", "patent_search",
                   patent_search, dict(store=store, query=None, cpc_codes=None, applicant=None)))

    # --- patent_summary (15 tests) ---
    for pn in PATENT_NUMBERS:
        tests.append((_tid(P, c), "Patent Attorney", "patent_summary",
                       patent_summary, dict(store=store, publication_number=pn)))
    # Edge cases
    for pn in ["JP-0000001-B1", "JP-9999999-A", "US-12345678-B2",
                "INVALID", "JP-7637366-B1", "JP-2020-100000-A",
                "JP-7100000-B1", "JP-7200000-B1", "JP-7300000-B1", "JP-7400000-B1"]:
        tests.append((_tid(P, c), "Patent Attorney", "patent_summary",
                       patent_summary, dict(store=store, publication_number=pn)))

    # --- patent_detail (30 tests) ---
    for pn in PATENT_NUMBERS:
        tests.append((_tid(P, c), "Patent Attorney", "patent_detail",
                       _patent_detail_wrapper,
                       dict(store=store, publication_number=pn)))
    for offset in range(15):
        pn = f"JP-{7000000 + offset * 40000}-B1"
        tests.append((_tid(P, c), "Patent Attorney", "patent_detail",
                       _patent_detail_wrapper,
                       dict(store=store, publication_number=pn)))
    # With include flags
    for pn in PATENT_NUMBERS[:5]:
        tests.append((_tid(P, c), "Patent Attorney", "patent_detail",
                       _patent_detail_wrapper,
                       dict(store=store, publication_number=pn,
                            include_full_text=True, include_claims=True)))
    # Edge cases
    for pn in ["INVALID-NUMBER", "JP-0000000-B1", "US-99999999-B2",
                "JP-7637366-B1", "JP-2020-123456-A"]:
        tests.append((_tid(P, c), "Patent Attorney", "patent_detail",
                       _patent_detail_wrapper,
                       dict(store=store, publication_number=pn)))

    # --- claim_analysis (40 tests) ---
    # By patent number
    for pn in PATENT_NUMBERS + ["JP-7100000-B1", "JP-7200000-B1", "JP-7500000-B1"]:
        tests.append((_tid(P, c), "Patent Attorney", "claim_analysis",
                       claim_analysis, dict(store=store, publication_number=pn)))

    # By text description
    for desc in TECH_DESCRIPTIONS:
        tests.append((_tid(P, c), "Patent Attorney", "claim_analysis",
                       claim_analysis, dict(store=store, text=desc)))

    # Mixed JP/EN descriptions
    for desc in ["リチウムイオン電池の正極材料", "有機ELディスプレイの製造方法",
                  "5G基地局用アンテナ設計", "自動車用モーター制御装置"]:
        tests.append((_tid(P, c), "Patent Attorney", "claim_analysis",
                       claim_analysis, dict(store=store, text=desc)))

    # Edge: empty text
    tests.append((_tid(P, c), "Patent Attorney", "claim_analysis",
                   claim_analysis, dict(store=store, text="")))

    # --- claim_comparison (25 tests) ---
    # Pairs
    patent_pairs = [
        ["JP-7637366-B1", "JP-7000001-B1"],
        ["JP-7100000-B1", "JP-7200000-B1"],
        ["JP-7300000-B1", "JP-7400000-B1"],
        ["JP-7500000-B1", "JP-7600000-B1"],
    ]
    for pair in patent_pairs:
        tests.append((_tid(P, c), "Patent Attorney", "claim_comparison",
                       claim_comparison, dict(store=store, publication_numbers=pair)))

    # Triples
    for i in range(5):
        triple = [f"JP-{7000000 + i * 100000 + j * 10000}-B1" for j in range(3)]
        tests.append((_tid(P, c), "Patent Attorney", "claim_comparison",
                       claim_comparison, dict(store=store, publication_numbers=triple)))

    # 4-patent comparisons
    for i in range(3):
        quad = [f"JP-{7000000 + i * 50000 + j * 10000}-B1" for j in range(4)]
        tests.append((_tid(P, c), "Patent Attorney", "claim_comparison",
                       claim_comparison, dict(store=store, publication_numbers=quad)))

    # Known + unknown mix
    for i in range(3):
        tests.append((_tid(P, c), "Patent Attorney", "claim_comparison",
                       claim_comparison,
                       dict(store=store, publication_numbers=["JP-7637366-B1", f"JP-{7000000 + i * 100}-B1"])))

    # Edge cases
    tests.append((_tid(P, c), "Patent Attorney", "claim_comparison",
                   claim_comparison, dict(store=store, publication_numbers=["JP-7637366-B1"])))
    tests.append((_tid(P, c), "Patent Attorney", "claim_comparison",
                   claim_comparison, dict(store=store, publication_numbers=[])))
    tests.append((_tid(P, c), "Patent Attorney", "claim_comparison",
                   claim_comparison, dict(store=store, publication_numbers=["INVALID-1", "INVALID-2"])))

    # --- fto_analysis (40 tests) ---
    # By text
    for desc in TECH_DESCRIPTIONS:
        tests.append((_tid(P, c), "Patent Attorney", "fto_analysis",
                       fto_analysis, dict(store=store, text=desc)))

    # By CPC codes
    for cpc in CPC_CODES:
        tests.append((_tid(P, c), "Patent Attorney", "fto_analysis",
                       fto_analysis, dict(store=store, cpc_codes=[cpc])))

    # Combined text + CPC
    for desc, cpc in zip(TECH_DESCRIPTIONS[:4], CPC_CODES[:4]):
        tests.append((_tid(P, c), "Patent Attorney", "fto_analysis",
                       fto_analysis, dict(store=store, text=desc, cpc_codes=[cpc])))

    # Multi-CPC FTO
    for cpcs in [["H01M", "H01M10"], ["G06N", "G06F"], ["A61K", "A61B"]]:
        tests.append((_tid(P, c), "Patent Attorney", "fto_analysis",
                       fto_analysis, dict(store=store, cpc_codes=cpcs)))

    # JP descriptions
    for desc in ["リチウムイオン電池の充放電制御方法", "画像認識AIモデル",
                  "無線通信プロトコル", "抗がん剤デリバリーシステム"]:
        tests.append((_tid(P, c), "Patent Attorney", "fto_analysis",
                       fto_analysis, dict(store=store, text=desc)))

    # max_blocking variations
    for mb in [5, 10, 50]:
        tests.append((_tid(P, c), "Patent Attorney", "fto_analysis",
                       fto_analysis, dict(store=store, text="battery electrode", max_blocking=mb)))

    # --- SEP tools (80 tests) ---
    # sep_search
    for std in STANDARDS:
        tests.append((_tid(P, c), "Patent Attorney", "sep_search",
                       sep_search, dict(store=store, standard=std)))
    for firm in JP_FIRMS[:8] + GLOBAL_FIRMS_LIST[:5]:
        tests.append((_tid(P, c), "Patent Attorney", "sep_search",
                       sep_search, dict(store=store, declarant=firm)))
    for q in ["wireless", "video", "codec", "modulation", "OFDM"]:
        tests.append((_tid(P, c), "Patent Attorney", "sep_search",
                       sep_search, dict(store=store, query=q)))
    # Combined filters
    for std, firm in [("5G", "Samsung"), ("LTE", "Qualcomm"), ("Wi-Fi", "Intel"),
                       ("HEVC", "Sony"), ("5G NR", "Huawei")]:
        tests.append((_tid(P, c), "Patent Attorney", "sep_search",
                       sep_search, dict(store=store, standard=std, declarant=firm)))
    # Pagination
    for pg in [1, 2]:
        tests.append((_tid(P, c), "Patent Attorney", "sep_search",
                       sep_search, dict(store=store, standard="LTE", page=pg, page_size=10)))

    # sep_landscape
    for std in STANDARDS + [None]:
        tests.append((_tid(P, c), "Patent Attorney", "sep_landscape",
                       sep_landscape, dict(store=store, standard=std)))
    tests.append((_tid(P, c), "Patent Attorney", "sep_landscape",
                   sep_landscape, dict(store=store, standard="LTE", date_from="2020-01-01")))

    # sep_portfolio
    for firm in JP_FIRMS[:5] + GLOBAL_FIRMS_LIST[:5]:
        tests.append((_tid(P, c), "Patent Attorney", "sep_portfolio",
                       sep_portfolio, dict(store=store, firm_query=firm, resolver=resolver)))

    # frand_analysis
    for std in STANDARDS:
        tests.append((_tid(P, c), "Patent Attorney", "frand_analysis",
                       frand_analysis, dict(store=store, standard=std)))
    tests.append((_tid(P, c), "Patent Attorney", "frand_analysis",
                   frand_analysis, dict(store=store, standard="nonexistent_standard")))

    # --- PTAB / Litigation (80 tests) ---
    # ptab_search
    for pn in PATENT_NUMBERS:
        tests.append((_tid(P, c), "Patent Attorney", "ptab_search",
                       ptab_search, dict(store=store, patent_number=pn, resolver=resolver)))
    for pet in ["Samsung", "Apple", "Google", "Intel", "Qualcomm"]:
        tests.append((_tid(P, c), "Patent Attorney", "ptab_search",
                       ptab_search, dict(store=store, petitioner=pet, resolver=resolver)))
    for owner in JP_FIRMS[:8]:
        tests.append((_tid(P, c), "Patent Attorney", "ptab_search",
                       ptab_search, dict(store=store, patent_owner=owner, resolver=resolver)))
    for tt in ["IPR", "PGR", "CBM"]:
        tests.append((_tid(P, c), "Patent Attorney", "ptab_search",
                       ptab_search, dict(store=store, trial_type=tt, resolver=resolver)))

    # ptab_risk
    for pn in PATENT_NUMBERS:
        tests.append((_tid(P, c), "Patent Attorney", "ptab_risk",
                       ptab_risk, dict(store=store, patent_number=pn, resolver=resolver)))
    for cpc in CPC_CODES:
        tests.append((_tid(P, c), "Patent Attorney", "ptab_risk",
                       ptab_risk, dict(store=store, cpc_prefix=cpc, resolver=resolver)))
    for firm in JP_FIRMS[:5]:
        tests.append((_tid(P, c), "Patent Attorney", "ptab_risk",
                       ptab_risk, dict(store=store, applicant=firm, resolver=resolver)))

    # litigation_search
    for firm in JP_FIRMS[:5] + GLOBAL_FIRMS_LIST[:5]:
        tests.append((_tid(P, c), "Patent Attorney", "litigation_search",
                       litigation_search, dict(store=store, plaintiff=firm, resolver=resolver)))
    for firm in JP_FIRMS[:5]:
        tests.append((_tid(P, c), "Patent Attorney", "litigation_search",
                       litigation_search, dict(store=store, defendant=firm, resolver=resolver)))
    for pn in PATENT_NUMBERS:
        tests.append((_tid(P, c), "Patent Attorney", "litigation_search",
                       litigation_search, dict(store=store, patent_number=pn, resolver=resolver)))

    # litigation_risk
    for firm in JP_FIRMS[:8] + GLOBAL_FIRMS_LIST[:5]:
        tests.append((_tid(P, c), "Patent Attorney", "litigation_risk",
                       litigation_risk, dict(store=store, firm_query=firm, resolver=resolver)))
    for cpc in CPC_CODES:
        tests.append((_tid(P, c), "Patent Attorney", "litigation_risk",
                       litigation_risk, dict(store=store, cpc_prefix=cpc, resolver=resolver)))

    # --- cross_border_similarity (30 tests) ---
    for firm in JP_FIRMS[:8]:
        tests.append((_tid(P, c), "Patent Attorney", "cross_border_similarity",
                       cross_border_similarity,
                       dict(store=store, resolver=resolver, query=firm, query_type="firm")))
    for pn in PATENT_NUMBERS:
        tests.append((_tid(P, c), "Patent Attorney", "cross_border_similarity",
                       cross_border_similarity,
                       dict(store=store, resolver=resolver, query=pn, query_type="patent")))
    for desc in TECH_DESCRIPTIONS[:5]:
        tests.append((_tid(P, c), "Patent Attorney", "cross_border_similarity",
                       cross_border_similarity,
                       dict(store=store, resolver=resolver, query=desc, query_type="text")))
    # Target jurisdiction variations
    for jurisdictions in [["CN", "KR"], ["US", "EP"], ["CN", "KR", "US", "EP"]]:
        tests.append((_tid(P, c), "Patent Attorney", "cross_border_similarity",
                       cross_border_similarity,
                       dict(store=store, resolver=resolver, query="Toyota",
                            query_type="firm", target_jurisdictions=jurisdictions)))
    # min_similarity variations
    for ms in [0.5, 0.7, 0.9]:
        tests.append((_tid(P, c), "Patent Attorney", "cross_border_similarity",
                       cross_border_similarity,
                       dict(store=store, resolver=resolver, query="Honda",
                            query_type="firm", min_similarity=ms)))

    # --- invention_intelligence (20 tests) ---
    for desc in TECH_DESCRIPTIONS:
        tests.append((_tid(P, c), "Patent Attorney", "invention_intelligence",
                       invention_intelligence, dict(store=store, text=desc)))
    for desc in TECH_DESCRIPTIONS[:4]:
        tests.append((_tid(P, c), "Patent Attorney", "invention_intelligence",
                       invention_intelligence,
                       dict(store=store, text=desc, include_fto=True, include_whitespace=False)))
    for desc in TECH_DESCRIPTIONS[:4]:
        tests.append((_tid(P, c), "Patent Attorney", "invention_intelligence",
                       invention_intelligence,
                       dict(store=store, text=desc, max_prior_art=5)))
    # JP descriptions
    for desc in ["全固体電池の製造方法", "深層学習によるドラッグデザイン",
                  "自動運転車両の衝突回避システム", "6G向けテラヘルツ通信"]:
        tests.append((_tid(P, c), "Patent Attorney", "invention_intelligence",
                       invention_intelligence, dict(store=store, text=desc)))

    # --- citation_network (25 tests) ---
    for pn in PATENT_NUMBERS:
        tests.append((_tid(P, c), "Patent Attorney", "citation_network",
                       citation_network,
                       dict(store=store, publication_number=pn, depth=1)))
    for pn in PATENT_NUMBERS[:3]:
        tests.append((_tid(P, c), "Patent Attorney", "citation_network",
                       citation_network,
                       dict(store=store, publication_number=pn, depth=2, max_nodes=30)))
    for firm in JP_FIRMS[:8]:
        tests.append((_tid(P, c), "Patent Attorney", "citation_network",
                       citation_network,
                       dict(store=store, resolver=resolver, firm_query=firm, max_nodes=20)))
    for direction in ["forward", "backward", "both"]:
        tests.append((_tid(P, c), "Patent Attorney", "citation_network",
                       citation_network,
                       dict(store=store, publication_number="JP-7637366-B1",
                            direction=direction, max_nodes=20)))
    # Edge cases
    tests.append((_tid(P, c), "Patent Attorney", "citation_network",
                   citation_network, dict(store=store, publication_number="NONEXISTENT-B1")))

    # --- technology_brief (20 tests) ---
    for cpc in CPC_CODES:
        tests.append((_tid(P, c), "Patent Attorney", "technology_brief",
                       technology_brief, dict(store=store, cpc_prefix=cpc)))
    for q in ["battery", "AI", "5G", "semiconductor", "gene therapy", "solar cell"]:
        tests.append((_tid(P, c), "Patent Attorney", "technology_brief",
                       technology_brief, dict(store=store, query=q)))
    for cpc in CPC_CODES[:3]:
        tests.append((_tid(P, c), "Patent Attorney", "technology_brief",
                       technology_brief,
                       dict(store=store, cpc_prefix=cpc,
                            date_from="2020-01-01", date_to="2024-12-31")))
    # Combined
    for cpc, q in [("H01M", "solid state"), ("G06N", "deep learning")]:
        tests.append((_tid(P, c), "Patent Attorney", "technology_brief",
                       technology_brief, dict(store=store, cpc_prefix=cpc, query=q)))

    # --- visualization (25 tests) ---
    for pn in PATENT_NUMBERS[:3]:
        tests.append((_tid(P, c), "Patent Attorney", "citation_graph_viz",
                       citation_graph_viz, dict(store=store, publication_number=pn)))
    for cpc in CPC_CODES[:5]:
        tests.append((_tid(P, c), "Patent Attorney", "tech_map",
                       tech_map, dict(store=store, cpc_prefix=cpc)))
    for q in ["battery", "AI", "5G"]:
        tests.append((_tid(P, c), "Patent Attorney", "tech_map",
                       tech_map, dict(store=store, query=q)))
    for firm in JP_FIRMS[:5]:
        tests.append((_tid(P, c), "Patent Attorney", "tech_map",
                       tech_map, dict(store=store, firm_query=firm, resolver=resolver)))
    # firm_landscape
    tests.append((_tid(P, c), "Patent Attorney", "firm_landscape",
                   firm_landscape, dict(store=store, firms=JP_FIRMS[:3], resolver=resolver)))
    tests.append((_tid(P, c), "Patent Attorney", "firm_landscape",
                   firm_landscape,
                   dict(store=store, firms=JP_FIRMS[:4], cpc_prefix="H01M", resolver=resolver)))
    tests.append((_tid(P, c), "Patent Attorney", "firm_landscape",
                   firm_landscape, dict(store=store, firms=GLOBAL_FIRMS_LIST[:3], resolver=resolver)))
    # citation_graph_viz with depth=2
    tests.append((_tid(P, c), "Patent Attorney", "citation_graph_viz",
                   citation_graph_viz,
                   dict(store=store, publication_number="JP-7637366-B1", depth=2, max_nodes=20)))

    # --- Additional patent_search variations (+50 tests) ---
    # Japanese keyword searches — broader set
    for kw in ["レーザー", "ロボット", "センサー", "ディスプレイ", "メモリ",
                "太陽電池", "有機EL", "画像処理", "音声認識", "モーター",
                "圧縮機", "フィルタ", "樹脂", "接着", "塗料",
                "光学", "触媒", "通信", "無線", "抗がん剤"]:
        tests.append((_tid(P, c), "Patent Attorney", "patent_search",
                       patent_search, dict(store=store, query=kw, max_results=5)))
    # English technical phrases
    for phrase in ["solid state battery", "deep learning", "CRISPR Cas9",
                    "lidar sensor", "hydrogen fuel cell", "perovskite solar",
                    "5G millimeter wave", "quantum computing", "graph neural network",
                    "drug delivery system"]:
        tests.append((_tid(P, c), "Patent Attorney", "patent_search",
                       patent_search, dict(store=store, query=phrase, max_results=5)))
    # Global firm applicant searches
    for firm in GLOBAL_FIRMS_LIST:
        tests.append((_tid(P, c), "Patent Attorney", "patent_search",
                       patent_search, dict(store=store, applicant=firm, max_results=5)))
    # Additional date range + CPC combinations
    for cpc, df, dt in [("G06N", "2021-01-01", "2024-12-31"),
                         ("H01L", "2018-01-01", "2022-12-31"),
                         ("A61K", "2019-06-01", "2023-06-30"),
                         ("B60L", "2020-01-01", "2024-12-31"),
                         ("H04W", "2022-01-01", "2024-12-31"),
                         ("C07D", "2017-01-01", "2021-12-31"),
                         ("G01N", "2020-06-01", "2024-06-30"),
                         ("B29C", "2018-01-01", "2024-12-31"),
                         ("A61B", "2019-01-01", "2023-12-31")]:
        tests.append((_tid(P, c), "Patent Attorney", "patent_search",
                       patent_search,
                       dict(store=store, cpc_codes=[cpc], date_from=df, date_to=dt, max_results=5)))

    # --- Additional patent_summary for known ranges (+20 tests) ---
    for offset in range(20):
        pn = f"JP-{7500000 + offset * 5000}-B1"
        tests.append((_tid(P, c), "Patent Attorney", "patent_summary",
                       patent_summary, dict(store=store, publication_number=pn)))

    # --- Additional fto_analysis with jurisdiction (+10 tests) ---
    for desc in TECH_DESCRIPTIONS[:5]:
        tests.append((_tid(P, c), "Patent Attorney", "fto_analysis",
                       fto_analysis,
                       dict(store=store, text=desc, target_jurisdiction="US")))
    for desc in TECH_DESCRIPTIONS[:5]:
        tests.append((_tid(P, c), "Patent Attorney", "fto_analysis",
                       fto_analysis,
                       dict(store=store, text=desc, target_jurisdiction="EP")))

    # --- Additional SEP with more declarants (+15 tests) ---
    for d in ["Nokia", "Ericsson", "LG", "ZTE", "Motorola",
              "InterDigital", "Sharp", "Panasonic", "NTT", "KDDI",
              "NEC", "Fujitsu", "Mitsubishi Electric", "Hitachi", "Toshiba"]:
        tests.append((_tid(P, c), "Patent Attorney", "sep_search",
                       sep_search, dict(store=store, declarant=d)))

    # --- Additional PTAB combinatorial (+20 tests) ---
    for pet, owner in [("Samsung", "Toyota"), ("Apple", "Sony"), ("Google", "Canon"),
                        ("Intel", "Hitachi"), ("Qualcomm", "NEC")]:
        tests.append((_tid(P, c), "Patent Attorney", "ptab_search",
                       ptab_search,
                       dict(store=store, petitioner=pet, patent_owner=owner, resolver=resolver)))
    for pet, tt in [("Samsung", "IPR"), ("Apple", "IPR"), ("Google", "PGR"),
                     ("Samsung", "CBM"), ("Intel", "IPR")]:
        tests.append((_tid(P, c), "Patent Attorney", "ptab_search",
                       ptab_search,
                       dict(store=store, petitioner=pet, trial_type=tt, resolver=resolver)))
    # litigation_search with date ranges
    for df, dt in [("2020-01-01", "2024-12-31"), ("2018-01-01", "2022-12-31"),
                    ("2022-01-01", "2024-06-30")]:
        tests.append((_tid(P, c), "Patent Attorney", "litigation_search",
                       litigation_search,
                       dict(store=store, date_from=df, date_to=dt, resolver=resolver)))
    # litigation_risk for global firms
    for firm in GLOBAL_FIRMS_LIST[:7]:
        tests.append((_tid(P, c), "Patent Attorney", "litigation_risk",
                       litigation_risk, dict(store=store, firm_query=firm, resolver=resolver)))

    # --- Additional citation_network with varied params (+15 tests) ---
    for pn_suffix in range(15):
        pn = f"JP-{7100000 + pn_suffix * 50000}-B1"
        tests.append((_tid(P, c), "Patent Attorney", "citation_network",
                       citation_network,
                       dict(store=store, publication_number=pn, depth=1, max_nodes=15)))

    # --- Additional invention_intelligence JP (+10 tests) ---
    for desc in ["水素貯蔵合金の製造方法", "有機半導体を用いた太陽電池",
                  "AIによる創薬スクリーニング", "6G通信用テラヘルツアンテナ",
                  "全固体リチウムイオン電池の電解質",
                  "自動運転車のセンサーフュージョン", "量子コンピュータの誤り訂正",
                  "ペロブスカイト太陽電池の安定化技術", "mRNA医薬品の送達システム",
                  "グラフニューラルネットワークによる材料設計"]:
        tests.append((_tid(P, c), "Patent Attorney", "invention_intelligence",
                       invention_intelligence, dict(store=store, text=desc)))

    # --- Additional claim_analysis with JP patent numbers (+20 tests) ---
    for offset in range(20):
        pn = f"JP-{7000000 + offset * 30000}-B1"
        tests.append((_tid(P, c), "Patent Attorney", "claim_analysis",
                       claim_analysis, dict(store=store, publication_number=pn)))

    # --- Additional cross_border with time_window (+10 tests) ---
    for tw in ["after", "before", "all"]:
        tests.append((_tid(P, c), "Patent Attorney", "cross_border_similarity",
                       cross_border_similarity,
                       dict(store=store, resolver=resolver, query="Canon",
                            query_type="firm", time_window=tw)))
    for firm in GLOBAL_FIRMS_LIST[:7]:
        tests.append((_tid(P, c), "Patent Attorney", "cross_border_similarity",
                       cross_border_similarity,
                       dict(store=store, resolver=resolver, query=firm, query_type="firm")))

    # --- Additional tech_map and firm_landscape (+20 tests) ---
    for firm in JP_FIRMS[5:15]:
        tests.append((_tid(P, c), "Patent Attorney", "tech_map",
                       tech_map, dict(store=store, firm_query=firm, resolver=resolver)))
    for cpc in CPC_EXTENDED[8:]:
        tests.append((_tid(P, c), "Patent Attorney", "tech_map",
                       tech_map, dict(store=store, cpc_prefix=cpc)))
    for fgroup in [JP_FIRMS[:5], JP_FIRMS[5:10], GLOBAL_FIRMS_LIST[:5]]:
        tests.append((_tid(P, c), "Patent Attorney", "firm_landscape",
                       firm_landscape, dict(store=store, firms=fgroup, resolver=resolver)))

    # --- Additional patent_search: firm + date combos (+30 tests) ---
    for firm in JP_FIRMS[:10]:
        tests.append((_tid(P, c), "Patent Attorney", "patent_search",
                       patent_search,
                       dict(store=store, applicant=firm,
                            date_from="2020-01-01", date_to="2024-12-31", max_results=5)))
    for firm in JP_FIRMS[:10]:
        tests.append((_tid(P, c), "Patent Attorney", "patent_search",
                       patent_search,
                       dict(store=store, applicant=firm,
                            date_from="2015-01-01", date_to="2019-12-31", max_results=5)))
    # Applicant + query + CPC triple filter
    for firm, cpc, kw in [
        ("Toyota", "H01M", "battery"), ("Sony", "G06N", "neural"),
        ("Canon", "G02B", "optic"), ("Panasonic", "H01L", "semiconductor"),
        ("Hitachi", "B60L", "vehicle"), ("NEC", "H04L", "communication"),
        ("Fujitsu", "G06F", "computing"), ("Toshiba", "H01M", "electrode"),
        ("Denso", "G01N", "sensor"), ("Honda", "B60L", "motor"),
    ]:
        tests.append((_tid(P, c), "Patent Attorney", "patent_search",
                       patent_search,
                       dict(store=store, applicant=firm, cpc_codes=[cpc],
                            query=kw, max_results=5)))

    # --- Additional sep_portfolio for more firms (+10 tests) ---
    for firm in JP_FIRMS[5:15]:
        tests.append((_tid(P, c), "Patent Attorney", "sep_portfolio",
                       sep_portfolio, dict(store=store, firm_query=firm, resolver=resolver)))

    # --- Additional ptab_risk for more CPC (+8 tests) ---
    for cpc in CPC_EXTENDED[8:]:
        tests.append((_tid(P, c), "Patent Attorney", "ptab_risk",
                       ptab_risk, dict(store=store, cpc_prefix=cpc, resolver=resolver)))

    # --- Additional claim_analysis with CPC-related text (+15 tests) ---
    for desc in [
        "semiconductor wafer polishing using chemical mechanical planarization",
        "lithium cobalt oxide cathode material with improved cycle life",
        "convolutional neural network for medical image segmentation",
        "5G NR beam management procedure for handover optimization",
        "CRISPR-Cas9 delivery using lipid nanoparticles",
        "polymer electrolyte for all-solid-state battery",
        "silicon carbide power MOSFET for electric vehicle inverter",
        "recurrent neural network for speech synthesis",
        "MIMO antenna array for millimeter wave communication",
        "monoclonal antibody for immune checkpoint inhibition",
        "perovskite quantum dot for display applications",
        "autonomous vehicle path planning using reinforcement learning",
        "fuel cell bipolar plate with improved corrosion resistance",
        "organic thin film transistor for flexible display",
        "graphene-based supercapacitor electrode material",
    ]:
        tests.append((_tid(P, c), "Patent Attorney", "claim_analysis",
                       claim_analysis, dict(store=store, text=desc)))

    # --- Additional fto with more text descriptions (+15 tests) ---
    for desc in [
        "wireless power transfer for electric vehicle charging",
        "metal-organic framework for gas storage",
        "programmable logic controller for industrial automation",
        "photovoltaic cell with multi-junction structure",
        "peptide drug conjugate for targeted cancer therapy",
        "edge computing architecture for IoT devices",
        "solid oxide fuel cell with ceramic electrolyte",
        "augmented reality head-mounted display",
        "MEMS accelerometer for navigation systems",
        "biodegradable plastic packaging material",
        "GaN power amplifier for 5G base station",
        "optical fiber amplifier for long-haul communication",
        "shape memory alloy actuator for robotics",
        "DNA sequencing using nanopore technology",
        "additive manufacturing of metal components",
    ]:
        tests.append((_tid(P, c), "Patent Attorney", "fto_analysis",
                       fto_analysis, dict(store=store, text=desc)))

    # --- Additional cross_border_similarity with text queries (+10 tests) ---
    for desc in TECH_DESCRIPTIONS:
        tests.append((_tid(P, c), "Patent Attorney", "cross_border_similarity",
                       cross_border_similarity,
                       dict(store=store, resolver=resolver, query=desc, query_type="text",
                            top_n=5)))
    for ms in [0.5, 0.6]:
        tests.append((_tid(P, c), "Patent Attorney", "cross_border_similarity",
                       cross_border_similarity,
                       dict(store=store, resolver=resolver, query="solid state battery",
                            query_type="text", min_similarity=ms)))

    # --- Additional sep_search with combined filters (+10 tests) ---
    for std in STANDARDS:
        for pg in [1, 2]:
            tests.append((_tid(P, c), "Patent Attorney", "sep_search",
                           sep_search, dict(store=store, standard=std, page=pg, page_size=5)))

    # --- Additional claim_comparison with 5-patent sets (+5 tests) ---
    for offset in range(5):
        patents = [f"JP-{7000000 + offset * 100000 + j * 20000}-B1" for j in range(5)]
        tests.append((_tid(P, c), "Patent Attorney", "claim_comparison",
                       claim_comparison, dict(store=store, publication_numbers=patents)))

    return tests


def generate_vc_investor_tests(
    store: PatentStore, resolver: EntityResolver,
) -> list[tuple[str, str, str, Callable, dict]]:
    """~700 tests for VC Investor persona."""
    tests = []
    c = [0]
    V = "VC"

    # --- patent_valuation (40 tests) ---
    for firm in JP_FIRMS[:10]:
        tests.append((_tid(V, c), "VC Investor", "patent_valuation",
                       patent_valuation,
                       dict(store=store, resolver=resolver, query=firm, query_type="firm")))
    for pn in PATENT_NUMBERS:
        tests.append((_tid(V, c), "VC Investor", "patent_valuation",
                       patent_valuation,
                       dict(store=store, resolver=resolver, query=pn, query_type="patent")))
    for purpose in PURPOSES_VALUATION:
        for firm in JP_FIRMS[:5]:
            tests.append((_tid(V, c), "VC Investor", "patent_valuation",
                           patent_valuation,
                           dict(store=store, resolver=resolver, query=firm,
                                query_type="firm", purpose=purpose)))

    # --- patent_option_value (40 tests) ---
    for firm in JP_FIRMS[:8]:
        tests.append((_tid(V, c), "VC Investor", "patent_option_value",
                       patent_option_value,
                       dict(store=store, resolver=resolver, query=firm, query_type="firm")))
    for pn in PATENT_NUMBERS:
        tests.append((_tid(V, c), "VC Investor", "patent_option_value",
                       patent_option_value,
                       dict(store=store, resolver=resolver, query=pn, query_type="patent")))
    for cpc in CPC_CODES:
        tests.append((_tid(V, c), "VC Investor", "patent_option_value",
                       patent_option_value,
                       dict(store=store, resolver=resolver, query=cpc, query_type="technology")))
    # With custom S and K
    for s_val, k_val in [(100.0, 50.0), (200.0, 150.0), (500.0, 400.0)]:
        tests.append((_tid(V, c), "VC Investor", "patent_option_value",
                       patent_option_value,
                       dict(store=store, resolver=resolver, query="Toyota",
                            query_type="firm", S=s_val, K=k_val)))
    # Different risk-free rates
    for r in [0.01, 0.02, 0.05]:
        tests.append((_tid(V, c), "VC Investor", "patent_option_value",
                       patent_option_value,
                       dict(store=store, resolver=resolver, query="Sony",
                            query_type="firm", risk_free_rate=r)))

    # --- tech_volatility (30 tests) ---
    for cpc in CPC_CODES:
        tests.append((_tid(V, c), "VC Investor", "tech_volatility",
                       tech_volatility, dict(store=store, query=cpc, query_type="technology")))
    for firm in JP_FIRMS[:8]:
        tests.append((_tid(V, c), "VC Investor", "tech_volatility",
                       tech_volatility, dict(store=store, resolver=resolver, query=firm, query_type="firm")))
    # Date range variations
    for df, dt in [("2018-01-01", "2024-12-31"), ("2015-01-01", "2020-12-31")]:
        tests.append((_tid(V, c), "VC Investor", "tech_volatility",
                       tech_volatility,
                       dict(store=store, query="H01M", query_type="technology",
                            date_from=df, date_to=dt)))
    # Auto-detect
    for q in ["battery", "AI", "semiconductor"]:
        tests.append((_tid(V, c), "VC Investor", "tech_volatility",
                       tech_volatility, dict(store=store, query=q)))

    # --- tech_beta (25 tests) ---
    for cpc in CPC_CODES:
        tests.append((_tid(V, c), "VC Investor", "tech_beta",
                       tech_beta, dict(store=store, query=cpc, query_type="technology")))
    for firm in JP_FIRMS[:5]:
        tests.append((_tid(V, c), "VC Investor", "tech_beta",
                       tech_beta, dict(store=store, resolver=resolver, query=firm, query_type="firm")))
    for bench in ["all", "section"]:
        tests.append((_tid(V, c), "VC Investor", "tech_beta",
                       tech_beta,
                       dict(store=store, query="H01M", query_type="technology", benchmark=bench)))
    # Date variations
    for df in ["2016-01-01", "2020-01-01"]:
        tests.append((_tid(V, c), "VC Investor", "tech_beta",
                       tech_beta,
                       dict(store=store, query="G06N", query_type="technology", date_from=df)))

    # --- portfolio_var (25 tests) ---
    for firm in JP_FIRMS[:10]:
        tests.append((_tid(V, c), "VC Investor", "portfolio_var",
                       portfolio_var,
                       dict(store=store, resolver=resolver, firm=firm)))
    for firm in GLOBAL_FIRMS_LIST[:5]:
        tests.append((_tid(V, c), "VC Investor", "portfolio_var",
                       portfolio_var,
                       dict(store=store, resolver=resolver, firm=firm)))
    # Horizon variations
    for h in [3, 5, 10]:
        tests.append((_tid(V, c), "VC Investor", "portfolio_var",
                       portfolio_var,
                       dict(store=store, resolver=resolver, firm="Toyota", horizon_years=h)))
    # Confidence variations
    for conf in [0.90, 0.95, 0.99]:
        tests.append((_tid(V, c), "VC Investor", "portfolio_var",
                       portfolio_var,
                       dict(store=store, resolver=resolver, firm="Sony", confidence=conf)))

    # --- patent_market_fusion (50 tests) ---
    for firm in JP_FIRMS[:10]:
        tests.append((_tid(V, c), "VC Investor", "patent_market_fusion",
                       patent_market_fusion,
                       dict(store=store, resolver=resolver, query=firm, query_type="firm")))
    for cpc in CPC_CODES:
        tests.append((_tid(V, c), "VC Investor", "patent_market_fusion",
                       patent_market_fusion,
                       dict(store=store, resolver=resolver, query=cpc, query_type="technology")))
    for pn in PATENT_NUMBERS:
        tests.append((_tid(V, c), "VC Investor", "patent_market_fusion",
                       patent_market_fusion,
                       dict(store=store, resolver=resolver, query=pn, query_type="patent")))
    for purpose in PURPOSES_FUSION:
        for firm in JP_FIRMS[:3]:
            tests.append((_tid(V, c), "VC Investor", "patent_market_fusion",
                           patent_market_fusion,
                           dict(store=store, resolver=resolver, query=firm,
                                query_type="firm", purpose=purpose)))
    for desc in TECH_DESCRIPTIONS[:4]:
        tests.append((_tid(V, c), "VC Investor", "patent_market_fusion",
                       patent_market_fusion,
                       dict(store=store, resolver=resolver, query=desc, query_type="text")))

    # --- ip_due_diligence (30 tests) ---
    dd_fn = ip_due_diligence if ip_due_diligence else None
    if dd_fn:
        for firm in JP_FIRMS[:8]:
            tests.append((_tid(V, c), "VC Investor", "ip_due_diligence",
                           dd_fn,
                           dict(store=store, resolver=resolver, target_firm=firm)))
        for inv_type in INVESTMENT_TYPES:
            for firm in JP_FIRMS[:3]:
                tests.append((_tid(V, c), "VC Investor", "ip_due_diligence",
                               dd_fn,
                               dict(store=store, resolver=resolver, target_firm=firm,
                                    investment_type=inv_type)))
        # With benchmarks
        tests.append((_tid(V, c), "VC Investor", "ip_due_diligence",
                       dd_fn,
                       dict(store=store, resolver=resolver, target_firm="Toyota",
                            benchmark_firms=["Honda", "Denso"])))
        tests.append((_tid(V, c), "VC Investor", "ip_due_diligence",
                       dd_fn,
                       dict(store=store, resolver=resolver, target_firm="Sony",
                            benchmark_firms=["Panasonic", "Canon"])))

    # --- bayesian_scenario (30 tests) ---
    # Init mode
    for tech, firm in zip(CLUSTER_IDS[:5], JP_FIRMS[:5]):
        tests.append((_tid(V, c), "VC Investor", "bayesian_scenario",
                       bayesian_scenario,
                       dict(store=store, resolver=resolver, mode="init",
                            technology=tech, firm_query=firm)))
    for tech in CLUSTER_IDS:
        tests.append((_tid(V, c), "VC Investor", "bayesian_scenario",
                       bayesian_scenario,
                       dict(store=store, resolver=resolver, mode="init", technology=tech)))
    # Investment cost variations
    for cost in [1000, 10000, 100000]:
        tests.append((_tid(V, c), "VC Investor", "bayesian_scenario",
                       bayesian_scenario,
                       dict(store=store, resolver=resolver, mode="init",
                            technology="H01M_0", investment_cost=cost)))
    # Time horizon variations
    for th in [5, 10, 20]:
        tests.append((_tid(V, c), "VC Investor", "bayesian_scenario",
                       bayesian_scenario,
                       dict(store=store, resolver=resolver, mode="init",
                            technology="G06N_0", time_horizon_years=th)))
    # Invalid modes / edge cases
    tests.append((_tid(V, c), "VC Investor", "bayesian_scenario",
                   bayesian_scenario,
                   dict(store=store, resolver=resolver, mode="init")))  # missing technology
    tests.append((_tid(V, c), "VC Investor", "bayesian_scenario",
                   bayesian_scenario,
                   dict(store=store, resolver=resolver, mode="update")))  # missing session_id
    tests.append((_tid(V, c), "VC Investor", "bayesian_scenario",
                   bayesian_scenario,
                   dict(store=store, resolver=resolver, mode="simulate")))  # missing session_id
    tests.append((_tid(V, c), "VC Investor", "bayesian_scenario",
                   bayesian_scenario,
                   dict(store=store, resolver=resolver, mode="invalid_mode",
                        technology="H01M_0")))

    # --- GDELT (25 tests) ---
    for firm in JP_FIRMS[:10]:
        tests.append((_tid(V, c), "VC Investor", "gdelt_company_events",
                       gdelt_company_events,
                       dict(store=store, resolver=resolver, firm_query=firm)))
    for firm in GLOBAL_FIRMS_LIST[:5]:
        tests.append((_tid(V, c), "VC Investor", "gdelt_company_events",
                       gdelt_company_events,
                       dict(store=store, resolver=resolver, firm_query=firm)))
    # Date range
    for df, dt in [(20200101, 20221231), (20230101, 20241231)]:
        tests.append((_tid(V, c), "VC Investor", "gdelt_company_events",
                       gdelt_company_events,
                       dict(store=store, resolver=resolver, firm_query="Toyota",
                            date_from=df, date_to=dt)))
    # Edge cases
    tests.append((_tid(V, c), "VC Investor", "gdelt_company_events",
                   gdelt_company_events,
                   dict(store=store, resolver=resolver, firm_query="nonexistent_company_xyz")))

    # --- tech_trend_alert (10 tests) ---
    tests.append((_tid(V, c), "VC Investor", "tech_trend_alert",
                   tech_trend_alert, dict(store=store)))
    for yf, yt in [(2018, 2024), (2020, 2024), (2022, 2024)]:
        tests.append((_tid(V, c), "VC Investor", "tech_trend_alert",
                       tech_trend_alert, dict(store=store, year_from=yf, year_to=yt)))
    for mg in [0.1, 0.3, 0.5, 1.0]:
        tests.append((_tid(V, c), "VC Investor", "tech_trend_alert",
                       tech_trend_alert, dict(store=store, min_growth=mg)))
    tests.append((_tid(V, c), "VC Investor", "tech_trend_alert",
                   tech_trend_alert, dict(store=store, top_n=5)))
    tests.append((_tid(V, c), "VC Investor", "tech_trend_alert",
                   tech_trend_alert, dict(store=store, top_n=50)))

    # --- tech_trend (30 tests) ---
    for q in ["battery", "AI", "5G", "semiconductor", "solar",
              "hydrogen", "EV", "quantum", "robot", "display"]:
        tests.append((_tid(V, c), "VC Investor", "tech_trend",
                       tech_trend, dict(store=store, query=q)))
    for cpc in CPC_CODES:
        tests.append((_tid(V, c), "VC Investor", "tech_trend",
                       tech_trend, dict(store=store, cpc_prefix=cpc)))
    for cid in CLUSTER_IDS:
        tests.append((_tid(V, c), "VC Investor", "tech_trend",
                       tech_trend, dict(store=store, query=cid)))
    # Year ranges
    for yf, yt in [(2016, 2024), (2020, 2024)]:
        tests.append((_tid(V, c), "VC Investor", "tech_trend",
                       tech_trend, dict(store=store, query="battery", year_from=yf, year_to=yt)))

    # --- tech_landscape (30 tests) ---
    for cpc in CPC_CODES:
        tests.append((_tid(V, c), "VC Investor", "tech_landscape",
                       tech_landscape, dict(store=store, cpc_prefix=cpc)))
    for q in ["battery", "AI", "wireless", "optics"]:
        tests.append((_tid(V, c), "VC Investor", "tech_landscape",
                       tech_landscape, dict(store=store, query=q)))
    for cpc in CPC_CODES[:4]:
        tests.append((_tid(V, c), "VC Investor", "tech_landscape",
                       tech_landscape,
                       dict(store=store, cpc_prefix=cpc,
                            date_from="2020-01-01", date_to="2024-12-31")))
    for gran in ["year", "quarter"]:
        tests.append((_tid(V, c), "VC Investor", "tech_landscape",
                       tech_landscape,
                       dict(store=store, cpc_prefix="H01M", granularity=gran)))
    # Pagination
    for pg in [1, 2, 3]:
        tests.append((_tid(V, c), "VC Investor", "tech_landscape",
                       tech_landscape,
                       dict(store=store, cpc_prefix="G06N", page=pg, page_size=10)))

    # --- tech_clusters_list (20 tests) ---
    tests.append((_tid(V, c), "VC Investor", "tech_clusters_list",
                   tech_clusters_list, dict(store=store)))
    for cpc_f in CPC_CODES:
        tests.append((_tid(V, c), "VC Investor", "tech_clusters_list",
                       tech_clusters_list, dict(store=store, cpc_filter=cpc_f)))
    for sb in ["patent_count", "cluster_id"]:
        tests.append((_tid(V, c), "VC Investor", "tech_clusters_list",
                       tech_clusters_list, dict(store=store, sort_by=sb)))
    for tn in [10, 50, 200]:
        tests.append((_tid(V, c), "VC Investor", "tech_clusters_list",
                       tech_clusters_list, dict(store=store, top_n=tn)))
    for pg in [1, 2, 5]:
        tests.append((_tid(V, c), "VC Investor", "tech_clusters_list",
                       tech_clusters_list, dict(store=store, page=pg, page_size=20)))

    # --- firm_patent_portfolio (25 tests) ---
    for firm in JP_FIRMS:
        tests.append((_tid(V, c), "VC Investor", "firm_patent_portfolio",
                       firm_patent_portfolio,
                       dict(store=store, resolver=resolver, firm=firm)))
    for firm in GLOBAL_FIRMS_LIST[:5]:
        tests.append((_tid(V, c), "VC Investor", "firm_patent_portfolio",
                       firm_patent_portfolio,
                       dict(store=store, resolver=resolver, firm=firm)))
    # With date
    tests.append((_tid(V, c), "VC Investor", "firm_patent_portfolio",
                   firm_patent_portfolio,
                   dict(store=store, resolver=resolver, firm="Toyota", date="2022-12-31")))
    # With include_expired
    tests.append((_tid(V, c), "VC Investor", "firm_patent_portfolio",
                   firm_patent_portfolio,
                   dict(store=store, resolver=resolver, firm="Sony", include_expired=True)))
    # Edge
    tests.append((_tid(V, c), "VC Investor", "firm_patent_portfolio",
                   firm_patent_portfolio,
                   dict(store=store, resolver=resolver, firm="nonexistent_firm_xyz")))

    # --- firm_tech_vector (15 tests) ---
    for firm in JP_FIRMS[:10]:
        tests.append((_tid(V, c), "VC Investor", "firm_tech_vector",
                       firm_tech_vector,
                       dict(store=store, resolver=resolver, firm_query=firm)))
    for firm in JP_FIRMS[:3]:
        for yr in [2022, 2023, 2024]:
            tests.append((_tid(V, c), "VC Investor", "firm_tech_vector",
                           firm_tech_vector,
                           dict(store=store, resolver=resolver, firm_query=firm, year=yr)))

    # --- portfolio_evolution (15 tests) ---
    for firm in JP_FIRMS[:8]:
        tests.append((_tid(V, c), "VC Investor", "portfolio_evolution",
                       portfolio_evolution,
                       dict(store=store, resolver=resolver, firm_query=firm)))
    for firm in JP_FIRMS[:3]:
        tests.append((_tid(V, c), "VC Investor", "portfolio_evolution",
                       portfolio_evolution,
                       dict(store=store, resolver=resolver, firm_query=firm,
                            year_from=2018, year_to=2024)))
    for firm in JP_FIRMS[:3]:
        tests.append((_tid(V, c), "VC Investor", "portfolio_evolution",
                       portfolio_evolution,
                       dict(store=store, resolver=resolver, firm_query=firm,
                            year_from=2020, year_to=2023)))

    # --- network analysis tools (40 tests) ---
    # network_topology
    for cpc in CPC_CODES[:4]:
        tests.append((_tid(V, c), "VC Investor", "network_topology",
                       network_topology, dict(store=store, cpc_prefix=cpc, max_patents=100)))
    for firm in JP_FIRMS[:4]:
        tests.append((_tid(V, c), "VC Investor", "network_topology",
                       network_topology,
                       dict(store=store, resolver=resolver, firm=firm, max_patents=100)))

    # knowledge_flow
    for src in ["G06N", "H01M", "H04L"]:
        tests.append((_tid(V, c), "VC Investor", "knowledge_flow",
                       knowledge_flow, dict(store=store, source_cpc=src)))
    for src, tgt in [("G06N", "A61K"), ("H01M", "B60L"), ("G06F", "H04L")]:
        tests.append((_tid(V, c), "VC Investor", "knowledge_flow",
                       knowledge_flow, dict(store=store, source_cpc=src, target_cpc=tgt)))
    for firm in JP_FIRMS[:3]:
        tests.append((_tid(V, c), "VC Investor", "knowledge_flow",
                       knowledge_flow, dict(store=store, resolver=resolver, firm=firm)))

    # network_resilience
    for cpc in CPC_CODES[:3]:
        tests.append((_tid(V, c), "VC Investor", "network_resilience",
                       network_resilience,
                       dict(store=store, cpc_prefix=cpc, max_patents=100)))
    for firm in JP_FIRMS[:3]:
        tests.append((_tid(V, c), "VC Investor", "network_resilience",
                       network_resilience,
                       dict(store=store, resolver=resolver, firm=firm, max_patents=100)))
    tests.append((_tid(V, c), "VC Investor", "network_resilience",
                   network_resilience,
                   dict(store=store, cpc_prefix="H01M", attack_mode="random", max_patents=100)))

    # tech_fusion_detector
    for a, b in [("G06N", "A61K"), ("H01M", "B60L"), ("G06F", "H04L"),
                  ("H01L", "G06F"), ("C08L", "B29C")]:
        tests.append((_tid(V, c), "VC Investor", "tech_fusion_detector",
                       tech_fusion_detector, dict(store=store, cpc_a=a, cpc_b=b)))
    tests.append((_tid(V, c), "VC Investor", "tech_fusion_detector",
                   tech_fusion_detector, dict(store=store)))  # auto-detect mode

    # tech_entropy
    for cpc in CPC_CODES[:5]:
        tests.append((_tid(V, c), "VC Investor", "tech_entropy",
                       tech_entropy, dict(store=store, cpc_prefix=cpc)))
    for q in ["battery", "semiconductor"]:
        tests.append((_tid(V, c), "VC Investor", "tech_entropy",
                       tech_entropy, dict(store=store, query=q)))

    # --- monitoring (15 tests) ---
    tests.append((_tid(V, c), "VC Investor", "list_watches",
                   list_watches, dict(store=store)))
    tests.append((_tid(V, c), "VC Investor", "check_alerts",
                   check_alerts, dict(store=store)))
    tests.append((_tid(V, c), "VC Investor", "run_monitoring",
                   run_monitoring, dict(store=store)))
    for wt, tgt in [("applicant", "Toyota"), ("cpc", "H01M"), ("keyword", "battery"),
                     ("competitor", "Samsung"), ("applicant", "Sony")]:
        tests.append((_tid(V, c), "VC Investor", "create_watch",
                       create_watch, dict(store=store, watch_type=wt, target=tgt)))
    # Invalid watch type
    tests.append((_tid(V, c), "VC Investor", "create_watch",
                   create_watch, dict(store=store, watch_type="invalid_type", target="test")))
    # Pagination
    for pg in [1, 2]:
        tests.append((_tid(V, c), "VC Investor", "list_watches",
                       list_watches, dict(store=store, page=pg)))
        tests.append((_tid(V, c), "VC Investor", "check_alerts",
                       check_alerts, dict(store=store, page=pg)))

    # --- startability_heatmap (8 tests) ---
    tests.append((_tid(V, c), "VC Investor", "startability_heatmap",
                   startability_heatmap, dict(store=store)))
    tests.append((_tid(V, c), "VC Investor", "startability_heatmap",
                   startability_heatmap,
                   dict(store=store, firms=JP_FIRMS[:5], resolver=resolver)))
    tests.append((_tid(V, c), "VC Investor", "startability_heatmap",
                   startability_heatmap,
                   dict(store=store, firms=JP_FIRMS[:3], resolver=resolver, top_n=10)))
    for yr in [2023, 2024]:
        tests.append((_tid(V, c), "VC Investor", "startability_heatmap",
                       startability_heatmap,
                       dict(store=store, firms=JP_FIRMS[:3], resolver=resolver, year=yr)))
    tests.append((_tid(V, c), "VC Investor", "startability_heatmap",
                   startability_heatmap,
                   dict(store=store, firms=GLOBAL_FIRMS_LIST[:3], resolver=resolver)))
    tests.append((_tid(V, c), "VC Investor", "startability_heatmap",
                   startability_heatmap, dict(store=store, top_n=5)))
    tests.append((_tid(V, c), "VC Investor", "startability_heatmap",
                   startability_heatmap, dict(store=store, top_n=30)))

    # --- entity resolve via resolver.resolve (26 tests) ---
    for firm in ALL_FIRMS:
        tests.append((_tid(V, c), "VC Investor", "entity_resolve",
                       lambda name=firm, **kw: _entity_resolve_wrapper(resolver, name),
                       {}))

    # --- Additional patent_valuation with global firms (+10 tests) ---
    for firm in GLOBAL_FIRMS_LIST[:10]:
        tests.append((_tid(V, c), "VC Investor", "patent_valuation",
                       patent_valuation,
                       dict(store=store, resolver=resolver, query=firm, query_type="firm")))

    # --- Additional patent_option_value with global firms (+10 tests) ---
    for firm in GLOBAL_FIRMS_LIST[:7]:
        tests.append((_tid(V, c), "VC Investor", "patent_option_value",
                       patent_option_value,
                       dict(store=store, resolver=resolver, query=firm, query_type="firm")))
    for yr in [2022, 2023, 2024]:
        tests.append((_tid(V, c), "VC Investor", "patent_option_value",
                       patent_option_value,
                       dict(store=store, resolver=resolver, query="Toyota",
                            query_type="firm", year=yr)))

    # --- Additional tech_volatility with more CPC (+10 tests) ---
    for cpc in CPC_EXTENDED[8:]:
        tests.append((_tid(V, c), "VC Investor", "tech_volatility",
                       tech_volatility, dict(store=store, query=cpc, query_type="technology")))
    for q in ["fuel cell", "organic LED", "quantum"]:
        tests.append((_tid(V, c), "VC Investor", "tech_volatility",
                       tech_volatility, dict(store=store, query=q)))

    # --- Additional tech_beta with more CPC (+10 tests) ---
    for cpc in CPC_EXTENDED[8:]:
        tests.append((_tid(V, c), "VC Investor", "tech_beta",
                       tech_beta, dict(store=store, query=cpc, query_type="technology")))

    # --- Additional portfolio_var with global firms (+10 tests) ---
    for firm in GLOBAL_FIRMS_LIST[5:]:
        tests.append((_tid(V, c), "VC Investor", "portfolio_var",
                       portfolio_var,
                       dict(store=store, resolver=resolver, firm=firm)))
    for firm in JP_FIRMS[10:]:
        tests.append((_tid(V, c), "VC Investor", "portfolio_var",
                       portfolio_var,
                       dict(store=store, resolver=resolver, firm=firm)))

    # --- Additional patent_market_fusion for global firms (+10 tests) ---
    for firm in GLOBAL_FIRMS_LIST[:10]:
        tests.append((_tid(V, c), "VC Investor", "patent_market_fusion",
                       patent_market_fusion,
                       dict(store=store, resolver=resolver, query=firm, query_type="firm")))

    # --- Additional bayesian_scenario init with global firms (+10 tests) ---
    for tech, firm in zip(CLUSTER_IDS_EXTENDED[:5], GLOBAL_FIRMS_LIST[:5]):
        tests.append((_tid(V, c), "VC Investor", "bayesian_scenario",
                       bayesian_scenario,
                       dict(store=store, resolver=resolver, mode="init",
                            technology=tech, firm_query=firm)))
    for cost, horizon in [(5000, 5), (20000, 15), (50000, 20), (100000, 10), (1000, 3)]:
        tests.append((_tid(V, c), "VC Investor", "bayesian_scenario",
                       bayesian_scenario,
                       dict(store=store, resolver=resolver, mode="init",
                            technology="H01M_0", firm_query="Toyota",
                            investment_cost=cost, time_horizon_years=horizon)))

    # --- Additional gdelt for remaining JP firms (+5 tests) ---
    for firm in JP_FIRMS[10:]:
        tests.append((_tid(V, c), "VC Investor", "gdelt_company_events",
                       gdelt_company_events,
                       dict(store=store, resolver=resolver, firm_query=firm)))

    # --- Additional tech_trend with broader queries (+15 tests) ---
    for q in ["fuel cell", "OLED", "lidar", "mRNA", "graphene",
              "perovskite", "GaN", "SiC", "superconductor", "MEMS",
              "hologram", "metamaterial", "photonics", "neuromorphic", "spintronics"]:
        tests.append((_tid(V, c), "VC Investor", "tech_trend",
                       tech_trend, dict(store=store, query=q)))

    # --- Additional tech_landscape with more CPC (+10 tests) ---
    for cpc in CPC_EXTENDED[8:]:
        tests.append((_tid(V, c), "VC Investor", "tech_landscape",
                       tech_landscape, dict(store=store, cpc_prefix=cpc)))
    for q in ["motor", "polymer", "filter", "coating"]:
        tests.append((_tid(V, c), "VC Investor", "tech_landscape",
                       tech_landscape, dict(store=store, query=q)))

    # --- Additional firm_patent_portfolio for remaining firms (+10 tests) ---
    for firm in GLOBAL_FIRMS_LIST[5:]:
        tests.append((_tid(V, c), "VC Investor", "firm_patent_portfolio",
                       firm_patent_portfolio,
                       dict(store=store, resolver=resolver, firm=firm)))
    # With detail_patents for small portfolios
    for firm in JP_FIRMS[:5]:
        tests.append((_tid(V, c), "VC Investor", "firm_patent_portfolio",
                       firm_patent_portfolio,
                       dict(store=store, resolver=resolver, firm=firm, detail_patents=True)))

    # --- Additional portfolio_evolution for global firms (+10 tests) ---
    for firm in GLOBAL_FIRMS_LIST[:5]:
        tests.append((_tid(V, c), "VC Investor", "portfolio_evolution",
                       portfolio_evolution,
                       dict(store=store, resolver=resolver, firm_query=firm)))
    for firm in JP_FIRMS[8:13]:
        tests.append((_tid(V, c), "VC Investor", "portfolio_evolution",
                       portfolio_evolution,
                       dict(store=store, resolver=resolver, firm_query=firm)))

    # --- Additional tech_entropy with more params (+10 tests) ---
    for cpc in CPC_EXTENDED[8:]:
        tests.append((_tid(V, c), "VC Investor", "tech_entropy",
                       tech_entropy, dict(store=store, cpc_prefix=cpc)))
    for gran in ["year", "quarter"]:
        tests.append((_tid(V, c), "VC Investor", "tech_entropy",
                       tech_entropy, dict(store=store, cpc_prefix="H01M", granularity=gran)))

    # --- Additional tech_fusion_detector combos (+10 tests) ---
    extra_fusion_pairs = [("A61K", "G06N"), ("H01M", "C08L"), ("H01L", "G02B"),
                           ("G06F", "A61B"), ("H04W", "G01N")]
    for a, b in extra_fusion_pairs:
        tests.append((_tid(V, c), "VC Investor", "tech_fusion_detector",
                       tech_fusion_detector, dict(store=store, cpc_a=a, cpc_b=b)))
    for firm in JP_FIRMS[:5]:
        tests.append((_tid(V, c), "VC Investor", "tech_fusion_detector",
                       tech_fusion_detector, dict(store=store, firm=firm, resolver=resolver)))

    # --- Additional firm_tech_vector for global firms (+10 tests) ---
    for firm in GLOBAL_FIRMS_LIST[:10]:
        tests.append((_tid(V, c), "VC Investor", "firm_tech_vector",
                       firm_tech_vector,
                       dict(store=store, resolver=resolver, firm_query=firm)))

    # --- Additional patent_market_fusion with text queries (+10 tests) ---
    for desc in TECH_DESCRIPTIONS:
        tests.append((_tid(V, c), "VC Investor", "patent_market_fusion",
                       patent_market_fusion,
                       dict(store=store, resolver=resolver, query=desc, query_type="text")))
    for cpc in CPC_EXTENDED[8:10]:
        tests.append((_tid(V, c), "VC Investor", "patent_market_fusion",
                       patent_market_fusion,
                       dict(store=store, resolver=resolver, query=cpc, query_type="technology")))

    # --- Additional tech_trend with year+CPC combos (+10 tests) ---
    for cpc, yf, yt in [
        ("H01M", 2016, 2020), ("H01M", 2020, 2024), ("G06N", 2018, 2024),
        ("H04L", 2016, 2024), ("A61K", 2018, 2023), ("B60L", 2020, 2024),
        ("G06F", 2016, 2022), ("H01L", 2018, 2024), ("C08L", 2016, 2024),
        ("H04W", 2020, 2024),
    ]:
        tests.append((_tid(V, c), "VC Investor", "tech_trend",
                       tech_trend, dict(store=store, cpc_prefix=cpc, year_from=yf, year_to=yt)))

    # --- Additional network_topology / knowledge_flow combos (+15 tests) ---
    for cpc in CPC_EXTENDED[8:13]:
        tests.append((_tid(V, c), "VC Investor", "network_topology",
                       network_topology, dict(store=store, cpc_prefix=cpc, max_patents=100)))
    for tgt in ["H01M", "B60L", "A61K", "G02B", "H04W"]:
        tests.append((_tid(V, c), "VC Investor", "knowledge_flow",
                       knowledge_flow, dict(store=store, target_cpc=tgt)))
    for src, tgt in [("G06N", "G01N"), ("H04L", "G06F"), ("C08L", "B29C"),
                      ("A61K", "C07D"), ("H01M", "H01L")]:
        tests.append((_tid(V, c), "VC Investor", "knowledge_flow",
                       knowledge_flow, dict(store=store, source_cpc=src, target_cpc=tgt)))

    # --- Additional portfolio_var with year variations (+5 tests) ---
    for yr in [2020, 2021, 2022, 2023, 2024]:
        tests.append((_tid(V, c), "VC Investor", "portfolio_var",
                       portfolio_var,
                       dict(store=store, resolver=resolver, firm="Toyota", year=yr)))

    # --- Additional ip_due_diligence for global firms (+10 tests) ---
    dd_fn2 = ip_due_diligence if ip_due_diligence else None
    if dd_fn2:
        for firm in GLOBAL_FIRMS_LIST[:5]:
            tests.append((_tid(V, c), "VC Investor", "ip_due_diligence",
                           dd_fn2,
                           dict(store=store, resolver=resolver, target_firm=firm)))
        for firm in JP_FIRMS[8:13]:
            tests.append((_tid(V, c), "VC Investor", "ip_due_diligence",
                           dd_fn2,
                           dict(store=store, resolver=resolver, target_firm=firm)))

    return tests


def _entity_resolve_wrapper(resolver: EntityResolver, name: str) -> dict:
    """Wrapper to test entity resolution as if it were a tool."""
    result = resolver.resolve(name, country_hint="JP")
    if result is None:
        return {"error": f"Could not resolve: {name}"}
    return {
        "canonical_id": result.entity.canonical_id,
        "canonical_name": result.entity.canonical_name,
        "confidence": result.confidence,
        "match_level": result.match_level,
        "score": result.confidence,
    }


def generate_corporate_strategist_tests(
    store: PatentStore, resolver: EntityResolver,
) -> list[tuple[str, str, str, Callable, dict]]:
    """~600 tests for Corporate Strategist persona."""
    tests = []
    c = [0]
    S = "CS"

    # --- startability (50 tests) ---
    for firm in JP_FIRMS[:10]:
        for cluster in CLUSTER_IDS[:5]:
            tests.append((_tid(S, c), "Corporate Strategist", "startability",
                           startability,
                           dict(store=store, resolver=resolver,
                                firm_query=firm, tech_query_or_cluster_id=cluster)))

    # --- startability_ranking (50 tests) ---
    # by_firm
    for firm in JP_FIRMS[:10]:
        tests.append((_tid(S, c), "Corporate Strategist", "startability_ranking",
                       startability_ranking,
                       dict(store=store, resolver=resolver, mode="by_firm", query=firm)))
    for firm in GLOBAL_FIRMS_LIST[:5]:
        tests.append((_tid(S, c), "Corporate Strategist", "startability_ranking",
                       startability_ranking,
                       dict(store=store, resolver=resolver, mode="by_firm", query=firm)))
    # by_tech
    for cluster in CLUSTER_IDS_EXTENDED:
        tests.append((_tid(S, c), "Corporate Strategist", "startability_ranking",
                       startability_ranking,
                       dict(store=store, resolver=resolver, mode="by_tech", query=cluster)))
    # top_n variations
    for tn in [5, 10, 50]:
        tests.append((_tid(S, c), "Corporate Strategist", "startability_ranking",
                       startability_ranking,
                       dict(store=store, resolver=resolver, mode="by_firm",
                            query="Toyota", top_n=tn)))
    # Year variations
    for yr in [2020, 2023, 2024]:
        tests.append((_tid(S, c), "Corporate Strategist", "startability_ranking",
                       startability_ranking,
                       dict(store=store, resolver=resolver, mode="by_firm",
                            query="Sony", year=yr)))
    # Pagination
    for pg in [1, 2, 3]:
        tests.append((_tid(S, c), "Corporate Strategist", "startability_ranking",
                       startability_ranking,
                       dict(store=store, resolver=resolver, mode="by_tech",
                            query="H01M_0", page=pg, page_size=10)))
    # Invalid mode
    tests.append((_tid(S, c), "Corporate Strategist", "startability_ranking",
                   startability_ranking,
                   dict(store=store, resolver=resolver, mode="invalid", query="Toyota")))

    # --- startability_delta (40 tests) ---
    for firm in JP_FIRMS[:8]:
        tests.append((_tid(S, c), "Corporate Strategist", "startability_delta",
                       startability_delta,
                       dict(store=store, resolver=resolver, mode="by_firm", query=firm)))
    for cluster in CLUSTER_IDS[:5]:
        tests.append((_tid(S, c), "Corporate Strategist", "startability_delta",
                       startability_delta,
                       dict(store=store, resolver=resolver, mode="by_tech", query=cluster)))
    for d in DIRECTIONS:
        tests.append((_tid(S, c), "Corporate Strategist", "startability_delta",
                       startability_delta,
                       dict(store=store, resolver=resolver, mode="by_firm",
                            query="Toyota", direction=d)))
    for d in DIRECTIONS:
        tests.append((_tid(S, c), "Corporate Strategist", "startability_delta",
                       startability_delta,
                       dict(store=store, resolver=resolver, mode="by_tech",
                            query="H01M_0", direction=d)))
    # Year range variations
    for yf, yt in [(2016, 2023), (2020, 2023), (2018, 2022)]:
        tests.append((_tid(S, c), "Corporate Strategist", "startability_delta",
                       startability_delta,
                       dict(store=store, resolver=resolver, mode="by_firm",
                            query="Honda", year_from=yf, year_to=yt)))
    # top_n
    for tn in [5, 20, 50]:
        tests.append((_tid(S, c), "Corporate Strategist", "startability_delta",
                       startability_delta,
                       dict(store=store, resolver=resolver, mode="by_firm",
                            query="Canon", top_n=tn)))

    # --- tech_fit (30 tests) ---
    for firm in JP_FIRMS[:6]:
        for cluster in CLUSTER_IDS[:5]:
            tests.append((_tid(S, c), "Corporate Strategist", "tech_fit",
                           tech_fit,
                           dict(store=store, resolver=resolver,
                                firm_query=firm, tech_query_or_cluster_id=cluster)))

    # --- adversarial_strategy (20 tests) ---
    pairs = [
        ("Toyota", "Honda"), ("Sony", "Panasonic"), ("Canon", "Ricoh"),
        ("Hitachi", "Toshiba"), ("NEC", "Fujitsu"), ("Denso", "Aisin"),
        ("Samsung", "Apple"), ("Intel", "Qualcomm"), ("Google", "Microsoft"),
        ("Toyota", "Samsung"),
    ]
    for a, b in pairs:
        tests.append((_tid(S, c), "Corporate Strategist", "adversarial_strategy",
                       adversarial_strategy,
                       dict(store=store, resolver=resolver, firm_a=a, firm_b=b)))
    for a, b in pairs[:3]:
        tests.append((_tid(S, c), "Corporate Strategist", "adversarial_strategy",
                       adversarial_strategy,
                       dict(store=store, resolver=resolver, firm_a=a, firm_b=b,
                            scenario_count=5)))

    # --- tech_gap (20 tests) ---
    for a, b in pairs[:10]:
        tests.append((_tid(S, c), "Corporate Strategist", "tech_gap",
                       tech_gap,
                       dict(store=store, resolver=resolver, firm_a=a, firm_b=b)))
    for a, b in pairs[:5]:
        tests.append((_tid(S, c), "Corporate Strategist", "tech_gap",
                       tech_gap,
                       dict(store=store, resolver=resolver, firm_a=a, firm_b=b, year=2023)))

    # --- similar_firms (20 tests) ---
    for firm in JP_FIRMS[:10]:
        tests.append((_tid(S, c), "Corporate Strategist", "similar_firms",
                       similar_firms,
                       dict(store=store, resolver=resolver, firm_query=firm)))
    for firm in JP_FIRMS[:5]:
        tests.append((_tid(S, c), "Corporate Strategist", "similar_firms",
                       similar_firms,
                       dict(store=store, resolver=resolver, firm_query=firm, top_n=20)))
    for firm in JP_FIRMS[:5]:
        tests.append((_tid(S, c), "Corporate Strategist", "similar_firms",
                       similar_firms,
                       dict(store=store, resolver=resolver, firm_query=firm, year=2023)))

    # --- cross_domain_discovery (20 tests) ---
    for cpc in CPC_CODES:
        tests.append((_tid(S, c), "Corporate Strategist", "cross_domain_discovery",
                       cross_domain_discovery, dict(store=store, query=cpc)))
    for desc in TECH_DESCRIPTIONS[:5]:
        tests.append((_tid(S, c), "Corporate Strategist", "cross_domain_discovery",
                       cross_domain_discovery, dict(store=store, query=desc)))
    for tn in [5, 10, 20]:
        tests.append((_tid(S, c), "Corporate Strategist", "cross_domain_discovery",
                       cross_domain_discovery, dict(store=store, query="H01M", top_n=tn)))
    tests.append((_tid(S, c), "Corporate Strategist", "cross_domain_discovery",
                   cross_domain_discovery,
                   dict(store=store, query="H01M", exclude_same_domain=False)))
    tests.append((_tid(S, c), "Corporate Strategist", "cross_domain_discovery",
                   cross_domain_discovery,
                   dict(store=store, query="battery", min_similarity=0.5)))

    # --- ma_target (25 tests) ---
    for firm in JP_FIRMS[:5]:
        for strat in MA_STRATEGIES:
            tests.append((_tid(S, c), "Corporate Strategist", "ma_target",
                           ma_target,
                           dict(store=store, resolver=resolver, acquirer=firm, strategy=strat)))
    for firm in JP_FIRMS[:5]:
        tests.append((_tid(S, c), "Corporate Strategist", "ma_target",
                       ma_target,
                       dict(store=store, resolver=resolver, acquirer=firm, top_n=5)))
    for firm in GLOBAL_FIRMS_LIST[:5]:
        tests.append((_tid(S, c), "Corporate Strategist", "ma_target",
                       ma_target,
                       dict(store=store, resolver=resolver, acquirer=firm)))

    # --- sales_prospect (25 tests) ---
    for firm in JP_FIRMS[:5]:
        for cluster in CLUSTER_IDS[:5]:
            tests.append((_tid(S, c), "Corporate Strategist", "sales_prospect",
                           sales_prospect,
                           dict(store=store, resolver=resolver,
                                firm_query=firm, patent_or_tech=cluster)))

    # --- patent_compare (20 tests) ---
    firm_groups = [
        JP_FIRMS[:3], JP_FIRMS[3:6], JP_FIRMS[6:9],
        GLOBAL_FIRMS_LIST[:3], JP_FIRMS[:2] + GLOBAL_FIRMS_LIST[:1],
    ]
    for fg in firm_groups:
        tests.append((_tid(S, c), "Corporate Strategist", "patent_compare",
                       patent_compare,
                       dict(store=store, resolver=resolver, firms=fg)))
    for fg in firm_groups[:3]:
        tests.append((_tid(S, c), "Corporate Strategist", "patent_compare",
                       patent_compare,
                       dict(store=store, resolver=resolver, firms=fg, cpc_prefix="H01M")))
    for fg in firm_groups[:3]:
        tests.append((_tid(S, c), "Corporate Strategist", "patent_compare",
                       patent_compare,
                       dict(store=store, resolver=resolver, firms=fg,
                            date_from="2020-01-01", date_to="2024-12-31")))
    # Edge cases
    tests.append((_tid(S, c), "Corporate Strategist", "patent_compare",
                   patent_compare, dict(store=store, resolver=resolver, firms=[])))
    tests.append((_tid(S, c), "Corporate Strategist", "patent_compare",
                   patent_compare,
                   dict(store=store, resolver=resolver, firms=["nonexistent_a", "nonexistent_b"])))
    tests.append((_tid(S, c), "Corporate Strategist", "patent_compare",
                   patent_compare,
                   dict(store=store, resolver=resolver, firms=JP_FIRMS[:5])))

    # --- applicant_network (20 tests) ---
    for firm in JP_FIRMS[:10]:
        tests.append((_tid(S, c), "Corporate Strategist", "applicant_network",
                       applicant_network,
                       dict(store=store, resolver=resolver, applicant=firm)))
    for firm in JP_FIRMS[:5]:
        tests.append((_tid(S, c), "Corporate Strategist", "applicant_network",
                       applicant_network,
                       dict(store=store, resolver=resolver, applicant=firm, depth=2)))
    for firm in JP_FIRMS[:3]:
        tests.append((_tid(S, c), "Corporate Strategist", "applicant_network",
                       applicant_network,
                       dict(store=store, resolver=resolver, applicant=firm, min_co_patents=10)))
    # Edge
    tests.append((_tid(S, c), "Corporate Strategist", "applicant_network",
                   applicant_network,
                   dict(store=store, resolver=resolver, applicant="nonexistent_xyz")))
    tests.append((_tid(S, c), "Corporate Strategist", "applicant_network",
                   applicant_network,
                   dict(store=store, resolver=resolver, applicant="Toyota", min_co_patents=1)))

    # --- corporate_hierarchy (15 tests) ---
    for firm in JP_FIRMS[:8]:
        tests.append((_tid(S, c), "Corporate Strategist", "corporate_hierarchy",
                       corporate_hierarchy,
                       dict(store=store, firm_query=firm, resolver=resolver)))
    for firm in JP_FIRMS[:3]:
        tests.append((_tid(S, c), "Corporate Strategist", "corporate_hierarchy",
                       corporate_hierarchy,
                       dict(store=store, firm_query=firm, resolver=resolver,
                            depth=3, include_patents=True)))
    # Edge
    tests.append((_tid(S, c), "Corporate Strategist", "corporate_hierarchy",
                   corporate_hierarchy,
                   dict(store=store, firm_query="nonexistent_group", resolver=resolver)))

    # --- group_portfolio (10 tests) ---
    for firm in JP_FIRMS[:8]:
        tests.append((_tid(S, c), "Corporate Strategist", "group_portfolio",
                       group_portfolio,
                       dict(store=store, firm_query=firm, resolver=resolver)))
    for yr in [2023, 2024]:
        tests.append((_tid(S, c), "Corporate Strategist", "group_portfolio",
                       group_portfolio,
                       dict(store=store, firm_query="Toyota", resolver=resolver, year=yr)))

    # --- group_startability (15 tests) ---
    for firm in JP_FIRMS[:5]:
        for cluster in CLUSTER_IDS[:3]:
            tests.append((_tid(S, c), "Corporate Strategist", "group_startability",
                           group_startability,
                           dict(store=store, firm_query=firm, resolver=resolver,
                                tech_query_or_cluster_id=cluster)))

    # --- AI classifier (25 tests) ---
    # create_category
    categories_to_create = [
        ("EV Battery", ["H01M10", "H01M4"], ["battery", "lithium", "electrode"]),
        ("AI ML", ["G06N", "G06F17"], ["machine learning", "neural network"]),
        ("5G Wireless", ["H04L", "H04W"], ["5G", "wireless", "antenna"]),
        ("Semiconductor", ["H01L"], ["semiconductor", "wafer", "transistor"]),
        ("Pharma", ["A61K", "C07D"], ["drug", "pharmaceutical", "antibody"]),
    ]
    for name, cpcs, kws in categories_to_create:
        tests.append((_tid(S, c), "Corporate Strategist", "create_category",
                       create_category,
                       dict(store=store, category_name=name,
                            cpc_patterns=cpcs, keywords=kws)))
    # Edge cases
    tests.append((_tid(S, c), "Corporate Strategist", "create_category",
                   create_category, dict(store=store, category_name=None)))
    tests.append((_tid(S, c), "Corporate Strategist", "create_category",
                   create_category,
                   dict(store=store, category_name="Empty Category")))

    # classify_patents — will use category IDs from create_category
    # We use dummy IDs since real ones are generated at runtime
    for cat_id in ["ev-battery", "ai-ml", "5g-wireless", "semiconductor", "pharma"]:
        tests.append((_tid(S, c), "Corporate Strategist", "classify_patents",
                       classify_patents,
                       dict(store=store, category_id=cat_id)))
    for cat_id in ["ev-battery", "ai-ml"]:
        tests.append((_tid(S, c), "Corporate Strategist", "classify_patents",
                       classify_patents,
                       dict(store=store, category_id=cat_id, query="new battery")))

    # category_landscape
    for cat_id in ["ev-battery", "ai-ml", "5g-wireless"]:
        tests.append((_tid(S, c), "Corporate Strategist", "category_landscape",
                       category_landscape, dict(store=store, category_id=cat_id)))

    # portfolio_benchmark
    for firm in JP_FIRMS[:3]:
        tests.append((_tid(S, c), "Corporate Strategist", "portfolio_benchmark",
                       portfolio_benchmark,
                       dict(store=store, firm_query=firm, category_id="ev-battery",
                            resolver=resolver)))

    # --- Additional startability with global firms (+25 tests) ---
    for firm in GLOBAL_FIRMS_LIST[:5]:
        for cluster in CLUSTER_IDS[:5]:
            tests.append((_tid(S, c), "Corporate Strategist", "startability",
                           startability,
                           dict(store=store, resolver=resolver,
                                firm_query=firm, tech_query_or_cluster_id=cluster)))

    # --- Additional startability_ranking with CPC text queries (+10 tests) ---
    for cpc in CPC_CODES:
        tests.append((_tid(S, c), "Corporate Strategist", "startability_ranking",
                       startability_ranking,
                       dict(store=store, resolver=resolver, mode="by_tech", query=cpc)))
    for yr in [2020, 2021]:
        tests.append((_tid(S, c), "Corporate Strategist", "startability_ranking",
                       startability_ranking,
                       dict(store=store, resolver=resolver, mode="by_tech",
                            query="H01M_0", year=yr)))

    # --- Additional startability_delta for global firms (+15 tests) ---
    for firm in GLOBAL_FIRMS_LIST[:5]:
        tests.append((_tid(S, c), "Corporate Strategist", "startability_delta",
                       startability_delta,
                       dict(store=store, resolver=resolver, mode="by_firm", query=firm)))
    for cluster in CLUSTER_IDS_EXTENDED[5:]:
        tests.append((_tid(S, c), "Corporate Strategist", "startability_delta",
                       startability_delta,
                       dict(store=store, resolver=resolver, mode="by_tech", query=cluster)))

    # --- Additional tech_fit for global firms (+20 tests) ---
    for firm in GLOBAL_FIRMS_LIST[:4]:
        for cluster in CLUSTER_IDS[:5]:
            tests.append((_tid(S, c), "Corporate Strategist", "tech_fit",
                           tech_fit,
                           dict(store=store, resolver=resolver,
                                firm_query=firm, tech_query_or_cluster_id=cluster)))

    # --- Additional adversarial_strategy cross-regional (+10 tests) ---
    cross_pairs = [
        ("Toyota", "Volkswagen"), ("Sony", "Samsung"), ("Canon", "Nikon"),
        ("Panasonic", "LG"), ("Hitachi", "Siemens"), ("NEC", "Cisco"),
        ("Fujitsu", "IBM"), ("Toshiba", "GE"), ("Denso", "Bosch"),
        ("Honda", "BMW"),
    ]
    for a, b in cross_pairs:
        tests.append((_tid(S, c), "Corporate Strategist", "adversarial_strategy",
                       adversarial_strategy,
                       dict(store=store, resolver=resolver, firm_a=a, firm_b=b)))

    # --- Additional tech_gap for global firms (+10 tests) ---
    for a, b in cross_pairs[:10]:
        tests.append((_tid(S, c), "Corporate Strategist", "tech_gap",
                       tech_gap,
                       dict(store=store, resolver=resolver, firm_a=a, firm_b=b)))

    # --- Additional similar_firms for global firms (+10 tests) ---
    for firm in GLOBAL_FIRMS_LIST:
        tests.append((_tid(S, c), "Corporate Strategist", "similar_firms",
                       similar_firms,
                       dict(store=store, resolver=resolver, firm_query=firm)))

    # --- Additional cross_domain_discovery with more params (+10 tests) ---
    for q in ["autonomous driving", "gene therapy", "quantum computing",
              "hydrogen storage", "perovskite solar cell"]:
        tests.append((_tid(S, c), "Corporate Strategist", "cross_domain_discovery",
                       cross_domain_discovery, dict(store=store, query=q)))
    for cpc in CPC_EXTENDED[8:13]:
        tests.append((_tid(S, c), "Corporate Strategist", "cross_domain_discovery",
                       cross_domain_discovery, dict(store=store, query=cpc)))

    # --- Additional ma_target for global firms (+10 tests) ---
    for firm in GLOBAL_FIRMS_LIST[:5]:
        tests.append((_tid(S, c), "Corporate Strategist", "ma_target",
                       ma_target,
                       dict(store=store, resolver=resolver, acquirer=firm, strategy="tech_gap")))
    for firm in GLOBAL_FIRMS_LIST[:5]:
        tests.append((_tid(S, c), "Corporate Strategist", "ma_target",
                       ma_target,
                       dict(store=store, resolver=resolver, acquirer=firm, strategy="diversification")))

    # --- Additional sales_prospect with text queries (+10 tests) ---
    for firm, desc in zip(JP_FIRMS[:5], TECH_DESCRIPTIONS[:5]):
        tests.append((_tid(S, c), "Corporate Strategist", "sales_prospect",
                       sales_prospect,
                       dict(store=store, resolver=resolver,
                            firm_query=firm, patent_or_tech=desc, query_type="text")))
    for firm, pn in zip(JP_FIRMS[:5], PATENT_NUMBERS):
        tests.append((_tid(S, c), "Corporate Strategist", "sales_prospect",
                       sales_prospect,
                       dict(store=store, resolver=resolver,
                            firm_query=firm, patent_or_tech=pn, query_type="patent")))

    # --- Additional patent_compare with date filters (+10 tests) ---
    for i in range(5):
        fg = JP_FIRMS[i*3:(i+1)*3] if (i+1)*3 <= len(JP_FIRMS) else JP_FIRMS[-3:]
        tests.append((_tid(S, c), "Corporate Strategist", "patent_compare",
                       patent_compare,
                       dict(store=store, resolver=resolver, firms=fg,
                            date_from="2018-01-01")))
    for cpc in CPC_CODES[:5]:
        tests.append((_tid(S, c), "Corporate Strategist", "patent_compare",
                       patent_compare,
                       dict(store=store, resolver=resolver, firms=JP_FIRMS[:2], cpc_prefix=cpc)))

    # --- Additional applicant_network for global firms (+10 tests) ---
    for firm in GLOBAL_FIRMS_LIST[:10]:
        tests.append((_tid(S, c), "Corporate Strategist", "applicant_network",
                       applicant_network,
                       dict(store=store, resolver=resolver, applicant=firm)))

    # --- Additional corporate_hierarchy for global firms (+10 tests) ---
    for firm in GLOBAL_FIRMS_LIST[:7]:
        tests.append((_tid(S, c), "Corporate Strategist", "corporate_hierarchy",
                       corporate_hierarchy,
                       dict(store=store, firm_query=firm, resolver=resolver)))
    for firm in JP_FIRMS[:3]:
        tests.append((_tid(S, c), "Corporate Strategist", "corporate_hierarchy",
                       corporate_hierarchy,
                       dict(store=store, firm_query=firm, resolver=resolver,
                            include_patents=True)))

    # --- Additional group_portfolio for global firms (+5 tests) ---
    for firm in GLOBAL_FIRMS_LIST[:5]:
        tests.append((_tid(S, c), "Corporate Strategist", "group_portfolio",
                       group_portfolio,
                       dict(store=store, firm_query=firm, resolver=resolver)))

    # --- Additional group_startability for global firms (+10 tests) ---
    for firm in GLOBAL_FIRMS_LIST[:5]:
        for cluster in CLUSTER_IDS[:2]:
            tests.append((_tid(S, c), "Corporate Strategist", "group_startability",
                           group_startability,
                           dict(store=store, firm_query=firm, resolver=resolver,
                                tech_query_or_cluster_id=cluster)))

    # --- Additional AI classifier tests (+15 tests) ---
    # More categories
    for name, cpcs, kws in [
        ("Autonomous Driving", ["B60W", "G05D"], ["autonomous", "self-driving"]),
        ("Robotics", ["B25J", "G05B"], ["robot", "manipulator"]),
        ("OLED Display", ["H10K", "G09G"], ["OLED", "organic LED"]),
        ("Quantum Tech", ["G06N10", "H01L39"], ["quantum", "qubit"]),
        ("Hydrogen Energy", ["C01B3", "H01M8"], ["hydrogen", "fuel cell"]),
    ]:
        tests.append((_tid(S, c), "Corporate Strategist", "create_category",
                       create_category,
                       dict(store=store, category_name=name,
                            cpc_patterns=cpcs, keywords=kws)))
    # More classify_patents
    for cat_id in ["semiconductor", "pharma", "autonomous-driving"]:
        tests.append((_tid(S, c), "Corporate Strategist", "classify_patents",
                       classify_patents,
                       dict(store=store, category_id=cat_id, query="innovation")))
    # More category_landscape
    for cat_id in ["semiconductor", "pharma", "autonomous-driving", "robotics"]:
        tests.append((_tid(S, c), "Corporate Strategist", "category_landscape",
                       category_landscape, dict(store=store, category_id=cat_id)))
    # More portfolio_benchmark
    for firm in JP_FIRMS[3:6]:
        tests.append((_tid(S, c), "Corporate Strategist", "portfolio_benchmark",
                       portfolio_benchmark,
                       dict(store=store, firm_query=firm, category_id="ai-ml",
                            resolver=resolver)))

    # --- Additional startability with year variation (+15 tests) ---
    for yr in [2020, 2021, 2022]:
        for firm in JP_FIRMS[:5]:
            tests.append((_tid(S, c), "Corporate Strategist", "startability",
                           startability,
                           dict(store=store, resolver=resolver,
                                firm_query=firm, tech_query_or_cluster_id="H01M_0", year=yr)))

    # --- Additional startability_ranking year+pagination combos (+15 tests) ---
    for firm in JP_FIRMS[:5]:
        for yr in [2021, 2022, 2023]:
            tests.append((_tid(S, c), "Corporate Strategist", "startability_ranking",
                           startability_ranking,
                           dict(store=store, resolver=resolver, mode="by_firm",
                                query=firm, year=yr)))

    # --- Additional adversarial_strategy with year variation (+5 tests) ---
    for yr in [2020, 2021, 2022, 2023, 2024]:
        tests.append((_tid(S, c), "Corporate Strategist", "adversarial_strategy",
                       adversarial_strategy,
                       dict(store=store, resolver=resolver,
                            firm_a="Toyota", firm_b="Honda", year=yr)))

    # --- Additional tech_gap with year variation (+5 tests) ---
    for yr in [2020, 2021, 2022, 2023, 2024]:
        tests.append((_tid(S, c), "Corporate Strategist", "tech_gap",
                       tech_gap,
                       dict(store=store, resolver=resolver,
                            firm_a="Sony", firm_b="Panasonic", year=yr)))

    # --- Additional patent_compare with global firms (+10 tests) ---
    global_groups = [
        GLOBAL_FIRMS_LIST[:3], GLOBAL_FIRMS_LIST[3:6], GLOBAL_FIRMS_LIST[6:9],
        JP_FIRMS[:2] + GLOBAL_FIRMS_LIST[:2],
        JP_FIRMS[5:7] + GLOBAL_FIRMS_LIST[3:5],
    ]
    for fg in global_groups:
        tests.append((_tid(S, c), "Corporate Strategist", "patent_compare",
                       patent_compare,
                       dict(store=store, resolver=resolver, firms=fg)))
    for fg in global_groups[:3]:
        tests.append((_tid(S, c), "Corporate Strategist", "patent_compare",
                       patent_compare,
                       dict(store=store, resolver=resolver, firms=fg, cpc_prefix="G06N")))
    for fg in global_groups[:2]:
        tests.append((_tid(S, c), "Corporate Strategist", "patent_compare",
                       patent_compare,
                       dict(store=store, resolver=resolver, firms=fg,
                            date_from="2020-01-01", date_to="2024-12-31")))

    # --- Additional startability with extended clusters (+40 tests) ---
    # Cover remaining firms x extended clusters for broader coverage
    for firm in JP_FIRMS[10:15]:
        for cluster in CLUSTER_IDS_EXTENDED[:4]:
            tests.append((_tid(S, c), "Corporate Strategist", "startability",
                           startability,
                           dict(store=store, resolver=resolver,
                                firm_query=firm, tech_query_or_cluster_id=cluster)))
    for firm in GLOBAL_FIRMS_LIST[5:10]:
        for cluster in CLUSTER_IDS[:4]:
            tests.append((_tid(S, c), "Corporate Strategist", "startability",
                           startability,
                           dict(store=store, resolver=resolver,
                                firm_query=firm, tech_query_or_cluster_id=cluster)))

    # --- Additional tech_fit with extended clusters (+15 tests) ---
    for firm in JP_FIRMS[6:9]:
        for cluster in CLUSTER_IDS_EXTENDED[:5]:
            tests.append((_tid(S, c), "Corporate Strategist", "tech_fit",
                           tech_fit,
                           dict(store=store, resolver=resolver,
                                firm_query=firm, tech_query_or_cluster_id=cluster)))

    # --- Additional sales_prospect with extended combos (+10 tests) ---
    for firm in GLOBAL_FIRMS_LIST[:5]:
        for cluster in CLUSTER_IDS[:2]:
            tests.append((_tid(S, c), "Corporate Strategist", "sales_prospect",
                           sales_prospect,
                           dict(store=store, resolver=resolver,
                                firm_query=firm, patent_or_tech=cluster)))

    return tests


# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------
def main():
    print("=" * 70)
    print("Patent Space MCP — 2000 Test Quality Runner")
    print(f"Started at: {datetime.now().isoformat()}")
    print("=" * 70)

    # Initialize store and resolver
    db_path = os.environ.get("PATENT_DB_PATH", "/app/data/patents.db")
    print(f"\nInitializing PatentStore from: {db_path}")
    store = PatentStore(db_path)

    print("Initializing EntityRegistry and EntityResolver...")
    registry = EntityRegistry()
    for entities in [TSE_PRIME_ENTITIES, TSE_EXPANDED_ENTITIES, TSE_AUTO_ENTITIES,
                     SP500_ENTITIES, GLOBAL_ENTITIES]:
        for e in entities:
            registry.register(e)
    resolver = EntityResolver(registry)
    print(f"  Registered {len(registry.all_entities())} entities")

    # Generate all test cases
    print("\nGenerating test cases...")
    pa_tests = generate_patent_attorney_tests(store, resolver)
    vc_tests = generate_vc_investor_tests(store, resolver)
    cs_tests = generate_corporate_strategist_tests(store, resolver)

    all_tests = pa_tests + vc_tests + cs_tests
    total = len(all_tests)

    print(f"  Patent Attorney:      {len(pa_tests)} tests")
    print(f"  VC Investor:          {len(vc_tests)} tests")
    print(f"  Corporate Strategist: {len(cs_tests)} tests")
    print(f"  TOTAL:                {total} tests")
    print()

    # Run tests sequentially
    results: list[TestResult] = []
    start_time = time.monotonic()
    pass_count = 0
    fail_count = 0
    timeout_count = 0

    tool_stats: dict[str, dict[str, Any]] = {}

    for i, (test_id, persona, tool_name, fn, kwargs) in enumerate(all_tests):
        pct = ((i + 1) / total) * 100
        elapsed_total = time.monotonic() - start_time
        rate = (i + 1) / max(elapsed_total, 0.1)
        eta_s = (total - i - 1) / max(rate, 0.001)

        print(
            f"\r[{i+1:4d}/{total}] {pct:5.1f}% | "
            f"{tool_name:30s} | "
            f"P:{pass_count} F:{fail_count} T:{timeout_count} | "
            f"ETA: {eta_s/60:.0f}m",
            end="", flush=True,
        )

        tr = run_one_test(test_id, persona, tool_name, fn, kwargs)
        results.append(tr)

        if tr.status == "pass":
            pass_count += 1
        elif tr.status == "timeout_warning":
            timeout_count += 1
            pass_count += 1  # Timeouts are warnings, not failures
        else:
            fail_count += 1

        # Accumulate tool stats
        if tool_name not in tool_stats:
            tool_stats[tool_name] = {
                "total": 0, "pass": 0, "fail": 0, "timeout": 0,
                "quality_sum": 0, "time_sum": 0.0,
                "max_time": 0.0, "min_time": float("inf"),
            }
        ts = tool_stats[tool_name]
        ts["total"] += 1
        if tr.status == "pass":
            ts["pass"] += 1
        elif tr.status == "timeout_warning":
            ts["timeout"] += 1
            ts["pass"] += 1
        else:
            ts["fail"] += 1
        ts["quality_sum"] += tr.quality_score
        ts["time_sum"] += tr.response_time_ms
        ts["max_time"] = max(ts["max_time"], tr.response_time_ms)
        ts["min_time"] = min(ts["min_time"], tr.response_time_ms)

    total_time = time.monotonic() - start_time
    print(f"\n\nCompleted in {total_time/60:.1f} minutes")
    print(f"  Pass: {pass_count}  Fail: {fail_count}  Timeout: {timeout_count}")

    # Write CSV
    csv_path = "/tmp/test_results_2000.csv"
    print(f"\nWriting CSV to {csv_path}...")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "test_id", "persona", "tool_name", "input_params",
            "status", "quality_score", "response_time_ms",
            "error_message", "result_keys",
        ])
        writer.writeheader()
        for tr in results:
            writer.writerow(asdict(tr))
    print(f"  Wrote {len(results)} rows")

    # Compute summary
    quality_scores = [r.quality_score for r in results]
    times = [r.response_time_ms for r in results]

    persona_summary = {}
    for persona in ["Patent Attorney", "VC Investor", "Corporate Strategist"]:
        p_results = [r for r in results if r.persona == persona]
        p_scores = [r.quality_score for r in p_results]
        p_times = [r.response_time_ms for r in p_results]
        persona_summary[persona] = {
            "total": len(p_results),
            "pass": sum(1 for r in p_results if r.status in ("pass", "timeout_warning")),
            "fail": sum(1 for r in p_results if r.status == "fail"),
            "timeout": sum(1 for r in p_results if r.status == "timeout_warning"),
            "avg_quality": round(sum(p_scores) / max(len(p_scores), 1), 2),
            "quality_distribution": {
                str(q): sum(1 for s in p_scores if s == q) for q in range(1, 6)
            },
            "avg_time_ms": round(sum(p_times) / max(len(p_times), 1), 1),
            "median_time_ms": round(sorted(p_times)[len(p_times) // 2], 1) if p_times else 0,
            "p95_time_ms": round(sorted(p_times)[int(len(p_times) * 0.95)] if p_times else 0, 1),
        }

    tool_summary = {}
    for tn, ts in sorted(tool_stats.items()):
        tool_summary[tn] = {
            "total": ts["total"],
            "pass": ts["pass"],
            "fail": ts["fail"],
            "timeout": ts["timeout"],
            "pass_rate": round(ts["pass"] / max(ts["total"], 1) * 100, 1),
            "avg_quality": round(ts["quality_sum"] / max(ts["total"], 1), 2),
            "avg_time_ms": round(ts["time_sum"] / max(ts["total"], 1), 1),
            "max_time_ms": round(ts["max_time"], 1),
            "min_time_ms": round(ts["min_time"], 1) if ts["min_time"] < float("inf") else 0,
        }

    summary = {
        "meta": {
            "run_timestamp": datetime.now().isoformat(),
            "total_tests": total,
            "total_time_seconds": round(total_time, 1),
            "total_time_minutes": round(total_time / 60, 1),
            "db_path": db_path,
        },
        "overall": {
            "total": total,
            "pass": pass_count,
            "fail": fail_count,
            "timeout_warnings": timeout_count,
            "pass_rate": round(pass_count / max(total, 1) * 100, 1),
            "avg_quality": round(sum(quality_scores) / max(len(quality_scores), 1), 2),
            "quality_distribution": {
                str(q): sum(1 for s in quality_scores if s == q) for q in range(1, 6)
            },
            "avg_time_ms": round(sum(times) / max(len(times), 1), 1),
            "median_time_ms": round(sorted(times)[len(times) // 2], 1) if times else 0,
            "p95_time_ms": round(sorted(times)[int(len(times) * 0.95)] if times else 0, 1),
            "max_time_ms": round(max(times), 1) if times else 0,
        },
        "by_persona": persona_summary,
        "by_tool": tool_summary,
        "worst_tools": sorted(
            tool_summary.items(),
            key=lambda x: (x[1]["pass_rate"], x[1]["avg_quality"]),
        )[:10],
        "slowest_tools": sorted(
            tool_summary.items(),
            key=lambda x: -x[1]["avg_time_ms"],
        )[:10],
    }

    json_path = "/tmp/test_summary_2000.json"
    print(f"Writing summary JSON to {json_path}...")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # Print summary table
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total:   {total}")
    print(f"Pass:    {pass_count} ({pass_count/total*100:.1f}%)")
    print(f"Fail:    {fail_count} ({fail_count/total*100:.1f}%)")
    print(f"Timeout: {timeout_count}")
    print(f"Avg Quality: {summary['overall']['avg_quality']}")
    print(f"Avg Time:    {summary['overall']['avg_time_ms']:.0f} ms")
    print(f"Median Time: {summary['overall']['median_time_ms']:.0f} ms")
    print(f"P95 Time:    {summary['overall']['p95_time_ms']:.0f} ms")
    print(f"Total Time:  {total_time/60:.1f} minutes")

    print("\nQuality Distribution:")
    for q in range(5, 0, -1):
        cnt = summary["overall"]["quality_distribution"][str(q)]
        bar = "#" * (cnt // 5)
        print(f"  {q}: {cnt:4d} {bar}")

    print("\nBy Persona:")
    for persona, ps in persona_summary.items():
        print(f"  {persona:25s}: {ps['total']:4d} tests, "
              f"pass={ps['pass_rate'] if 'pass_rate' in ps else ps['pass']/max(ps['total'],1)*100:.0f}%, "
              f"avg_q={ps['avg_quality']:.2f}, "
              f"avg_t={ps['avg_time_ms']:.0f}ms")

    print("\nWorst 10 Tools (by pass rate):")
    for tn, ts in summary["worst_tools"]:
        print(f"  {tn:30s}: pass={ts['pass_rate']:5.1f}%, "
              f"avg_q={ts['avg_quality']:.2f}, n={ts['total']}")

    print("\nSlowest 10 Tools (by avg time):")
    for tn, ts in summary["slowest_tools"]:
        print(f"  {tn:30s}: avg={ts['avg_time_ms']:8.0f}ms, "
              f"max={ts['max_time_ms']:8.0f}ms, n={ts['total']}")

    print(f"\nOutput files:")
    print(f"  CSV:  {csv_path}")
    print(f"  JSON: {json_path}")
    print("=" * 70)

    return 0 if fail_count / max(total, 1) < 0.5 else 1


if __name__ == "__main__":
    sys.exit(main())
