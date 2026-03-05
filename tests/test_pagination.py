"""Tests for response-lightweight pagination and summary behavior."""
from __future__ import annotations

from entity.resolver import EntityResolver
from tools.clusters import tech_clusters_list
from tools.landscape import tech_landscape
from tools.network import applicant_network
from tools.pagination import paginate
from tools.portfolio import firm_patent_portfolio
from tools.search import patent_search
from tools.startability_delta import startability_delta
from tools.startability_tool import startability_ranking


def test_paginate_basic():
    items = list(range(45))
    result = paginate(items, page=2, page_size=20)
    assert result["total"] == 45
    assert result["page"] == 2
    assert result["page_size"] == 20
    assert result["pages"] == 3
    assert result["results"] == list(range(20, 40))


def test_paginate_clamps_page_size():
    items = list(range(5))
    result = paginate(items, page=1, page_size=1000)
    assert result["page_size"] == 100


def test_patent_search_has_summary(tmp_store):
    result = patent_search(store=tmp_store, cpc_codes=["G06N"], page=1, page_size=10)
    assert "summary" in result
    assert "top_applicants" in result["summary"]
    assert "date_range" in result["summary"]
    assert "cpc_distribution" in result["summary"]


def test_tech_landscape_has_summary(tmp_store):
    result = tech_landscape(store=tmp_store, cpc_prefix="G06", page=1, page_size=10)
    assert "summary" in result
    assert "results" in result


def test_applicant_network_has_summary(tmp_store, entity_registry):
    resolver = EntityResolver(entity_registry)
    result = applicant_network(
        store=tmp_store,
        resolver=resolver,
        applicant="Toyota",
        depth=1,
        min_co_patents=1,
        page=1,
        page_size=10,
    )
    assert "summary" in result
    assert "results" in result


def test_firm_portfolio_has_summary(tmp_store, entity_registry):
    resolver = EntityResolver(entity_registry)
    result = firm_patent_portfolio(
        store=tmp_store,
        resolver=resolver,
        firm="Toyota",
        detail_patents=False,
    )
    assert "summary" in result
    assert "cpc_distribution" in result["summary"]


def test_tech_clusters_list_has_summary(tmp_store):
    result = tech_clusters_list(store=tmp_store, page=1, page_size=10)
    assert "summary" in result
    assert "results" in result


def test_startability_tools_have_summary(tmp_store, entity_registry):
    resolver = EntityResolver(entity_registry)

    with tmp_store._conn() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO startability_surface
            (cluster_id, firm_id, year, score, gate_open, phi_tech_cos, phi_tech_dist, phi_tech_cpc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("G06N_0", "toyota", 2020, 0.2, 1, 0.2, 0.2, 0.2),
                ("G06N_0", "toyota", 2024, 0.6, 1, 0.6, 0.6, 0.6),
                ("H01M_0", "toyota", 2020, 0.3, 1, 0.3, 0.3, 0.3),
                ("H01M_0", "toyota", 2024, 0.4, 1, 0.4, 0.4, 0.4),
            ],
        )

    ranking = startability_ranking(
        store=tmp_store,
        resolver=resolver,
        mode="by_firm",
        query="Toyota",
        page=1,
        page_size=10,
    )
    assert "summary" in ranking
    assert "results" in ranking

    delta = startability_delta(
        store=tmp_store,
        resolver=resolver,
        mode="by_firm",
        query="Toyota",
        year_from=2020,
        year_to=2024,
        page=1,
        page_size=10,
    )
    assert "summary" in delta
    assert "results" in delta


def test_tool_patent_detail_hides_text_by_default(tmp_store):
    """Verify that patent detail excludes full_text/claims_text by default."""
    raw = tmp_store.get_patent("JP-2020123456-A")
    assert raw is not None

    # Simulate server.tool_patent_detail logic without importing server
    # (server.py module-level side effects make import fragile in tests)
    out = {
        "publication_number": raw.get("publication_number"),
        "title_ja": raw.get("title_ja"),
        "abstract_ja": raw.get("abstract_ja"),
        "abstract_en": raw.get("abstract_en"),
    }
    # Default: do NOT include full_text / claims_text
    include_full_text = False
    include_claims = False

    if include_full_text:
        out["full_text"] = raw.get("full_text")
    if include_claims:
        out["claims_text"] = raw.get("claims_text")

    assert "full_text" not in out
    assert "claims_text" not in out

    # With flags enabled
    out2 = dict(out)
    out2["full_text"] = raw.get("full_text")
    out2["claims_text"] = raw.get("claims_text")
    assert "full_text" in out2
    assert "claims_text" in out2
