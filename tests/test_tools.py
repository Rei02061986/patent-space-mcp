"""Integration tests for MCP tools."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.sqlite_store import PatentStore
from entity.registry import Entity, EntityRegistry
from entity.resolver import EntityResolver
from tools.compare import patent_compare
from tools.landscape import tech_landscape
from tools.network import applicant_network
from tools.portfolio import firm_patent_portfolio
from tools.search import patent_search


def test_patent_search_by_cpc(tmp_store):
    result = patent_search(
        store=tmp_store,
        cpc_codes=["G06N"],
    )
    assert result["result_count"] >= 1
    assert result["total_count"] >= 1
    assert "G06N3/08" in result["patents"][0]["cpc_codes"]


def test_patent_search_by_assignee(tmp_store):
    result = patent_search(
        store=tmp_store,
        applicant="TOYOTA",
    )
    assert result["result_count"] >= 1


def test_patent_search_by_date(tmp_store):
    result = patent_search(
        store=tmp_store,
        date_from="2021-01-01",
    )
    assert result["result_count"] >= 1
    for p in result["patents"]:
        assert p["publication_date"] >= 20210101


def test_patent_search_empty(tmp_store):
    result = patent_search(
        store=tmp_store,
        applicant="NONEXISTENT_CORP",
    )
    assert result["result_count"] == 0
    assert result["total_count"] == 0


def test_firm_portfolio(tmp_store, entity_registry):
    resolver = EntityResolver(entity_registry)

    result = firm_patent_portfolio(
        store=tmp_store,
        resolver=resolver,
        firm="トヨタ",
    )

    assert "error" not in result
    assert result["entity"]["canonical_id"] == "toyota"
    assert result["patent_count"] == 2
    assert len(result["cpc_distribution"]) > 0


def test_firm_portfolio_english(tmp_store, entity_registry):
    resolver = EntityResolver(entity_registry)

    result = firm_patent_portfolio(
        store=tmp_store,
        resolver=resolver,
        firm="Toyota Motor Corporation",
    )

    assert "error" not in result
    assert result["entity"]["canonical_id"] == "toyota"


def test_firm_portfolio_not_found(tmp_store, entity_registry):
    resolver = EntityResolver(entity_registry)

    result = firm_patent_portfolio(
        store=tmp_store,
        resolver=resolver,
        firm="Unknown Corp XYZ",
    )

    assert "error" in result


def test_tech_landscape(tmp_store):
    result = tech_landscape(
        store=tmp_store,
        cpc_prefix="G06",
        granularity="year",
    )
    assert result["total_patents"] >= 1
    assert len(result["cpc_trend"]) >= 1
    assert any(row["cpc_class"] == "G06N" for row in result["cpc_trend"])
    assert len(result["top_applicants"]) >= 1


def test_applicant_network(tmp_store, entity_registry):
    resolver = EntityResolver(entity_registry)
    result = applicant_network(
        store=tmp_store,
        resolver=resolver,
        applicant="Toyota",
        depth=1,
        min_co_patents=1,
    )

    assert "error" not in result
    assert result["center"]["firm_id"] == "toyota"
    assert any(node["id"] == "panasonic" for node in result["nodes"])
    assert any(edge["target"] == "panasonic" for edge in result["edges"])


def test_patent_compare(tmp_store, entity_registry):
    resolver = EntityResolver(entity_registry)
    result = patent_compare(
        store=tmp_store,
        resolver=resolver,
        firms=["Toyota", "Honda"],
    )

    assert "error" not in result
    assert len(result["firms"]) == 2
    firm_ids = {f["firm_id"] for f in result["firms"]}
    assert firm_ids == {"toyota", "honda"}
    assert "toyota" in result["unique_cpc"]
