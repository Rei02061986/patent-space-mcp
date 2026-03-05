"""Tests for patent-number path in patent_market_fusion."""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.sqlite_store import PatentStore
from entity.registry import EntityRegistry
from entity.resolver import EntityResolver
from tools.market_fusion import _detect_query_type, patent_market_fusion

TEST_DB_PATH = Path("/tmp/test_patent.db")
_FALLBACK_NOTE = (
    "Cluster/startability data not yet populated. Run clustering pipeline first."
)


def setup_module() -> None:
    os.environ["PATENT_DB_PATH"] = str(TEST_DB_PATH)
    for suffix in ("", "-wal", "-shm"):
        path = Path(f"{TEST_DB_PATH}{suffix}")
        if path.exists():
            path.unlink()

    store = PatentStore(TEST_DB_PATH)
    store.upsert_patent(
        {
            "publication_number": "JP-7637366-B1",
            "country_code": "JP",
            "kind_code": "B1",
            "title_ja": "ろ過装置",
            "title_en": "Filtration Apparatus",
            "filing_date": 20210115,
            "publication_date": 20240110,
            "cpc_codes": [
                {"code": "B01D53/22", "inventive": True, "first": True},
            ],
            "applicants": [
                {
                    "raw_name": "TEST ASSIGNEE KK",
                    "harmonized_name": "TEST ASSIGNEE KK",
                    "country_code": "JP",
                    "firm_id": "test_firm",
                },
            ],
            "source": "bigquery",
        }
    )


def teardown_module() -> None:
    for suffix in ("", "-wal", "-shm"):
        path = Path(f"{TEST_DB_PATH}{suffix}")
        if path.exists():
            path.unlink()


def _resolver() -> EntityResolver:
    return EntityResolver(EntityRegistry())


def test_detect_query_type_patent() -> None:
    assert _detect_query_type("JP-7637366-B1") == "patent"
    assert _detect_query_type("US-11234567-B2") == "patent"
    assert _detect_query_type("EP-1234567-A1") == "patent"
    assert _detect_query_type("Toyota") == "firm"
    assert _detect_query_type("G06N") == "technology"


def test_get_patent_cluster(tmp_path: Path) -> None:
    store = PatentStore(tmp_path / "empty_cluster.db")
    assert store.get_patent_cluster("JP-7637366-B1") is None


def test_patent_mode_missing_patent() -> None:
    store = PatentStore(TEST_DB_PATH)
    result = patent_market_fusion(
        store=store,
        resolver=_resolver(),
        query="JP-0000000-A1",
        query_type="patent",
    )

    assert result["query_type"] == "patent"
    assert result["query"] == "JP-0000000-A1"
    assert "error" in result
    assert "Patent not found" in result["error"]


def test_patent_mode_no_clusters() -> None:
    store = PatentStore(TEST_DB_PATH)
    result = patent_market_fusion(
        store=store,
        resolver=_resolver(),
        query="JP-7637366-B1",
        query_type="patent",
        purpose="general",
        year=2024,
        max_results=5,
    )

    assert result["endpoint"] == "patent_market_fusion"
    assert result["query_type"] == "patent"
    assert result["patent"]["publication_number"] == "JP-7637366-B1"
    assert result["patent"]["cpc_codes"] == ["B01D53/22"]
    assert result["patent"]["assignees"] == ["TEST ASSIGNEE KK"]
    assert result["technology_context"]["primary_cluster"] is None
    assert result["ranked_firms"] == []
    assert result["note"] == _FALLBACK_NOTE
