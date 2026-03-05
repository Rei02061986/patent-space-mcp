"""Tests for UnifiedPatent schema normalization."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from normalize.schema import normalize_bigquery_row


def test_basic_normalization():
    row = {
        "publication_number": "JP-2020123456-A",
        "application_number": "JP-2020-001234",
        "family_id": "FAM001",
        "country_code": "JP",
        "kind_code": "A",
        "title_ja": "テスト特許",
        "title_en": "Test Patent",
        "abstract_ja": "概要テスト",
        "filing_date": 20200115,
        "publication_date": 20200715,
        "entity_status": "GRANT",
        "cpc_codes": [
            {"code": "G06N3/08", "inventive": True, "first": True},
            {"code": "G06V10/82", "inventive": False, "first": False},
        ],
        "assignees": [
            {"name": "TOYOTA MOTOR CORP", "country_code": "JP"},
        ],
        "inventors": [
            {"name": "田中太郎", "country_code": "JP"},
        ],
        "citations": [
            {"publication_number": "US-10000001-B2", "type": "patent"},
        ],
    }

    result = normalize_bigquery_row(row)

    assert result["publication_number"] == "JP-2020123456-A"
    assert result["cpc_codes"] == ["G06N3/08", "G06V10/82"]
    assert result["cpc_primary"] == "G06N3/08"
    assert len(result["applicants"]) == 1
    assert result["applicants"][0]["raw_name"] == "TOYOTA MOTOR CORP"
    assert result["inventors"] == ["田中太郎"]
    assert "US-10000001-B2" in result["citations_backward"]


def test_empty_fields():
    row = {
        "publication_number": "JP-2020000001-A",
        "country_code": "JP",
    }

    result = normalize_bigquery_row(row)

    assert result["publication_number"] == "JP-2020000001-A"
    assert result["cpc_codes"] == []
    assert result["cpc_primary"] is None
    assert result["applicants"] == []
    assert result["inventors"] == []
    assert result["citations_backward"] == []


def test_duplicate_cpc_codes():
    row = {
        "publication_number": "JP-2020000002-A",
        "country_code": "JP",
        "cpc_codes": [
            {"code": "G06N3/08", "inventive": True, "first": True},
            {"code": "G06N3/08", "inventive": False, "first": False},
            {"code": "H04L9/00", "inventive": False, "first": False},
        ],
    }

    result = normalize_bigquery_row(row)
    assert result["cpc_codes"] == ["G06N3/08", "H04L9/00"]


def test_raw_assignees():
    row = {
        "publication_number": "JP-2020000003-A",
        "country_code": "JP",
        "assignee": ["トヨタ自動車株式会社", "Toyota Motor Corp"],
    }

    result = normalize_bigquery_row(row)
    assert result["raw_assignees"] == ["トヨタ自動車株式会社", "Toyota Motor Corp"]


def test_cpc_primary_selection():
    row = {
        "publication_number": "JP-2020000004-A",
        "country_code": "JP",
        "cpc_codes": [
            {"code": "A01B1/00", "inventive": False, "first": False},
            {"code": "G06N3/08", "inventive": True, "first": True},
        ],
    }

    result = normalize_bigquery_row(row)
    assert result["cpc_primary"] == "G06N3/08"
