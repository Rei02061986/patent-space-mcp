"""Tests for SQLite patent store."""


def test_upsert_and_get(tmp_store):
    patent = tmp_store.get_patent("JP-2020123456-A")
    assert patent is not None
    assert patent["title_ja"] == "人工知能による画像認識装置"
    assert patent["country_code"] == "JP"
    assert patent["filing_date"] == 20200115


def test_cpc_codes_stored(tmp_store):
    patent = tmp_store.get_patent("JP-2020123456-A")
    codes = [c["cpc_code"] for c in patent["cpc_codes"]]
    assert "G06N3/08" in codes
    assert "G06V10/82" in codes


def test_assignees_stored(tmp_store):
    patent = tmp_store.get_patent("JP-2020123456-A")
    names = [a["harmonized_name"] for a in patent["assignees"]]
    assert "TOYOTA MOTOR CORP" in names


def test_citations_stored(tmp_store):
    patent = tmp_store.get_patent("JP-2020123456-A")
    assert "US-10000001-B2" in patent["citations_backward"]


def test_fts_search(tmp_store):
    results = tmp_store.search(query="画像認識")
    assert len(results) >= 1
    assert results[0]["publication_number"] == "JP-2020123456-A"


def test_cpc_prefix_search(tmp_store):
    results = tmp_store.search(cpc_prefix="G06N")
    assert len(results) >= 1
    pubs = [r["publication_number"] for r in results]
    assert "JP-2020123456-A" in pubs


def test_assignee_search(tmp_store):
    results = tmp_store.search(assignee="TOYOTA")
    assert len(results) >= 1


def test_date_filter(tmp_store):
    results = tmp_store.search(date_from=20210101)
    pubs = [r["publication_number"] for r in results]
    assert "JP-2020123456-A" not in pubs
    assert "JP-2021234567-A" in pubs


def test_count(tmp_store):
    total = tmp_store.count()
    assert total == 3


def test_firm_portfolio(tmp_store):
    portfolio = tmp_store.get_firm_portfolio("toyota")
    assert portfolio["count"] == 2
    assert len(portfolio["cpc_distribution"]) > 0


def test_get_nonexistent(tmp_store):
    patent = tmp_store.get_patent("NONEXISTENT")
    assert patent is None


def test_upsert_replaces(tmp_store):
    updated = {
        "publication_number": "JP-2020123456-A",
        "country_code": "JP",
        "title_ja": "更新されたタイトル",
        "cpc_codes": [{"code": "G06N3/08", "inventive": True, "first": True}],
        "applicants": [],
        "source": "bigquery",
    }
    tmp_store.upsert_patent(updated)
    patent = tmp_store.get_patent("JP-2020123456-A")
    assert patent["title_ja"] == "更新されたタイトル"
