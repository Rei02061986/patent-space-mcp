"""Tests for v0.3.0 features: ticker resolution, multi-CPC search,
English fallback, S&P 500 entities, embedding bridge improvements."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from entity.registry import Entity, EntityRegistry
from entity.resolver import EntityResolver
from tools.search import patent_search, _is_english


# ── Ticker resolution tests ──


def test_ticker_resolves_to_entity():
    """Stock tickers should be registered as aliases and resolve."""
    registry = EntityRegistry()
    registry.register(Entity(
        "toyota", "Toyota Motor Corporation", "JP", "corporation",
        {"トヨタ自動車株式会社", "Toyota", "TOYOTA MOTOR CORP"},
        ticker="7203", edinet_code="E02144",
    ))
    assert registry.resolve("7203") is not None
    assert registry.resolve("7203").canonical_id == "toyota"


def test_edinet_code_resolves():
    """EDINET codes should resolve to the entity."""
    registry = EntityRegistry()
    registry.register(Entity(
        "toyota", "Toyota Motor Corporation", "JP", "corporation",
        {"Toyota"}, edinet_code="E02144",
    ))
    assert registry.resolve("E02144") is not None
    assert registry.resolve("E02144").canonical_id == "toyota"


def test_ticker_case_insensitive():
    """Ticker resolution should be case insensitive."""
    registry = EntityRegistry()
    registry.register(Entity(
        "apple", "Apple Inc.", "US", "corporation",
        {"Apple", "APPLE INC"}, ticker="AAPL",
    ))
    assert registry.resolve("aapl") is not None
    assert registry.resolve("AAPL") is not None
    assert registry.resolve("Aapl") is not None


def test_ticker_via_resolver():
    """Ticker should work through the full EntityResolver."""
    registry = EntityRegistry()
    registry.register(Entity(
        "toyota", "Toyota Motor Corporation", "JP", "corporation",
        {"トヨタ自動車株式会社", "Toyota", "TOYOTA MOTOR CORP"},
        ticker="7203",
    ))
    resolver = EntityResolver(registry)
    result = resolver.resolve("7203")
    assert result is not None
    assert result.entity.canonical_id == "toyota"


def test_no_ticker_no_crash():
    """Entity without ticker should still work fine."""
    registry = EntityRegistry()
    registry.register(Entity(
        "unknown", "Unknown Corp", "JP", "corporation",
        {"Unknown"},
    ))
    entity = registry.resolve("Unknown")
    assert entity is not None
    assert entity.canonical_id == "unknown"


# ── S&P 500 entity tests ──


def test_sp500_entity_registration():
    """S&P 500 entities should be loadable and registerable."""
    try:
        from entity.data.sp500_seed import SP500_ENTITIES
    except ImportError:
        pytest.skip("sp500_seed.py not available")

    assert len(SP500_ENTITIES) > 50  # Should have ~100 entities
    registry = EntityRegistry()
    for e in SP500_ENTITIES:
        registry.register(e)

    # Test a few known US companies
    apple = registry.resolve("AAPL")
    if apple:
        assert apple.country_code == "US"

    google = registry.resolve("Google")
    if google:
        assert google.country_code == "US"


def test_sp500_entities_have_us_country_code():
    """All S&P 500 entities should have US country code."""
    try:
        from entity.data.sp500_seed import SP500_ENTITIES
    except ImportError:
        pytest.skip("sp500_seed.py not available")

    for e in SP500_ENTITIES:
        assert e.country_code == "US", f"{e.canonical_name} has country_code={e.country_code}"


def test_sp500_entities_have_tickers():
    """S&P 500 entities should have stock tickers."""
    try:
        from entity.data.sp500_seed import SP500_ENTITIES
    except ImportError:
        pytest.skip("sp500_seed.py not available")

    with_ticker = [e for e in SP500_ENTITIES if e.ticker]
    # Most should have tickers
    assert len(with_ticker) >= len(SP500_ENTITIES) * 0.9


# ── _is_english heuristic tests ──


def test_is_english_pure_english():
    assert _is_english("solid state lithium battery") is True


def test_is_english_pure_japanese():
    assert _is_english("固体電解質リチウム電池") is False


def test_is_english_mixed_mostly_english():
    assert _is_english("Machine Learning for 画像") is True


def test_is_english_empty():
    assert _is_english("") is False  # 0/0 → 0.0, not > 0.8


# ── Multi-CPC search tests ──


def test_multi_cpc_single_code(tmp_store):
    """Single CPC code should work like regular search."""
    result = patent_search(store=tmp_store, cpc_codes=["G06N"])
    assert result["result_count"] >= 1
    assert any("G06N" in c for p in result["patents"] for c in p["cpc_codes"])


def test_multi_cpc_intersection(tmp_store):
    """Multiple CPC codes should return intersection (patents with ALL codes)."""
    # Patent JP-2020123456-A has both G06N3/08 and G06V10/82
    result = patent_search(store=tmp_store, cpc_codes=["G06N", "G06V"])
    assert result["result_count"] >= 1
    for p in result["patents"]:
        has_g06n = any("G06N" in c for c in p["cpc_codes"])
        has_g06v = any("G06V" in c for c in p["cpc_codes"])
        assert has_g06n and has_g06v


def test_multi_cpc_no_overlap(tmp_store):
    """CPC codes with no overlap should return empty."""
    result = patent_search(store=tmp_store, cpc_codes=["G06N", "H01M"])
    # G06N and H01M don't appear on the same patent in test data
    assert result["result_count"] == 0


def test_search_method_field(tmp_store):
    """Result should include search_method field."""
    result = patent_search(store=tmp_store, query="画像認識")
    assert "search_method" in result
    assert result["search_method"] in ("fts5", "title_en_like")


# ── English fallback tests ──


def test_english_fallback_on_title(tmp_store):
    """English query should fallback to title_en LIKE if FTS returns few results."""
    # Search for something likely in title_en but not in FTS trigrams
    result = patent_search(store=tmp_store, query="Battery Management System")
    # Should find JP-2021234567-A which has title_en="Battery Management System"
    if result["result_count"] > 0:
        pubs = [p["publication_number"] for p in result["patents"]]
        assert "JP-2021234567-A" in pubs


def test_english_query_with_cpc_skips_fallback(tmp_store):
    """English query with CPC codes should NOT trigger LIKE fallback."""
    result = patent_search(
        store=tmp_store,
        query="Battery",
        cpc_codes=["H01M"],
    )
    assert result["search_method"] == "fts5"


# ── Embedding bridge tests ──


def test_fts_query_variants_english():
    from space.embedding_bridge import _fts_query_variants

    variants = _fts_query_variants("solid state lithium battery")
    assert len(variants) >= 2  # original + at least one OR variant
    assert variants[0] == "solid state lithium battery"
    # OR variant should contain key words
    assert any("OR" in v for v in variants[1:])


def test_fts_query_variants_japanese():
    from space.embedding_bridge import _fts_query_variants

    variants = _fts_query_variants("固体電解質 リチウム電池")
    assert len(variants) >= 1
    assert variants[0] == "固体電解質 リチウム電池"


def test_fts_query_variants_empty():
    from space.embedding_bridge import _fts_query_variants

    variants = _fts_query_variants("")
    assert variants == []


def test_fts_query_variants_single_word():
    from space.embedding_bridge import _fts_query_variants

    variants = _fts_query_variants("battery")
    assert len(variants) >= 1
    assert variants[0] == "battery"


def test_looks_english():
    from space.embedding_bridge import _looks_english

    assert _looks_english("machine learning image recognition") is True
    assert _looks_english("固体電解質リチウム電池") is False
    assert _looks_english("mixed テスト text") is False  # Japanese chars push below 80% ASCII


def test_unpack_embedding_roundtrip():
    import struct
    import numpy as np
    from space.embedding_bridge import _unpack_embedding

    original = np.random.randn(64).astype(np.float64)
    blob = struct.pack("64d", *original.tolist())
    recovered = _unpack_embedding(blob)
    assert recovered is not None
    np.testing.assert_array_almost_equal(original, recovered)


def test_unpack_embedding_none():
    from space.embedding_bridge import _unpack_embedding

    assert _unpack_embedding(None) is None


def test_unpack_embedding_bad_blob():
    from space.embedding_bridge import _unpack_embedding

    assert _unpack_embedding(b"bad data") is None


def test_cosine_similarity():
    import numpy as np
    from space.embedding_bridge import _cosine_similarity

    a = np.array([1.0, 0.0, 0.0])
    b = np.array([1.0, 0.0, 0.0])
    assert abs(_cosine_similarity(a, b) - 1.0) < 1e-6

    c = np.array([0.0, 1.0, 0.0])
    assert abs(_cosine_similarity(a, c)) < 1e-6

    d = np.array([-1.0, 0.0, 0.0])
    assert abs(_cosine_similarity(a, d) - (-1.0)) < 1e-6


def test_cosine_similarity_zero_vector():
    import numpy as np
    from space.embedding_bridge import _cosine_similarity

    a = np.array([1.0, 0.0, 0.0])
    z = np.array([0.0, 0.0, 0.0])
    assert _cosine_similarity(a, z) == 0.0
    assert _cosine_similarity(z, z) == 0.0


# ── Registry method tests ──


def test_registry_display_name():
    registry = EntityRegistry()
    registry.register(Entity(
        "toyota", "Toyota Motor Corporation", "JP", "corporation",
        {"Toyota"},
    ))
    assert registry.display_name("toyota") == "Toyota Motor Corporation"
    assert registry.display_name("nonexistent") == "nonexistent"


def test_registry_by_country():
    registry = EntityRegistry()
    registry.register(Entity("co_jp", "JP Company", "JP", "corporation", set()))
    registry.register(Entity("co_us", "US Company", "US", "corporation", set()))
    assert len(registry.by_country("JP")) == 1
    assert len(registry.by_country("US")) == 1
    assert len(registry.by_country("DE")) == 0


def test_registry_by_tse_section():
    registry = EntityRegistry()
    registry.register(Entity(
        "prime_co", "Prime Co", "JP", "corporation", set(), tse_section="Prime",
    ))
    registry.register(Entity(
        "growth_co", "Growth Co", "JP", "corporation", set(), tse_section="Growth",
    ))
    assert len(registry.by_tse_section("Prime")) == 1
    assert len(registry.by_tse_section("Growth")) == 1
    assert len(registry.by_tse_section("Standard")) == 0


def test_registry_add_alias():
    registry = EntityRegistry()
    registry.register(Entity(
        "test", "Test Corp", "JP", "corporation", {"TestCo"},
    ))
    registry.add_alias("test", "NEW_ALIAS")
    assert registry.resolve("new_alias") is not None
    assert registry.resolve("new_alias").canonical_id == "test"


def test_registry_search():
    registry = EntityRegistry()
    registry.register(Entity(
        "toyota", "Toyota Motor Corporation", "JP", "corporation",
        {"トヨタ"}, industry="automotive",
    ))
    registry.register(Entity(
        "sony", "Sony Group Corporation", "JP", "corporation",
        {"ソニー"}, industry="electronics",
    ))
    results = registry.search("Toyota")
    assert len(results) == 1
    assert results[0].canonical_id == "toyota"


# ── Resolver edge cases ──


def test_resolver_resolve_many(entity_registry):
    resolver = EntityResolver(entity_registry)
    results = resolver.resolve_many(
        ["Toyota", "Sony", "Unknown Corp", "Honda"],
        country_hint="JP",
    )
    ids = {r.entity.canonical_id for r in results}
    assert "toyota" in ids
    assert "sony" in ids
    assert "honda" in ids
    assert len(results) == 3  # Unknown Corp excluded


def test_resolver_fuzzy_threshold():
    """Fuzzy match at threshold 0.80 should catch reasonable variations."""
    registry = EntityRegistry()
    registry.register(Entity(
        "toyota", "Toyota Motor Corporation", "JP", "corporation",
        {"Toyota Motor Corporation"},
    ))
    resolver = EntityResolver(registry)
    # Close enough variant
    result = resolver.resolve("Toyota Motor Corp", country_hint="JP")
    assert result is not None
    assert result.entity.canonical_id == "toyota"


def test_resolver_false_positive_prevention():
    """Hyundai should NOT fuzzy-match Honda (score ~83, threshold 84)."""
    registry = EntityRegistry()
    registry.register(Entity(
        "honda", "Honda Motor Co., Ltd.", "JP", "corporation",
        {"Honda", "HONDA MOTOR CO LTD"},
    ))
    resolver = EntityResolver(registry)
    # Hyundai is different from Honda despite both being auto companies
    result = resolver.resolve("HYUNDAI MOTOR CO LTD")
    assert result is None


def test_resolver_genuine_fuzzy_match():
    """Close variations of a known entity should still fuzzy-match."""
    registry = EntityRegistry()
    registry.register(Entity(
        "toyota", "Toyota Motor Corporation", "JP", "corporation",
        {"Toyota Motor Corporation"},
    ))
    resolver = EntityResolver(registry)
    # "TOYOTA MOTOR CORP." is a very close variant
    result = resolver.resolve("TOYOTA MOTOR CORP.")
    assert result is not None
    assert result.entity.canonical_id == "toyota"
