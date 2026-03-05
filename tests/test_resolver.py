"""Regression tests for resolver fuzzy matching false positives."""
from entity.resolver import EntityResolver


def test_false_positive_hyundai_must_not_match_honda(entity_registry):
    resolver = EntityResolver(entity_registry)
    assert resolver.resolve("HYUNDAI MOTOR CO LTD") is None


def test_false_positive_amada_must_not_match_registry(entity_registry):
    resolver = EntityResolver(entity_registry)
    assert resolver.resolve("AMADA CO LTD") is None


def test_false_positive_hino_must_not_match_registry(entity_registry):
    resolver = EntityResolver(entity_registry)
    assert resolver.resolve("日野自動車株式会社") is None


def test_false_positive_koito_must_not_match_registry(entity_registry):
    resolver = EntityResolver(entity_registry)
    assert resolver.resolve("株式会社小糸製作所") is None


def test_positive_exact_alias_toyota_en(entity_registry):
    resolver = EntityResolver(entity_registry)
    result = resolver.resolve("TOYOTA MOTOR CORP")
    assert result is not None
    assert result.entity.canonical_id == "toyota"


def test_positive_exact_alias_toyota_ja(entity_registry):
    resolver = EntityResolver(entity_registry)
    result = resolver.resolve("トヨタ自動車株式会社")
    assert result is not None
    assert result.entity.canonical_id == "toyota"


def test_positive_exact_alias_sony(entity_registry):
    resolver = EntityResolver(entity_registry)
    result = resolver.resolve("Sony")
    assert result is not None
    assert result.entity.canonical_id == "sony"
