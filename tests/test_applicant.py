"""Tests for entity resolution and applicant name normalization."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from entity.resolver import EntityResolver, normalize


def test_normalize_jp_prefix(entity_registry):
    assert normalize("株式会社トヨタ") == "トヨタ".casefold()


def test_normalize_jp_suffix(entity_registry):
    assert normalize("トヨタ自動車株式会社") == "トヨタ自動車".casefold()


def test_normalize_en_suffix(entity_registry):
    assert normalize("Toyota Motor Corp.") == "toyota motor"


def test_normalize_mixed(entity_registry):
    assert normalize("Panasonic Holdings Corporation") == "panasonic"


def test_exact_match(entity_registry):
    resolver = EntityResolver(entity_registry)
    result = resolver.resolve("トヨタ自動車株式会社")
    assert result is not None
    assert result.entity.canonical_id == "toyota"
    assert result.match_level == 1
    assert result.confidence == 1.0


def test_exact_match_en(entity_registry):
    resolver = EntityResolver(entity_registry)
    result = resolver.resolve("TOYOTA MOTOR CORP")
    assert result is not None
    assert result.entity.canonical_id == "toyota"


def test_normalized_match(entity_registry):
    resolver = EntityResolver(entity_registry)
    result = resolver.resolve("Toyota Motor Corporation")
    assert result is not None
    assert result.entity.canonical_id == "toyota"
    assert result.match_level <= 2


def test_fuzzy_match(entity_registry):
    resolver = EntityResolver(entity_registry)
    result = resolver.resolve("TOYOTA MOTOR CO., LTD.", country_hint="JP")
    assert result is not None
    assert result.entity.canonical_id == "toyota"
    assert result.match_level <= 3


def test_no_match(entity_registry):
    resolver = EntityResolver(entity_registry)
    result = resolver.resolve("Unknown Company XYZ")
    assert result is None


def test_sony_variants(entity_registry):
    resolver = EntityResolver(entity_registry)
    for name in ["Sony", "SONY CORP", "ソニー", "ソニーグループ株式会社"]:
        result = resolver.resolve(name)
        assert result is not None, f"Failed to resolve: {name}"
        assert result.entity.canonical_id == "sony"


def test_resolve_many(entity_registry):
    resolver = EntityResolver(entity_registry)
    names = ["Toyota", "Sony", "Unknown Corp", "Panasonic"]
    results = resolver.resolve_many(names, country_hint="JP")
    ids = [r.entity.canonical_id for r in results]
    assert "toyota" in ids
    assert "sony" in ids
    assert "panasonic" in ids
    assert len(results) == 3
