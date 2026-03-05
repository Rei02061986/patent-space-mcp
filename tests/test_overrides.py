"""Tests for manual overrides and fuzzy exclusions."""

from normalize.applicant import ApplicantNormalizer


def test_manual_override_resolves_panasonic(entity_registry):
    normalizer = ApplicantNormalizer(entity_registry)
    result = normalizer.normalize("松下電器産業株式会社")
    assert result is not None
    assert result.entity.canonical_id == "panasonic"
    assert result.match_level == 0


def test_excluded_name_skips_fuzzy_matching(entity_registry):
    normalizer = ApplicantNormalizer(entity_registry)
    result = normalizer.normalize("HYUNDAI MOTOR CO LTD")
    assert result is None
