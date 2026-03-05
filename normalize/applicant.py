"""Applicant name normalization: raw BigQuery assignee -> canonical entity."""
from __future__ import annotations

from typing import Any

from entity.data.manual_overrides import FUZZY_EXCLUSIONS, MANUAL_OVERRIDES
from entity.registry import EntityRegistry
from entity.resolver import EntityResolver, ResolveResult


class ApplicantNormalizer:
    """Normalize patent assignee names to canonical entities."""

    def __init__(self, registry: EntityRegistry) -> None:
        self.resolver = EntityResolver(registry)
        self._manual_overrides: dict[str, str] = dict(MANUAL_OVERRIDES)
        self._exclusions: set[str] = set(FUZZY_EXCLUSIONS)

    def load_overrides(self, mapping: dict[str, str]) -> None:
        """Add manual raw_name -> canonical_id overrides."""
        self._manual_overrides.update(mapping)

    def load_exclusions(self, exclusions: set[str]) -> None:
        """Add raw names that should skip fuzzy matching."""
        self._exclusions.update(exclusions)

    def normalize(self, raw_name: str) -> ResolveResult | None:
        """Resolve a raw assignee name to a canonical entity."""
        # Level 0: Manual override
        if raw_name in self._manual_overrides:
            entity = self.resolver.registry.get(
                self._manual_overrides[raw_name]
            )
            if entity:
                return ResolveResult(
                    entity=entity, confidence=1.0, match_level=0
                )

        # Levels 1-3: EntityResolver handles exact, normalized, fuzzy
        return self.resolver.resolve(
            raw_name,
            country_hint="JP",
            exclusions=self._exclusions,
        )

    def normalize_batch(
        self, names: list[str]
    ) -> dict[str, ResolveResult | None]:
        """Normalize a batch of assignee names."""
        return {name: self.normalize(name) for name in names}

    def coverage_report(self, names: list[str]) -> dict[str, Any]:
        """Report on normalization coverage for a set of names."""
        results = self.normalize_batch(names)
        resolved = {k: v for k, v in results.items() if v is not None}
        unresolved = [k for k, v in results.items() if v is None]

        by_level: dict[int, int] = {}
        for r in resolved.values():
            by_level[r.match_level] = by_level.get(r.match_level, 0) + 1

        return {
            "total": len(names),
            "resolved": len(resolved),
            "unresolved_count": len(unresolved),
            "coverage_pct": (
                round(len(resolved) / len(names) * 100, 1) if names else 0
            ),
            "by_match_level": {
                "exact": by_level.get(1, 0),
                "normalized": by_level.get(2, 0),
                "fuzzy": by_level.get(3, 0),
                "manual": by_level.get(0, 0),
            },
            "top_unresolved": sorted(unresolved)[:20],
        }

    def link_firm_ids(
        self, patents: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Add firm_id to each patent's applicants list."""
        for patent in patents:
            for applicant in patent.get("applicants", []):
                raw = applicant.get("raw_name") or applicant.get(
                    "harmonized_name", ""
                )
                result = self.normalize(raw)
                if result:
                    applicant["firm_id"] = result.entity.canonical_id
                    applicant["harmonized_name"] = (
                        result.entity.canonical_name
                    )
        return patents
