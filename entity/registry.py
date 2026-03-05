"""EntityRegistry: canonical entity names + alias reverse-lookup.

Adapted from legal_mcp with patent-specific extensions.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Entity:
    canonical_id: str
    canonical_name: str
    country_code: str
    entity_type: str  # corporation, university, government, npe, individual
    aliases: set[str]
    parent_id: str | None = None
    industry: str | None = None
    edinet_code: str | None = None
    ticker: str | None = None
    tse_section: str | None = None  # Prime, Standard, Growth


class EntityRegistry:
    def __init__(self) -> None:
        self._entities: dict[str, Entity] = {}
        self._alias_map: dict[str, str] = {}

    def register(self, entity: Entity) -> None:
        self._entities[entity.canonical_id] = entity
        names = entity.aliases | {entity.canonical_name, entity.canonical_id}
        # Also register ticker and edinet_code as aliases
        if entity.ticker:
            names.add(entity.ticker)
        if entity.edinet_code:
            names.add(entity.edinet_code)
        for alias in names:
            self._alias_map[alias.lower().strip()] = entity.canonical_id

    def resolve(self, name: str) -> Entity | None:
        return self._entities.get(
            self._alias_map.get(name.lower().strip(), "")
        )

    def get(self, canonical_id: str) -> Entity | None:
        return self._entities.get(canonical_id)

    def search(self, query: str, limit: int = 10) -> list[Entity]:
        q = query.lower()
        results = [
            e
            for e in self._entities.values()
            if q in e.canonical_id
            or q in e.canonical_name.lower()
            or any(q in a.lower() for a in e.aliases)
        ]
        return results[:limit]

    def add_alias(self, canonical_id: str, alias: str) -> None:
        if canonical_id in self._entities:
            self._entities[canonical_id].aliases.add(alias)
            self._alias_map[alias.lower().strip()] = canonical_id

    def all_entities(self) -> list[Entity]:
        return list(self._entities.values())

    def by_type(self, entity_type: str) -> list[Entity]:
        return [
            e for e in self._entities.values() if e.entity_type == entity_type
        ]

    def by_country(self, country_code: str) -> list[Entity]:
        return [
            e for e in self._entities.values() if e.country_code == country_code
        ]

    def by_tse_section(self, section: str) -> list[Entity]:
        return [
            e
            for e in self._entities.values()
            if e.tse_section and e.tse_section.lower() == section.lower()
        ]

    def display_name(self, firm_id: str) -> str:
        """Return human-readable display name for a firm_id.

        Falls back to the firm_id itself if not found in the registry.
        """
        entity = self._entities.get(firm_id)
        if entity is not None:
            return entity.canonical_name
        return firm_id
