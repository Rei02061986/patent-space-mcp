"""3-level entity name resolution: exact -> normalized -> fuzzy.

v2: Added instance-level resolve cache to avoid repeated 69ms fuzzy matching
    for the same names. Cache is per-resolver-instance and thread-safe via GIL.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher

from .registry import Entity, EntityRegistry

# Corporate suffixes to strip (EN + JA)
_SUFFIXES = re.compile(
    r"\b(co\.?,?\s*ltd\.?|inc\.?|corp\.?|corporation|limited|llc|llp|plc|ag|"
    r"sa|s\.a\.|gmbh|n\.v\.|b\.v\.|oy|ab|as|asa|kk|k\.k\.|"
    r"holdings?|group|technologies?|technology|systems?|solutions?|the\s+)\s*$",
    re.IGNORECASE,
)

# Japanese corporate suffixes (postfix and prefix forms)
_JP_SUFFIXES = re.compile(
    r"(株式会社|有限会社|合同会社|合名会社|合資会社|"
    r"一般社団法人|一般財団法人|公益社団法人|公益財団法人|"
    r"国立大学法人|独立行政法人|学校法人|医療法人|"
    r"グループ|ホールディングス|KABUSHIKI KAISHA|YUGEN KAISHA)\s*$",
    re.IGNORECASE,
)

_JP_PREFIXES = re.compile(
    r"^\s*(株式会社|有限会社|合同会社|合名会社|合資会社|"
    r"一般社団法人|一般財団法人|公益社団法人|公益財団法人|"
    r"国立大学法人|独立行政法人|学校法人|医療法人)\s*",
)

_PUNCT = re.compile(r"[,\.&\-]+")
_SPACE = re.compile(r"\s+")

# Maximum cache size to prevent unbounded memory growth
_MAX_CACHE_SIZE = 10000


def normalize(name: str) -> str:
    """Normalize a name for matching: strip suffixes, punctuation, case."""
    s = unicodedata.normalize("NFKC", name)
    s = s.casefold()
    s = s.removeprefix("the ")
    s = s.replace("&", "and")
    s = _PUNCT.sub(" ", s)

    # Strip Japanese prefixes
    s = _JP_PREFIXES.sub("", s).strip()

    # Iteratively strip suffixes (EN + JA)
    while True:
        stripped = _SUFFIXES.sub("", s).strip()
        stripped = _JP_SUFFIXES.sub("", stripped).strip()
        if stripped == s:
            break
        s = stripped

    s = _SPACE.sub(" ", s).strip()
    return s


def fuzzy_score(a: str, b: str) -> int:
    """Compute fuzzy match score (0-100) between two strings."""
    try:
        from thefuzz import fuzz

        return fuzz.token_sort_ratio(a, b)
    except ImportError:
        tokens_a = sorted(a.lower().split())
        tokens_b = sorted(b.lower().split())
        return int(
            SequenceMatcher(
                None, " ".join(tokens_a), " ".join(tokens_b)
            ).ratio()
            * 100
        )


@dataclass
class ResolveResult:
    entity: Entity
    confidence: float
    match_level: int  # 1=exact, 2=normalized, 3=fuzzy


class EntityResolver:
    def __init__(self, registry: EntityRegistry) -> None:
        self.registry = registry
        self._norm_map: dict[str, str] = {}
        self._resolve_cache: dict[tuple[str, str | None], ResolveResult | None] = {}
        self._build_norm_map()

    def _build_norm_map(self) -> None:
        for entity in self.registry.all_entities():
            for alias in entity.aliases | {entity.canonical_name}:
                n = normalize(alias)
                if n and n not in self._norm_map:
                    self._norm_map[n] = entity.canonical_id

    def resolve(
        self,
        name: str,
        country_hint: str | None = None,
        exclusions: set[str] | None = None,
    ) -> ResolveResult | None:
        """Resolve a name to a canonical entity using 3-level matching.

        Results are cached per (name, country_hint) pair for fast repeated
        lookups. The cache is bypassed when exclusions are provided.
        """
        # Cache lookup (skip if exclusions provided, as they change results)
        if not exclusions:
            cache_key = (name, country_hint)
            cached = self._resolve_cache.get(cache_key)
            if cached is not None:
                return cached
            # Also check for cached None (not-found)
            if cache_key in self._resolve_cache:
                return None

        result = self._resolve_uncached(name, country_hint, exclusions)

        # Cache the result (including None for not-found)
        if not exclusions:
            if len(self._resolve_cache) < _MAX_CACHE_SIZE:
                self._resolve_cache[cache_key] = result

        return result

    def _resolve_uncached(
        self,
        name: str,
        country_hint: str | None = None,
        exclusions: set[str] | None = None,
    ) -> ResolveResult | None:
        """Actual resolution logic (uncached)."""
        # Level 1: Exact match via alias map
        entity = self.registry.resolve(name)
        if entity:
            return ResolveResult(entity=entity, confidence=1.0, match_level=1)

        # Level 2: Normalized match
        n = normalize(name)
        cid = self._norm_map.get(n)
        if cid:
            entity = self.registry.get(cid)
            if entity:
                return ResolveResult(
                    entity=entity, confidence=0.95, match_level=2
                )

        if len(n) < 3:
            return None

        if exclusions and name in exclusions:
            return None

        # Level 3: Fuzzy match
        best_score = 0
        best_entity: Entity | None = None
        for candidate in self.registry.all_entities():
            for alias in candidate.aliases | {candidate.canonical_name}:
                score = fuzzy_score(n, normalize(alias))
                if country_hint and candidate.country_code == country_hint:
                    score = min(score + 3, 100)
                if score > best_score:
                    best_score = score
                    best_entity = candidate

        # Short-name protection: names <= 4 chars need higher confidence
        # to prevent false matches like TSMC -> SMC
        min_score = 95 if len(n) <= 4 else 84
        if best_entity and best_score >= min_score:
            return ResolveResult(
                entity=best_entity,
                confidence=best_score / 100.0,
                match_level=3,
            )
        return None

    def resolve_many(
        self,
        names: list[str],
        country_hint: str | None = None,
        limit: int = 5,
    ) -> list[ResolveResult]:
        results: list[ResolveResult] = []
        seen: set[str] = set()
        for name in names:
            r = self.resolve(name, country_hint)
            if r and r.entity.canonical_id not in seen:
                seen.add(r.entity.canonical_id)
                results.append(r)
                if len(results) >= limit:
                    break
        return results
