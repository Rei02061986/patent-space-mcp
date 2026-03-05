"""Post-processing: link patent assignees to canonical firm IDs.

Two-phase approach for performance:
  Phase 1: Fast exact + normalized matching (handles 99%+ of linkable names)
  Phase 2: Fuzzy matching only for top-N most frequent remaining names
"""
from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from entity.data.manual_overrides import FUZZY_EXCLUSIONS, MANUAL_OVERRIDES
from entity.data.tse_auto_seed import TSE_AUTO_ENTITIES
from entity.data.tse_expanded_seed import TSE_EXPANDED_ENTITIES
from entity.data.tse_prime_seed import TSE_PRIME_ENTITIES
from entity.registry import EntityRegistry
from entity.resolver import EntityResolver, normalize


def link_firm_ids(
    db_path: str = "data/patents.db",
    fuzzy_top_n: int = 500,
):
    # Build registry
    registry = EntityRegistry()
    for e in TSE_PRIME_ENTITIES:
        registry.register(e)
    for e in TSE_EXPANDED_ENTITIES:
        registry.register(e)
    for e in TSE_AUTO_ENTITIES:
        registry.register(e)

    resolver = EntityResolver(registry)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # First clear existing firm_ids to re-link from scratch
    print("Clearing existing firm_id links...")
    conn.execute("UPDATE patent_assignees SET firm_id = NULL")
    conn.commit()

    # ─── Phase 1: Fast exact + normalized matching ───
    print("\n=== Phase 1: Exact + Normalized Matching ===")
    start = time.time()

    # Build lookup: harmonized_name -> firm_id for manual overrides
    override_map: dict[str, str] = dict(MANUAL_OVERRIDES)

    # Build lookup: normalized_name -> firm_id from all entity aliases
    norm_map: dict[str, str] = {}
    alias_map: dict[str, str] = {}  # exact alias -> firm_id
    for entity in registry.all_entities():
        for alias in entity.aliases | {entity.canonical_name}:
            alias_map[alias] = entity.canonical_id
            n = normalize(alias)
            if n and n not in norm_map:
                norm_map[n] = entity.canonical_id

    # Get all distinct unlinked names with frequency
    rows = conn.execute(
        """SELECT harmonized_name, COUNT(*) as cnt
           FROM patent_assignees
           WHERE harmonized_name IS NOT NULL
           GROUP BY harmonized_name
           ORDER BY cnt DESC"""
    ).fetchall()

    total_names = len(rows)
    print(f"Total distinct assignee names: {total_names:,}")

    linked_exact = 0
    linked_norm = 0
    linked_override = 0
    remaining: list[tuple[str, int]] = []  # (name, count)

    for name, cnt in rows:
        # Level 0: Manual override
        if name in override_map:
            firm_id = override_map[name]
            conn.execute(
                "UPDATE patent_assignees SET firm_id = ? WHERE harmonized_name = ?",
                (firm_id, name),
            )
            linked_override += 1
            continue

        # Level 1: Exact alias match
        if name in alias_map:
            conn.execute(
                "UPDATE patent_assignees SET firm_id = ? WHERE harmonized_name = ?",
                (alias_map[name], name),
            )
            linked_exact += 1
            continue

        # Level 2: Normalized match
        n = normalize(name)
        if n in norm_map:
            conn.execute(
                "UPDATE patent_assignees SET firm_id = ? WHERE harmonized_name = ?",
                (norm_map[n], name),
            )
            linked_norm += 1
            continue

        remaining.append((name, cnt))

    conn.commit()
    elapsed1 = time.time() - start

    print(f"  Override matches: {linked_override:,}")
    print(f"  Exact matches:   {linked_exact:,}")
    print(f"  Normalized:      {linked_norm:,}")
    print(f"  Total Phase 1:   {linked_override + linked_exact + linked_norm:,}")
    print(f"  Remaining:       {len(remaining):,}")
    print(f"  Time:            {elapsed1:.1f}s")

    # ─── Phase 2: Fuzzy matching (top-N most frequent remaining names) ───
    print(f"\n=== Phase 2: Fuzzy Matching (top {fuzzy_top_n} remaining names) ===")
    start2 = time.time()

    fuzzy_candidates = remaining[:fuzzy_top_n]
    linked_fuzzy = 0

    for i, (name, cnt) in enumerate(fuzzy_candidates):
        n = normalize(name)
        if len(n) < 3:
            continue
        if name in FUZZY_EXCLUSIONS:
            continue

        result = resolver.resolve(name, country_hint="JP", exclusions=FUZZY_EXCLUSIONS)
        if result and result.match_level == 3:
            conn.execute(
                "UPDATE patent_assignees SET firm_id = ? WHERE harmonized_name = ?",
                (result.entity.canonical_id, name),
            )
            linked_fuzzy += 1
            print(
                f"  [{i+1}/{len(fuzzy_candidates)}] Fuzzy: {name} -> "
                f"{result.entity.canonical_id} (score={result.confidence:.2f}, patents={cnt:,})"
            )

        if (i + 1) % 100 == 0:
            conn.commit()
            elapsed = time.time() - start2
            print(f"  ... processed {i+1}/{len(fuzzy_candidates)} ({elapsed:.1f}s)")

    conn.commit()
    elapsed2 = time.time() - start2

    print(f"  Fuzzy matches:   {linked_fuzzy:,}")
    print(f"  Time:            {elapsed2:.1f}s")

    # ─── Summary ───
    total_linked = linked_override + linked_exact + linked_norm + linked_fuzzy
    print(f"\n=== Summary ===")
    print(f"  Override: {linked_override:,}")
    print(f"  Exact:    {linked_exact:,}")
    print(f"  Normalized: {linked_norm:,}")
    print(f"  Fuzzy:    {linked_fuzzy:,}")
    print(f"  Total linked names: {total_linked:,} / {total_names:,}")
    print(f"  Total time: {elapsed1 + elapsed2:.1f}s")

    # Quick patent coverage check
    row = conn.execute(
        """SELECT
             COUNT(DISTINCT pa.publication_number) as linked_patents,
             (SELECT COUNT(*) FROM patents) as total_patents
           FROM patent_assignees pa
           WHERE pa.firm_id IS NOT NULL"""
    ).fetchone()
    if row:
        linked_p, total_p = row[0], row[1]
        print(f"\n  Patent coverage: {linked_p:,} / {total_p:,} ({linked_p/total_p*100:.1f}%)")

    conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Link firm IDs to patent assignees")
    parser.add_argument("db", nargs="?", default="data/patents.db", help="SQLite DB path")
    parser.add_argument(
        "--fuzzy-top-n",
        type=int,
        default=500,
        help="Number of top remaining names to fuzzy match (default: 500)",
    )
    args = parser.parse_args()
    link_firm_ids(args.db, fuzzy_top_n=args.fuzzy_top_n)
