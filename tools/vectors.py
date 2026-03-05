"""firm_tech_vector tool implementation."""
from __future__ import annotations

import struct
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver


def _unpack_float_blob(blob: bytes | None) -> list[float]:
    if not blob:
        return []
    if len(blob) % 8 != 0:
        return []
    count = len(blob) // 8
    return [float(x) for x in struct.unpack(f"{count}d", blob)]


def firm_tech_vector(
    store: PatentStore,
    resolver: EntityResolver,
    firm_query: str,
    year: int = 2024,
) -> dict[str, Any]:
    """Get a firm's precomputed technology vector and related metadata."""
    resolved = resolver.resolve(firm_query, country_hint="JP")
    if resolved is None:
        return {
            "error": f"Could not resolve firm: '{firm_query}'",
            "suggestion": "Try the exact company name, Japanese name, or stock ticker",
        }

    firm_id = resolved.entity.canonical_id

    with store._conn() as conn:
        row = conn.execute(
            """
            SELECT firm_id, year, tech_vector, patent_count, dominant_cpc,
                   tech_diversity, tech_concentration
            FROM firm_tech_vectors
            WHERE firm_id = ? AND year = ?
            """,
            (firm_id, year),
        ).fetchone()

    if row is None:
        return {
            "error": f"No firm_tech_vector found for firm_id='{firm_id}' and year={year}",
            "firm_id": firm_id,
            "year": year,
        }

    return {
        "firm_id": row["firm_id"],
        "year": row["year"],
        "tech_vector": _unpack_float_blob(row["tech_vector"]),
        "patent_count": row["patent_count"],
        "dominant_cpc": row["dominant_cpc"],
        "tech_diversity": row["tech_diversity"],
        "tech_concentration": row["tech_concentration"],
    }
