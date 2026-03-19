"""applicant_network tool implementation."""
from __future__ import annotations

import math
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver
from tools.pagination import paginate


# Mapping: entity canonical_id -> patent_assignees firm_id
_NETWORK_FIRM_REMAP = {
    "samsung": "samsung_electronics",
    "bosch": "bosch_robert",
    "huawei": "huawei_tech",
    "lg": "lg_electronics",
    "siemens": "siemens_ag",
    "byd": "byd_co",
}

def applicant_network(
    store: PatentStore,
    resolver: EntityResolver,
    applicant: str,
    depth: int = 1,
    min_co_patents: int = 5,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """Build co-applicant network centered on a resolved applicant."""
    result = resolver.resolve(applicant, country_hint="JP")
    if result is None:
        return {
            "error": f"Could not resolve applicant: '{applicant}'",
            "suggestion": "Try the exact company name, Japanese name, or stock ticker",
        }

    entity = result.entity
    # Remap canonical_id to actual patent_assignees firm_id
    _effective_firm_id = _NETWORK_FIRM_REMAP.get(entity.canonical_id, entity.canonical_id)

    center_count = store.get_firm_patent_count_fast(_effective_firm_id) or 0

    depth = max(1, depth)
    min_co_patents = max(1, min_co_patents)

    queue: list[tuple[str, int]] = [(_effective_firm_id, 0)]
    expanded: set[str] = set()
    node_map: dict[str, dict[str, Any]] = {}
    edge_map: dict[tuple[str, str], dict[str, Any]] = {}

    while queue:
        source_firm_id, level = queue.pop(0)
        if source_firm_id in expanded or level >= depth:
            continue
        expanded.add(source_firm_id)

        neighbors = store.get_co_applicant_network(
            firm_id=source_firm_id,
            min_count=min_co_patents,
        )

        for row in neighbors:
            target_id = row["co_firm_id"] or row["co_id"]
            if not target_id or target_id == _effective_firm_id:
                continue

            if target_id not in node_map:
                patent_count = row["co_patent_count"]
                if row["co_firm_id"]:
                    fast_count = store.get_firm_patent_count_fast(row["co_firm_id"])
                    if fast_count is not None:
                        patent_count = fast_count
                node_map[target_id] = {
                    "id": target_id,
                    "name": row["co_name"],
                    "patent_count": patent_count,
                }

            edge_key = (source_firm_id, target_id)
            edge_map[edge_key] = {
                "source": source_firm_id,
                "target": target_id,
                "co_patent_count": row["co_patent_count"],
                "shared_cpc_classes": row["shared_cpc_classes"],
            }

            if row["co_firm_id"] and (level + 1) < depth:
                queue.append((row["co_firm_id"], level + 1))

    edges = list(edge_map.values())
    paged = paginate(edges, page=page, page_size=page_size)
    page_size_clamped = paged["page_size"]
    pages = math.ceil(len(edges) / page_size_clamped) if edges else 1

    top_applicants = sorted(
        node_map.values(),
        key=lambda n: n.get("patent_count", 0),
        reverse=True,
    )[:5]

    return {
        "total": len(edges),
        "page": paged["page"],
        "page_size": page_size_clamped,
        "pages": pages,
        "results": paged["results"],
        "summary": {
            "node_count": len(node_map),
            "edge_count": len(edges),
            "top_applicants": [
                {
                    "name": n["name"],
                    "firm_id": n["id"],
                    "count": n["patent_count"],
                }
                for n in top_applicants
            ],
            "date_range": {"earliest": None, "latest": None},
            "cpc_distribution": [],
        },
        "center": {
            "firm_id": entity.canonical_id,
            "name": entity.canonical_name,
            "patent_count": center_count,
        },
        "nodes": list(node_map.values()),
        "edges": edges,
    }
