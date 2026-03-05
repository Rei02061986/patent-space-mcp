"""patent_compare tool implementation.

v2: Limit shared_cpc/unique_cpc to top 10, add totals,
add note for unresolved or zero-patent firms.
"""
from __future__ import annotations

from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver


def patent_compare(
    store: PatentStore,
    resolver: EntityResolver,
    firms: list[str],
    cpc_prefix: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict[str, Any]:
    """Compare patent portfolios across firms."""
    if not firms:
        return {
            "firms": [],
            "shared_cpc": [],
            "shared_cpc_total": 0,
            "unique_cpc": {},
        }

    date_from_int = int(date_from.replace("-", "")) if date_from else None
    date_to_int = int(date_to.replace("-", "")) if date_to else None

    firm_rows = []
    unresolved = []
    zero_patent_firms = []
    cpc_sets: dict[str, set[str]] = {}
    cluster_sets: dict[str, set[str]] = {}

    for firm in firms:
        resolved = resolver.resolve(firm, country_hint="JP")
        if resolved is None:
            unresolved.append(firm)
            continue

        entity = resolved.entity
        portfolio = store.get_firm_portfolio(
            firm_id=entity.canonical_id,
            date_from=date_from_int,
            date_to=date_to_int,
            cpc_prefix=cpc_prefix,
        )

        patent_count = portfolio["count"]
        if patent_count == 0:
            zero_patent_firms.append(entity.canonical_name)

        firm_rows.append(
            {
                "firm_id": entity.canonical_id,
                "name": entity.canonical_name,
                "patent_count": patent_count,
                "cpc_distribution": portfolio["cpc_distribution"][:20],
                "filing_trend": portfolio["filing_trend"],
            }
        )
        cpc_sets[entity.canonical_id] = {
            row["code"] for row in portfolio["cpc_distribution"]
        }

    if unresolved:
        result = {
            "error": "Could not resolve one or more firms",
            "unresolved": unresolved,
            "resolved_count": len(firm_rows),
        }
        if firm_rows:
            result["firms"] = firm_rows
        return result

    # Compute shared CPC
    if not cpc_sets:
        shared_cpc_full = []
    else:
        shared_cpc_full = sorted(set.intersection(*cpc_sets.values()))

    # Compute unique CPC per firm
    all_firm_ids = list(cpc_sets.keys())
    unique_cpc_full: dict[str, list[str]] = {}
    for firm_id in all_firm_ids:
        others = [cpc_sets[fid] for fid in all_firm_ids if fid != firm_id]
        other_union = set().union(*others) if others else set()
        unique_cpc_full[firm_id] = sorted(cpc_sets[firm_id] - other_union)

    # Limit to top 10 with totals
    _TOP_LIMIT = 10
    shared_cpc_limited = shared_cpc_full[:_TOP_LIMIT]
    unique_cpc_limited: dict[str, list[str]] = {}
    unique_cpc_totals: dict[str, int] = {}
    for fid, cpc_list in unique_cpc_full.items():
        unique_cpc_limited[fid] = cpc_list[:_TOP_LIMIT]
        unique_cpc_totals[fid] = len(cpc_list)

    result = {
        "firms": firm_rows,
        "shared_cpc": shared_cpc_limited,
        "shared_cpc_total": len(shared_cpc_full),
        "unique_cpc": unique_cpc_limited,
        "unique_cpc_totals": unique_cpc_totals,
    }

    # Notes for zero-patent firms
    if zero_patent_firms:
        result["note"] = (
            f"以下の企業は現在のデータベースでの特許件数が0件です: "
            f"{', '.join(zero_patent_firms)}。"
            "グローバルデータ拡充後に対応予定です。"
        )

    return result
