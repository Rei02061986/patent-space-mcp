"""Unified patent schema definitions and BigQuery row normalization."""
from __future__ import annotations

from typing import Any, TypedDict, NotRequired


class NormalizedEntity(TypedDict):
    raw_name: str
    harmonized_name: str
    firm_id: NotRequired[str | None]
    country_code: NotRequired[str | None]


class UnifiedPatent(TypedDict):
    publication_number: str
    application_number: NotRequired[str | None]
    family_id: NotRequired[str | None]
    country_code: str
    kind_code: NotRequired[str | None]

    title_ja: NotRequired[str | None]
    title_en: NotRequired[str | None]
    abstract_ja: NotRequired[str | None]
    abstract_en: NotRequired[str | None]

    cpc_codes: list[str]
    cpc_primary: NotRequired[str | None]
    ipc_codes: NotRequired[list[str]]

    applicants: list[NormalizedEntity]
    raw_assignees: NotRequired[list[str]]
    inventors: NotRequired[list[str]]

    filing_date: NotRequired[int | None]
    publication_date: NotRequired[int | None]
    grant_date: NotRequired[int | None]

    citations_backward: NotRequired[list[str]]
    citation_count_forward: NotRequired[int]

    entity_status: NotRequired[str | None]
    source: str


def normalize_bigquery_row(row: dict[str, Any]) -> dict[str, Any]:
    """Convert a BigQuery result row to UnifiedPatent-compatible dict.

    Handles both the ingestion query format (with nested arrays)
    and the raw BigQuery Row object format.
    """
    # CPC codes
    cpc_raw = row.get("cpc_codes") or row.get("cpc") or []
    cpc_codes = []
    cpc_primary = None
    for c in cpc_raw:
        code = c.get("code") if isinstance(c, dict) else c
        if code and code not in cpc_codes:
            cpc_codes.append(code)
        if isinstance(c, dict) and c.get("first") and cpc_primary is None:
            cpc_primary = code
    if cpc_codes and cpc_primary is None:
        cpc_primary = cpc_codes[0]

    # Assignees from harmonized data
    assignees_raw = row.get("assignees") or row.get("assignee_harmonized") or []
    applicants = []
    for a in assignees_raw:
        if isinstance(a, dict):
            applicants.append(
                NormalizedEntity(
                    raw_name=a.get("name", ""),
                    harmonized_name=a.get("name", ""),
                    firm_id=None,
                    country_code=a.get("country_code"),
                )
            )

    # Raw assignee names (Japanese + English)
    raw_assignees = row.get("assignee") or row.get("raw_assignees") or []
    if isinstance(raw_assignees, str):
        raw_assignees = [raw_assignees]

    # Inventors
    inventors_raw = row.get("inventors") or row.get("inventor_harmonized") or []
    inventors = []
    for i in inventors_raw:
        if isinstance(i, dict):
            inventors.append(i.get("name", ""))
        elif isinstance(i, str):
            inventors.append(i)

    # Citations
    citations_raw = row.get("citations") or row.get("citation") or []
    citations_backward = []
    for c in citations_raw:
        if isinstance(c, dict):
            pub = c.get("publication_number")
            if pub:
                citations_backward.append(pub)

    return {
        "publication_number": row.get("publication_number", ""),
        "application_number": row.get("application_number"),
        "family_id": row.get("family_id"),
        "country_code": row.get("country_code", ""),
        "kind_code": row.get("kind_code"),
        "title_ja": row.get("title_ja"),
        "title_en": row.get("title_en"),
        "abstract_ja": row.get("abstract_ja"),
        "abstract_en": row.get("abstract_en"),
        "cpc_codes": cpc_codes,
        "cpc_primary": cpc_primary,
        "applicants": applicants,
        "raw_assignees": raw_assignees,
        "inventors": inventors,
        "filing_date": row.get("filing_date"),
        "publication_date": row.get("publication_date"),
        "grant_date": row.get("grant_date"),
        "citations_backward": citations_backward,
        "entity_status": row.get("entity_status"),
        "source": row.get("source", "bigquery"),
    }
