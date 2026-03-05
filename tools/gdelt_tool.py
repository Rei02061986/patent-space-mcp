"""gdelt_company_events tool implementation."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver

try:
    from sources.gdelt_bigquery import GDELTBigQuerySource
except ImportError:
    GDELTBigQuerySource = None  # type: ignore[assignment,misc]


def _yyyymmdd_to_iso(value: int) -> str:
    text = str(value)
    if len(text) != 8 or not text.isdigit():
        raise ValueError("date must be YYYYMMDD")
    return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"


def _default_date_range() -> tuple[str, str]:
    today = date.today()
    return (today - timedelta(days=90)).isoformat(), today.isoformat()


def gdelt_company_events(
    store: PatentStore,
    resolver: EntityResolver,
    firm_query: str,
    date_from: int | None = None,
    date_to: int | None = None,
) -> dict[str, Any]:
    """Fetch GDELT event/GKG records and cached five-axis features for a firm."""
    resolved = resolver.resolve(firm_query, country_hint="JP")
    if resolved is None:
        return {
            "error": f"Could not resolve firm: '{firm_query}'",
            "suggestion": "Try the exact company name, Japanese name, or stock ticker",
        }

    entity = resolved.entity
    firm_id = entity.canonical_id

    if date_from is None and date_to is None:
        date_from_iso, date_to_iso = _default_date_range()
    else:
        if date_to is None:
            date_to_iso = date.today().isoformat()
        else:
            date_to_iso = _yyyymmdd_to_iso(date_to)
        if date_from is None:
            end_dt = date.fromisoformat(date_to_iso)
            date_from_iso = (end_dt - timedelta(days=90)).isoformat()
        else:
            date_from_iso = _yyyymmdd_to_iso(date_from)

    aliases = sorted({entity.canonical_name, *entity.aliases})

    events: list[dict[str, Any]] = []
    gkg_records: list[dict[str, Any]] = []
    query_errors: list[str] = []

    try:
        if GDELTBigQuerySource is None:
            raise ImportError("google-cloud-bigquery not installed")
        source = GDELTBigQuerySource()
        for name in aliases:
            if not name.strip():
                continue
            try:
                gkg_records.extend(
                    source.query_gkg_for_company(name, date_from_iso, date_to_iso)
                )
                events.extend(
                    source.query_events_for_company(name, date_from_iso, date_to_iso)
                )
            except Exception as exc:
                query_errors.append(f"{name}: {exc}")
    except Exception as exc:
        query_errors.append(f"GDELT source init failed: {exc}")

    events_sorted = sorted(events, key=lambda r: (r.get("SQLDATE") or 0), reverse=True)

    with store._conn() as conn:
        feature_row = conn.execute(
            """
            SELECT year, quarter, direction_score, openness_score, investment_score,
                   governance_friction_score, leadership_score,
                   total_mentions, total_sources
            FROM gdelt_company_features
            WHERE firm_id = ?
            ORDER BY year DESC, quarter DESC
            LIMIT 1
            """,
            (firm_id,),
        ).fetchone()

    five_axis_features = None
    if feature_row is not None:
        five_axis_features = {
            "year": feature_row["year"],
            "quarter": feature_row["quarter"],
            "direction_score": feature_row["direction_score"],
            "openness_score": feature_row["openness_score"],
            "investment_score": feature_row["investment_score"],
            "governance_friction_score": feature_row["governance_friction_score"],
            "leadership_score": feature_row["leadership_score"],
            "total_mentions": feature_row["total_mentions"],
            "total_sources": feature_row["total_sources"],
        }

    response = {
        "firm_id": firm_id,
        "date_from": date_from_iso,
        "date_to": date_to_iso,
        "events_count": len(events),
        "gkg_count": len(gkg_records),
        "five_axis_features": five_axis_features,
        "recent_events": events_sorted[:10],
    }
    if query_errors:
        response["warnings"] = query_errors[:10]
    return response
