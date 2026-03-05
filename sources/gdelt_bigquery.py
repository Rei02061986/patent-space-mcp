"""GDELT BigQuery source for company-level feature extraction."""
from __future__ import annotations

import json
import os
from datetime import date
from typing import Any

from dotenv import load_dotenv
from google.cloud import bigquery

from .base import BaseSource
from .bigquery import BigQuerySource

load_dotenv()

GKG_TABLE = "gdelt-bq.gdeltv2.gkg_partitioned"
EVENTS_TABLE = "gdelt-bq.gdeltv2.events"

_DIRECTION_THEMES = (
    "NEW_PRODUCT",
    "EXPANSION",
    "STRATEGY",
    "ENV_GREEN",
    "ECON_ENTREPRENEURSHIP",
)

_INVESTMENT_THEMES = (
    "INVESTMENT",
    "ACQUISITION",
    "MERGER",
    "ECON_DEBT",
)

_GOVERNANCE_THEMES = (
    "SCANDAL",
    "LAWSUIT",
    "CRISISLEX",
)


class GDELTBigQuerySource(BaseSource):
    def __init__(self, project: str | None = None, client: bigquery.Client | None = None):
        self.project = project or os.getenv("BIGQUERY_PROJECT", "unique-sentinel-473401-s0")

        if client is not None:
            self.client = client
            return

        # Reuse the existing source configuration when available.
        try:
            self.client = BigQuerySource(project=self.project).client
        except Exception:
            creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if creds_path:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
            self.client = bigquery.Client(project=self.project)

    def query_gkg_for_company(
        self,
        company_name: str,
        date_from: str,
        date_to: str,
    ) -> list[dict[str, Any]]:
        query = f"""
        SELECT
            DATE,
            V2Organizations,
            V2Persons,
            V2Themes,
            V2Tone,
            DocumentIdentifier
        FROM `{GKG_TABLE}`
        WHERE _PARTITIONTIME >= TIMESTAMP(@date_from)
          AND _PARTITIONTIME < TIMESTAMP(@date_to)
          AND LOWER(V2Organizations) LIKE @company_pattern
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date_from", "STRING", date_from),
                bigquery.ScalarQueryParameter("date_to", "STRING", date_to),
                bigquery.ScalarQueryParameter(
                    "company_pattern", "STRING", f"%{company_name.lower()}%"
                ),
            ]
        )

        rows = self.client.query(query, job_config=job_config).result()
        parsed: list[dict[str, Any]] = []
        for row in rows:
            row_dict = dict(row)
            parsed.append(
                {
                    "DATE": row_dict.get("DATE"),
                    "DocumentIdentifier": row_dict.get("DocumentIdentifier"),
                    "V2Organizations": parse_v2_organizations(
                        row_dict.get("V2Organizations")
                    ),
                    "V2Persons": _parse_names_with_offsets(row_dict.get("V2Persons")),
                    "V2Themes": parse_v2_themes(row_dict.get("V2Themes")),
                    "V2Tone": parse_v2_tone(row_dict.get("V2Tone")),
                }
            )
        return parsed

    def query_events_for_company(
        self,
        company_name: str,
        date_from: str,
        date_to: str,
    ) -> list[dict[str, Any]]:
        date_from_int = _date_to_yyyymmdd(date_from)
        date_to_int = _date_to_yyyymmdd(date_to)

        query = f"""
        SELECT
            SQLDATE,
            Actor1Name,
            Actor2Name,
            EventCode,
            QuadClass,
            GoldsteinScale,
            NumMentions,
            AvgTone,
            SOURCEURL
        FROM `{EVENTS_TABLE}`
        WHERE SQLDATE >= @date_from_int
          AND SQLDATE < @date_to_int
          AND (
            LOWER(Actor1Name) LIKE @company_pattern
            OR LOWER(Actor2Name) LIKE @company_pattern
          )
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date_from_int", "INT64", date_from_int),
                bigquery.ScalarQueryParameter("date_to_int", "INT64", date_to_int),
                bigquery.ScalarQueryParameter(
                    "company_pattern", "STRING", f"%{company_name.lower()}%"
                ),
            ]
        )

        rows = self.client.query(query, job_config=job_config).result()
        return [dict(row) for row in rows]

    def search_patents(
        self,
        query: str | None = None,
        cpc_codes: list[str] | None = None,
        applicant: str | None = None,
        jurisdiction: str | None = None,
        date_from: int | None = None,
        date_to: int | None = None,
        max_results: int = 20,
    ) -> list[dict[str, Any]]:
        company_name = applicant or query
        if not company_name:
            return []

        date_from_text = _coerce_to_iso_date(date_from) or date.today().replace(
            month=1, day=1
        ).isoformat()
        date_to_text = _coerce_to_iso_date(date_to) or date.today().isoformat()
        return self.query_events_for_company(
            company_name=company_name,
            date_from=date_from_text,
            date_to=date_to_text,
        )[:max_results]

    def get_applicant_patents(
        self,
        applicant_names: list[str],
        jurisdiction: str | None = None,
        date_from: int | None = None,
        date_to: int | None = None,
    ) -> list[dict[str, Any]]:
        if not applicant_names:
            return []
        return self.search_patents(
            applicant=applicant_names[0],
            jurisdiction=jurisdiction,
            date_from=date_from,
            date_to=date_to,
            max_results=100,
        )

    def compute_five_axis_features(
        self,
        firm_id: str,
        company_names: list[str],
        year: int,
        quarter: int,
    ) -> dict[str, Any]:
        if quarter not in (1, 2, 3, 4):
            raise ValueError("quarter must be in [1, 2, 3, 4]")

        date_from, date_to = _quarter_date_range(year=year, quarter=quarter)

        gkg_records: list[dict[str, Any]] = []
        event_records: list[dict[str, Any]] = []

        for name in company_names:
            if not name or not name.strip():
                continue
            gkg_records.extend(self.query_gkg_for_company(name.strip(), date_from, date_to))
            event_records.extend(
                self.query_events_for_company(name.strip(), date_from, date_to)
            )

        direction_count = _count_theme_matches(gkg_records, _DIRECTION_THEMES)

        coop_events = sum(1 for event in event_records if event.get("QuadClass") in (1, 2))
        org_co_occurrence = sum(
            max(len(record.get("V2Organizations", [])) - 1, 0) for record in gkg_records
        )
        openness_count = coop_events + org_co_occurrence

        investment_theme_count = _count_theme_matches(gkg_records, _INVESTMENT_THEMES)
        high_goldstein_count = sum(
            1
            for event in event_records
            if _safe_float(event.get("GoldsteinScale"), 0.0) > 5.0
        )
        investment_count = investment_theme_count + high_goldstein_count

        governance_theme_count = _count_theme_matches(gkg_records, _GOVERNANCE_THEMES)
        governance_event_count = sum(
            1 for event in event_records if event.get("QuadClass") in (3, 4)
        )
        negative_goldstein_count = sum(
            1
            for event in event_records
            if _safe_float(event.get("GoldsteinScale"), 0.0) < -5.0
        )
        governance_count = (
            governance_theme_count + governance_event_count + negative_goldstein_count
        )

        persons_mention_count = sum(len(record.get("V2Persons", [])) for record in gkg_records)
        tones_with_persons = [
            _safe_float(record.get("V2Tone", {}).get("avgTone"), None)
            for record in gkg_records
            if record.get("V2Persons")
        ]
        tones_with_persons = [tone for tone in tones_with_persons if tone is not None]
        leadership_tone_avg = (
            sum(tones_with_persons) / len(tones_with_persons) if tones_with_persons else 0.0
        )
        leadership_count = persons_mention_count + max(leadership_tone_avg, 0.0)

        total_mentions = len(gkg_records) + sum(
            _safe_int(event.get("NumMentions"), 0) for event in event_records
        )
        total_sources = len(
            {
                record.get("DocumentIdentifier")
                for record in gkg_records
                if record.get("DocumentIdentifier")
            }
            | {event.get("SOURCEURL") for event in event_records if event.get("SOURCEURL")}
        )

        result = {
            "firm_id": firm_id,
            "year": year,
            "quarter": quarter,
            "direction_score": _normalize_count(direction_count),
            "openness_score": _normalize_count(openness_count),
            "investment_score": _normalize_count(investment_count),
            "governance_friction_score": _normalize_count(governance_count),
            "leadership_score": _normalize_count(leadership_count),
            "total_mentions": total_mentions,
            "total_sources": total_sources,
            "raw_data": json.dumps(
                {
                    "date_from": date_from,
                    "date_to": date_to,
                    "company_names": company_names,
                    "gkg_records": len(gkg_records),
                    "event_records": len(event_records),
                    "direction_count": direction_count,
                    "openness_count": openness_count,
                    "investment_count": investment_count,
                    "governance_count": governance_count,
                    "leadership_mentions": persons_mention_count,
                    "leadership_tone_avg": leadership_tone_avg,
                }
            ),
        }
        return result


def parse_v2_organizations(field: str | None) -> list[str]:
    return _parse_names_with_offsets(field)


def parse_v2_themes(field: str | None) -> list[str]:
    return _parse_names_with_offsets(field)


def parse_v2_tone(field: str | None) -> dict[str, float | int | None]:
    keys = [
        "avgTone",
        "posScore",
        "negScore",
        "polarity",
        "activityDensity",
        "selfGroupDensity",
        "wordCount",
    ]
    default: dict[str, float | int | None] = {
        "avgTone": None,
        "posScore": None,
        "negScore": None,
        "polarity": None,
        "activityDensity": None,
        "selfGroupDensity": None,
        "wordCount": None,
    }

    if not field:
        return default

    parts = [part.strip() for part in field.split(",")]
    for idx, key in enumerate(keys):
        if idx >= len(parts) or parts[idx] == "":
            continue
        if key == "wordCount":
            default[key] = _safe_int(parts[idx], None)
        else:
            default[key] = _safe_float(parts[idx], None)
    return default


def _parse_names_with_offsets(field: str | None) -> list[str]:
    if not field:
        return []

    values: list[str] = []
    for raw_part in field.split(";"):
        part = raw_part.strip()
        if not part:
            continue
        name, _, _offset = part.rpartition(",")
        extracted = (name if name else part).strip()
        if extracted:
            values.append(extracted)
    return values


def _count_theme_matches(records: list[dict[str, Any]], terms: tuple[str, ...]) -> int:
    total = 0
    for record in records:
        for theme in record.get("V2Themes", []):
            if any(term in theme for term in terms):
                total += 1
    return total


def _normalize_count(value: float, k: float = 10.0) -> float:
    v = max(float(value), 0.0)
    return v / (v + k)


def _date_to_yyyymmdd(date_text: str) -> int:
    parsed = date.fromisoformat(date_text)
    return int(parsed.strftime("%Y%m%d"))


def _coerce_to_iso_date(value: int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    return None


def _quarter_date_range(year: int, quarter: int) -> tuple[str, str]:
    start_month = (quarter - 1) * 3 + 1
    start_date = date(year, start_month, 1)
    if quarter == 4:
        end_date = date(year + 1, 1, 1)
    else:
        end_date = date(year, start_month + 3, 1)
    return start_date.isoformat(), end_date.isoformat()


def _safe_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int | None = 0) -> int | None:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default
