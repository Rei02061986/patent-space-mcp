"""Google Patents BigQuery data source."""
from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from google.cloud import bigquery

from .base import BaseSource

load_dotenv()

PUBLICATIONS_TABLE = "patents-public-data.patents.publications"


class BigQuerySource(BaseSource):
    def __init__(self, project: str | None = None):
        self.project = project or os.getenv(
            "BIGQUERY_PROJECT", "unique-sentinel-473401-s0"
        )
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if creds_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
        self.client = bigquery.Client(project=self.project)

    def test_connection(self) -> dict[str, Any]:
        """Verify BigQuery access and count JP patents."""
        query = """
        SELECT COUNT(*) as total_count
        FROM `patents-public-data.patents.publications`
        WHERE country_code = 'JP'
        """
        result = self.client.query(query).result()
        row = next(iter(result))
        return {"status": "ok", "jp_patent_count": row.total_count}

    def sample_patent(self, country_code: str = "JP") -> dict[str, Any] | None:
        """Fetch a single patent record to inspect schema."""
        query = f"""
        SELECT
            p.publication_number,
            p.application_number,
            p.family_id,
            p.country_code,
            p.kind_code,
            p.filing_date,
            p.publication_date,
            p.grant_date,
            p.entity_status,
            (SELECT t.text FROM UNNEST(p.title_localized) t
             WHERE t.language = 'ja' LIMIT 1) as title_ja,
            (SELECT t.text FROM UNNEST(p.title_localized) t
             WHERE t.language = 'en' LIMIT 1) as title_en,
            (SELECT a.text FROM UNNEST(p.abstract_localized) a
             WHERE a.language = 'ja' LIMIT 1) as abstract_ja,
            (SELECT a.text FROM UNNEST(p.abstract_localized) a
             WHERE a.language = 'en' LIMIT 1) as abstract_en,
            ARRAY(SELECT AS STRUCT c.code, c.inventive, c.first
                  FROM UNNEST(p.cpc) c) as cpc_codes,
            ARRAY(SELECT AS STRUCT a.name, a.country_code
                  FROM UNNEST(p.assignee_harmonized) a) as assignees,
            ARRAY(SELECT AS STRUCT i.name, i.country_code
                  FROM UNNEST(p.inventor_harmonized) i) as inventors
        FROM `{PUBLICATIONS_TABLE}` p
        WHERE p.country_code = @country_code
          AND p.publication_date > 20200101
        LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "country_code", "STRING", country_code
                ),
            ]
        )
        result = self.client.query(query, job_config=job_config).result()
        for row in result:
            return dict(row)
        return None

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
        """Search patents via BigQuery (for ad-hoc queries, not bulk ingestion)."""
        conditions = []
        params = []

        if jurisdiction:
            conditions.append("p.country_code = @jurisdiction")
            params.append(
                bigquery.ScalarQueryParameter("jurisdiction", "STRING", jurisdiction)
            )

        if date_from:
            conditions.append("p.publication_date >= @date_from")
            params.append(
                bigquery.ScalarQueryParameter("date_from", "INT64", date_from)
            )

        if date_to:
            conditions.append("p.publication_date <= @date_to")
            params.append(
                bigquery.ScalarQueryParameter("date_to", "INT64", date_to)
            )

        if applicant:
            conditions.append(
                "EXISTS (SELECT 1 FROM UNNEST(p.assignee_harmonized) a "
                "WHERE LOWER(a.name) LIKE @applicant)"
            )
            params.append(
                bigquery.ScalarQueryParameter(
                    "applicant", "STRING", f"%{applicant.lower()}%"
                )
            )

        if cpc_codes:
            conditions.append(
                "EXISTS (SELECT 1 FROM UNNEST(p.cpc) c "
                "WHERE STARTS_WITH(c.code, @cpc_prefix))"
            )
            params.append(
                bigquery.ScalarQueryParameter("cpc_prefix", "STRING", cpc_codes[0])
            )

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        sql = f"""
        SELECT
            p.publication_number,
            p.family_id,
            p.country_code,
            p.kind_code,
            p.filing_date,
            p.publication_date,
            p.grant_date,
            p.entity_status,
            (SELECT t.text FROM UNNEST(p.title_localized) t
             WHERE t.language = 'ja' LIMIT 1) as title_ja,
            (SELECT t.text FROM UNNEST(p.title_localized) t
             WHERE t.language = 'en' LIMIT 1) as title_en,
            (SELECT a.text FROM UNNEST(p.abstract_localized) a
             WHERE a.language = 'ja' LIMIT 1) as abstract_ja,
            ARRAY(SELECT AS STRUCT c.code, c.inventive, c.first
                  FROM UNNEST(p.cpc) c) as cpc_codes,
            ARRAY(SELECT AS STRUCT a.name, a.country_code
                  FROM UNNEST(p.assignee_harmonized) a) as assignees
        FROM `{PUBLICATIONS_TABLE}` p
        {where}
        ORDER BY p.publication_date DESC
        LIMIT @max_results
        """
        params.append(
            bigquery.ScalarQueryParameter("max_results", "INT64", max_results)
        )

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        result = self.client.query(sql, job_config=job_config).result()
        return [dict(row) for row in result]

    def get_applicant_patents(
        self,
        applicant_names: list[str],
        jurisdiction: str | None = None,
        date_from: int | None = None,
        date_to: int | None = None,
    ) -> list[dict[str, Any]]:
        """Get all patents for given applicant names."""
        return self.search_patents(
            applicant=applicant_names[0] if applicant_names else None,
            jurisdiction=jurisdiction,
            date_from=date_from,
            date_to=date_to,
            max_results=100,
        )
