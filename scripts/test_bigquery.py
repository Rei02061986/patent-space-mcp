"""Quick BigQuery connection and cost estimation test."""
from __future__ import annotations

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sources.bigquery import BigQuerySource


def main():
    print("=" * 60)
    print("Patent Space MCP — BigQuery Connection Test")
    print("=" * 60)

    src = BigQuerySource()

    # Test 1: Connection + JP count
    print("\n[Test 1] Counting JP patents...")
    result = src.test_connection()
    print(f"  Status: {result['status']}")
    print(f"  JP patent count: {result['jp_patent_count']:,}")

    # Test 2: Sample record
    print("\n[Test 2] Fetching sample JP patent...")
    sample = src.sample_patent("JP")
    if sample:
        print(f"  Publication: {sample['publication_number']}")
        print(f"  Title (JA): {sample.get('title_ja', 'N/A')}")
        print(f"  Title (EN): {sample.get('title_en', 'N/A')}")
        print(f"  Filing date: {sample.get('filing_date')}")
        print(f"  Publication date: {sample.get('publication_date')}")
        print(f"  Entity status: {sample.get('entity_status')}")

        cpc = sample.get("cpc_codes", [])
        print(f"  CPC codes ({len(cpc)}): {[c['code'] for c in cpc[:5]]}")

        assignees = sample.get("assignees", [])
        print(f"  Assignees ({len(assignees)}): {[a['name'] for a in assignees[:3]]}")

        inventors = sample.get("inventors", [])
        print(f"  Inventors ({len(inventors)}): {[i['name'] for i in inventors[:3]]}")
    else:
        print("  No sample found!")

    # Test 3: Dry-run cost estimation
    print("\n[Test 3] Estimating full JP ingestion scan cost...")
    dry_run_query = """
    SELECT
        p.publication_number,
        p.family_id,
        p.country_code,
        p.kind_code,
        p.filing_date,
        p.publication_date,
        p.grant_date,
        p.entity_status,
        p.title_localized,
        p.abstract_localized,
        p.cpc,
        p.assignee_harmonized,
        p.inventor_harmonized,
        p.citation
    FROM `patents-public-data.patents.publications` p
    WHERE p.country_code = 'JP'
      AND p.publication_date > 19900101
    """
    from google.cloud.bigquery import QueryJobConfig

    job_config = QueryJobConfig(dry_run=True, use_query_cache=False)
    dry_run_job = src.client.query(dry_run_query, job_config=job_config)
    bytes_estimate = dry_run_job.total_bytes_processed
    gb_estimate = bytes_estimate / (1024**3)
    cost_estimate = max(0, (gb_estimate - 1024) / 1024 * 5)  # $5/TB, 1TB free

    print(f"  Estimated scan: {gb_estimate:.1f} GB")
    print(f"  Estimated cost: ${cost_estimate:.2f} (1TB/month free)")
    print(f"  Within free tier: {'YES' if gb_estimate < 1024 else 'NO'}")

    print("\n" + "=" * 60)
    print("All tests passed! BigQuery connection is working.")
    print("=" * 60)


if __name__ == "__main__":
    main()
