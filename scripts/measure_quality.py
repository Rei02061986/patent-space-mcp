"""Measure firm_id name-matching quality on the patent SQLite database."""
from __future__ import annotations

import argparse
import random
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from entity.data.tse_prime_seed import TSE_PRIME_ENTITIES
from entity.resolver import fuzzy_score, normalize


FUZZY_DIFF_THRESHOLD = 65
FALSE_POSITIVE_SAMPLE_SIZE = 100
TOP_LINKED_FIRM_LIMIT = 20
TOP_UNLINKED_ASSIGNEE_LIMIT = 30


@dataclass
class CoverageMetric:
    linked: int
    total: int

    @property
    def pct(self) -> float:
        if self.total == 0:
            return 0.0
        return (self.linked / self.total) * 100


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def metric_entity_coverage(conn: sqlite3.Connection) -> tuple[CoverageMetric, list[str]]:
    entity_ids = {e.canonical_id for e in TSE_PRIME_ENTITIES}
    rows = conn.execute(
        "SELECT DISTINCT firm_id FROM patent_assignees WHERE firm_id IS NOT NULL"
    ).fetchall()
    linked_ids = {r["firm_id"] for r in rows if r["firm_id"] in entity_ids}
    missing = sorted(entity_ids - linked_ids)
    return CoverageMetric(linked=len(linked_ids), total=len(entity_ids)), missing


def metric_patent_coverage(conn: sqlite3.Connection) -> CoverageMetric:
    total_patents = conn.execute("SELECT COUNT(*) AS n FROM patents").fetchone()["n"]
    covered_patents = conn.execute(
        """
        SELECT COUNT(DISTINCT publication_number) AS n
        FROM patent_assignees
        WHERE firm_id IS NOT NULL
        """
    ).fetchone()["n"]
    return CoverageMetric(linked=covered_patents, total=total_patents)


def metric_match_distribution(
    conn: sqlite3.Connection, canonical_name_by_id: dict[str, str]
) -> list[sqlite3.Row]:
    rows = conn.execute(
        f"""
        SELECT firm_id,
               COUNT(*) AS assignee_rows,
               COUNT(DISTINCT publication_number) AS patent_count
        FROM patent_assignees
        WHERE firm_id IS NOT NULL
        GROUP BY firm_id
        ORDER BY assignee_rows DESC, patent_count DESC
        LIMIT {TOP_LINKED_FIRM_LIMIT}
        """
    ).fetchall()
    return rows


def metric_false_positive_sample(
    conn: sqlite3.Connection, canonical_name_by_id: dict[str, str]
) -> tuple[list[dict[str, object]], int, int]:
    sample: list[dict[str, object]] = []
    seen_candidates = 0
    scanned_rows = 0

    cursor = conn.execute(
        """
        SELECT harmonized_name,
               firm_id,
               COUNT(*) AS assignee_rows,
               COUNT(DISTINCT publication_number) AS patent_count
        FROM patent_assignees
        WHERE firm_id IS NOT NULL
          AND harmonized_name IS NOT NULL
          AND TRIM(harmonized_name) != ''
        GROUP BY harmonized_name, firm_id
        """
    )

    for row in cursor:
        scanned_rows += 1
        firm_id = row["firm_id"]
        canonical_name = canonical_name_by_id.get(firm_id)
        if not canonical_name:
            continue

        assignee_name = row["harmonized_name"]
        score = fuzzy_score(normalize(assignee_name), normalize(canonical_name))
        if score > FUZZY_DIFF_THRESHOLD:
            continue

        seen_candidates += 1
        record = {
            "similarity_score": score,
            "firm_id": firm_id,
            "canonical_name": canonical_name,
            "assignee_name": assignee_name,
            "assignee_rows": row["assignee_rows"],
            "patent_count": row["patent_count"],
        }

        if len(sample) < FALSE_POSITIVE_SAMPLE_SIZE:
            sample.append(record)
            continue

        idx = random.randint(0, seen_candidates - 1)
        if idx < FALSE_POSITIVE_SAMPLE_SIZE:
            sample[idx] = record

    sample.sort(
        key=lambda x: (
            int(x["similarity_score"]),
            -int(x["patent_count"]),
            str(x["assignee_name"]),
        )
    )
    return sample, seen_candidates, scanned_rows


def metric_top_unlinked_assignees(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    rows = conn.execute(
        f"""
        SELECT name,
               COUNT(DISTINCT publication_number) AS patent_count,
               COUNT(*) AS assignee_rows
        FROM (
            SELECT publication_number,
                   COALESCE(NULLIF(TRIM(harmonized_name), ''), raw_name) AS name,
                   firm_id
            FROM patent_assignees
        ) x
        WHERE firm_id IS NULL
          AND name IS NOT NULL
          AND TRIM(name) != ''
        GROUP BY name
        ORDER BY patent_count DESC, assignee_rows DESC, name ASC
        LIMIT {TOP_UNLINKED_ASSIGNEE_LIMIT}
        """
    ).fetchall()
    return rows


def _fmt_ratio(metric: CoverageMetric) -> str:
    return f"{metric.linked:,}/{metric.total:,} ({metric.pct:.1f}%)"


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure name-matching quality metrics")
    parser.add_argument(
        "--db",
        default="data/patents.db",
        help="Path to SQLite DB (default: data/patents.db)",
    )
    args = parser.parse_args()

    canonical_name_by_id = {
        entity.canonical_id: entity.canonical_name for entity in TSE_PRIME_ENTITIES
    }

    conn = _connect(args.db)
    try:
        entity_coverage, missing_entity_ids = metric_entity_coverage(conn)
        patent_coverage = metric_patent_coverage(conn)
        top_linked = metric_match_distribution(conn, canonical_name_by_id)
        fuzzy_sample, fuzzy_candidates, fuzzy_scanned = metric_false_positive_sample(
            conn, canonical_name_by_id
        )
        top_unlinked = metric_top_unlinked_assignees(conn)
    finally:
        conn.close()

    print("=" * 78)
    print("Patent Name-Matching Quality Report")
    print("=" * 78)
    print(f"Database: {args.db}")
    print("")

    print("[1] Entity Coverage Rate (Target: 80%+)")
    print(f"  Linked entities: {_fmt_ratio(entity_coverage)}")
    print(
        "  Status: "
        + ("PASS" if entity_coverage.pct >= 80.0 else "BELOW TARGET")
    )
    if missing_entity_ids:
        print(f"  Unlinked entity IDs ({len(missing_entity_ids)}):")
        print("   " + ", ".join(missing_entity_ids))
    print("")

    print("[2] Patent Coverage Rate (Target: 40%+)")
    print(f"  Patents covered by at least one firm_id: {_fmt_ratio(patent_coverage)}")
    print(
        "  Status: "
        + ("PASS" if patent_coverage.pct >= 40.0 else "BELOW TARGET")
    )
    print("")

    print("[3] Linked Assignee Distribution (Top 20 firm_id by assignee rows)")
    print("  Note: match_level is not persisted; using firm_id distribution as proxy.")
    print("  firm_id | canonical_name | assignee_rows | patent_count")
    for row in top_linked:
        firm_id = row["firm_id"]
        canonical = canonical_name_by_id.get(firm_id, "<unknown>")
        print(
            f"  {firm_id} | {canonical} | {row['assignee_rows']:,} | {row['patent_count']:,}"
        )
    print("")

    print("[4] False Positive Estimation (Manual Review Sample)")
    print(
        f"  Criteria: similarity_score <= {FUZZY_DIFF_THRESHOLD} "
        "(normalized assignee vs canonical name)"
    )
    print(
        f"  Candidate fuzzy-like pairs: {fuzzy_candidates:,} "
        f"(scanned grouped pairs: {fuzzy_scanned:,})"
    )
    print(
        f"  Random sample shown: {len(fuzzy_sample):,}/{FALSE_POSITIVE_SAMPLE_SIZE:,}"
    )
    print(
        "  similarity_score | firm_id | canonical_name | assignee_name | patent_count"
    )
    for row in fuzzy_sample:
        print(
            f"  {row['similarity_score']:>3} | {row['firm_id']} | {row['canonical_name']} "
            f"| {row['assignee_name']} | {int(row['patent_count']):,}"
        )
    print("")

    print("[5] Top Unlinked Assignees (Top 30 by patent count)")
    print("  assignee_name | patent_count | assignee_rows")
    for row in top_unlinked:
        print(f"  {row['name']} | {row['patent_count']:,} | {row['assignee_rows']:,}")
    print("")
    print("=" * 78)


if __name__ == "__main__":
    main()
