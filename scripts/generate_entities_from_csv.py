"""Generate auto-seeded TSE entities from companies_master.csv and patent assignees."""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from entity.data.tse_expanded_seed import TSE_EXPANDED_ENTITIES
from entity.data.tse_prime_seed import TSE_PRIME_ENTITIES
from entity.registry import Entity
from entity.resolver import normalize

TARGET_MARKETS = {"Prime", "Standard", "Growth"}
OUTPUT_PATH = Path("entity/data/tse_auto_seed.py")


@dataclass
class CompanyRow:
    company_id: str
    name_ja: str
    market: str
    industry: str | None
    edinet_code: str | None


@dataclass
class GeneratedEntity:
    company_id: str
    canonical_name: str
    market: str
    industry: str | None
    edinet_code: str | None
    aliases: set[str]



def _nfkc_key(value: str) -> str:
    return unicodedata.normalize("NFKC", value).casefold().strip()



def load_company_rows(csv_path: Path) -> list[CompanyRow]:
    rows: list[CompanyRow] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            market = (row.get("market") or "").strip()
            name_ja = (row.get("name_ja") or "").strip()
            company_id = (row.get("company_id") or "").strip()
            if market not in TARGET_MARKETS or not name_ja or not company_id:
                continue
            industry = (row.get("industry") or "").strip() or None
            edinet_code = (row.get("edinet_code") or "").strip() or None
            rows.append(
                CompanyRow(
                    company_id=company_id,
                    name_ja=name_ja,
                    market=market,
                    industry=industry,
                    edinet_code=edinet_code,
                )
            )
    return rows



def load_assignee_counts(db_path: Path) -> dict[str, int]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT harmonized_name, COUNT(*) AS n
            FROM patent_assignees
            WHERE harmonized_name IS NOT NULL AND TRIM(harmonized_name) != ''
            GROUP BY harmonized_name
            """
        ).fetchall()
    finally:
        conn.close()
    return {str(r["harmonized_name"]): int(r["n"]) for r in rows}



def collect_existing_name_keys() -> tuple[set[str], set[str]]:
    existing_raw: set[str] = set()
    existing_nfkc: set[str] = set()
    for entity in [*TSE_PRIME_ENTITIES, *TSE_EXPANDED_ENTITIES]:
        for name in entity.aliases | {entity.canonical_name}:
            if not name:
                continue
            existing_raw.add(name)
            existing_nfkc.add(_nfkc_key(name))
    return existing_raw, existing_nfkc



def build_assignee_indexes(
    assignee_counts: dict[str, int],
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    by_nfkc: dict[str, set[str]] = defaultdict(set)
    by_normalized: dict[str, set[str]] = defaultdict(set)

    for assignee in assignee_counts:
        by_nfkc[_nfkc_key(assignee)].add(assignee)
        norm = normalize(assignee)
        if norm:
            by_normalized[norm].add(assignee)

    return by_nfkc, by_normalized



def _sort_key_company_id(company_id: str) -> tuple[int, str]:
    if company_id.isdigit():
        return (0, f"{int(company_id):010d}")
    return (1, company_id)



def render_entities_file(entities: list[GeneratedEntity], output_path: Path) -> None:
    lines: list[str] = []
    lines.append('"""Auto-generated TSE entity seed from companies_master.csv."""')
    lines.append("from entity.registry import Entity")
    lines.append("")
    lines.append("TSE_AUTO_ENTITIES: list[Entity] = [")

    for item in sorted(entities, key=lambda x: _sort_key_company_id(x.company_id)):
        aliases = sorted(item.aliases)
        lines.append("    Entity(")
        lines.append(f"        canonical_id={repr(f'company_{item.company_id}')},")
        lines.append(f"        canonical_name={repr(item.canonical_name)},")
        lines.append("        country_code='JP',")
        lines.append("        entity_type='corporation',")
        lines.append("        aliases={")
        for alias in aliases:
            lines.append(f"            {repr(alias)},")
        lines.append("        },")
        if item.industry:
            lines.append(f"        industry={repr(item.industry)},")
        if item.edinet_code:
            lines.append(f"        edinet_code={repr(item.edinet_code)},")
        lines.append(f"        ticker={repr(item.company_id)},")
        lines.append(f"        tse_section={repr(item.market)},")
        lines.append("    ),")

    lines.append("]")
    lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")



def generate(csv_path: Path, db_path: Path, output_path: Path = OUTPUT_PATH) -> None:
    companies = load_company_rows(csv_path)
    assignee_counts = load_assignee_counts(db_path)
    by_nfkc, by_normalized = build_assignee_indexes(assignee_counts)
    existing_raw, existing_nfkc = collect_existing_name_keys()

    generated: list[GeneratedEntity] = []
    unmatched: list[tuple[CompanyRow, int]] = []
    skipped_existing = 0

    for company in companies:
        if (
            company.name_ja in existing_raw
            or _nfkc_key(company.name_ja) in existing_nfkc
        ):
            skipped_existing += 1
            continue

        aliases: set[str] = set()

        if company.name_ja in assignee_counts:
            aliases.add(company.name_ja)

        aliases.update(by_nfkc.get(_nfkc_key(company.name_ja), set()))

        normalized_key = normalize(company.name_ja)
        if normalized_key:
            aliases.update(by_normalized.get(normalized_key, set()))

        if aliases:
            generated.append(
                GeneratedEntity(
                    company_id=company.company_id,
                    canonical_name=company.name_ja,
                    market=company.market,
                    industry=company.industry,
                    edinet_code=company.edinet_code,
                    aliases=aliases,
                )
            )
        else:
            unmatched_count = assignee_counts.get(company.name_ja, 0)
            unmatched.append((company, unmatched_count))

    render_entities_file(generated, output_path)

    print(f"CSV companies in target markets: {len(companies)}")
    print(f"Skipped (already in seed): {skipped_existing}")
    print(f"Matched and generated: {len(generated)}")
    print(f"Unmatched: {len(unmatched)}")
    print(f"Output: {output_path}")

    print("\nTop 30 unmatched companies by patent assignee row count:")
    print("company_id | market | count | name_ja")
    for company, count in sorted(
        unmatched,
        key=lambda x: (-x[1], _sort_key_company_id(x[0].company_id), x[0].name_ja),
    )[:30]:
        print(
            f"{company.company_id} | {company.market} | {count} | {company.name_ja}"
        )



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate JP TSE auto-entities from CSV and patents DB"
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Path to companies_master.csv",
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to patents.db",
    )
    parser.add_argument(
        "--out",
        default=str(OUTPUT_PATH),
        help="Output file path (default: entity/data/tse_auto_seed.py)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    generate(Path(args.csv), Path(args.db), Path(args.out))
