"""Extract GDELT 5-axis features for top N firms × recent quarters.

Stores results in gdelt_company_features table.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.migrations import SCHEMA_SQL

# Mapping from firm_id to GDELT search names
# Only include firms with recognizable English names in GDELT
FIRM_GDELT_NAMES: dict[str, list[str]] = {
    "panasonic": ["PANASONIC"],
    "canon": ["CANON"],
    "toyota": ["TOYOTA", "TOYOTA MOTOR"],
    "toshiba": ["TOSHIBA"],
    "mitsubishi_electric": ["MITSUBISHI ELECTRIC"],
    "hitachi": ["HITACHI"],
    "ricoh": ["RICOH"],
    "fujifilm": ["FUJIFILM", "FUJI FILM"],
    "denso": ["DENSO"],
    "sharp": ["SHARP"],
    "fujitsu": ["FUJITSU"],
    "honda": ["HONDA", "HONDA MOTOR"],
    "seiko_epson": ["SEIKO EPSON", "EPSON"],
    "kyocera": ["KYOCERA"],
    "sony": ["SONY"],
    "ntt": ["NTT", "NIPPON TELEGRAPH"],
    "dnp": ["DAI NIPPON PRINTING"],
    "nissan": ["NISSAN", "NISSAN MOTOR"],
    "konica_minolta": ["KONICA MINOLTA"],
    "nec": ["NEC"],
    "sumitomo_electric": ["SUMITOMO ELECTRIC"],
    "nippon_steel": ["NIPPON STEEL"],
    "toppan": ["TOPPAN"],
    "kao": ["KAO"],
    "jfe": ["JFE"],
    "yazaki": ["YAZAKI"],
    "murata": ["MURATA"],
    "toray": ["TORAY"],
    "bridgestone": ["BRIDGESTONE"],
    "sekisui_chemical": ["SEKISUI CHEMICAL", "SEKISUI"],
    "mitsubishi_heavy": ["MITSUBISHI HEAVY"],
    "sumitomo_chemical": ["SUMITOMO CHEMICAL"],
    "brother": ["BROTHER INDUSTRIES", "BROTHER"],
    "resonac": ["RESONAC", "SHOWA DENKO"],
    "fuji_electric": ["FUJI ELECTRIC"],
    "daikin": ["DAIKIN"],
    "olympus": ["OLYMPUS"],
    "mazda": ["MAZDA"],
    "suzuki": ["SUZUKI", "SUZUKI MOTOR"],
    "subaru": ["SUBARU", "FUJI HEAVY"],
    "asahi_kasei": ["ASAHI KASEI"],
    "nikon": ["NIKON"],
    "omron": ["OMRON"],
    "ihi": ["IHI"],
    "kobe_steel": ["KOBE STEEL"],
    "yaskawa": ["YASKAWA"],
    "kubota": ["KUBOTA"],
    "mitsubishi_chemical": ["MITSUBISHI CHEMICAL"],
    "sumitomo_metal_mining": ["SUMITOMO METAL MINING"],
    "aisin": ["AISIN"],
}


def run_extraction(
    db_path: str = "data/patents.db",
    year_from: int = 2020,
    year_to: int = 2024,
    max_firms: int = 50,
) -> int:
    from sources.gdelt_bigquery import GDELTBigQuerySource

    gdelt = GDELTBigQuerySource()

    conn = sqlite3.connect(db_path, timeout=60)
    conn.executescript(SCHEMA_SQL)
    conn.execute("PRAGMA journal_mode=WAL")

    firms = list(FIRM_GDELT_NAMES.items())[:max_firms]
    quarters = [(y, q) for y in range(year_from, year_to + 1) for q in range(1, 5)]

    total = len(firms) * len(quarters)
    print(f"Extracting GDELT features: {len(firms)} firms × {len(quarters)} quarters = {total} cells")
    sys.stdout.flush()

    inserted = 0
    errors = 0
    start_time = time.time()

    for fi, (firm_id, search_names) in enumerate(firms):
        firm_start = time.time()

        for year, quarter in quarters:
            try:
                features = gdelt.compute_five_axis_features(
                    firm_id=firm_id,
                    company_names=search_names,
                    year=year,
                    quarter=quarter,
                )

                conn.execute(
                    """
                    INSERT OR REPLACE INTO gdelt_company_features (
                        firm_id, year, quarter,
                        direction_score, openness_score, investment_score,
                        governance_friction_score, leadership_score,
                        total_mentions, total_sources, raw_data
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        firm_id,
                        year,
                        quarter,
                        features.get("direction_score"),
                        features.get("openness_score"),
                        features.get("investment_score"),
                        features.get("governance_friction_score"),
                        features.get("leadership_score"),
                        features.get("total_mentions"),
                        features.get("total_sources"),
                        features.get("raw_data") if isinstance(features.get("raw_data"), str) else json.dumps(features.get("raw_data")),
                    ),
                )
                conn.commit()
                inserted += 1
            except Exception as e:
                errors += 1
                print(f"  ERROR {firm_id} {year}Q{quarter}: {e}")
                sys.stdout.flush()

        firm_elapsed = time.time() - firm_start
        total_elapsed = time.time() - start_time
        print(
            f"  [{fi+1}/{len(firms)}] {firm_id}: {len(quarters)} quarters in {firm_elapsed:.1f}s "
            f"(total: {inserted} inserted, {errors} errors, {total_elapsed:.0f}s)"
        )
        sys.stdout.flush()

    conn.close()
    elapsed = time.time() - start_time

    print(f"\nGDELT extraction complete.")
    print(f"Inserted: {inserted}")
    print(f"Errors:   {errors}")
    print(f"Elapsed:  {elapsed:.1f}s ({elapsed/60:.1f}m)")
    sys.stdout.flush()

    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract GDELT 5-axis features for top firms"
    )
    parser.add_argument("--db", default="data/patents.db", help="SQLite DB path")
    parser.add_argument("--year-from", type=int, default=2020, help="Start year")
    parser.add_argument("--year-to", type=int, default=2024, help="End year")
    parser.add_argument("--max-firms", type=int, default=50, help="Max firms to process")
    args = parser.parse_args()

    run_extraction(
        db_path=args.db,
        year_from=args.year_from,
        year_to=args.year_to,
        max_firms=args.max_firms,
    )


if __name__ == "__main__":
    main()
