#!/usr/bin/env python3
"""Seed the corporate_hierarchy table with major Japanese corporate groups.

Maps subsidiaries to parent companies using TSE company IDs.
Focus on top 30 manufacturing/tech groups with significant patent portfolios.
"""
from __future__ import annotations

import os
import sqlite3

DB_PATH = os.getenv("PATENT_DB_PATH", "/app/data/patents.db")

# Format: (child_firm_id, parent_firm_id, relationship, ownership_pct)
# relationship: "subsidiary", "affiliate", "joint_venture"
HIERARCHY_DATA = [
    # ── Toyota Group ──
    ("company_6902", "company_7203", "subsidiary", 24.2),   # DENSO
    ("company_7259", "company_7203", "subsidiary", 100.0),  # Aisin
    ("company_7282", "company_7203", "subsidiary", 100.0),  # Toyoda Gosei
    ("company_6201", "company_7203", "subsidiary", 100.0),  # Toyota Industries
    ("company_7205", "company_7203", "subsidiary", 50.1),   # Hino Motors
    ("company_7262", "company_7203", "subsidiary", 20.0),   # Daihatsu
    ("company_7270", "company_7203", "affiliate", 20.0),    # SUBARU

    # ── Honda Group ──
    ("company_7251", "company_7267", "subsidiary", 100.0),  # Keihin → merged into Honda

    # ── Hitachi Group ──
    ("company_6305", "company_6501", "subsidiary", 51.0),   # Hitachi Construction Machinery
    ("company_6966", "company_6501", "subsidiary", 100.0),  # Hitachi Metals → rebranded Proterial
    ("company_6501", "company_6501", "subsidiary", None),   # Hitachi (self, root)

    # ── Panasonic Group ──
    ("company_6752", "company_6752", "subsidiary", None),   # Panasonic Holdings (root)

    # ── Sony Group ──
    ("company_6758", "company_6758", "subsidiary", None),   # Sony Group (root)

    # ── Mitsubishi Electric Group ──
    ("company_6503", "company_6503", "subsidiary", None),   # Mitsubishi Electric (root)

    # ── Mitsubishi Heavy Industries Group ──
    ("company_7011", "company_7011", "subsidiary", None),   # MHI (root)

    # ── Toshiba Group ──
    ("company_6502", "company_6502", "subsidiary", None),   # Toshiba (root)

    # ── NEC Group ──
    ("company_6701", "company_6701", "subsidiary", None),   # NEC (root)

    # ── Fujitsu Group ──
    ("company_6702", "company_6702", "subsidiary", None),   # Fujitsu (root)

    # ── Canon Group ──
    ("company_7751", "company_7751", "subsidiary", None),   # Canon (root)

    # ── Nissan-Renault Alliance ──
    ("company_7201", "company_7201", "subsidiary", None),   # Nissan (root)

    # ── Komatsu Group ──
    ("company_6301", "company_6301", "subsidiary", None),   # Komatsu (root)

    # ── Murata Group ──
    ("company_6981", "company_6981", "subsidiary", None),   # Murata Manufacturing (root)

    # ── FANUC Group ──
    ("company_6954", "company_6954", "subsidiary", None),   # FANUC (root)

    # ── Keyence Group ──
    ("company_6861", "company_6861", "subsidiary", None),   # Keyence (root)

    # ── Shin-Etsu Group ──
    ("company_4063", "company_4063", "subsidiary", None),   # Shin-Etsu Chemical (root)

    # ── Tokyo Electron Group ──
    ("company_8035", "company_8035", "subsidiary", None),   # TEL (root)

    # ── Ricoh Group ──
    ("company_7752", "company_7752", "subsidiary", None),   # Ricoh (root)

    # ── Fujifilm Group ──
    ("company_4901", "company_4901", "subsidiary", None),   # Fujifilm Holdings (root)

    # ── Olympus Group ──
    ("company_7733", "company_7733", "subsidiary", None),   # Olympus (root)

    # ── Nikon Group ──
    ("company_7731", "company_7731", "subsidiary", None),   # Nikon (root)

    # ── Yaskawa Group ──
    ("company_6506", "company_6506", "subsidiary", None),   # Yaskawa Electric (root)

    # ── Daikin Group ──
    ("company_6367", "company_6367", "subsidiary", None),   # Daikin Industries (root)

    # ── Suzuki Group ──
    ("company_7269", "company_7269", "subsidiary", None),   # Suzuki Motor (root)

    # ── Mazda Group ──
    ("company_7261", "company_7261", "subsidiary", None),   # Mazda Motor (root)

    # ── Kyocera Group ──
    ("company_6971", "company_6971", "subsidiary", None),   # Kyocera (root)

    # ── TDK Group ──
    ("company_6762", "company_6762", "subsidiary", None),   # TDK (root)

    # ── Omron Group ──
    ("company_6645", "company_6645", "subsidiary", None),   # Omron (root)

    # ── SMC Group ──
    ("company_6273", "company_6273", "subsidiary", None),   # SMC (root)

    # ── IHI Group ──
    ("company_7013", "company_7013", "subsidiary", None),   # IHI (root)
]


def seed():
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")

    # Ensure table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS corporate_hierarchy (
            firm_id TEXT NOT NULL,
            parent_firm_id TEXT NOT NULL,
            relationship TEXT DEFAULT 'subsidiary',
            ownership_pct REAL,
            source TEXT DEFAULT 'manual',
            PRIMARY KEY (firm_id, parent_firm_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hierarchy_parent ON corporate_hierarchy(parent_firm_id)")

    inserted = 0
    for child, parent, rel, pct in HIERARCHY_DATA:
        if child == parent:
            continue  # Skip self-references for roots
        try:
            conn.execute(
                "INSERT OR REPLACE INTO corporate_hierarchy "
                "(firm_id, parent_firm_id, relationship, ownership_pct, source) "
                "VALUES (?, ?, ?, ?, 'manual')",
                (child, parent, rel, pct),
            )
            inserted += 1
        except Exception as e:
            print(f"  Error {child} -> {parent}: {e}")

    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM corporate_hierarchy").fetchone()[0]
    groups = conn.execute("SELECT COUNT(DISTINCT parent_firm_id) FROM corporate_hierarchy").fetchone()[0]
    conn.close()

    print(f"Seeded corporate_hierarchy: {inserted} relationships, {total} total, {groups} groups")


if __name__ == "__main__":
    seed()
