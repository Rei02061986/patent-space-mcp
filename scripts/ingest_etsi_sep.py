#!/usr/bin/env python3
"""Ingest ETSI ISLD (IPR Search & Licensing Database) into sep_declarations table.

Usage:
    python ingest_etsi_sep.py /path/to/ISLD-export.csv

The CSV from ETSI has columns like:
    FULL PATENT REFERENCE, LATEST DECLARED STANDARD, SSO, SSO PROJECT,
    LAST MODIFIED DATE, DECLARANT, TECHNICAL AREA, etc.

We map these to our sep_declarations schema.
"""
from __future__ import annotations

import csv
import os
import re
import sqlite3
import sys


DB_PATH = os.getenv("PATENT_DB_PATH", "/app/data/patents.db")

# Column name mapping — ETSI CSV column names can vary slightly
_COL_MAPS = {
    "patent_number": [
        "FULL PATENT REFERENCE",
        "Full patent reference",
        "Patent Number",
        "patent_number",
    ],
    "standard_name": [
        "LATEST DECLARED STANDARD",
        "Latest declared standard",
        "Standard",
        "standard_name",
    ],
    "standard_org": ["SSO", "sso", "Standard Organization"],
    "sso_project": [
        "SSO PROJECT",
        "SSO Project",
        "sso_project",
        "Project",
    ],
    "declarant": ["DECLARANT", "Declarant", "declarant", "Company"],
    "declaration_date": [
        "LAST MODIFIED DATE",
        "Last modified date",
        "Declaration Date",
        "declaration_date",
    ],
    "technical_area": [
        "TECHNICAL AREA",
        "Technical area",
        "Technical Area",
        "technical_area",
    ],
}


def _find_col(header: list[str], candidates: list[str]) -> int | None:
    """Find the column index for a field, trying multiple candidate names."""
    header_lower = [h.strip().lower() for h in header]
    for c in candidates:
        c_lower = c.strip().lower()
        if c_lower in header_lower:
            return header_lower.index(c_lower)
    return None


def _normalize_patent_number(raw: str) -> str | None:
    """Try to normalize patent number to our format (e.g., JP-1234567-B1)."""
    if not raw:
        return None
    raw = raw.strip()

    # EP patents: EP1234567
    m = re.match(r"EP\s*(\d+)", raw, re.IGNORECASE)
    if m:
        return f"EP-{m.group(1)}-A1"

    # US patents: US1234567, US 12/345,678
    m = re.match(r"US\s*(\d[\d,/]+)", raw, re.IGNORECASE)
    if m:
        num = re.sub(r"[,/\s]", "", m.group(1))
        return f"US-{num}-B2"

    # JP patents: JP1234567
    m = re.match(r"JP\s*(\d+)", raw, re.IGNORECASE)
    if m:
        return f"JP-{m.group(1)}-B1"

    # WO patents
    m = re.match(r"WO\s*(\d+)", raw, re.IGNORECASE)
    if m:
        return f"WO-{m.group(1)}-A1"

    return None


def _extract_standard_short(raw: str) -> str:
    """Extract a short standard name from ETSI's verbose format."""
    if not raw:
        return ""
    raw = raw.strip()

    # Common patterns
    for pattern, label in [
        (r"5G\s*NR|NR\s*5G|TS\s*38\.", "5G NR"),
        (r"LTE|TS\s*36\.", "LTE"),
        (r"UMTS|TS\s*25\.", "UMTS"),
        (r"GSM|TS\s*05\.", "GSM"),
        (r"Wi-?Fi\s*6|802\.11ax", "Wi-Fi 6"),
        (r"Wi-?Fi\s*5|802\.11ac", "Wi-Fi 5"),
        (r"Wi-?Fi|802\.11", "Wi-Fi"),
        (r"HEVC|H\.265", "HEVC"),
        (r"AVC|H\.264", "AVC"),
        (r"VVC|H\.266", "VVC"),
        (r"MPEG", "MPEG"),
        (r"Bluetooth", "Bluetooth"),
        (r"NFC", "NFC"),
        (r"DVB", "DVB"),
    ]:
        if re.search(pattern, raw, re.IGNORECASE):
            return label

    # Truncate to 80 chars if too long
    if len(raw) > 80:
        return raw[:77] + "..."
    return raw


def ingest(csv_path: str) -> dict:
    """Ingest ETSI CSV into sep_declarations table."""
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")

    # Ensure table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sep_declarations (
            declaration_id INTEGER PRIMARY KEY AUTOINCREMENT,
            patent_number TEXT,
            standard_name TEXT NOT NULL,
            standard_org TEXT DEFAULT 'ETSI',
            sso_project TEXT,
            declarant TEXT NOT NULL,
            declaration_date TEXT,
            technical_area TEXT,
            publication_number TEXT,
            UNIQUE(patent_number, standard_name, declarant)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sep_standard ON sep_declarations(standard_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sep_declarant ON sep_declarations(declarant)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sep_pub ON sep_declarations(publication_number)")

    # Detect encoding
    for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
        try:
            with open(csv_path, encoding=enc) as f:
                f.read(1024)
            break
        except (UnicodeDecodeError, UnicodeError):
            continue

    inserted = 0
    skipped = 0
    errors = 0

    with open(csv_path, encoding=enc) as f:
        reader = csv.reader(f)
        header = next(reader)

        # Find columns
        col_idx = {}
        for field, candidates in _COL_MAPS.items():
            idx = _find_col(header, candidates)
            col_idx[field] = idx

        if col_idx["declarant"] is None:
            print(f"ERROR: Could not find 'declarant' column in header: {header}")
            return {"error": "Missing declarant column"}

        batch = []
        for row_num, row in enumerate(reader, start=2):
            try:
                def _get(field):
                    idx = col_idx.get(field)
                    if idx is not None and idx < len(row):
                        return row[idx].strip()
                    return ""

                patent_num = _get("patent_number")
                standard_raw = _get("standard_name")
                declarant = _get("declarant")
                sso = _get("standard_org") or "ETSI"
                sso_project = _get("sso_project")
                decl_date = _get("declaration_date")
                tech_area = _get("technical_area")

                if not declarant:
                    skipped += 1
                    continue

                standard_short = _extract_standard_short(standard_raw) or standard_raw
                if not standard_short:
                    standard_short = "Unknown"

                pub_num = _normalize_patent_number(patent_num)

                batch.append((
                    patent_num, standard_short, sso, sso_project,
                    declarant, decl_date, tech_area, pub_num,
                ))

                if len(batch) >= 5000:
                    conn.executemany(
                        "INSERT OR IGNORE INTO sep_declarations "
                        "(patent_number, standard_name, standard_org, sso_project, "
                        "declarant, declaration_date, technical_area, publication_number) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        batch,
                    )
                    inserted += len(batch)
                    conn.commit()
                    batch = []
                    if inserted % 50000 == 0:
                        print(f"  ... {inserted:,} rows inserted")

            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"  Row {row_num}: {e}")

        # Final batch
        if batch:
            conn.executemany(
                "INSERT OR IGNORE INTO sep_declarations "
                "(patent_number, standard_name, standard_org, sso_project, "
                "declarant, declaration_date, technical_area, publication_number) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                batch,
            )
            inserted += len(batch)
            conn.commit()

    # Stats
    total = conn.execute("SELECT COUNT(*) FROM sep_declarations").fetchone()[0]
    standards = conn.execute("SELECT COUNT(DISTINCT standard_name) FROM sep_declarations").fetchone()[0]
    declarants = conn.execute("SELECT COUNT(DISTINCT declarant) FROM sep_declarations").fetchone()[0]

    conn.close()

    result = {
        "inserted": inserted,
        "skipped": skipped,
        "errors": errors,
        "total_rows": total,
        "unique_standards": standards,
        "unique_declarants": declarants,
    }
    print(f"\nDone: {result}")
    return result


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ingest_etsi_sep.py <csv_path>")
        sys.exit(1)
    ingest(sys.argv[1])
