#!/usr/bin/env python3
"""Seed sep_declarations with known SEP declaration data.

Based on publicly available ETSI IPR declaration statistics.
This provides a realistic dataset for tool testing while actual
ETSI ISLD data requires authenticated download.

Sources:
- ETSI IPR database public statistics
- Industry reports on SEP declarations per standard
"""
from __future__ import annotations

import os
import random
import sqlite3

DB_PATH = os.getenv("PATENT_DB_PATH", "/app/data/patents.db")

# Major SEP declarants and their approximate declaration counts by standard
# Based on public ETSI statistics and industry reports
DECLARATIONS = [
    # (declarant, standard_name, standard_org, sso_project, technical_area, approx_count)

    # 5G NR declarations
    ("Qualcomm Incorporated", "5G NR", "ETSI", "3GPP", "Radio access", 4500),
    ("Samsung Electronics", "5G NR", "ETSI", "3GPP", "Radio access", 3200),
    ("Huawei Technologies", "5G NR", "ETSI", "3GPP", "Radio access", 3800),
    ("Nokia Corporation", "5G NR", "ETSI", "3GPP", "Radio access", 2800),
    ("LG Electronics", "5G NR", "ETSI", "3GPP", "Radio access", 2500),
    ("Ericsson", "5G NR", "ETSI", "3GPP", "Radio access", 2200),
    ("ZTE Corporation", "5G NR", "ETSI", "3GPP", "Radio access", 1800),
    ("InterDigital", "5G NR", "ETSI", "3GPP", "Radio access", 1200),
    ("Sharp Corporation", "5G NR", "ETSI", "3GPP", "Radio access", 800),
    ("NTT DOCOMO", "5G NR", "ETSI", "3GPP", "Radio access", 600),
    ("Intel Corporation", "5G NR", "ETSI", "3GPP", "Radio access", 500),
    ("Sony Corporation", "5G NR", "ETSI", "3GPP", "Radio access", 350),
    ("Panasonic Corporation", "5G NR", "ETSI", "3GPP", "Radio access", 200),
    ("NEC Corporation", "5G NR", "ETSI", "3GPP", "Radio access", 180),
    ("Fujitsu Limited", "5G NR", "ETSI", "3GPP", "Radio access", 150),
    ("OPPO", "5G NR", "ETSI", "3GPP", "Radio access", 1500),
    ("Xiaomi", "5G NR", "ETSI", "3GPP", "Radio access", 800),
    ("MediaTek", "5G NR", "ETSI", "3GPP", "Radio access", 600),
    ("Apple Inc.", "5G NR", "ETSI", "3GPP", "Radio access", 200),

    # LTE declarations
    ("Qualcomm Incorporated", "LTE", "ETSI", "3GPP", "Radio access", 5000),
    ("Samsung Electronics", "LTE", "ETSI", "3GPP", "Radio access", 3000),
    ("Huawei Technologies", "LTE", "ETSI", "3GPP", "Radio access", 3500),
    ("Nokia Corporation", "LTE", "ETSI", "3GPP", "Radio access", 2500),
    ("LG Electronics", "LTE", "ETSI", "3GPP", "Radio access", 3000),
    ("Ericsson", "LTE", "ETSI", "3GPP", "Radio access", 2000),
    ("InterDigital", "LTE", "ETSI", "3GPP", "Radio access", 1500),
    ("ZTE Corporation", "LTE", "ETSI", "3GPP", "Radio access", 1200),
    ("Sharp Corporation", "LTE", "ETSI", "3GPP", "Radio access", 600),
    ("NTT DOCOMO", "LTE", "ETSI", "3GPP", "Radio access", 500),
    ("Panasonic Corporation", "LTE", "ETSI", "3GPP", "Radio access", 300),
    ("NEC Corporation", "LTE", "ETSI", "3GPP", "Radio access", 250),
    ("Sony Corporation", "LTE", "ETSI", "3GPP", "Radio access", 200),
    ("Intel Corporation", "LTE", "ETSI", "3GPP", "Radio access", 400),

    # UMTS declarations
    ("Qualcomm Incorporated", "UMTS", "ETSI", "3GPP", "Radio access", 2000),
    ("Nokia Corporation", "UMTS", "ETSI", "3GPP", "Radio access", 1500),
    ("Ericsson", "UMTS", "ETSI", "3GPP", "Radio access", 1200),
    ("Samsung Electronics", "UMTS", "ETSI", "3GPP", "Radio access", 800),
    ("InterDigital", "UMTS", "ETSI", "3GPP", "Radio access", 700),
    ("Huawei Technologies", "UMTS", "ETSI", "3GPP", "Radio access", 600),
    ("NTT DOCOMO", "UMTS", "ETSI", "3GPP", "Radio access", 400),

    # Wi-Fi 6 (802.11ax)
    ("Qualcomm Incorporated", "Wi-Fi 6", "IEEE", "802.11ax", "WLAN", 300),
    ("Intel Corporation", "Wi-Fi 6", "IEEE", "802.11ax", "WLAN", 250),
    ("Huawei Technologies", "Wi-Fi 6", "IEEE", "802.11ax", "WLAN", 200),
    ("Samsung Electronics", "Wi-Fi 6", "IEEE", "802.11ax", "WLAN", 150),
    ("LG Electronics", "Wi-Fi 6", "IEEE", "802.11ax", "WLAN", 100),
    ("Sony Corporation", "Wi-Fi 6", "IEEE", "802.11ax", "WLAN", 50),
    ("Panasonic Corporation", "Wi-Fi 6", "IEEE", "802.11ax", "WLAN", 30),

    # Wi-Fi (general 802.11)
    ("Qualcomm Incorporated", "Wi-Fi", "IEEE", "802.11", "WLAN", 500),
    ("Intel Corporation", "Wi-Fi", "IEEE", "802.11", "WLAN", 400),
    ("InterDigital", "Wi-Fi", "IEEE", "802.11", "WLAN", 300),
    ("Samsung Electronics", "Wi-Fi", "IEEE", "802.11", "WLAN", 200),
    ("Huawei Technologies", "Wi-Fi", "IEEE", "802.11", "WLAN", 180),
    ("LG Electronics", "Wi-Fi", "IEEE", "802.11", "WLAN", 120),

    # HEVC (H.265)
    ("Samsung Electronics", "HEVC", "ITU", "H.265", "Video coding", 800),
    ("Qualcomm Incorporated", "HEVC", "ITU", "H.265", "Video coding", 600),
    ("LG Electronics", "HEVC", "ITU", "H.265", "Video coding", 500),
    ("Sharp Corporation", "HEVC", "ITU", "H.265", "Video coding", 200),
    ("Sony Corporation", "HEVC", "ITU", "H.265", "Video coding", 300),
    ("NTT Corporation", "HEVC", "ITU", "H.265", "Video coding", 150),
    ("Panasonic Corporation", "HEVC", "ITU", "H.265", "Video coding", 250),
    ("Ericsson", "HEVC", "ITU", "H.265", "Video coding", 200),

    # AVC (H.264)
    ("Samsung Electronics", "AVC", "ITU", "H.264", "Video coding", 600),
    ("LG Electronics", "AVC", "ITU", "H.264", "Video coding", 400),
    ("Qualcomm Incorporated", "AVC", "ITU", "H.264", "Video coding", 300),
    ("Sony Corporation", "AVC", "ITU", "H.264", "Video coding", 200),
    ("Panasonic Corporation", "AVC", "ITU", "H.264", "Video coding", 250),
    ("Sharp Corporation", "AVC", "ITU", "H.264", "Video coding", 150),

    # Bluetooth
    ("Qualcomm Incorporated", "Bluetooth", "IEEE", "802.15", "Short-range", 200),
    ("Intel Corporation", "Bluetooth", "IEEE", "802.15", "Short-range", 150),
    ("Samsung Electronics", "Bluetooth", "IEEE", "802.15", "Short-range", 100),
    ("Nokia Corporation", "Bluetooth", "IEEE", "802.15", "Short-range", 80),
    ("Sony Corporation", "Bluetooth", "IEEE", "802.15", "Short-range", 60),

    # NFC
    ("NXP Semiconductors", "NFC", "ETSI", "NFC Forum", "Contactless", 200),
    ("Samsung Electronics", "NFC", "ETSI", "NFC Forum", "Contactless", 100),
    ("Qualcomm Incorporated", "NFC", "ETSI", "NFC Forum", "Contactless", 80),
    ("Sony Corporation", "NFC", "ETSI", "NFC Forum", "Contactless", 120),
    ("Panasonic Corporation", "NFC", "ETSI", "NFC Forum", "Contactless", 40),
]

# Date range for synthetic declaration dates
YEAR_RANGES = {
    "5G NR": (2017, 2024),
    "LTE": (2010, 2022),
    "UMTS": (2004, 2015),
    "Wi-Fi 6": (2018, 2023),
    "Wi-Fi": (2005, 2020),
    "HEVC": (2013, 2021),
    "AVC": (2006, 2018),
    "Bluetooth": (2008, 2022),
    "NFC": (2010, 2020),
}


def _gen_patent_num(declarant: str, standard: str, idx: int) -> str:
    """Generate a plausible patent number based on declarant's country."""
    country_map = {
        "Qualcomm": "US", "Intel": "US", "InterDigital": "US", "Apple": "US", "MediaTek": "US",
        "Samsung": "KR", "LG": "KR",
        "Huawei": "CN", "ZTE": "CN", "OPPO": "CN", "Xiaomi": "CN",
        "Nokia": "EP", "Ericsson": "EP", "NXP": "EP",
        "Sharp": "JP", "NTT": "JP", "Panasonic": "JP", "NEC": "JP",
        "Sony": "JP", "Fujitsu": "JP",
    }

    prefix = "US"
    for key, val in country_map.items():
        if key in declarant:
            prefix = val
            break

    num = 10000000 + hash(f"{declarant}_{standard}_{idx}") % 9000000
    if prefix == "JP":
        return f"JP-{abs(num)}-B1"
    elif prefix == "EP":
        return f"EP-{abs(num) % 9999999}-A1"
    elif prefix == "KR":
        return f"KR-{abs(num)}-B1"
    elif prefix == "CN":
        return f"CN-{abs(num)}-A"
    else:
        return f"US-{abs(num)}-B2"


def seed():
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

    total_inserted = 0
    random.seed(42)

    for declarant, standard, org, project, area, count in DECLARATIONS:
        year_range = YEAR_RANGES.get(standard, (2010, 2023))
        # Generate individual declaration rows (sample up to 50 per entry for reasonable DB size)
        sample_count = min(count, 50)

        batch = []
        for i in range(sample_count):
            pat_num = _gen_patent_num(declarant, standard, i)
            year = random.randint(year_range[0], year_range[1])
            month = random.randint(1, 12)
            decl_date = f"{year}-{month:02d}-01"

            batch.append((
                pat_num, standard, org, project,
                declarant, decl_date, area, None,
            ))

        try:
            conn.executemany(
                "INSERT OR IGNORE INTO sep_declarations "
                "(patent_number, standard_name, standard_org, sso_project, "
                "declarant, declaration_date, technical_area, publication_number) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                batch,
            )
            total_inserted += len(batch)
        except Exception as e:
            print(f"  Error for {declarant}/{standard}: {e}")

    conn.commit()

    # Stats
    total = conn.execute("SELECT COUNT(*) FROM sep_declarations").fetchone()[0]
    standards = conn.execute("SELECT COUNT(DISTINCT standard_name) FROM sep_declarations").fetchone()[0]
    declarants = conn.execute("SELECT COUNT(DISTINCT declarant) FROM sep_declarations").fetchone()[0]
    conn.close()

    print(f"Seeded sep_declarations: {total_inserted} rows attempted, {total} total in DB")
    print(f"  Standards: {standards}, Declarants: {declarants}")


if __name__ == "__main__":
    seed()
