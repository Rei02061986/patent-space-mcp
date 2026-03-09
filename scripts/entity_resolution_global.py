#!/usr/bin/env python3
"""entity_resolution_global.py -- Expand entity resolution from ~4,303 TSE firms to 10,000+ global firms.

Reads a top-assignees CSV (from patent_assignees), normalizes names,
groups aliases, merges with known entities (TSE/SP500/global seeds),
and outputs a global companies master CSV + SQL for display_names table.

Usage:
    python entity_resolution_global.py \
        --csv /tmp/top_assignees_20k.csv \
        --db /app/data/patents.db \
        --output /tmp/companies_master_global.csv \
        --sql /tmp/display_names_update.sql

Runs inside Docker container on Hetzner (78.46.57.151).
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sqlite3
import sys
import time
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Optional


# =============================================================================
# Step 2: Name Normalization
# =============================================================================

# Legal suffixes to remove (comprehensive, multi-language)
_LEGAL_SUFFIXES: list[str] = [
    # English
    r"\bCO\.?,?\s*LTD\.?",
    r"\bCORP(ORATION)?\.?",
    r"\bINC(ORPORATED)?\.?",
    r"\bLLC\b",
    r"\bLLP\b",
    r"\bLIMITED\b",
    r"\bLTD\.?\b",
    r"\bPLC\b",
    r"\bPTE\.?\s*LTD\.?",
    r"\bHOLDINGS?\b",
    r"\bGROUP\b",
    r"\bINTERNATIONAL\b",
    r"\bENTERPRISES?\b",
    r"\bINDUSTRIES?\b",
    r"\bTECHNOLOG(Y|IES)\b",
    r"\bSYSTEMS?\b",
    r"\bSOLUTIONS?\b",
    r"\bSERVICES?\b",
    r"\bMANUFACTURING\b",
    r"\bMFG\.?\b",
    r"\bTHE\s+",
    # German
    r"\bGMBH\b",
    r"\bAG\b",
    r"\bAKTIENGESELLSCHAFT\b",
    r"\bKG\b",
    r"\bE\.?\s*V\.?\b",
    # French
    r"\bSA\b",
    r"\bS\.?A\.?S\.?\b",
    r"\bS\.?A\.?R\.?L\.?\b",
    r"\bS\.?A\.?\b",
    # Dutch
    r"\bBV\b",
    r"\bB\.?V\.?\b",
    r"\bNV\b",
    r"\bN\.?V\.?\b",
    # Italian / Spanish
    r"\bSPA\b",
    r"\bS\.?P\.?A\.?\b",
    r"\bS\.?R\.?L\.?\b",
    r"\bS\.?L\.?\b",
    # Nordic
    r"\bOY\b",
    r"\bOYJ\b",
    r"\bAB\b",
    r"\bAS\b",
    r"\bASA\b",
    r"\bA/S\b",
    # Japanese romanized
    r"\bKABUSHIKI\s*KAISHA\b",
    r"\bKK\b",
    r"\bK\.?K\.?\b",
    r"\bYUGEN\s*KAISHA\b",
    # Japanese
    r"株式会社",
    r"有限会社",
    r"合同会社",
    r"合名会社",
    r"合資会社",
    r"特許業務法人",
    r"一般社団法人",
    r"一般財団法人",
    r"公益社団法人",
    r"公益財団法人",
    r"国立大学法人",
    r"独立行政法人",
    r"学校法人",
    r"医療法人",
    r"ホールディングス",
    r"グループ",
    r"\(株\)",
    r"（株）",
]


def normalize_assignee(name: str) -> str:
    """Normalize patent assignee name for matching.

    Steps:
      1. NFKC normalization (full-width -> half-width)
      2. Uppercase
      3. Remove legal suffixes iteratively
      4. Remove parenthetical country codes like (JP), (US)
      5. Collapse whitespace
      6. Strip trailing punctuation
    """
    if not name:
        return ""
    # Full-width to half-width
    name = unicodedata.normalize("NFKC", name)
    # Uppercase
    name = name.upper().strip()
    # Iteratively remove legal suffixes (may need multiple passes)
    for _ in range(3):
        prev = name
        for pattern in _LEGAL_SUFFIXES:
            name = re.sub(pattern, "", name, flags=re.IGNORECASE)
        name = name.strip()
        if name == prev:
            break
    # Remove parenthetical country codes e.g. (JP), (US), (DE)
    name = re.sub(r"\s*\([A-Z]{2}\)\s*", "", name)
    # Remove parenthetical content that is just numbers or single chars
    name = re.sub(r"\s*\(\d+\)\s*", "", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    # Remove trailing punctuation
    name = name.rstrip(".,;:-")
    return name.strip()


def _proper_case(name: str) -> str:
    """Convert UPPERCASE name to Proper Title Case with common fixes."""
    # Keep certain tokens uppercase
    keep_upper = {"IBM", "NEC", "LG", "SK", "3M", "GE", "HP", "SAP", "BMW",
                  "AMD", "ARM", "NTT", "TSMC", "NVIDIA", "ASML", "ABB", "AES",
                  "BASF", "KDDI", "CATL", "BYD", "JFE", "IHI", "NGK", "TDK",
                  "NTN", "NSK", "THK", "SMC", "NOK", "KYB", "DMG", "ZTE",
                  "BOE", "TCL", "OPPO", "VIVO", "BYD", "SMIC", "UMC", "AUO"}
    words = name.split()
    result = []
    for w in words:
        if w in keep_upper:
            result.append(w)
        elif len(w) <= 2 and w.isalpha():
            result.append(w.upper())
        else:
            result.append(w.capitalize())
    return " ".join(result)


# =============================================================================
# Step 6: Global Aliases for Top Companies (150+ companies)
# =============================================================================

# Canonical display name -> list of known aliases (raw names as they appear
# in patent_assignees.harmonized_name, which is typically UPPERCASE).
GLOBAL_ALIASES: dict[str, dict] = {
    # =========================================================================
    # Electronics / Semiconductor
    # =========================================================================
    "SAMSUNG ELECTRONICS": {
        "aliases": [
            "SAMSUNG ELECTRONICS CO", "SAMSUNG ELECTRONICS CO LTD",
            "SAMSUNG ELECTRONICS CO., LTD.", "삼성전자", "삼성전자주식회사",
            "サムスン電子", "SAMSUNG ELECTRO MECHANICS CO LTD",
            "SAMSUNG ELECTRO-MECHANICS",
        ],
        "country": "KR", "sector": "Electronics",
    },
    "SAMSUNG SDI": {
        "aliases": ["SAMSUNG SDI CO LTD", "SAMSUNG SDI CO., LTD."],
        "country": "KR", "sector": "Electronics",
    },
    "SAMSUNG DISPLAY": {
        "aliases": ["SAMSUNG DISPLAY CO LTD", "SAMSUNG DISPLAY CO., LTD."],
        "country": "KR", "sector": "Electronics",
    },
    "LG ELECTRONICS": {
        "aliases": [
            "LG ELECTRONICS INC", "LG ELECTRONICS INC.", "LG전자",
            "LG電子", "LGエレクトロニクス", "엘지전자",
        ],
        "country": "KR", "sector": "Electronics",
    },
    "LG CHEM": {
        "aliases": ["LG CHEM LTD", "LG CHEM, LTD."],
        "country": "KR", "sector": "Chemicals",
    },
    "LG DISPLAY": {
        "aliases": ["LG DISPLAY CO LTD"],
        "country": "KR", "sector": "Electronics",
    },
    "LG ENERGY SOLUTION": {
        "aliases": ["LG ENERGY SOLUTION LTD", "LG ENERGY SOLUTION, LTD."],
        "country": "KR", "sector": "Energy",
    },
    "SK HYNIX": {
        "aliases": [
            "SK HYNIX INC", "HYNIX SEMICONDUCTOR INC",
            "HYNIX SEMICONDUCTOR", "SKハイニックス",
        ],
        "country": "KR", "sector": "Semiconductor",
    },
    "SK INNOVATION": {
        "aliases": ["SK INNOVATION CO LTD", "SK INNOVATION CO., LTD."],
        "country": "KR", "sector": "Energy",
    },
    "TSMC": {
        "aliases": [
            "TAIWAN SEMICONDUCTOR", "TAIWAN SEMICONDUCTOR MANUFACTURING",
            "TAIWAN SEMICONDUCTOR MANUFACTURING CO LTD",
            "TAIWAN SEMICONDUCTOR MFG", "台湾積体電路製造",
        ],
        "country": "TW", "sector": "Semiconductor",
    },
    "INTEL": {
        "aliases": ["INTEL CORP", "INTEL CORPORATION", "インテル"],
        "country": "US", "sector": "Semiconductor",
    },
    "NVIDIA": {
        "aliases": ["NVIDIA CORP", "NVIDIA CORPORATION", "エヌビディア"],
        "country": "US", "sector": "Semiconductor",
    },
    "AMD": {
        "aliases": [
            "ADVANCED MICRO DEVICES", "ADVANCED MICRO DEVICES INC",
            "AMD INC",
        ],
        "country": "US", "sector": "Semiconductor",
    },
    "QUALCOMM": {
        "aliases": [
            "QUALCOMM INC", "QUALCOMM INCORPORATED",
            "QUALCOMM TECHNOLOGIES INC", "クアルコム",
        ],
        "country": "US", "sector": "Semiconductor",
    },
    "BROADCOM": {
        "aliases": [
            "BROADCOM INC", "BROADCOM CORP", "BROADCOM CORPORATION",
            "AVAGO TECHNOLOGIES", "ブロードコム",
        ],
        "country": "US", "sector": "Semiconductor",
    },
    "TEXAS INSTRUMENTS": {
        "aliases": [
            "TEXAS INSTRUMENTS INC", "TEXAS INSTRUMENTS INCORPORATED",
            "テキサス・インスツルメンツ",
        ],
        "country": "US", "sector": "Semiconductor",
    },
    "MICRON": {
        "aliases": [
            "MICRON TECHNOLOGY", "MICRON TECHNOLOGY INC",
            "マイクロン",
        ],
        "country": "US", "sector": "Semiconductor",
    },
    "ARM": {
        "aliases": ["ARM LTD", "ARM LIMITED", "ARM HOLDINGS"],
        "country": "GB", "sector": "Semiconductor",
    },
    "ASML": {
        "aliases": ["ASML HOLDING", "ASML HOLDING NV", "ASML NETHERLANDS BV"],
        "country": "NL", "sector": "Semiconductor",
    },
    "INFINEON": {
        "aliases": [
            "INFINEON TECHNOLOGIES", "INFINEON TECHNOLOGIES AG",
            "インフィニオン",
        ],
        "country": "DE", "sector": "Semiconductor",
    },
    "STMicroelectronics": {
        "aliases": [
            "STMICROELECTRONICS", "STMICROELECTRONICS NV",
            "STMICROELECTRONICS SA",
        ],
        "country": "CH", "sector": "Semiconductor",
    },
    "NXP SEMICONDUCTORS": {
        "aliases": ["NXP SEMICONDUCTORS NV", "NXP BV"],
        "country": "NL", "sector": "Semiconductor",
    },
    "MEDIATEK": {
        "aliases": ["MEDIATEK INC", "聯発科技"],
        "country": "TW", "sector": "Semiconductor",
    },
    "UMC": {
        "aliases": [
            "UNITED MICROELECTRONICS", "UNITED MICROELECTRONICS CORP",
            "聯華電子",
        ],
        "country": "TW", "sector": "Semiconductor",
    },
    "RENESAS": {
        "aliases": [
            "RENESAS ELECTRONICS", "RENESAS ELECTRONICS CORP",
            "ルネサスエレクトロニクス",
        ],
        "country": "JP", "sector": "Semiconductor",
    },
    "ROHM": {
        "aliases": ["ROHM CO LTD", "ローム"],
        "country": "JP", "sector": "Semiconductor",
    },

    # =========================================================================
    # Software / Internet
    # =========================================================================
    "APPLE": {
        "aliases": [
            "APPLE INC", "APPLE COMPUTER", "APPLE COMPUTER INC", "アップル",
        ],
        "country": "US", "sector": "Technology",
    },
    "GOOGLE": {
        "aliases": [
            "GOOGLE LLC", "GOOGLE INC", "ALPHABET", "ALPHABET INC",
            "WAYMO LLC", "DEEPMIND TECHNOLOGIES",
            "グーグル", "アルファベット",
        ],
        "country": "US", "sector": "Technology",
    },
    "MICROSOFT": {
        "aliases": [
            "MICROSOFT CORP", "MICROSOFT CORPORATION",
            "MICROSOFT TECHNOLOGY LICENSING",
            "MICROSOFT TECHNOLOGY LICENSING LLC",
            "マイクロソフト",
        ],
        "country": "US", "sector": "Technology",
    },
    "AMAZON": {
        "aliases": [
            "AMAZON COM INC", "AMAZON.COM INC",
            "AMAZON TECHNOLOGIES INC", "AMAZON TECHNOLOGIES",
            "アマゾン",
        ],
        "country": "US", "sector": "Technology",
    },
    "META": {
        "aliases": [
            "META PLATFORMS", "META PLATFORMS INC", "FACEBOOK",
            "FACEBOOK INC", "FACEBOOK TECHNOLOGIES",
            "メタ", "フェイスブック",
        ],
        "country": "US", "sector": "Technology",
    },
    "IBM": {
        "aliases": [
            "INTERNATIONAL BUSINESS MACHINES",
            "INTERNATIONAL BUSINESS MACHINES CORP",
            "INTERNATIONAL BUSINESS MACHINES CORPORATION",
            "IBM CORP", "アイビーエム",
        ],
        "country": "US", "sector": "Technology",
    },
    "ORACLE": {
        "aliases": [
            "ORACLE CORP", "ORACLE CORPORATION", "ORACLE INTERNATIONAL",
            "オラクル",
        ],
        "country": "US", "sector": "Technology",
    },
    "SALESFORCE": {
        "aliases": [
            "SALESFORCE INC", "SALESFORCE.COM", "SALESFORCE COM INC",
            "セールスフォース",
        ],
        "country": "US", "sector": "Technology",
    },
    "SAP": {
        "aliases": ["SAP SE", "SAP AG"],
        "country": "DE", "sector": "Technology",
    },
    "CISCO": {
        "aliases": [
            "CISCO SYSTEMS", "CISCO SYSTEMS INC", "CISCO TECHNOLOGY",
            "CISCO TECHNOLOGY INC", "シスコ",
        ],
        "country": "US", "sector": "Technology",
    },
    "ADOBE": {
        "aliases": ["ADOBE INC", "ADOBE SYSTEMS", "ADOBE SYSTEMS INC"],
        "country": "US", "sector": "Technology",
    },
    "TENCENT": {
        "aliases": [
            "TENCENT TECHNOLOGY", "TENCENT HOLDINGS",
            "腾讯科技", "腾讯控股", "テンセント",
        ],
        "country": "CN", "sector": "Technology",
    },
    "ALIBABA": {
        "aliases": [
            "ALIBABA GROUP", "ALIBABA GROUP HOLDING",
            "阿里巴巴", "阿里巴巴集团", "アリババ",
        ],
        "country": "CN", "sector": "Technology",
    },
    "BAIDU": {
        "aliases": [
            "BAIDU INC", "BAIDU ONLINE NETWORK TECHNOLOGY",
            "百度", "百度在线网络技术", "バイドゥ",
        ],
        "country": "CN", "sector": "Technology",
    },
    "BYTEDANCE": {
        "aliases": [
            "BYTEDANCE LTD", "BEIJING BYTEDANCE TECHNOLOGY",
            "字节跳动", "バイトダンス",
        ],
        "country": "CN", "sector": "Technology",
    },
    "HUAWEI": {
        "aliases": [
            "HUAWEI TECHNOLOGIES", "HUAWEI TECHNOLOGIES CO LTD",
            "HUAWEI TECH", "HUAWEI DEVICE CO LTD",
            "华为技术有限公司", "华为", "ファーウェイ",
        ],
        "country": "CN", "sector": "Telecom",
    },
    "ZTE": {
        "aliases": [
            "ZTE CORP", "ZTE CORPORATION", "中兴通讯",
        ],
        "country": "CN", "sector": "Telecom",
    },
    "XIAOMI": {
        "aliases": [
            "XIAOMI INC", "XIAOMI COMMUNICATIONS",
            "BEIJING XIAOMI MOBILE SOFTWARE",
            "小米", "シャオミ",
        ],
        "country": "CN", "sector": "Electronics",
    },
    "OPPO": {
        "aliases": [
            "GUANGDONG OPPO MOBILE TELECOMMUNICATIONS",
            "OPPO MOBILE TELECOMMUNICATIONS",
        ],
        "country": "CN", "sector": "Electronics",
    },
    "VIVO": {
        "aliases": [
            "VIVO MOBILE COMMUNICATION",
            "VIVO MOBILE COMMUNICATION CO LTD",
        ],
        "country": "CN", "sector": "Electronics",
    },
    "BOE TECHNOLOGY": {
        "aliases": [
            "BOE TECHNOLOGY GROUP", "BOE TECHNOLOGY GROUP CO LTD",
            "京东方科技集团",
        ],
        "country": "CN", "sector": "Electronics",
    },

    # =========================================================================
    # Automotive
    # =========================================================================
    "TOYOTA": {
        "aliases": [
            "TOYOTA MOTOR", "TOYOTA MOTOR CORP", "TOYOTA MOTOR CORPORATION",
            "TOYOTA JIDOSHA", "TOYOTA JIDOSHA KK", "トヨタ自動車",
        ],
        "country": "JP", "sector": "Automotive",
    },
    "HONDA": {
        "aliases": [
            "HONDA MOTOR", "HONDA MOTOR CO LTD", "HONDA GIKEN",
            "本田技研工業", "ホンダ",
        ],
        "country": "JP", "sector": "Automotive",
    },
    "NISSAN": {
        "aliases": [
            "NISSAN MOTOR", "NISSAN MOTOR CO LTD", "日産自動車",
        ],
        "country": "JP", "sector": "Automotive",
    },
    "MAZDA": {
        "aliases": ["MAZDA MOTOR", "MAZDA MOTOR CORP", "マツダ"],
        "country": "JP", "sector": "Automotive",
    },
    "SUBARU": {
        "aliases": [
            "SUBARU CORP", "FUJI HEAVY INDUSTRIES",
            "FUJI HEAVY INDUSTRIES LTD", "富士重工業",
        ],
        "country": "JP", "sector": "Automotive",
    },
    "SUZUKI": {
        "aliases": ["SUZUKI MOTOR", "SUZUKI MOTOR CORP", "スズキ"],
        "country": "JP", "sector": "Automotive",
    },
    "MITSUBISHI MOTORS": {
        "aliases": [
            "MITSUBISHI MOTORS CORP", "MITSUBISHI JIDOSHA", "三菱自動車",
        ],
        "country": "JP", "sector": "Automotive",
    },
    "DENSO": {
        "aliases": [
            "DENSO CORP", "DENSO CORPORATION", "デンソー", "株式会社デンソー",
        ],
        "country": "JP", "sector": "Automotive",
    },
    "AISIN": {
        "aliases": [
            "AISIN CORP", "AISIN SEIKI", "AISIN SEIKI CO LTD",
            "AISIN AW CO LTD", "アイシン",
        ],
        "country": "JP", "sector": "Automotive",
    },
    "HYUNDAI": {
        "aliases": [
            "HYUNDAI MOTOR", "HYUNDAI MOTOR CO", "HYUNDAI MOTOR CO LTD",
            "현대자동차", "ヒュンダイ",
        ],
        "country": "KR", "sector": "Automotive",
    },
    "KIA": {
        "aliases": ["KIA MOTORS", "KIA MOTORS CORP", "KIA CORP", "起亜"],
        "country": "KR", "sector": "Automotive",
    },
    "VOLKSWAGEN": {
        "aliases": [
            "VOLKSWAGEN AG", "VOLKSWAGEN AKTIENGESELLSCHAFT", "VW",
            "フォルクスワーゲン",
        ],
        "country": "DE", "sector": "Automotive",
    },
    "BMW": {
        "aliases": [
            "BAYERISCHE MOTOREN WERKE", "BAYERISCHE MOTOREN WERKE AG",
            "BMW AG",
        ],
        "country": "DE", "sector": "Automotive",
    },
    "MERCEDES-BENZ": {
        "aliases": [
            "DAIMLER AG", "DAIMLER", "MERCEDES BENZ GROUP",
            "MERCEDES-BENZ GROUP AG", "DAIMLERCHRYSLER AG",
            "メルセデス・ベンツ", "ダイムラー",
        ],
        "country": "DE", "sector": "Automotive",
    },
    "AUDI": {
        "aliases": ["AUDI AG", "アウディ"],
        "country": "DE", "sector": "Automotive",
    },
    "PORSCHE": {
        "aliases": [
            "DR ING HCF PORSCHE", "DR. ING. H.C. F. PORSCHE AG",
            "PORSCHE AG",
        ],
        "country": "DE", "sector": "Automotive",
    },
    "FORD": {
        "aliases": [
            "FORD MOTOR", "FORD MOTOR CO", "FORD MOTOR COMPANY",
            "FORD GLOBAL TECHNOLOGIES", "FORD GLOBAL TECHNOLOGIES LLC",
            "フォード",
        ],
        "country": "US", "sector": "Automotive",
    },
    "GM": {
        "aliases": [
            "GENERAL MOTORS", "GENERAL MOTORS CO",
            "GENERAL MOTORS LLC", "GM GLOBAL TECHNOLOGY OPERATIONS",
            "GM GLOBAL TECHNOLOGY OPERATIONS LLC",
            "ゼネラルモーターズ",
        ],
        "country": "US", "sector": "Automotive",
    },
    "TESLA": {
        "aliases": ["TESLA INC", "TESLA MOTORS", "テスラ"],
        "country": "US", "sector": "Automotive",
    },
    "STELLANTIS": {
        "aliases": [
            "STELLANTIS NV", "FIAT CHRYSLER AUTOMOBILES",
            "FCA US LLC", "PSA AUTOMOBILES",
        ],
        "country": "NL", "sector": "Automotive",
    },
    "VOLVO": {
        "aliases": [
            "VOLVO CAR", "VOLVO CAR CORP", "VOLVO TRUCK",
        ],
        "country": "SE", "sector": "Automotive",
    },
    "BYD": {
        "aliases": [
            "BYD CO LTD", "BYD COMPANY", "比亚迪",
        ],
        "country": "CN", "sector": "Automotive",
    },
    "CATL": {
        "aliases": [
            "CONTEMPORARY AMPEREX TECHNOLOGY",
            "CONTEMPORARY AMPEREX TECHNOLOGY CO LTD",
            "宁德时代", "宁德时代新能源科技",
        ],
        "country": "CN", "sector": "Automotive",
    },
    "BOSCH": {
        "aliases": [
            "ROBERT BOSCH", "ROBERT BOSCH GMBH", "ROBERT BOSCH LLC",
            "ボッシュ",
        ],
        "country": "DE", "sector": "Industrial",
    },
    "CONTINENTAL": {
        "aliases": [
            "CONTINENTAL AG", "CONTINENTAL AUTOMOTIVE",
            "CONTINENTAL AUTOMOTIVE GMBH",
        ],
        "country": "DE", "sector": "Automotive",
    },
    "ZF FRIEDRICHSHAFEN": {
        "aliases": ["ZF FRIEDRICHSHAFEN AG", "ZF", "ZF AUTOMOTIVE"],
        "country": "DE", "sector": "Automotive",
    },

    # =========================================================================
    # Pharma / Biotech
    # =========================================================================
    "PFIZER": {
        "aliases": ["PFIZER INC", "PFIZER INC.", "ファイザー"],
        "country": "US", "sector": "Pharma",
    },
    "JOHNSON & JOHNSON": {
        "aliases": [
            "JOHNSON AND JOHNSON", "JOHNSON & JOHNSON",
            "JANSSEN PHARMACEUTICA", "JANSSEN BIOTECH",
            "JANSSEN PHARMACEUTICALS", "ジョンソン・エンド・ジョンソン",
        ],
        "country": "US", "sector": "Pharma",
    },
    "MERCK": {
        "aliases": [
            "MERCK AND CO", "MERCK & CO INC", "MERCK SHARP & DOHME",
            "MERCK SHARP AND DOHME", "MSD",
        ],
        "country": "US", "sector": "Pharma",
    },
    "ABBVIE": {
        "aliases": ["ABBVIE INC", "ABBVIE DEUTSCHLAND", "アッヴィ"],
        "country": "US", "sector": "Pharma",
    },
    "ELI LILLY": {
        "aliases": [
            "ELI LILLY AND CO", "ELI LILLY AND COMPANY",
            "LILLY", "イーライリリー",
        ],
        "country": "US", "sector": "Pharma",
    },
    "BRISTOL MYERS SQUIBB": {
        "aliases": [
            "BRISTOL-MYERS SQUIBB", "BRISTOL-MYERS SQUIBB CO",
            "BRISTOL MYERS SQUIBB CO",
        ],
        "country": "US", "sector": "Pharma",
    },
    "AMGEN": {
        "aliases": ["AMGEN INC", "アムジェン"],
        "country": "US", "sector": "Pharma",
    },
    "GILEAD": {
        "aliases": [
            "GILEAD SCIENCES", "GILEAD SCIENCES INC", "ギリアド",
        ],
        "country": "US", "sector": "Pharma",
    },
    "ROCHE": {
        "aliases": [
            "F HOFFMANN LA ROCHE", "F. HOFFMANN-LA ROCHE",
            "HOFFMANN LA ROCHE", "ROCHE HOLDING",
            "ROCHE HOLDING AG", "ロシュ",
        ],
        "country": "CH", "sector": "Pharma",
    },
    "NOVARTIS": {
        "aliases": ["NOVARTIS AG", "NOVARTIS PHARMA", "ノバルティス"],
        "country": "CH", "sector": "Pharma",
    },
    "ASTRAZENECA": {
        "aliases": [
            "ASTRAZENECA AB", "ASTRAZENECA PLC", "ASTRAZENECA UK",
            "アストラゼネカ",
        ],
        "country": "GB", "sector": "Pharma",
    },
    "GSK": {
        "aliases": [
            "GLAXOSMITHKLINE", "GLAXOSMITHKLINE PLC",
            "SMITHKLINE BEECHAM", "グラクソ・スミスクライン",
        ],
        "country": "GB", "sector": "Pharma",
    },
    "SANOFI": {
        "aliases": [
            "SANOFI SA", "SANOFI AVENTIS", "SANOFI-AVENTIS",
            "サノフィ",
        ],
        "country": "FR", "sector": "Pharma",
    },
    "BAYER": {
        "aliases": ["BAYER AG", "BAYER AKTIENGESELLSCHAFT", "バイエル"],
        "country": "DE", "sector": "Pharma",
    },
    "TAKEDA": {
        "aliases": [
            "TAKEDA PHARMACEUTICAL", "TAKEDA PHARMACEUTICAL CO LTD",
            "TAKEDA YAKUHIN KOGYO", "武田薬品工業",
        ],
        "country": "JP", "sector": "Pharma",
    },
    "ASTELLAS": {
        "aliases": [
            "ASTELLAS PHARMA", "ASTELLAS PHARMA INC",
            "アステラス製薬",
        ],
        "country": "JP", "sector": "Pharma",
    },
    "DAIICHI SANKYO": {
        "aliases": [
            "DAIICHI SANKYO CO LTD", "DAIICHI SANKYO COMPANY",
            "第一三共",
        ],
        "country": "JP", "sector": "Pharma",
    },
    "EISAI": {
        "aliases": ["EISAI CO LTD", "EISAI R&D MANAGEMENT", "エーザイ"],
        "country": "JP", "sector": "Pharma",
    },
    "OTSUKA": {
        "aliases": [
            "OTSUKA PHARMACEUTICAL", "OTSUKA PHARMACEUTICAL CO LTD",
            "大塚製薬",
        ],
        "country": "JP", "sector": "Pharma",
    },
    "SHIONOGI": {
        "aliases": ["SHIONOGI & CO", "SHIONOGI AND CO LTD", "塩野義製薬"],
        "country": "JP", "sector": "Pharma",
    },

    # =========================================================================
    # Industrial / Conglomerate
    # =========================================================================
    "SIEMENS": {
        "aliases": [
            "SIEMENS AG", "SIEMENS AKTIENGESELLSCHAFT",
            "SIEMENS ENERGY", "シーメンス",
        ],
        "country": "DE", "sector": "Industrial",
    },
    "GE": {
        "aliases": [
            "GENERAL ELECTRIC", "GENERAL ELECTRIC CO",
            "GE HEALTHCARE", "GE AVIATION",
            "ゼネラル・エレクトリック",
        ],
        "country": "US", "sector": "Industrial",
    },
    "HONEYWELL": {
        "aliases": [
            "HONEYWELL INTERNATIONAL", "HONEYWELL INTERNATIONAL INC",
            "ハネウェル",
        ],
        "country": "US", "sector": "Industrial",
    },
    "3M": {
        "aliases": [
            "3M COMPANY", "3M INNOVATIVE PROPERTIES",
            "3M INNOVATIVE PROPERTIES CO", "スリーエム",
        ],
        "country": "US", "sector": "Industrial",
    },
    "ABB": {
        "aliases": ["ABB LTD", "ABB SCHWEIZ AG"],
        "country": "CH", "sector": "Industrial",
    },
    "EMERSON": {
        "aliases": [
            "EMERSON ELECTRIC", "EMERSON ELECTRIC CO",
        ],
        "country": "US", "sector": "Industrial",
    },
    "CATERPILLAR": {
        "aliases": ["CATERPILLAR INC", "キャタピラー"],
        "country": "US", "sector": "Industrial",
    },
    "BOEING": {
        "aliases": [
            "THE BOEING COMPANY", "BOEING CO", "BOEING COMPANY",
            "ボーイング",
        ],
        "country": "US", "sector": "Aerospace",
    },
    "AIRBUS": {
        "aliases": [
            "AIRBUS SAS", "AIRBUS OPERATIONS", "AIRBUS OPERATIONS SAS",
            "AIRBUS DEFENCE AND SPACE", "エアバス",
        ],
        "country": "FR", "sector": "Aerospace",
    },
    "LOCKHEED MARTIN": {
        "aliases": [
            "LOCKHEED MARTIN CORP", "LOCKHEED MARTIN CORPORATION",
        ],
        "country": "US", "sector": "Aerospace",
    },
    "RAYTHEON": {
        "aliases": [
            "RAYTHEON TECHNOLOGIES", "RAYTHEON CO",
            "RTX CORP", "UNITED TECHNOLOGIES",
        ],
        "country": "US", "sector": "Aerospace",
    },
    "THALES": {
        "aliases": ["THALES SA", "THALES DIS"],
        "country": "FR", "sector": "Aerospace",
    },
    "HITACHI": {
        "aliases": [
            "HITACHI LTD", "HITACHI ASTEMO", "HITACHI METALS",
            "日立製作所",
        ],
        "country": "JP", "sector": "Industrial",
    },
    "MITSUBISHI ELECTRIC": {
        "aliases": [
            "MITSUBISHI ELECTRIC CORP", "MITSUBISHI DENKI",
            "三菱電機",
        ],
        "country": "JP", "sector": "Industrial",
    },
    "MITSUBISHI HEAVY INDUSTRIES": {
        "aliases": [
            "MITSUBISHI HEAVY IND", "MITSUBISHI HEAVY INDUSTRIES LTD",
            "三菱重工業",
        ],
        "country": "JP", "sector": "Industrial",
    },
    "TOSHIBA": {
        "aliases": [
            "TOSHIBA CORP", "TOSHIBA CORPORATION", "東芝",
        ],
        "country": "JP", "sector": "Industrial",
    },
    "NEC": {
        "aliases": [
            "NEC CORP", "NEC CORPORATION", "NIPPON ELECTRIC",
            "日本電気",
        ],
        "country": "JP", "sector": "Technology",
    },
    "FUJITSU": {
        "aliases": [
            "FUJITSU LTD", "FUJITSU LIMITED", "富士通",
        ],
        "country": "JP", "sector": "Technology",
    },
    "PANASONIC": {
        "aliases": [
            "PANASONIC HOLDINGS", "PANASONIC CORP",
            "PANASONIC CORPORATION", "MATSUSHITA ELECTRIC",
            "MATSUSHITA ELECTRIC INDUSTRIAL", "パナソニック", "松下電器",
        ],
        "country": "JP", "sector": "Electronics",
    },
    "SONY": {
        "aliases": [
            "SONY GROUP", "SONY CORP", "SONY CORPORATION",
            "SONY SEMICONDUCTOR SOLUTIONS", "ソニーグループ",
        ],
        "country": "JP", "sector": "Electronics",
    },
    "CANON": {
        "aliases": [
            "CANON INC", "CANON KK", "CANON KABUSHIKI KAISHA",
            "キヤノン",
        ],
        "country": "JP", "sector": "Electronics",
    },
    "RICOH": {
        "aliases": ["RICOH CO LTD", "RICOH COMPANY", "リコー"],
        "country": "JP", "sector": "Electronics",
    },
    "FUJIFILM": {
        "aliases": [
            "FUJIFILM CORP", "FUJIFILM HOLDINGS",
            "FUJI PHOTO FILM", "富士フイルム",
        ],
        "country": "JP", "sector": "Electronics",
    },
    "KONICA MINOLTA": {
        "aliases": [
            "KONICA MINOLTA INC", "KONICA MINOLTA BUSINESS",
            "コニカミノルタ",
        ],
        "country": "JP", "sector": "Electronics",
    },
    "EPSON": {
        "aliases": [
            "SEIKO EPSON", "SEIKO EPSON CORP", "セイコーエプソン",
        ],
        "country": "JP", "sector": "Electronics",
    },
    "SHARP": {
        "aliases": ["SHARP CORP", "SHARP KK", "シャープ"],
        "country": "JP", "sector": "Electronics",
    },
    "KYOCERA": {
        "aliases": ["KYOCERA CORP", "KYOCERA CORPORATION", "京セラ"],
        "country": "JP", "sector": "Electronics",
    },
    "MURATA": {
        "aliases": [
            "MURATA MANUFACTURING", "MURATA MFG", "MURATA MFG CO LTD",
            "村田製作所",
        ],
        "country": "JP", "sector": "Electronics",
    },
    "TDK": {
        "aliases": ["TDK CORP", "TDK CORPORATION"],
        "country": "JP", "sector": "Electronics",
    },
    "NIDEC": {
        "aliases": ["NIDEC CORP", "NIDEC CORPORATION", "日本電産"],
        "country": "JP", "sector": "Electronics",
    },
    "OMRON": {
        "aliases": ["OMRON CORP", "OMRON CORPORATION", "オムロン"],
        "country": "JP", "sector": "Electronics",
    },
    "KEYENCE": {
        "aliases": ["KEYENCE CORP", "KEYENCE CORPORATION", "キーエンス"],
        "country": "JP", "sector": "Electronics",
    },
    "FANUC": {
        "aliases": ["FANUC CORP", "FANUC LTD", "ファナック"],
        "country": "JP", "sector": "Industrial",
    },
    "SMC": {
        "aliases": ["SMC CORP", "SMC CORPORATION"],
        "country": "JP", "sector": "Industrial",
    },
    "DAIKIN": {
        "aliases": [
            "DAIKIN INDUSTRIES", "DAIKIN INDUSTRIES LTD", "ダイキン工業",
        ],
        "country": "JP", "sector": "Industrial",
    },
    "KOMATSU": {
        "aliases": ["KOMATSU LTD", "KOMATSU SEISAKUSHO", "小松製作所"],
        "country": "JP", "sector": "Industrial",
    },
    "KUBOTA": {
        "aliases": ["KUBOTA CORP", "KUBOTA CORPORATION", "クボタ"],
        "country": "JP", "sector": "Industrial",
    },
    "IHI": {
        "aliases": ["IHI CORP", "IHI CORPORATION"],
        "country": "JP", "sector": "Industrial",
    },
    "YASKAWA": {
        "aliases": [
            "YASKAWA ELECTRIC", "YASKAWA ELECTRIC CORP", "安川電機",
        ],
        "country": "JP", "sector": "Industrial",
    },
    "YOKOGAWA": {
        "aliases": [
            "YOKOGAWA ELECTRIC", "YOKOGAWA ELECTRIC CORP", "横河電機",
        ],
        "country": "JP", "sector": "Industrial",
    },

    # =========================================================================
    # Chemicals / Materials
    # =========================================================================
    "BASF": {
        "aliases": ["BASF SE", "BASF AG"],
        "country": "DE", "sector": "Chemicals",
    },
    "DOW": {
        "aliases": [
            "DOW INC", "DOW CHEMICAL", "THE DOW CHEMICAL COMPANY",
            "DOW CHEMICAL CO", "ダウ",
        ],
        "country": "US", "sector": "Chemicals",
    },
    "DUPONT": {
        "aliases": [
            "DUPONT DE NEMOURS", "EI DU PONT DE NEMOURS",
            "E I DU PONT DE NEMOURS", "DUPONT",
            "デュポン",
        ],
        "country": "US", "sector": "Chemicals",
    },
    "LINDE": {
        "aliases": ["LINDE PLC", "LINDE AG", "LINDE GMBH"],
        "country": "IE", "sector": "Chemicals",
    },
    "AIR LIQUIDE": {
        "aliases": [
            "L AIR LIQUIDE", "AIR LIQUIDE SA",
            "L'AIR LIQUIDE SOCIETE ANONYME",
        ],
        "country": "FR", "sector": "Chemicals",
    },
    "SHIN-ETSU CHEMICAL": {
        "aliases": [
            "SHIN ETSU CHEMICAL", "SHIN ETSU CHEM CO LTD",
            "SHIN-ETSU CHEMICAL CO LTD", "信越化学工業",
        ],
        "country": "JP", "sector": "Chemicals",
    },
    "SUMITOMO CHEMICAL": {
        "aliases": [
            "SUMITOMO CHEMICAL CO LTD", "SUMITOMO CHEM",
            "住友化学",
        ],
        "country": "JP", "sector": "Chemicals",
    },
    "MITSUBISHI CHEMICAL": {
        "aliases": [
            "MITSUBISHI CHEMICAL CORP", "MITSUBISHI CHEMICAL GROUP",
            "三菱ケミカル",
        ],
        "country": "JP", "sector": "Chemicals",
    },
    "TORAY": {
        "aliases": [
            "TORAY INDUSTRIES", "TORAY INDUSTRIES INC", "東レ",
        ],
        "country": "JP", "sector": "Chemicals",
    },
    "NIPPON STEEL": {
        "aliases": [
            "NIPPON STEEL CORP", "NIPPON STEEL CORPORATION",
            "NIPPON STEEL & SUMITOMO METAL", "新日鉄住金", "日本製鉄",
        ],
        "country": "JP", "sector": "Materials",
    },
    "JFE": {
        "aliases": [
            "JFE STEEL", "JFE STEEL CORP", "JFE HOLDINGS",
            "JFEスチール",
        ],
        "country": "JP", "sector": "Materials",
    },
    "SUMITOMO ELECTRIC": {
        "aliases": [
            "SUMITOMO ELECTRIC INDUSTRIES", "SUMITOMO ELECTRIC IND",
            "SUMITOMO ELECTRIC INDUSTRIES LTD", "住友電気工業",
        ],
        "country": "JP", "sector": "Materials",
    },
    "FURUKAWA ELECTRIC": {
        "aliases": [
            "FURUKAWA ELECTRIC CO LTD", "FURUKAWA ELECTRIC",
            "古河電気工業",
        ],
        "country": "JP", "sector": "Materials",
    },
    "NGK": {
        "aliases": [
            "NGK INSULATORS", "NGK INSULATORS LTD",
            "NGK SPARK PLUG", "日本ガイシ",
        ],
        "country": "JP", "sector": "Materials",
    },

    # =========================================================================
    # Telecom / Network
    # =========================================================================
    "ERICSSON": {
        "aliases": [
            "TELEFONAKTIEBOLAGET LM ERICSSON",
            "ERICSSON TELEFON AB LM", "LM ERICSSON",
            "エリクソン",
        ],
        "country": "SE", "sector": "Telecom",
    },
    "NOKIA": {
        "aliases": [
            "NOKIA CORP", "NOKIA OYJ", "NOKIA SOLUTIONS AND NETWORKS",
            "ノキア",
        ],
        "country": "FI", "sector": "Telecom",
    },
    "NTT": {
        "aliases": [
            "NIPPON TELEGRAPH AND TELEPHONE",
            "NIPPON TELEGRAPH & TELEPHONE", "NTT CORP",
            "NTT DOCOMO", "日本電信電話",
        ],
        "country": "JP", "sector": "Telecom",
    },
    "KDDI": {
        "aliases": ["KDDI CORP", "KDDI CORPORATION"],
        "country": "JP", "sector": "Telecom",
    },
    "SOFTBANK": {
        "aliases": [
            "SOFTBANK GROUP", "SOFTBANK CORP", "ソフトバンクグループ",
        ],
        "country": "JP", "sector": "Telecom",
    },

    # =========================================================================
    # Energy
    # =========================================================================
    "EXXONMOBIL": {
        "aliases": [
            "EXXON MOBIL", "EXXON MOBIL CORP",
            "EXXONMOBIL CHEMICAL", "EXXON",
        ],
        "country": "US", "sector": "Energy",
    },
    "CHEVRON": {
        "aliases": ["CHEVRON CORP", "CHEVRON USA"],
        "country": "US", "sector": "Energy",
    },
    "SHELL": {
        "aliases": [
            "SHELL PLC", "ROYAL DUTCH SHELL",
            "SHELL INTERNATIONALE RESEARCH", "SHELL OIL",
        ],
        "country": "GB", "sector": "Energy",
    },
    "TOTALENERGIES": {
        "aliases": [
            "TOTALENERGIES SE", "TOTAL SA", "TOTAL",
            "TOTAL ENERGIES",
        ],
        "country": "FR", "sector": "Energy",
    },
    "BP": {
        "aliases": ["BP PLC", "BRITISH PETROLEUM"],
        "country": "GB", "sector": "Energy",
    },
    "SCHLUMBERGER": {
        "aliases": [
            "SCHLUMBERGER TECHNOLOGY", "SCHLUMBERGER LTD",
            "SLB", "シュルンベルジェ",
        ],
        "country": "US", "sector": "Energy",
    },

    # =========================================================================
    # Consumer / Diversified
    # =========================================================================
    "PROCTER & GAMBLE": {
        "aliases": [
            "PROCTER AND GAMBLE", "THE PROCTER & GAMBLE COMPANY",
            "PROCTER & GAMBLE CO", "P&G",
        ],
        "country": "US", "sector": "Consumer",
    },
    "UNILEVER": {
        "aliases": ["UNILEVER PLC", "UNILEVER NV", "UNILEVER IP HOLDINGS"],
        "country": "GB", "sector": "Consumer",
    },
    "NESTLE": {
        "aliases": ["NESTLE SA", "SOCIETE DES PRODUITS NESTLE", "ネスレ"],
        "country": "CH", "sector": "Consumer",
    },
    "LOREAL": {
        "aliases": ["L OREAL", "L'OREAL", "LOREAL SA"],
        "country": "FR", "sector": "Consumer",
    },
    "SAMSUNG BIOLOGICS": {
        "aliases": ["SAMSUNG BIOLOGICS CO LTD"],
        "country": "KR", "sector": "Pharma",
    },
    "POSCO": {
        "aliases": [
            "POSCO HOLDINGS", "POSCO HOLDINGS INC", "포스코",
        ],
        "country": "KR", "sector": "Materials",
    },
    "HP": {
        "aliases": [
            "HEWLETT PACKARD", "HEWLETT-PACKARD",
            "HP INC", "HP DEVELOPMENT COMPANY",
            "HEWLETT PACKARD ENTERPRISE",
        ],
        "country": "US", "sector": "Technology",
    },
    "DELL": {
        "aliases": [
            "DELL TECHNOLOGIES", "DELL INC",
            "DELL PRODUCTS",
        ],
        "country": "US", "sector": "Technology",
    },
    "XEROX": {
        "aliases": [
            "XEROX CORP", "XEROX CORPORATION",
        ],
        "country": "US", "sector": "Technology",
    },
    "PHILIPS": {
        "aliases": [
            "KONINKLIJKE PHILIPS", "KONINKLIJKE PHILIPS NV",
            "PHILIPS ELECTRONICS", "フィリップス",
        ],
        "country": "NL", "sector": "Electronics",
    },
    "SCHNEIDER ELECTRIC": {
        "aliases": [
            "SCHNEIDER ELECTRIC SE", "SCHNEIDER ELECTRIC SA",
        ],
        "country": "FR", "sector": "Industrial",
    },
    "DANFOSS": {
        "aliases": ["DANFOSS AS", "DANFOSS A/S"],
        "country": "DK", "sector": "Industrial",
    },
    "BRIDGESTONE": {
        "aliases": [
            "BRIDGESTONE CORP", "BRIDGESTONE CORPORATION",
            "ブリヂストン",
        ],
        "country": "JP", "sector": "Automotive",
    },
    "YOKOHAMA RUBBER": {
        "aliases": [
            "THE YOKOHAMA RUBBER CO LTD", "YOKOHAMA RUBBER CO",
            "横浜ゴム",
        ],
        "country": "JP", "sector": "Automotive",
    },
    "SUMITOMO RUBBER": {
        "aliases": [
            "SUMITOMO RUBBER INDUSTRIES", "SUMITOMO RUBBER IND",
            "住友ゴム工業",
        ],
        "country": "JP", "sector": "Automotive",
    },
    "NIPPON PAINT": {
        "aliases": [
            "NIPPON PAINT HOLDINGS", "日本ペイントホールディングス",
        ],
        "country": "JP", "sector": "Chemicals",
    },
    "SEKISUI CHEMICAL": {
        "aliases": ["SEKISUI CHEMICAL CO LTD", "積水化学工業"],
        "country": "JP", "sector": "Chemicals",
    },
    "ASAHI KASEI": {
        "aliases": [
            "ASAHI KASEI CORP", "ASAHI KASEI CORPORATION", "旭化成",
        ],
        "country": "JP", "sector": "Chemicals",
    },
    "MITSUI CHEMICALS": {
        "aliases": [
            "MITSUI CHEMICALS INC", "三井化学",
        ],
        "country": "JP", "sector": "Chemicals",
    },
    "AGC": {
        "aliases": [
            "AGC INC", "ASAHI GLASS", "ASAHI GLASS CO LTD",
            "旭硝子",
        ],
        "country": "JP", "sector": "Materials",
    },
    "NIPPON ELECTRIC GLASS": {
        "aliases": [
            "NIPPON ELECTRIC GLASS CO LTD", "日本電気硝子",
        ],
        "country": "JP", "sector": "Materials",
    },
    "NIKON": {
        "aliases": ["NIKON CORP", "NIKON CORPORATION", "ニコン"],
        "country": "JP", "sector": "Electronics",
    },
    "OLYMPUS": {
        "aliases": [
            "OLYMPUS CORP", "OLYMPUS CORPORATION", "オリンパス",
        ],
        "country": "JP", "sector": "Electronics",
    },
    "HAMAMATSU PHOTONICS": {
        "aliases": [
            "HAMAMATSU PHOTONICS KK", "浜松ホトニクス",
        ],
        "country": "JP", "sector": "Electronics",
    },
    "BROTHER": {
        "aliases": [
            "BROTHER INDUSTRIES", "BROTHER INDUSTRIES LTD",
            "ブラザー工業",
        ],
        "country": "JP", "sector": "Electronics",
    },
    "SHIMADZU": {
        "aliases": ["SHIMADZU CORP", "SHIMADZU CORPORATION", "島津製作所"],
        "country": "JP", "sector": "Industrial",
    },
    "THK": {
        "aliases": ["THK CO LTD"],
        "country": "JP", "sector": "Industrial",
    },
    "NSK": {
        "aliases": ["NSK LTD", "日本精工"],
        "country": "JP", "sector": "Industrial",
    },
    "NTN": {
        "aliases": ["NTN CORP", "NTN CORPORATION"],
        "country": "JP", "sector": "Industrial",
    },
    "KOBELCO": {
        "aliases": [
            "KOBE STEEL", "KOBE STEEL LTD", "神戸製鋼所",
        ],
        "country": "JP", "sector": "Materials",
    },
    "TOYODA GOSEI": {
        "aliases": ["TOYODA GOSEI CO LTD", "豊田合成"],
        "country": "JP", "sector": "Automotive",
    },
    "TOYOTA BOSHOKU": {
        "aliases": ["TOYOTA BOSHOKU CORP", "トヨタ紡織"],
        "country": "JP", "sector": "Automotive",
    },
    "JTEKT": {
        "aliases": ["JTEKT CORP", "JTEKT CORPORATION", "ジェイテクト"],
        "country": "JP", "sector": "Automotive",
    },
    "SCREEN HOLDINGS": {
        "aliases": [
            "SCREEN HOLDINGS CO LTD", "DAINIPPON SCREEN MFG",
            "SCREENホールディングス",
        ],
        "country": "JP", "sector": "Semiconductor",
    },
    "TOKYO ELECTRON": {
        "aliases": [
            "TOKYO ELECTRON LTD", "TOKYO ELECTRON LIMITED",
            "東京エレクトロン",
        ],
        "country": "JP", "sector": "Semiconductor",
    },
    "ADVANTEST": {
        "aliases": ["ADVANTEST CORP", "ADVANTEST CORPORATION", "アドバンテスト"],
        "country": "JP", "sector": "Semiconductor",
    },
    "DISCO": {
        "aliases": ["DISCO CORP", "ディスコ"],
        "country": "JP", "sector": "Semiconductor",
    },
    "LASERTEC": {
        "aliases": ["LASERTEC CORP", "レーザーテック"],
        "country": "JP", "sector": "Semiconductor",
    },
    "APPLIED MATERIALS": {
        "aliases": [
            "APPLIED MATERIALS INC", "アプライドマテリアルズ",
        ],
        "country": "US", "sector": "Semiconductor",
    },
    "LAM RESEARCH": {
        "aliases": ["LAM RESEARCH CORP", "LAM RESEARCH CORPORATION"],
        "country": "US", "sector": "Semiconductor",
    },
    "KLA": {
        "aliases": ["KLA CORP", "KLA TENCOR", "KLA-TENCOR"],
        "country": "US", "sector": "Semiconductor",
    },
    "SAMSUNG SDS": {
        "aliases": ["SAMSUNG SDS CO LTD"],
        "country": "KR", "sector": "Technology",
    },
    "NAVER": {
        "aliases": ["NAVER CORP", "네이버"],
        "country": "KR", "sector": "Technology",
    },
    "KAKAO": {
        "aliases": ["KAKAO CORP", "카카오"],
        "country": "KR", "sector": "Technology",
    },
    "HON HAI": {
        "aliases": [
            "HON HAI PRECISION INDUSTRY", "HON HAI PRECISION IND",
            "FOXCONN", "FOXCONN TECHNOLOGY", "鴻海精密工業",
        ],
        "country": "TW", "sector": "Electronics",
    },
    "AU OPTRONICS": {
        "aliases": ["AU OPTRONICS CORP", "AUO", "友達光電"],
        "country": "TW", "sector": "Electronics",
    },
    "INNOLUX": {
        "aliases": ["INNOLUX CORP", "群創光電"],
        "country": "TW", "sector": "Electronics",
    },
    "DELTA ELECTRONICS": {
        "aliases": [
            "DELTA ELECTRONICS INC", "台達電子",
        ],
        "country": "TW", "sector": "Electronics",
    },
    "SMIC": {
        "aliases": [
            "SEMICONDUCTOR MANUFACTURING INTERNATIONAL",
            "SEMICONDUCTOR MFG INTL", "中芯国際",
        ],
        "country": "CN", "sector": "Semiconductor",
    },
    "LENOVO": {
        "aliases": [
            "LENOVO GROUP", "LENOVO BEIJING",
            "联想", "レノボ",
        ],
        "country": "CN", "sector": "Technology",
    },
}


# =============================================================================
# Data structures
# =============================================================================

@dataclass
class GlobalEntity:
    """A resolved global entity with all known aliases and metadata."""
    canonical_name: str
    display_name_en: str
    display_name_ja: str
    country: str
    sector: str
    patent_count: int
    aliases: set[str] = field(default_factory=set)
    matched_tse_id: Optional[str] = None


# =============================================================================
# Utility: fuzzy matching
# =============================================================================

def _fuzzy_ratio(a: str, b: str) -> float:
    """Return Levenshtein-like similarity ratio in [0, 1]."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


# =============================================================================
# Main pipeline
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Expand entity resolution to 10k+ global firms",
    )
    parser.add_argument(
        "--csv",
        default="/tmp/top_assignees_20k.csv",
        help="Path to top_assignees CSV (assignee_name, patent_count)",
    )
    parser.add_argument(
        "--db",
        default="/app/data/patents.db",
        help="Path to patents.db (SQLite)",
    )
    parser.add_argument(
        "--output",
        default="/tmp/companies_master_global.csv",
        help="Output CSV path for global companies master",
    )
    parser.add_argument(
        "--sql",
        default="/tmp/display_names_update.sql",
        help="Output SQL file for display_names table updates",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write to DB; only produce CSV and SQL files",
    )
    parser.add_argument(
        "--apply-sql",
        action="store_true",
        help="Apply SQL directly to the database (default: only write .sql file)",
    )
    args = parser.parse_args()

    t0 = time.time()

    # -------------------------------------------------------------------------
    # Step 1: Read top assignees CSV
    # -------------------------------------------------------------------------
    print(f"[Step 1] Reading top assignees from {args.csv} ...")
    raw_assignees: list[tuple[str, int]] = []
    if not os.path.exists(args.csv):
        print(f"  ERROR: CSV not found at {args.csv}")
        print("  Falling back to DB query for top assignees ...")
        raw_assignees = _query_top_assignees_from_db(args.db, limit=20_000)
    else:
        with open(args.csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get("assignee_name", row.get("name", "")).strip()
                count = int(row.get("patent_count", row.get("count", "0")))
                if name and count > 0:
                    raw_assignees.append((name, count))
    print(f"  Loaded {len(raw_assignees):,} assignees")

    # -------------------------------------------------------------------------
    # Step 2 & 3: Normalize and group by normalized name
    # -------------------------------------------------------------------------
    print("[Step 2] Normalizing assignee names ...")
    norm_groups: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for raw_name, count in raw_assignees:
        norm = normalize_assignee(raw_name)
        if len(norm) < 2:
            continue
        norm_groups[norm].append((raw_name, count))
    print(f"  {len(raw_assignees):,} raw names -> {len(norm_groups):,} normalized groups")

    # -------------------------------------------------------------------------
    # Step 4: Build canonical entities from groups
    # -------------------------------------------------------------------------
    print("[Step 4] Building canonical entities from groups ...")
    entities: dict[str, GlobalEntity] = {}

    for norm, members in norm_groups.items():
        # Most frequent raw name becomes canonical
        members.sort(key=lambda x: x[1], reverse=True)
        canonical_raw = members[0][0]
        total_count = sum(c for _, c in members)

        # Display name: proper-cased version of the normalized form
        display_en = _proper_case(norm) if norm.isupper() or norm.islower() else norm
        # If canonical raw is Japanese, keep it for display_ja
        display_ja = ""
        for raw, _ in members:
            if any("\u3000" <= ch <= "\u9fff" or "\uff00" <= ch <= "\uffef" for ch in raw):
                display_ja = raw.strip()
                break

        all_aliases = set()
        for raw, _ in members:
            all_aliases.add(raw.strip())

        entities[norm] = GlobalEntity(
            canonical_name=canonical_raw,
            display_name_en=display_en,
            display_name_ja=display_ja,
            country="",
            sector="",
            patent_count=total_count,
            aliases=all_aliases,
        )

    print(f"  Built {len(entities):,} canonical entities")

    # -------------------------------------------------------------------------
    # Step 4b: Enrich with country from DB (if available)
    # -------------------------------------------------------------------------
    if os.path.exists(args.db):
        print("[Step 4b] Enriching country codes from patent_assignees ...")
        _enrich_country_from_db(args.db, entities)
    else:
        print("[Step 4b] DB not available, skipping country enrichment")

    # -------------------------------------------------------------------------
    # Step 5: Match against existing TSE entities (display_names table)
    # -------------------------------------------------------------------------
    tse_matches = 0
    if os.path.exists(args.db):
        print("[Step 5] Matching against existing display_names table ...")
        tse_matches = _match_existing_display_names(args.db, entities)
        print(f"  Matched {tse_matches:,} entities against existing display_names")
    else:
        print("[Step 5] DB not available, skipping TSE matching")

    # -------------------------------------------------------------------------
    # Step 6: Apply global alias overrides
    # -------------------------------------------------------------------------
    print("[Step 6] Applying global alias overrides (hardcoded top companies) ...")
    global_applied = 0
    for display_key, info in GLOBAL_ALIASES.items():
        norm_key = normalize_assignee(display_key)
        aliases = info["aliases"]
        country = info["country"]
        sector = info["sector"]

        # Find if this entity already exists via normalized match
        matched_entity: Optional[GlobalEntity] = None

        # Try exact norm match
        if norm_key in entities:
            matched_entity = entities[norm_key]

        # Try matching via any alias
        if matched_entity is None:
            for alias in aliases:
                norm_alias = normalize_assignee(alias)
                if norm_alias in entities:
                    matched_entity = entities[norm_alias]
                    break

        if matched_entity is not None:
            # Merge: add aliases, update metadata
            for alias in aliases:
                matched_entity.aliases.add(alias)
            matched_entity.aliases.add(display_key)
            if not matched_entity.country:
                matched_entity.country = country
            if not matched_entity.sector:
                matched_entity.sector = sector
            if not matched_entity.display_name_en or matched_entity.display_name_en == norm_key:
                matched_entity.display_name_en = _proper_case(display_key)
            global_applied += 1
        else:
            # Create new entity from global alias (may not be in top assignees CSV)
            all_aliases_set = set(aliases)
            all_aliases_set.add(display_key)
            entities[norm_key] = GlobalEntity(
                canonical_name=display_key,
                display_name_en=_proper_case(display_key),
                display_name_ja="",
                country=country,
                sector=sector,
                patent_count=0,
                aliases=all_aliases_set,
            )
            global_applied += 1

    print(f"  Applied {global_applied} global alias entries ({len(GLOBAL_ALIASES)} defined)")

    # Also try fuzzy-matching remaining unmatched globals against entities
    print("  Running fuzzy match for unmatched global aliases ...")
    fuzzy_merged = _fuzzy_merge_globals(entities)
    print(f"  Fuzzy-merged {fuzzy_merged} additional alias groups")

    # -------------------------------------------------------------------------
    # Step 7: Output CSV
    # -------------------------------------------------------------------------
    print(f"[Step 7] Writing global companies master CSV to {args.output} ...")
    sorted_entities = sorted(
        entities.values(),
        key=lambda e: e.patent_count,
        reverse=True,
    )
    with open(args.output, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "canonical_name", "display_name_en", "display_name_ja",
            "country", "sector", "patent_count", "alias_count", "aliases",
        ])
        for entity in sorted_entities:
            writer.writerow([
                entity.canonical_name,
                entity.display_name_en,
                entity.display_name_ja,
                entity.country,
                entity.sector,
                entity.patent_count,
                len(entity.aliases),
                "|".join(sorted(entity.aliases)),
            ])
    print(f"  Wrote {len(sorted_entities):,} entities to CSV")

    # -------------------------------------------------------------------------
    # Step 8: Generate SQL for display_names table
    # -------------------------------------------------------------------------
    print(f"[Step 8] Generating SQL for display_names table -> {args.sql} ...")
    sql_count = 0
    with open(args.sql, "w", encoding="utf-8") as f:
        f.write("-- Auto-generated by entity_resolution_global.py\n")
        f.write("-- Expands display_names table with global entity resolution\n")
        f.write(f"-- Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("BEGIN TRANSACTION;\n\n")
        f.write(
            "CREATE TABLE IF NOT EXISTS display_names (\n"
            "    assignee_raw TEXT PRIMARY KEY,\n"
            "    canonical_name TEXT NOT NULL,\n"
            "    display_name TEXT NOT NULL\n"
            ");\n\n"
        )

        for entity in sorted_entities:
            display = entity.display_name_en or entity.canonical_name
            canonical = entity.canonical_name
            # Escape single quotes for SQL
            canonical_sql = canonical.replace("'", "''")
            display_sql = display.replace("'", "''")

            for alias in sorted(entity.aliases):
                alias_sql = alias.replace("'", "''")
                f.write(
                    f"INSERT OR IGNORE INTO display_names "
                    f"(assignee_raw, canonical_name, display_name) "
                    f"VALUES ('{alias_sql}', '{canonical_sql}', '{display_sql}');\n"
                )
                sql_count += 1

        f.write("\nCOMMIT;\n")
    print(f"  Generated {sql_count:,} INSERT statements")

    # -------------------------------------------------------------------------
    # Optional: Apply SQL to database
    # -------------------------------------------------------------------------
    if args.apply_sql and os.path.exists(args.db) and not args.dry_run:
        print(f"[Apply] Executing SQL against {args.db} ...")
        _apply_sql_to_db(args.db, args.sql)
    elif args.apply_sql:
        print("[Apply] Skipped: --dry-run is set or DB not found")

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    elapsed = time.time() - t0

    # Statistics
    countries = defaultdict(int)
    sectors = defaultdict(int)
    total_aliases = 0
    entities_with_count = 0
    for e in sorted_entities:
        if e.country:
            countries[e.country] += 1
        if e.sector:
            sectors[e.sector] += 1
        total_aliases += len(e.aliases)
        if e.patent_count > 0:
            entities_with_count += 1

    print("\n" + "=" * 60)
    print("ENTITY RESOLUTION SUMMARY")
    print("=" * 60)
    print(f"  Total entities:            {len(sorted_entities):,}")
    print(f"  Entities with patents:     {entities_with_count:,}")
    print(f"  Total alias mappings:      {total_aliases:,}")
    print(f"  TSE matches:               {tse_matches:,}")
    print(f"  Global overrides applied:  {global_applied}")
    print(f"  Fuzzy merges:              {fuzzy_merged}")
    print()
    print("  Top countries:")
    for cc, cnt in sorted(countries.items(), key=lambda x: -x[1])[:15]:
        print(f"    {cc}: {cnt:,}")
    print()
    print("  Top sectors:")
    for sec, cnt in sorted(sectors.items(), key=lambda x: -x[1])[:15]:
        print(f"    {sec}: {cnt:,}")
    print()
    print(f"  Top 20 entities by patent count:")
    for i, e in enumerate(sorted_entities[:20], 1):
        print(f"    {i:3d}. {e.display_name_en:<40s} {e.country:>4s}  {e.patent_count:>10,} patents  ({len(e.aliases)} aliases)")
    print()
    print(f"  Elapsed: {elapsed:.1f}s")
    print(f"  Output CSV: {args.output}")
    print(f"  Output SQL: {args.sql}")
    print("=" * 60)


# =============================================================================
# Helper: query top assignees directly from DB
# =============================================================================

def _query_top_assignees_from_db(
    db_path: str, limit: int = 20_000,
) -> list[tuple[str, int]]:
    """Fallback: query top assignees from patent_assignees table."""
    print(f"  Querying top {limit:,} assignees from {db_path} ...")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA query_only=ON")

    # Try harmonized_name first, fall back to raw_name
    try:
        cursor = conn.execute(
            """
            SELECT harmonized_name AS name, COUNT(*) AS cnt
            FROM patent_assignees
            WHERE harmonized_name IS NOT NULL AND harmonized_name != ''
            GROUP BY harmonized_name
            ORDER BY cnt DESC
            LIMIT ?
            """,
            (limit,),
        )
    except sqlite3.OperationalError:
        cursor = conn.execute(
            """
            SELECT raw_name AS name, COUNT(*) AS cnt
            FROM patent_assignees
            WHERE raw_name IS NOT NULL AND raw_name != ''
            GROUP BY raw_name
            ORDER BY cnt DESC
            LIMIT ?
            """,
            (limit,),
        )

    results = [(row[0], row[1]) for row in cursor]
    conn.close()
    print(f"  Retrieved {len(results):,} assignees from DB")
    return results


# =============================================================================
# Helper: enrich entities with country code from DB
# =============================================================================

def _enrich_country_from_db(
    db_path: str, entities: dict[str, GlobalEntity],
) -> None:
    """Look up the most frequent country_code for each entity's canonical name."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA query_only=ON")

    # Check if country_code column exists
    try:
        conn.execute("SELECT country_code FROM patent_assignees LIMIT 1")
        has_country = True
    except sqlite3.OperationalError:
        has_country = False

    if not has_country:
        print("  patent_assignees has no country_code column, skipping")
        conn.close()
        return

    enriched = 0
    # Process in batches to avoid huge queries
    entity_list = [(norm, e) for norm, e in entities.items() if not e.country]
    batch_size = 500

    for batch_start in range(0, len(entity_list), batch_size):
        batch = entity_list[batch_start:batch_start + batch_size]
        # Use canonical names (raw) to look up
        names = []
        name_to_norms: dict[str, list[str]] = defaultdict(list)
        for norm, e in batch:
            names.append(e.canonical_name)
            name_to_norms[e.canonical_name].append(norm)

        if not names:
            continue

        placeholders = ",".join("?" * len(names))
        try:
            cursor = conn.execute(
                f"""
                SELECT harmonized_name, country_code, COUNT(*) as cnt
                FROM patent_assignees
                WHERE harmonized_name IN ({placeholders})
                  AND country_code IS NOT NULL AND country_code != ''
                GROUP BY harmonized_name, country_code
                ORDER BY cnt DESC
                """,
                names,
            )
        except sqlite3.OperationalError:
            cursor = conn.execute(
                f"""
                SELECT raw_name, country_code, COUNT(*) as cnt
                FROM patent_assignees
                WHERE raw_name IN ({placeholders})
                  AND country_code IS NOT NULL AND country_code != ''
                GROUP BY raw_name, country_code
                ORDER BY cnt DESC
                """,
                names,
            )

        # Assign top country per name
        seen_names: set[str] = set()
        for row in cursor:
            name_val, cc, _ = row
            if name_val not in seen_names:
                seen_names.add(name_val)
                for norm_key in name_to_norms.get(name_val, []):
                    if norm_key in entities and not entities[norm_key].country:
                        entities[norm_key].country = cc
                        enriched += 1

    conn.close()
    print(f"  Enriched {enriched:,} entities with country codes")


# =============================================================================
# Helper: match against existing display_names in DB
# =============================================================================

def _match_existing_display_names(
    db_path: str, entities: dict[str, GlobalEntity],
) -> int:
    """Check display_names table for existing canonical names, merge matches."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA query_only=ON")

    # Check if display_names table exists
    tables = [
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    ]
    if "display_names" not in tables:
        print("  display_names table does not exist, skipping")
        conn.close()
        return 0

    # Load existing display_names
    existing: list[tuple[str, str, str]] = []
    try:
        cursor = conn.execute(
            "SELECT assignee_raw, canonical_name, display_name FROM display_names"
        )
        existing = cursor.fetchall()
    except sqlite3.OperationalError as e:
        print(f"  Error reading display_names: {e}")
        conn.close()
        return 0

    conn.close()
    print(f"  Loaded {len(existing):,} existing display_names entries")

    # Build a reverse lookup: normalized existing canonical -> existing entries
    existing_norm: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    for raw, canonical, display in existing:
        norm = normalize_assignee(canonical)
        existing_norm[norm].append((raw, canonical, display))

    matched = 0
    for norm_key, entity in entities.items():
        # Try exact normalized match
        if norm_key in existing_norm:
            for raw, canonical, display in existing_norm[norm_key]:
                entity.aliases.add(raw)
                if not entity.display_name_en or entity.display_name_en == norm_key:
                    entity.display_name_en = display
                entity.matched_tse_id = canonical
            matched += 1
            continue

        # Try fuzzy against existing canonical names (only for high-count entities)
        if entity.patent_count >= 100:
            best_score = 0.0
            best_norm = ""
            for ex_norm in existing_norm:
                score = _fuzzy_ratio(norm_key, ex_norm)
                if score > best_score:
                    best_score = score
                    best_norm = ex_norm
            if best_score >= 0.85 and best_norm:
                for raw, canonical, display in existing_norm[best_norm]:
                    entity.aliases.add(raw)
                entity.matched_tse_id = existing_norm[best_norm][0][1]
                matched += 1

    return matched


# =============================================================================
# Helper: fuzzy merge global alias groups
# =============================================================================

def _fuzzy_merge_globals(entities: dict[str, GlobalEntity]) -> int:
    """Find normalized keys that are very similar and merge them.

    This catches cases like "TOYOTA MOTOR" and "TOYOTA MOTOR CORP"
    that normalize to slightly different strings.
    """
    merged = 0
    # Only consider entities with sufficient patent counts to avoid noise
    high_count = [
        (norm, e) for norm, e in entities.items() if e.patent_count >= 50
    ]
    high_count.sort(key=lambda x: -x[1].patent_count)

    # Build list of norms for comparison
    norms = [norm for norm, _ in high_count]
    to_merge: list[tuple[str, str]] = []  # (smaller, larger)

    for i, (norm_i, entity_i) in enumerate(high_count):
        if norm_i not in entities:
            continue  # already merged away
        for j in range(i + 1, min(i + 200, len(high_count))):
            norm_j = high_count[j][0]
            if norm_j not in entities:
                continue
            entity_j = high_count[j][1]

            # Quick length check to skip obvious non-matches
            len_i, len_j = len(norm_i), len(norm_j)
            if abs(len_i - len_j) > max(len_i, len_j) * 0.3:
                continue

            # Check if one is a prefix of the other (common pattern)
            if norm_i.startswith(norm_j) or norm_j.startswith(norm_i):
                score = 0.90
            else:
                score = _fuzzy_ratio(norm_i, norm_j)

            if score >= 0.88:
                # Merge j into i (i has higher patent count)
                to_merge.append((norm_j, norm_i))

    for src, dst in to_merge:
        if src in entities and dst in entities and src != dst:
            entities[dst].aliases |= entities[src].aliases
            entities[dst].patent_count += entities[src].patent_count
            if not entities[dst].country and entities[src].country:
                entities[dst].country = entities[src].country
            if not entities[dst].sector and entities[src].sector:
                entities[dst].sector = entities[src].sector
            if not entities[dst].display_name_ja and entities[src].display_name_ja:
                entities[dst].display_name_ja = entities[src].display_name_ja
            del entities[src]
            merged += 1

    return merged


# =============================================================================
# Helper: apply SQL file to database
# =============================================================================

def _apply_sql_to_db(db_path: str, sql_path: str) -> None:
    """Execute the generated SQL file against the database."""
    conn = sqlite3.connect(db_path, isolation_level=None)
    try:
        with open(sql_path, "r", encoding="utf-8") as f:
            sql = f.read()
        conn.executescript(sql)
        # Verify count
        count = conn.execute("SELECT COUNT(*) FROM display_names").fetchone()[0]
        print(f"  Applied successfully. display_names now has {count:,} rows.")
    except Exception as e:
        print(f"  ERROR applying SQL: {e}")
    finally:
        conn.close()


# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    main()
