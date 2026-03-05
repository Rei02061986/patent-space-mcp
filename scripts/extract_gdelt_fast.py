"""Fast GDELT 5-axis extraction: 500 firms × 20 quarters.

Key optimization: compute all features in SQL (2 queries per firm instead of 40).
GKG and Events queries aggregate by quarter in BigQuery, avoiding downloading
millions of raw records.

Expected time: ~10-30s per firm (vs ~90s per quarter = 30min per firm in v1).
Total ETA: ~3-5 hours for 500 firms.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "/app/patent-mcp-sa.json")

from google.cloud import bigquery

GKG_TABLE = "gdelt-bq.gdeltv2.gkg_partitioned"
EVENTS_TABLE = "gdelt-bq.gdeltv2.events"

# ──────────────────────────────────────────────────────────────────
# FIRM_GDELT_NAMES: firm_id → [GDELT English search names]
# ──────────────────────────────────────────────────────────────────
FIRM_GDELT_NAMES: dict[str, list[str]] = {
    # ═══ Named firms ═══
    "panasonic": ["PANASONIC"],
    "canon": ["CANON INC"],
    "toyota": ["TOYOTA MOTOR"],
    "toshiba": ["TOSHIBA"],
    "mitsubishi_electric": ["MITSUBISHI ELECTRIC"],
    "hitachi": ["HITACHI"],
    "ricoh": ["RICOH"],
    "fujifilm": ["FUJIFILM"],
    "denso": ["DENSO"],
    "sharp": ["SHARP CORP"],
    "fujitsu": ["FUJITSU"],
    "honda": ["HONDA MOTOR"],
    "seiko_epson": ["SEIKO EPSON"],
    "kyocera": ["KYOCERA"],
    "sony": ["SONY"],
    "ntt": ["NIPPON TELEGRAPH"],
    "dnp": ["DAI NIPPON PRINTING"],
    "nissan": ["NISSAN MOTOR"],
    "konica_minolta": ["KONICA MINOLTA"],
    "nec": ["NEC CORP"],
    "sumitomo_electric": ["SUMITOMO ELECTRIC"],
    "nippon_steel": ["NIPPON STEEL"],
    "toppan": ["TOPPAN"],
    "kao": ["KAO CORP"],
    "jfe": ["JFE"],
    "yazaki": ["YAZAKI"],
    "murata": ["MURATA"],
    "toray": ["TORAY"],
    "bridgestone": ["BRIDGESTONE"],
    "sekisui_chemical": ["SEKISUI CHEMICAL"],
    "mitsubishi_heavy": ["MITSUBISHI HEAVY"],
    "sumitomo_chemical": ["SUMITOMO CHEMICAL"],
    "brother": ["BROTHER INDUSTRIES"],
    "resonac": ["RESONAC"],
    "fuji_electric": ["FUJI ELECTRIC"],
    "daikin": ["DAIKIN"],
    "olympus": ["OLYMPUS"],
    "mazda": ["MAZDA"],
    "suzuki": ["SUZUKI MOTOR"],
    "subaru": ["SUBARU"],
    "asahi_kasei": ["ASAHI KASEI"],
    "nikon": ["NIKON"],
    "omron": ["OMRON"],
    "ihi": ["IHI CORP"],
    "kobe_steel": ["KOBE STEEL"],
    "yaskawa": ["YASKAWA"],
    "kubota": ["KUBOTA"],
    "mitsubishi_chemical": ["MITSUBISHI CHEMICAL"],
    "sumitomo_metal_mining": ["SUMITOMO METAL MINING"],
    "aisin": ["AISIN"],

    # ═══ Ticker-based firms ═══
    "company_6724": ["SEIKO EPSON"],
    "company_6758": ["SONY"],
    "company_6701": ["NEC CORP"],
    "company_6752": ["PANASONIC"],
    "company_4901": ["FUJIFILM"],
    "company_6448": ["BROTHER INDUSTRIES"],
    "company_7011": ["MITSUBISHI HEAVY"],
    "company_7731": ["NIKON"],
    "company_6952": ["CASIO"],
    "company_7733": ["OLYMPUS"],
    "company_6417": ["SANKYO"],
    "company_6367": ["DAIKIN"],
    "company_6326": ["KUBOTA"],
    "company_6201": ["TOYOTA INDUSTRIES"],
    "company_5110": ["SUMITOMO RUBBER"],
    "company_6473": ["JTEKT"],
    "company_5101": ["YOKOHAMA RUBBER"],
    "company_4063": ["SHIN-ETSU CHEMICAL"],
    "company_4183": ["MITSUI CHEMICALS"],
    "company_6425": ["UNIVERSAL ENTERTAINMENT"],
    "company_9504": ["CHUGOKU ELECTRIC"],
    "company_7012": ["KAWASAKI HEAVY"],
    "company_9532": ["OSAKA GAS"],
    "company_6723": ["RENESAS"],
    "company_7282": ["TOYODA GOSEI"],
    "company_6412": ["HEIWA"],
    "company_6841": ["YOKOGAWA ELECTRIC"],
    "company_9984": ["SOFTBANK"],
    "company_5332": ["TOTO"],
    "company_1801": ["TAISEI CORP"],
    "company_4042": ["TOSOH"],
    "company_4205": ["ZEON CORP"],
    "company_1802": ["OBAYASHI"],
    "company_1812": ["KAJIMA"],
    "company_6923": ["STANLEY ELECTRIC"],
    "company_9531": ["TOKYO GAS"],
    "company_6632": ["JVC KENWOOD"],
    "company_6506": ["YASKAWA"],
    "company_4188": ["MITSUBISHI CHEMICAL"],
    "company_6257": ["FUJISHOJI"],
    "company_3880": ["DAIO PAPER"],
    "company_5019": ["IDEMITSU"],
    "company_8113": ["UNICHARM"],
    "company_5947": ["RINNAI"],
    "company_6508": ["MEIDENSHA"],
    "company_7966": ["LINTEC"],
    "company_3401": ["TEIJIN"],
    "company_6965": ["HAMAMATSU PHOTONICS"],
    "company_5938": ["LIXIL"],
    "company_4182": ["MITSUBISHI GAS CHEMICAL"],
    "company_4912": ["LION CORP"],
    "company_7276": ["KOITO"],
    "company_6370": ["KURITA WATER"],
    "company_5214": ["NIPPON ELECTRIC GLASS"],
    "company_9501": ["TOKYO ELECTRIC POWER"],
    "company_6740": ["JAPAN DISPLAY"],
    "company_5233": ["TAIHEIYO CEMENT"],
    "company_5943": ["NORITZ"],
    "company_7240": ["NOK CORP"],
    "company_4062": ["IBIDEN"],
    "company_7287": ["NIPPON SEIKI"],
    "company_4911": ["SHISEIDO"],
    "company_7309": ["SHIMANO"],
    "company_1925": ["DAIWA HOUSE"],
    "company_6586": ["MAKITA"],
    "company_3864": ["MITSUBISHI PAPER"],
    "company_6728": ["ULVAC"],
    "company_7205": ["HINO MOTORS"],
    "company_4471": ["SANYO CHEMICAL"],
    "company_4272": ["NIPPON KAYAKU"],
    "company_2802": ["AJINOMOTO"],
    "company_5901": ["TOYO SEIKAN"],
    "company_7735": ["SCREEN HOLDINGS"],
    "company_3861": ["OJI HOLDINGS"],
    "company_6383": ["DAIFUKU"],
    "company_6925": ["USHIO"],
    "company_1928": ["SEKISUI HOUSE"],
    "company_7280": ["MITSUBA"],
    "company_7762": ["CITIZEN"],
    "company_3103": ["UNITIKA"],
    "company_6976": ["TAIYO YUDEN"],
    "company_2654": ["ASMO"],
    "company_6113": ["AMADA"],
    "company_6413": ["RISO KAGAKU"],
    "company_3101": ["TOYOBO"],
    "company_7313": ["TS TECH"],
    "company_7970": ["SHIN-ETSU POLYMER"],
    "company_4186": ["TOKYO OHKA"],
    "company_3863": ["NIPPON PAPER"],
    "company_6807": ["JAPAN AVIATION ELECTRONICS"],
    "company_5232": ["SUMITOMO OSAKA CEMENT"],
    "company_6457": ["GLORY"],
    "company_6444": ["SANDEN"],
    "company_6845": ["AZBIL"],
    "company_4043": ["TOKUYAMA"],
    "company_7984": ["KOKUYO"],
    "company_6622": ["DAIHEN"],
    "company_6744": ["NOHMI BOSAI"],
    "company_7744": ["NORITSU KOKI"],
    "company_4228": ["SEKISUI KASEI"],
    "company_5471": ["DAIDO STEEL"],
    "company_8060": ["CANON MARKETING"],
    "company_6857": ["ADVANTEST"],
    "company_9020": ["EAST JAPAN RAILWAY"],
    "company_5631": ["JAPAN STEEL WORKS"],
    "company_6741": ["NIPPON SIGNAL"],
    "company_6754": ["ANRITSU"],
    "company_6134": ["FUJI CORP"],
    "company_6454": ["MAX CO"],
    "company_4044": ["CENTRAL GLASS"],
    "company_6745": ["HOCHIKI"],
    "company_6005": ["MIURA CO"],
    "company_7974": ["NINTENDO"],
    "company_7283": ["AISAN INDUSTRY"],
    "company_5202": ["NIPPON SHEET GLASS"],
    "company_7976": ["MITSUBISHI PENCIL"],
    "company_6440": ["JUKI"],
    "company_6810": ["MAXELL"],
    "company_9503": ["KANSAI ELECTRIC"],
    "company_4613": ["KANSAI PAINT"],
    "company_7259": ["AISIN CORP"],
    "company_6287": ["SATO HOLDINGS"],
    "company_2914": ["JAPAN TOBACCO"],
    "company_4401": ["ADEKA"],
    "company_5991": ["NHK SPRING"],
    "company_7729": ["TOKYO SEIMITSU"],
    "company_6951": ["JEOL"],
    "company_6844": ["SHINDENGEN"],
    "company_6590": ["SHIBAURA"],
    "company_7224": ["SHINMAYWA"],
    "company_7988": ["NIFCO"],
    "company_4755": ["RAKUTEN"],
    "company_5930": ["BUNKA SHUTTER"],
    "company_6368": ["ORGANO"],
    "company_3002": ["GUNZE"],
    "company_6866": ["HIOKI"],
    "company_4967": ["KOBAYASHI PHARMACEUTICAL"],
    "company_3436": ["SUMCO"],
    "company_4045": ["TOAGOSEI"],
    "company_7102": ["NIPPON SHARYO"],
    "company_7244": ["ICHIKOH"],
    "company_4061": ["DENKA"],
    "company_6268": ["NABTESCO"],
    "company_4202": ["DAICEL"],
    "company_4403": ["NOF CORP"],
    "company_8086": ["NIPRO"],
    "company_6869": ["SYSMEX"],
    "company_5602": ["KURIMOTO"],
    "company_5909": ["CORONA CORP"],
    "company_4922": ["KOSE"],
    "company_3632": ["GREE"],
    "company_5703": ["NIPPON LIGHT METAL"],
    "company_6856": ["HORIBA"],
    "company_6707": ["SANKEN ELECTRIC"],
    "company_4502": ["TAKEDA PHARMACEUTICAL"],
    "company_6942": ["SOPHIA HOLDINGS"],
    "company_7972": ["ITOKI"],
    "company_6779": ["NDK"],
    "company_7911": ["TOPPAN HOLDINGS"],
    "company_9502": ["CHUBU ELECTRIC"],
    "company_7739": ["CANON ELECTRONICS"],
    "company_6371": ["TSUBAKIMOTO"],
    "company_6430": ["DAIKOKU DENKI"],
    "company_5192": ["MITSUBOSHI BELTING"],
    "company_4980": ["DEXERIALS"],
    "company_6479": ["MINEBEA MITSUMI"],
    "company_5105": ["TOYO TIRE"],
    "company_6768": ["TAMURA CORP"],
    "company_1833": ["OKUMURA CORP"],
    "company_6718": ["AIPHONE"],
    "company_4307": ["NOMURA RESEARCH INSTITUTE"],
    "company_6406": ["FUJITEC"],
    "company_1861": ["KUMAGAI GUMI"],
    "company_6339": ["SINTOKOGIO"],
    "company_9735": ["SECOM"],
    "company_7931": ["MIRAI INDUSTRIES"],
    "company_7846": ["PILOT CORP"],
    "company_5195": ["BANDO CHEMICAL"],
    "company_4041": ["NIPPON SODA"],
    "company_6340": ["SHIBUYA CORP"],
    "company_5932": ["SANKYO TATEYAMA"],
    "company_4212": ["SEKISUI JUSHI"],
    "company_7291": ["NIHON PLAST"],
    "company_6395": ["TADANO"],
    "company_6376": ["NIKKISO"],
    "company_6996": ["NICHICON"],
    "company_6103": ["OKUMA"],
    "company_7990": ["GLOBERIDE"],
    "company_6961": ["ENPLAS"],
    "company_7965": ["ZOJIRUSHI"],
    "company_4612": ["NIPPON PAINT"],
    "company_3941": ["RENGO"],
    "company_5016": ["JX METALS"],
    "company_7239": ["TACHI-S"],
    "company_2809": ["KEWPIE"],
    "company_4684": ["OBIC"],
    "company_6407": ["CKD CORP"],
    "company_1893": ["PENTA-OCEAN"],
    "company_6474": ["NACHI-FUJIKOSHI"],
    "company_6651": ["NITTO KOGYO"],
    "company_9022": ["CENTRAL JAPAN RAILWAY"],
    "company_3668": ["COLOPL"],
    "company_2607": ["FUJI OIL"],
    "company_4116": ["DAINICHISEIKA"],
    "company_6997": ["NIPPON CHEMI-CON"],
    "company_7458": ["DAIICHIKOSHO"],
    "company_7952": ["KAWAI MUSICAL"],
    "company_6507": ["SYMPHONIA"],
    "company_9533": ["TOHO GAS"],
    "company_4461": ["DKS CO"],
    "company_9697": ["CAPCOM"],
    "company_6638": ["MIMAKI"],
    "company_6814": ["FURUNO"],
    "company_6273": ["SMC CORP"],
    "company_6481": ["THK"],
    "company_5161": ["NISHIKAWA RUBBER"],
    "company_6955": ["FDK"],
    "company_6770": ["ALPS ALPINE"],
    "company_4021": ["NISSAN CHEMICAL"],
    "company_6675": ["SAXA"],
    "company_1860": ["TODA CORP"],
    "company_6282": ["OILES"],
    "company_4527": ["ROHTO"],
    "company_5142": ["ACHILLES"],
    "company_4519": ["CHUGAI PHARMACEUTICAL"],
    "company_6804": ["HOSIDEN"],
    "company_7914": ["KYODO PRINTING"],
    "company_4023": ["KUREHA"],
    "company_7256": ["KASAI KOGYO"],
    "company_5393": ["NICHIAS"],
    "company_1820": ["NISHIMATSU"],
    "company_6806": ["HIROSE ELECTRIC"],
    "company_2602": ["NISSHIN OILLIO"],
    "company_5988": ["PIOLAX"],
    "company_4008": ["SUMITOMO SEIKA"],
    "company_4206": ["AICA KOGYO"],
    "company_5186": ["NITTA CORP"],
    "company_6798": ["SMK CORP"],
    "company_6875": ["MEGACHIPS"],
    "company_1911": ["SUMITOMO FORESTRY"],
    "company_7278": ["EXEDY"],
    "company_7994": ["OKAMURA"],
    "company_4968": ["ARAKAWA CHEMICAL"],
    "company_3569": ["SEIREN"],
    "company_3526": ["ASHIMORI"],
    "company_7989": ["TACHIKAWA"],
    "company_8022": ["MIZUNO"],
    "company_5851": ["RYOBI"],
    "company_7864": ["FUJI SEAL"],
    "company_2264": ["MORINAGA MILK"],
    "company_7238": ["AKEBONO BRAKE"],
    "company_7817": ["PARAMOUNT BED"],
    "company_6525": ["KOKUSAI ELECTRIC"],
    "company_6742": ["KYOSAN ELECTRIC"],
    "company_6486": ["EAGLE INDUSTRY"],
    "company_2432": ["DENA"],
    "company_1961": ["SANKI ENGINEERING"],
    "company_9766": ["KONAMI"],
    "company_6505": ["TOYO DENKI"],
    "company_6013": ["TAKUMA"],
    "company_6470": ["TAIHO KOGYO"],
    "company_4760": ["ALPHA CORP"],
    "company_4100": ["TODA KOGYO"],
    "company_1969": ["TAKASAGO THERMAL"],
    "company_3106": ["KURABO"],
    "company_7226": ["KYOKUTO KAIHATSU"],
    "company_5715": ["FURUKAWA MACHINERY"],
    "company_6849": ["NIHON KOHDEN"],
    "company_6222": ["SHIMA SEIKI"],
    "company_6966": ["MITSUI HIGH-TEC"],
    "company_5352": ["KROSAKI HARIMA"],
    "company_2269": ["MEIJI HOLDINGS"],
    "company_5741": ["UACJ"],
    "company_4611": ["DAI NIPPON TORYO"],
    "company_6986": ["FUTABA ELECTRONICS"],
    "company_2897": ["NISSIN FOODS"],
    "company_6141": ["DMG MORI"],
    "company_4917": ["MANDOM"],
    "company_6803": ["TEAC"],
    "company_4985": ["EARTH CHEMICAL"],
    "company_4507": ["SHIONOGI"],
    "company_6315": ["TOWA CORP"],
    "company_4246": ["DAIKYO NISHIKAWA"],
    "company_2121": ["MIXI"],
    "company_6293": ["NISSEI PLASTIC"],
    "company_7943": ["NICHIHA"],
    "company_5218": ["OHARA"],
    "company_5976": ["NETUREN"],
    "company_2801": ["KIKKOMAN"],
    "company_6349": ["KOMORI"],
    "company_7740": ["TAMRON"],
    "company_4092": ["NIPPON CHEMICAL"],
    "company_6436": ["AMANO"],
    "company_6465": ["HOSHIZAKI"],
    "company_7247": ["MIKUNI"],
    "company_7885": ["TAKANO"],
    "company_6960": ["FUKUDA DENSHI"],
    "company_5384": ["FUJIMI"],
    "company_9551": ["METAWATER"],
    "company_6727": ["WACOM"],
    "company_4928": ["NOEVIR"],
    "company_6217": ["TSUDAKOMA"],
    "company_7747": ["ASAHI INTECC"],
    "company_4633": ["SAKATA INX"],
    "company_9506": ["TOHOKU ELECTRIC"],
    "company_3110": ["NITTO BOSEKI"],
    "company_5727": ["TOHO TITANIUM"],
    "company_7241": ["FUTABA INDUSTRIAL"],
    "company_6238": ["FURYU"],
    "company_6871": ["JAPAN MICRONICS"],
    "company_6824": ["NEW COSMOS"],
    "company_6962": ["DAISHINKU"],
    "company_5957": ["NITTO SEIKO"],
    "company_1964": ["CHUGAI RO"],
    "company_6859": ["ESPEC"],
    "company_1720": ["TOKYU CONSTRUCTION"],
    "company_7871": ["FUKUVI"],
    "company_6820": ["ICOM"],
    "company_6999": ["KOA CORP"],
    "company_4095": ["NIHON PARKERIZING"],
    "company_3352": ["BUFFALO"],
    "company_6143": ["SODICK"],
    "company_7944": ["ROLAND"],
    "company_4689": ["LINE YAHOO"],
    "company_4064": ["NIPPON CARBIDE"],
    "company_6800": ["YOKOWO"],
    "company_4220": ["RIKEN TECHNOS"],
    "company_7822": ["EIDAI"],
    "company_4028": ["ISHIHARA SANGYO"],
    "company_6526": ["SOCIONEXT"],
    "company_2503": ["KIRIN"],
    "company_4088": ["AIR WATER"],
    "company_1885": ["TOA CORP"],
    "company_4463": ["NICCA CHEMICAL"],
    "company_5482": ["AICHI STEEL"],
    "company_4503": ["ASTELLAS"],
    "company_4914": ["TAKASAGO INTERNATIONAL"],
    "company_9021": ["WEST JAPAN RAILWAY"],
    "company_4530": ["HISAMITSU"],
    "company_7279": ["HI-LEX"],
    "company_6203": ["HOWA MACHINERY"],
    "company_7231": ["TOPY INDUSTRIES"],
    "company_5959": ["OKABE"],
    "company_9508": ["KYUSHU ELECTRIC"],
    "company_6366": ["CHIYODA CORP"],
    "company_7702": ["JMS"],
    "company_2267": ["YAKULT"],
    "company_7717": ["V TECHNOLOGY"],
    "company_7955": ["CLEANUP"],
    "company_4112": ["HODOGAYA CHEMICAL"],
    "company_6135": ["MAKINO MILLING"],
    "company_7718": ["STAR MICRONICS"],
    "company_4958": ["HASEGAWA FRAGRANCE"],
    "company_6498": ["KITZ"],
    "company_7942": ["JSP CORP"],
    "company_4404": ["MIYOSHI OIL"],
    "company_6941": ["YAMAICHI ELECTRONICS"],
    "company_6516": ["SANYO DENKI"],
    "company_7245": ["DAIDO METAL"],
    "company_2593": ["ITO EN"],
    "company_3104": ["FUJIBO"],
    "company_7266": ["IMASEN ELECTRIC"],
    "company_5363": ["TYK"],
    "company_6151": ["NITTO KOHKI"],
    "company_6817": ["SUMIDA"],
    "company_4626": ["TAIYO HOLDINGS"],
    "company_4221": ["OKURA INDUSTRIAL"],
    "company_7250": ["PACIFIC INDUSTRIAL"],
    "company_4628": ["SK KAKEN"],
    "company_9837": ["MORITO"],
    "company_4634": ["ARTIENCE"],
    "company_6118": ["AIDA ENGINEERING"],
    "company_7236": ["TRAD"],
    "company_4998": ["FUMAKILLA"],
    "company_6763": ["TEIKOKU TSUSHIN"],
    "company_6316": ["MARUYAMA MFG"],
    "company_7956": ["PIGEON"],
    "company_7936": ["ASICS"],
    "company_4365": ["MATSUMOTO YUSHI"],
    "company_5933": ["ALINCO"],
    "company_6946": ["JAPAN AVIONICS"],
    "company_4528": ["ONO PHARMACEUTICAL"],
    "company_7908": ["KIMOTO"],
    "company_5992": ["CHUHATSU"],
    "company_4526": ["RIKEN VITAMIN"],
    "company_5020": ["ENEOS"],
    "company_6858": ["ONO SOKKI"],
    "company_6489": ["MAEZAWA"],
    "company_9513": ["J-POWER"],
    "company_5210": ["NIHON YAMAMURA"],
    "company_8012": ["NAGASE"],
    "company_5906": ["MK SEIKO"],
    "company_7979": ["SHOFU"],
    "company_7105": ["MITSUBISHI LOGISNEXT"],
    "company_6351": ["TSURUMI MFG"],
    "company_1719": ["HAZAMA ANDO"],
    "company_4536": ["SANTEN"],
    "company_7867": ["TAKARA TOMY"],
    "company_7723": ["AICHI TOKEI"],
    "company_4078": ["SAKAI CHEMICAL"],
    "company_7780": ["MENICON"],
    "company_2270": ["MEGMILK SNOW BRAND"],
    "company_7292": ["MURAKAMI CORP"],
    "company_5714": ["DOWA HOLDINGS"],
}

# Duplicate mapping (same company, two firm_ids)
DUPLICATE_MAP: dict[str, list[str]] = {
    "seiko_epson": ["company_6724"],
    "sony": ["company_6758"],
    "nec": ["company_6701"],
    "panasonic": ["company_6752"],
    "fujifilm": ["company_4901"],
    "brother": ["company_6448"],
    "mitsubishi_heavy": ["company_7011"],
    "nikon": ["company_7731"],
    "olympus": ["company_7733"],
    "daikin": ["company_6367"],
    "kubota": ["company_6326"],
    "yaskawa": ["company_6506"],
    "mitsubishi_chemical": ["company_4188"],
    "aisin": ["company_7259"],
}

_TICKER_TO_CANONICAL = {}
for _c, _ts in DUPLICATE_MAP.items():
    for _t in _ts:
        _TICKER_TO_CANONICAL[_t] = _c


def _normalize(v: float, k: float = 10.0) -> float:
    v = max(float(v), 0.0)
    return v / (v + k)


def _quarter_bounds(year_from: int, year_to: int) -> list[tuple[int, int, str, str]]:
    """Return [(year, quarter, date_from, date_to), ...]."""
    from datetime import date
    result = []
    for y in range(year_from, year_to + 1):
        for q in range(1, 5):
            m = (q - 1) * 3 + 1
            d_from = date(y, m, 1)
            d_to = date(y, m + 3, 1) if q < 4 else date(y + 1, 1, 1)
            result.append((y, q, d_from.isoformat(), d_to.isoformat()))
    return result


def extract_firm_features_fast(
    client: bigquery.Client,
    company_names: list[str],
    year_from: int = 2020,
    year_to: int = 2024,
) -> list[dict]:
    """Extract 5-axis features for ONE firm across ALL quarters in 2 BigQuery queries."""

    # Build company patterns for LIKE matching
    like_clauses_gkg = " OR ".join(
        f"LOWER(V2Organizations) LIKE @p{i}" for i in range(len(company_names))
    )
    like_clauses_events = " OR ".join(
        f"(LOWER(Actor1Name) LIKE @p{i} OR LOWER(Actor2Name) LIKE @p{i})"
        for i in range(len(company_names))
    )

    params = [
        bigquery.ScalarQueryParameter(f"p{i}", "STRING", f"%{name.lower()}%")
        for i, name in enumerate(company_names)
    ]
    params.extend([
        bigquery.ScalarQueryParameter("date_from", "STRING", f"{year_from}-01-01"),
        bigquery.ScalarQueryParameter("date_to", "STRING", f"{year_to + 1}-01-01"),
    ])

    # ── GKG Query: aggregate by quarter ──
    gkg_sql = f"""
    SELECT
        EXTRACT(YEAR FROM _PARTITIONTIME) AS yr,
        EXTRACT(QUARTER FROM _PARTITIONTIME) AS qtr,
        COUNT(*) AS gkg_count,
        -- Direction themes
        COUNTIF(
            REGEXP_CONTAINS(LOWER(IFNULL(V2Themes, '')),
                r'new_product|expansion|strategy|env_green|econ_entrepreneurship')
        ) AS direction_count,
        -- Investment themes
        COUNTIF(
            REGEXP_CONTAINS(LOWER(IFNULL(V2Themes, '')),
                r'investment|acquisition|merger|econ_debt')
        ) AS investment_theme_count,
        -- Governance themes
        COUNTIF(
            REGEXP_CONTAINS(LOWER(IFNULL(V2Themes, '')),
                r'scandal|lawsuit|crisislex')
        ) AS governance_theme_count,
        -- Org co-occurrence (multiple orgs in same article)
        SUM(GREATEST(ARRAY_LENGTH(SPLIT(IFNULL(V2Organizations, ''), ';')) - 1, 0)) AS org_co_occurrence,
        -- Person mentions
        SUM(ARRAY_LENGTH(SPLIT(IFNULL(V2Persons, ''), ';'))) AS person_mentions,
        -- Average tone (for leadership)
        AVG(SAFE_CAST(SPLIT(IFNULL(V2Tone, '0'), ',')[OFFSET(0)] AS FLOAT64)) AS avg_tone,
        -- Unique sources
        COUNT(DISTINCT DocumentIdentifier) AS unique_sources_gkg
    FROM `{GKG_TABLE}`
    WHERE _PARTITIONTIME >= TIMESTAMP(@date_from)
      AND _PARTITIONTIME < TIMESTAMP(@date_to)
      AND ({like_clauses_gkg})
    GROUP BY yr, qtr
    ORDER BY yr, qtr
    """

    # ── Events Query: aggregate by quarter ──
    events_sql = f"""
    SELECT
        CAST(FLOOR(SQLDATE / 10000) AS INT64) AS yr,
        CAST(CEIL(MOD(CAST(FLOOR(SQLDATE / 100) AS INT64), 100) / 3.0) AS INT64) AS qtr,
        COUNT(*) AS event_count,
        SUM(IFNULL(NumMentions, 0)) AS total_mentions_events,
        -- Cooperative events (QuadClass 1-2)
        COUNTIF(QuadClass IN (1, 2)) AS coop_events,
        -- Adversarial events (QuadClass 3-4)
        COUNTIF(QuadClass IN (3, 4)) AS adverse_events,
        -- High Goldstein (investment signal)
        COUNTIF(GoldsteinScale > 5.0) AS high_goldstein,
        -- Negative Goldstein (governance signal)
        COUNTIF(GoldsteinScale < -5.0) AS neg_goldstein,
        -- Unique sources
        COUNT(DISTINCT SOURCEURL) AS unique_sources_events
    FROM `{EVENTS_TABLE}`
    WHERE SQLDATE >= CAST(FORMAT('%d0101', @date_from) AS INT64)
      AND SQLDATE < CAST(FORMAT('%d0101', @date_to) AS INT64)
      AND ({like_clauses_events})
    GROUP BY yr, qtr
    ORDER BY yr, qtr
    """

    # Fix: SQLDATE date parsing
    events_sql = f"""
    SELECT
        CAST(FLOOR(SQLDATE / 10000) AS INT64) AS yr,
        CASE
            WHEN MOD(CAST(FLOOR(SQLDATE / 100) AS INT64), 100) <= 3 THEN 1
            WHEN MOD(CAST(FLOOR(SQLDATE / 100) AS INT64), 100) <= 6 THEN 2
            WHEN MOD(CAST(FLOOR(SQLDATE / 100) AS INT64), 100) <= 9 THEN 3
            ELSE 4
        END AS qtr,
        COUNT(*) AS event_count,
        SUM(IFNULL(NumMentions, 0)) AS total_mentions_events,
        COUNTIF(QuadClass IN (1, 2)) AS coop_events,
        COUNTIF(QuadClass IN (3, 4)) AS adverse_events,
        COUNTIF(GoldsteinScale > 5.0) AS high_goldstein,
        COUNTIF(GoldsteinScale < -5.0) AS neg_goldstein,
        COUNT(DISTINCT SOURCEURL) AS unique_sources_events
    FROM `{EVENTS_TABLE}`
    WHERE SQLDATE >= {year_from}0101
      AND SQLDATE < {year_to + 1}0101
      AND ({like_clauses_events})
    GROUP BY yr, qtr
    ORDER BY yr, qtr
    """

    job_config = bigquery.QueryJobConfig(query_parameters=params)

    # Run both queries (could parallelize but BQ handles it)
    gkg_results = {}
    try:
        for row in client.query(gkg_sql, job_config=job_config).result():
            key = (int(row.yr), int(row.qtr))
            gkg_results[key] = dict(row)
    except Exception as e:
        print(f"    GKG query error: {e}")

    events_params = [
        bigquery.ScalarQueryParameter(f"p{i}", "STRING", f"%{name.lower()}%")
        for i, name in enumerate(company_names)
    ]
    events_config = bigquery.QueryJobConfig(query_parameters=events_params)

    events_results = {}
    try:
        for row in client.query(events_sql, job_config=events_config).result():
            key = (int(row.yr), int(row.qtr))
            events_results[key] = dict(row)
    except Exception as e:
        print(f"    Events query error: {e}")

    # Combine into quarterly features
    features_list = []
    for y in range(year_from, year_to + 1):
        for q in range(1, 5):
            gkg = gkg_results.get((y, q), {})
            evt = events_results.get((y, q), {})

            direction_count = gkg.get("direction_count", 0) or 0
            openness_count = (evt.get("coop_events", 0) or 0) + (gkg.get("org_co_occurrence", 0) or 0)
            investment_count = (gkg.get("investment_theme_count", 0) or 0) + (evt.get("high_goldstein", 0) or 0)
            governance_count = (
                (gkg.get("governance_theme_count", 0) or 0)
                + (evt.get("adverse_events", 0) or 0)
                + (evt.get("neg_goldstein", 0) or 0)
            )

            person_mentions = gkg.get("person_mentions", 0) or 0
            avg_tone = gkg.get("avg_tone", 0) or 0
            leadership_count = person_mentions + max(float(avg_tone), 0.0)

            total_mentions = (gkg.get("gkg_count", 0) or 0) + (evt.get("total_mentions_events", 0) or 0)
            total_sources = (gkg.get("unique_sources_gkg", 0) or 0) + (evt.get("unique_sources_events", 0) or 0)

            features_list.append({
                "year": y,
                "quarter": q,
                "direction_score": _normalize(direction_count),
                "openness_score": _normalize(openness_count),
                "investment_score": _normalize(investment_count),
                "governance_friction_score": _normalize(governance_count),
                "leadership_score": _normalize(leadership_count),
                "total_mentions": total_mentions,
                "total_sources": total_sources,
                "raw_data": json.dumps({
                    "gkg_count": gkg.get("gkg_count", 0),
                    "event_count": evt.get("event_count", 0),
                    "direction_count": direction_count,
                    "openness_count": openness_count,
                    "investment_count": investment_count,
                    "governance_count": governance_count,
                    "person_mentions": person_mentions,
                    "avg_tone": avg_tone,
                }),
            })

    return features_list


def run_extraction(
    db_path: str = "data/patents.db",
    year_from: int = 2020,
    year_to: int = 2024,
    max_firms: int = 600,
    skip_existing: bool = True,
) -> int:
    client = bigquery.Client(project="unique-sentinel-473401-s0")

    conn = sqlite3.connect(db_path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")

    # Ensure table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gdelt_company_features (
            firm_id TEXT NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            direction_score REAL,
            openness_score REAL,
            investment_score REAL,
            governance_friction_score REAL,
            leadership_score REAL,
            total_mentions INTEGER,
            total_sources INTEGER,
            raw_data TEXT,
            PRIMARY KEY (firm_id, year, quarter)
        )
    """)

    existing_firms = set()
    if skip_existing:
        rows = conn.execute("SELECT DISTINCT firm_id FROM gdelt_company_features").fetchall()
        existing_firms = {r[0] for r in rows}
        print(f"Existing firms in DB: {len(existing_firms)}")

    # Build work list
    firms_to_query = []
    firms_to_copy = []

    all_firms = list(FIRM_GDELT_NAMES.items())[:max_firms]

    for firm_id, search_names in all_firms:
        canonical = _TICKER_TO_CANONICAL.get(firm_id)
        if canonical:
            firms_to_copy.append((canonical, firm_id))
            continue
        if skip_existing and firm_id in existing_firms:
            continue
        firms_to_query.append((firm_id, search_names))

    n_quarters = (year_to - year_from + 1) * 4
    print(f"\nWork plan:")
    print(f"  Firms to query: {len(firms_to_query)} (2 BQ queries each)")
    print(f"  Firms to copy (dup): {len(firms_to_copy)}")
    print(f"  Quarters: {n_quarters}")
    sys.stdout.flush()

    inserted = 0
    errors = 0
    start_time = time.time()

    for fi, (firm_id, search_names) in enumerate(firms_to_query):
        firm_start = time.time()
        try:
            features_list = extract_firm_features_fast(
                client, search_names, year_from, year_to
            )

            for feat in features_list:
                conn.execute(
                    """INSERT OR REPLACE INTO gdelt_company_features
                    (firm_id, year, quarter, direction_score, openness_score,
                     investment_score, governance_friction_score, leadership_score,
                     total_mentions, total_sources, raw_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (firm_id, feat["year"], feat["quarter"],
                     feat["direction_score"], feat["openness_score"],
                     feat["investment_score"], feat["governance_friction_score"],
                     feat["leadership_score"], feat["total_mentions"],
                     feat["total_sources"], feat["raw_data"]),
                )
                inserted += 1

            conn.commit()
        except Exception as e:
            errors += 1
            print(f"  ERROR {firm_id}: {e}")
            sys.stdout.flush()

        firm_elapsed = time.time() - firm_start
        total_elapsed = time.time() - start_time
        remaining = len(firms_to_query) - (fi + 1)
        avg_per_firm = total_elapsed / (fi + 1)
        eta_min = avg_per_firm * remaining / 60

        mentions = sum(f.get("total_mentions", 0) for f in features_list) if "features_list" in dir() else 0
        print(
            f"  [{fi+1}/{len(firms_to_query)}] {firm_id}: {firm_elapsed:.1f}s "
            f"({n_quarters}q, {mentions} mentions) "
            f"[total: {inserted} ok, {errors} err, ETA: {eta_min:.0f}m]"
        )
        sys.stdout.flush()

    # Phase 2: Copy duplicates
    copied = 0
    for source_firm, dest_firm in firms_to_copy:
        if dest_firm in existing_firms and skip_existing:
            continue
        rows = conn.execute(
            "SELECT year, quarter, direction_score, openness_score, "
            "investment_score, governance_friction_score, leadership_score, "
            "total_mentions, total_sources, raw_data "
            "FROM gdelt_company_features WHERE firm_id = ?",
            (source_firm,),
        ).fetchall()

        for row in rows:
            conn.execute(
                """INSERT OR REPLACE INTO gdelt_company_features
                (firm_id, year, quarter, direction_score, openness_score,
                 investment_score, governance_friction_score, leadership_score,
                 total_mentions, total_sources, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (dest_firm, *row),
            )
            copied += 1
        conn.commit()
        if rows:
            print(f"  COPIED {source_firm} → {dest_firm}: {len(rows)}q")

    conn.close()
    elapsed = time.time() - start_time

    print(f"\n{'='*60}")
    print(f"GDELT fast extraction complete.")
    print(f"Queried: {inserted} quarter-records ({errors} firm errors)")
    print(f"Copied:  {copied} (duplicates)")
    print(f"Elapsed: {elapsed:.1f}s ({elapsed/60:.1f}m)")
    print(f"{'='*60}")
    sys.stdout.flush()

    return inserted + copied


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/patents.db")
    parser.add_argument("--year-from", type=int, default=2020)
    parser.add_argument("--year-to", type=int, default=2024)
    parser.add_argument("--max-firms", type=int, default=600)
    parser.add_argument("--no-skip", action="store_true")
    args = parser.parse_args()

    run_extraction(
        db_path=args.db,
        year_from=args.year_from,
        year_to=args.year_to,
        max_firms=args.max_firms,
        skip_existing=not args.no_skip,
    )


if __name__ == "__main__":
    main()
