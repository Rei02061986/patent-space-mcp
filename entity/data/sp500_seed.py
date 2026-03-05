"""S&P 500 companies with high patent filing activity and name aliases.

Initial seed: top ~100 US patent filers from S&P 500 constituents.
BigQuery assignee_harmonized names are UPPERCASE; aliases include
common variations, Japanese katakana transliterations, and ticker symbols.
"""
from entity.registry import Entity

SP500_ENTITIES: list[Entity] = [
    # =========================================================================
    # Technology — Hardware, Software, Semiconductors
    # =========================================================================
    Entity(
        "apple", "Apple Inc.", "US", "corporation",
        {
            "アップル", "Apple", "APPLE INC", "APPLE INC.",
            "Apple Computer", "APPLE COMPUTER INC",
            "Apple Computer, Inc.",
        },
        industry="technology", ticker="AAPL",
    ),
    Entity(
        "microsoft", "Microsoft Corporation", "US", "corporation",
        {
            "マイクロソフト", "Microsoft", "MICROSOFT CORP",
            "MICROSOFT CORPORATION", "Microsoft Corp",
            "Microsoft Corp.",
        },
        industry="technology", ticker="MSFT",
    ),
    Entity(
        "alphabet", "Alphabet Inc.", "US", "corporation",
        {
            "グーグル", "アルファベット", "Google", "Alphabet",
            "ALPHABET INC", "GOOGLE INC", "GOOGLE LLC",
            "Google Inc.", "Google LLC",
        },
        industry="technology", ticker="GOOG",
    ),
    Entity(
        "amazon", "Amazon.com, Inc.", "US", "corporation",
        {
            "アマゾン", "Amazon", "AMAZON COM INC", "AMAZON.COM INC",
            "AMAZON TECHNOLOGIES INC", "Amazon.com",
            "Amazon Technologies, Inc.",
        },
        industry="technology", ticker="AMZN",
    ),
    Entity(
        "meta", "Meta Platforms, Inc.", "US", "corporation",
        {
            "メタ", "フェイスブック", "Meta", "Meta Platforms",
            "META PLATFORMS INC", "FACEBOOK INC", "Facebook",
            "Facebook, Inc.",
        },
        industry="technology", ticker="META",
    ),
    Entity(
        "intel", "Intel Corporation", "US", "corporation",
        {
            "インテル", "Intel", "INTEL CORP", "INTEL CORPORATION",
            "Intel Corp",
        },
        industry="semiconductors", ticker="INTC",
    ),
    Entity(
        "ibm", "International Business Machines Corporation", "US", "corporation",
        {
            "アイビーエム", "IBM", "INTERNATIONAL BUSINESS MACHINES CORP",
            "INTERNATIONAL BUSINESS MACHINES CORPORATION",
            "International Business Machines",
        },
        industry="technology", ticker="IBM",
    ),
    Entity(
        "qualcomm", "QUALCOMM Incorporated", "US", "corporation",
        {
            "クアルコム", "Qualcomm", "QUALCOMM INC",
            "QUALCOMM INCORPORATED", "Qualcomm Inc",
        },
        industry="semiconductors", ticker="QCOM",
    ),
    Entity(
        "broadcom", "Broadcom Inc.", "US", "corporation",
        {
            "ブロードコム", "Broadcom", "BROADCOM INC",
            "BROADCOM CORP", "Broadcom Corporation",
            "AVAGO TECHNOLOGIES",
        },
        industry="semiconductors", ticker="AVGO",
    ),
    Entity(
        "texas_instruments", "Texas Instruments Incorporated", "US", "corporation",
        {
            "テキサス・インスツルメンツ", "Texas Instruments",
            "TEXAS INSTRUMENTS INC", "TEXAS INSTRUMENTS INCORPORATED",
            "TI",
        },
        industry="semiconductors", ticker="TXN",
    ),
    Entity(
        "nvidia", "NVIDIA Corporation", "US", "corporation",
        {
            "エヌビディア", "NVIDIA", "NVIDIA CORP",
            "NVIDIA CORPORATION", "Nvidia",
        },
        industry="semiconductors", ticker="NVDA",
    ),
    Entity(
        "amd", "Advanced Micro Devices, Inc.", "US", "corporation",
        {
            "エーエムディー", "AMD", "ADVANCED MICRO DEVICES INC",
            "Advanced Micro Devices",
        },
        industry="semiconductors", ticker="AMD",
    ),
    Entity(
        "cisco", "Cisco Systems, Inc.", "US", "corporation",
        {
            "シスコ", "シスコシステムズ", "Cisco", "Cisco Systems",
            "CISCO SYSTEMS INC", "CISCO TECHNOLOGY INC",
            "Cisco Technology, Inc.",
        },
        industry="technology", ticker="CSCO",
    ),
    Entity(
        "oracle", "Oracle Corporation", "US", "corporation",
        {
            "オラクル", "Oracle", "ORACLE CORP", "ORACLE CORPORATION",
            "Oracle International Corporation",
            "ORACLE INTERNATIONAL CORP",
        },
        industry="technology", ticker="ORCL",
    ),
    Entity(
        "salesforce", "Salesforce, Inc.", "US", "corporation",
        {
            "セールスフォース", "Salesforce", "SALESFORCE INC",
            "SALESFORCE COM INC", "Salesforce.com",
            "Salesforce.com, Inc.",
        },
        industry="technology", ticker="CRM",
    ),
    Entity(
        "adobe", "Adobe Inc.", "US", "corporation",
        {
            "アドビ", "Adobe", "ADOBE INC", "ADOBE SYSTEMS INC",
            "Adobe Systems", "Adobe Systems Incorporated",
        },
        industry="technology", ticker="ADBE",
    ),
    Entity(
        "applied_materials", "Applied Materials, Inc.", "US", "corporation",
        {
            "アプライドマテリアルズ", "Applied Materials",
            "APPLIED MATERIALS INC",
        },
        industry="semiconductors", ticker="AMAT",
    ),
    Entity(
        "lam_research", "Lam Research Corporation", "US", "corporation",
        {
            "ラムリサーチ", "Lam Research", "LAM RESEARCH CORP",
            "LAM RESEARCH CORPORATION",
        },
        industry="semiconductors", ticker="LRCX",
    ),
    Entity(
        "kla", "KLA Corporation", "US", "corporation",
        {
            "ケーエルエー", "KLA", "KLA CORP", "KLA CORPORATION",
            "KLA-Tencor", "KLA TENCOR CORP",
        },
        industry="semiconductors", ticker="KLAC",
    ),
    Entity(
        "micron", "Micron Technology, Inc.", "US", "corporation",
        {
            "マイクロン", "Micron", "Micron Technology",
            "MICRON TECHNOLOGY INC",
        },
        industry="semiconductors", ticker="MU",
    ),
    Entity(
        "hp_inc", "HP Inc.", "US", "corporation",
        {
            "エイチピー", "HP", "HP INC", "HEWLETT PACKARD",
            "Hewlett-Packard", "HEWLETT PACKARD CO",
        },
        industry="technology", ticker="HPQ",
    ),
    Entity(
        "hpe", "Hewlett Packard Enterprise Company", "US", "corporation",
        {
            "ヒューレット・パッカード・エンタープライズ",
            "Hewlett Packard Enterprise", "HPE",
            "HEWLETT PACKARD ENTERPRISE CO",
            "HEWLETT PACKARD ENTERPRISE COMPANY",
        },
        industry="technology", ticker="HPE",
    ),
    Entity(
        "dell", "Dell Technologies Inc.", "US", "corporation",
        {
            "デル", "Dell", "Dell Technologies", "DELL TECHNOLOGIES INC",
            "DELL INC",
        },
        industry="technology", ticker="DELL",
    ),
    Entity(
        "western_digital", "Western Digital Corporation", "US", "corporation",
        {
            "ウエスタンデジタル", "Western Digital",
            "WESTERN DIGITAL CORP", "WESTERN DIGITAL CORPORATION",
        },
        industry="technology", ticker="WDC",
    ),
    Entity(
        "seagate", "Seagate Technology Holdings plc", "US", "corporation",
        {
            "シーゲイト", "Seagate", "SEAGATE TECHNOLOGY",
            "SEAGATE TECHNOLOGY LLC", "SEAGATE TECHNOLOGY HOLDINGS",
        },
        industry="technology", ticker="STX",
    ),
    Entity(
        "juniper", "Juniper Networks, Inc.", "US", "corporation",
        {
            "ジュニパーネットワークス", "Juniper Networks",
            "JUNIPER NETWORKS INC",
        },
        industry="technology", ticker="JNPR",
    ),
    Entity(
        "marvell", "Marvell Technology, Inc.", "US", "corporation",
        {
            "マーベル", "Marvell", "Marvell Technology",
            "MARVELL TECHNOLOGY INC",
            "MARVELL INTERNATIONAL LTD",
            "MARVELL SEMICONDUCTOR INC",
        },
        industry="semiconductors", ticker="MRVL",
    ),
    Entity(
        "on_semiconductor", "ON Semiconductor Corporation", "US", "corporation",
        {
            "オン・セミコンダクター", "ON Semiconductor", "onsemi",
            "ON SEMICONDUCTOR CORP", "ONSEMI",
        },
        industry="semiconductors", ticker="ON",
    ),
    Entity(
        "microchip", "Microchip Technology Incorporated", "US", "corporation",
        {
            "マイクロチップ", "Microchip Technology",
            "MICROCHIP TECHNOLOGY INC",
        },
        industry="semiconductors", ticker="MCHP",
    ),
    Entity(
        "analog_devices", "Analog Devices, Inc.", "US", "corporation",
        {
            "アナログ・デバイセズ", "Analog Devices",
            "ANALOG DEVICES INC",
        },
        industry="semiconductors", ticker="ADI",
    ),
    Entity(
        "synopsys", "Synopsys, Inc.", "US", "corporation",
        {
            "シノプシス", "Synopsys", "SYNOPSYS INC",
        },
        industry="semiconductors", ticker="SNPS",
    ),
    Entity(
        "cadence", "Cadence Design Systems, Inc.", "US", "corporation",
        {
            "ケイデンス", "Cadence", "Cadence Design Systems",
            "CADENCE DESIGN SYSTEMS INC",
        },
        industry="semiconductors", ticker="CDNS",
    ),
    Entity(
        "servicenow", "ServiceNow, Inc.", "US", "corporation",
        {
            "サービスナウ", "ServiceNow", "SERVICENOW INC",
        },
        industry="technology", ticker="NOW",
    ),
    Entity(
        "palantir", "Palantir Technologies Inc.", "US", "corporation",
        {
            "パランティア", "Palantir", "Palantir Technologies",
            "PALANTIR TECHNOLOGIES INC",
        },
        industry="technology", ticker="PLTR",
    ),

    # =========================================================================
    # Pharmaceutical / Biotechnology
    # =========================================================================
    Entity(
        "pfizer", "Pfizer Inc.", "US", "corporation",
        {
            "ファイザー", "Pfizer", "PFIZER INC", "PFIZER INC.",
        },
        industry="pharmaceutical", ticker="PFE",
    ),
    Entity(
        "johnson_johnson", "Johnson & Johnson", "US", "corporation",
        {
            "ジョンソン・エンド・ジョンソン", "ジョンソン&ジョンソン",
            "Johnson & Johnson", "JOHNSON & JOHNSON",
            "J&J", "JNJ",
        },
        industry="pharmaceutical", ticker="JNJ",
    ),
    Entity(
        "merck", "Merck & Co., Inc.", "US", "corporation",
        {
            "メルク", "Merck", "MERCK & CO INC", "MERCK & CO",
            "Merck Sharp & Dohme", "MERCK SHARP & DOHME",
            "MSD",
        },
        industry="pharmaceutical", ticker="MRK",
    ),
    Entity(
        "abbvie", "AbbVie Inc.", "US", "corporation",
        {
            "アッヴィ", "AbbVie", "ABBVIE INC",
        },
        industry="pharmaceutical", ticker="ABBV",
    ),
    Entity(
        "amgen", "Amgen Inc.", "US", "corporation",
        {
            "アムジェン", "Amgen", "AMGEN INC",
        },
        industry="biotechnology", ticker="AMGN",
    ),
    Entity(
        "gilead", "Gilead Sciences, Inc.", "US", "corporation",
        {
            "ギリアド", "ギリアド・サイエンシズ",
            "Gilead", "Gilead Sciences",
            "GILEAD SCIENCES INC",
        },
        industry="biotechnology", ticker="GILD",
    ),
    Entity(
        "bms", "Bristol-Myers Squibb Company", "US", "corporation",
        {
            "ブリストル・マイヤーズ スクイブ",
            "Bristol-Myers Squibb", "BMS",
            "BRISTOL MYERS SQUIBB CO",
            "BRISTOL-MYERS SQUIBB COMPANY",
        },
        industry="pharmaceutical", ticker="BMY",
    ),
    Entity(
        "eli_lilly", "Eli Lilly and Company", "US", "corporation",
        {
            "イーライリリー", "イーライ・リリー",
            "Eli Lilly", "Lilly",
            "ELI LILLY AND CO", "ELI LILLY & CO",
        },
        industry="pharmaceutical", ticker="LLY",
    ),
    Entity(
        "regeneron", "Regeneron Pharmaceuticals, Inc.", "US", "corporation",
        {
            "リジェネロン", "Regeneron",
            "Regeneron Pharmaceuticals",
            "REGENERON PHARMACEUTICALS INC",
        },
        industry="biotechnology", ticker="REGN",
    ),
    Entity(
        "moderna", "Moderna, Inc.", "US", "corporation",
        {
            "モデルナ", "Moderna", "MODERNA INC",
        },
        industry="biotechnology", ticker="MRNA",
    ),
    Entity(
        "thermo_fisher", "Thermo Fisher Scientific Inc.", "US", "corporation",
        {
            "サーモフィッシャー", "サーモフィッシャーサイエンティフィック",
            "Thermo Fisher", "Thermo Fisher Scientific",
            "THERMO FISHER SCIENTIFIC INC",
        },
        industry="life_sciences", ticker="TMO",
    ),
    Entity(
        "abbott", "Abbott Laboratories", "US", "corporation",
        {
            "アボット", "Abbott", "Abbott Laboratories",
            "ABBOTT LABORATORIES", "ABBOTT LABS",
        },
        industry="medical_devices", ticker="ABT",
    ),
    Entity(
        "medtronic", "Medtronic plc", "US", "corporation",
        {
            "メドトロニック", "Medtronic", "MEDTRONIC INC",
            "MEDTRONIC PLC",
        },
        industry="medical_devices", ticker="MDT",
    ),
    Entity(
        "becton_dickinson", "Becton, Dickinson and Company", "US", "corporation",
        {
            "ベクトン・ディッキンソン", "Becton Dickinson", "BD",
            "BECTON DICKINSON AND CO", "BECTON DICKINSON & CO",
        },
        industry="medical_devices", ticker="BDX",
    ),
    Entity(
        "boston_scientific", "Boston Scientific Corporation", "US", "corporation",
        {
            "ボストン・サイエンティフィック", "Boston Scientific",
            "BOSTON SCIENTIFIC CORP",
        },
        industry="medical_devices", ticker="BSX",
    ),
    Entity(
        "edwards_lifesciences", "Edwards Lifesciences Corporation", "US", "corporation",
        {
            "エドワーズライフサイエンス", "Edwards Lifesciences",
            "EDWARDS LIFESCIENCES CORP",
        },
        industry="medical_devices", ticker="EW",
    ),
    Entity(
        "stryker", "Stryker Corporation", "US", "corporation",
        {
            "ストライカー", "Stryker", "STRYKER CORP",
        },
        industry="medical_devices", ticker="SYK",
    ),

    # =========================================================================
    # Industrials / Machinery / Conglomerate
    # =========================================================================
    Entity(
        "3m", "3M Company", "US", "corporation",
        {
            "スリーエム", "3M", "3M COMPANY", "3M CO",
            "MINNESOTA MINING AND MFG CO",
            "Minnesota Mining & Manufacturing",
            "MINNESOTA MINING & MFG",
        },
        industry="industrials", ticker="MMM",
    ),
    Entity(
        "ge", "General Electric Company", "US", "corporation",
        {
            "ゼネラル・エレクトリック", "ジーイー",
            "General Electric", "GE",
            "GENERAL ELECTRIC CO", "GENERAL ELECTRIC COMPANY",
        },
        industry="industrials", ticker="GE",
    ),
    Entity(
        "honeywell", "Honeywell International Inc.", "US", "corporation",
        {
            "ハネウェル", "Honeywell",
            "HONEYWELL INTERNATIONAL INC", "HONEYWELL INT INC",
        },
        industry="industrials", ticker="HON",
    ),
    Entity(
        "caterpillar", "Caterpillar Inc.", "US", "corporation",
        {
            "キャタピラー", "Caterpillar", "CAT",
            "CATERPILLAR INC",
        },
        industry="industrials", ticker="CAT",
    ),
    Entity(
        "emerson", "Emerson Electric Co.", "US", "corporation",
        {
            "エマソン", "エマソン・エレクトリック",
            "Emerson", "Emerson Electric",
            "EMERSON ELECTRIC CO",
        },
        industry="industrials", ticker="EMR",
    ),
    Entity(
        "parker_hannifin", "Parker-Hannifin Corporation", "US", "corporation",
        {
            "パーカー・ハネフィン", "Parker Hannifin",
            "PARKER HANNIFIN CORP",
        },
        industry="industrials", ticker="PH",
    ),
    Entity(
        "illinois_tool_works", "Illinois Tool Works Inc.", "US", "corporation",
        {
            "イリノイ・ツール・ワークス", "Illinois Tool Works", "ITW",
            "ILLINOIS TOOL WORKS INC",
        },
        industry="industrials", ticker="ITW",
    ),
    Entity(
        "deere", "Deere & Company", "US", "corporation",
        {
            "ディア", "ジョン・ディア", "Deere", "John Deere",
            "DEERE & CO", "DEERE & COMPANY",
        },
        industry="industrials", ticker="DE",
    ),
    Entity(
        "eaton", "Eaton Corporation plc", "US", "corporation",
        {
            "イートン", "Eaton", "EATON CORP",
            "EATON CORPORATION",
        },
        industry="industrials", ticker="ETN",
    ),
    Entity(
        "carrier", "Carrier Global Corporation", "US", "corporation",
        {
            "キャリア", "Carrier", "CARRIER GLOBAL CORP",
            "CARRIER CORP",
        },
        industry="industrials", ticker="CARR",
    ),

    # =========================================================================
    # Automotive
    # =========================================================================
    Entity(
        "gm", "General Motors Company", "US", "corporation",
        {
            "ゼネラルモーターズ", "GM",
            "General Motors", "GENERAL MOTORS CO",
            "GENERAL MOTORS COMPANY", "GENERAL MOTORS CORP",
        },
        industry="automotive", ticker="GM",
    ),
    Entity(
        "ford", "Ford Motor Company", "US", "corporation",
        {
            "フォード", "Ford", "Ford Motor",
            "FORD MOTOR CO", "FORD MOTOR COMPANY",
            "FORD GLOBAL TECHNOLOGIES LLC",
        },
        industry="automotive", ticker="F",
    ),
    Entity(
        "tesla", "Tesla, Inc.", "US", "corporation",
        {
            "テスラ", "Tesla", "TESLA INC", "TESLA MOTORS INC",
            "Tesla Motors",
        },
        industry="automotive", ticker="TSLA",
    ),

    # =========================================================================
    # Chemicals / Materials
    # =========================================================================
    Entity(
        "dow", "Dow Inc.", "US", "corporation",
        {
            "ダウ", "Dow", "DOW INC", "DOW CHEMICAL CO",
            "Dow Chemical", "THE DOW CHEMICAL COMPANY",
            "Dow Chemical Company",
        },
        industry="chemicals", ticker="DOW",
    ),
    Entity(
        "dupont", "DuPont de Nemours, Inc.", "US", "corporation",
        {
            "デュポン", "DuPont", "DUPONT DE NEMOURS INC",
            "E I DU PONT DE NEMOURS AND CO",
            "E. I. du Pont de Nemours",
            "EI DU PONT DE NEMOURS & CO",
        },
        industry="chemicals", ticker="DD",
    ),
    Entity(
        "air_products", "Air Products and Chemicals, Inc.", "US", "corporation",
        {
            "エアープロダクツ", "Air Products",
            "AIR PRODUCTS AND CHEMICALS INC",
            "AIR PRODUCTS & CHEMICALS INC",
        },
        industry="chemicals", ticker="APD",
    ),
    Entity(
        "linde", "Linde plc", "US", "corporation",
        {
            "リンデ", "Linde", "LINDE PLC",
            "PRAXAIR INC", "Praxair",
        },
        industry="chemicals", ticker="LIN",
    ),
    Entity(
        "corning", "Corning Incorporated", "US", "corporation",
        {
            "コーニング", "Corning", "CORNING INC",
            "CORNING INCORPORATED",
        },
        industry="materials", ticker="GLW",
    ),
    Entity(
        "ppg", "PPG Industries, Inc.", "US", "corporation",
        {
            "ピーピージー", "PPG", "PPG Industries",
            "PPG INDUSTRIES INC",
        },
        industry="chemicals", ticker="PPG",
    ),

    # =========================================================================
    # Aerospace / Defense
    # =========================================================================
    Entity(
        "boeing", "The Boeing Company", "US", "corporation",
        {
            "ボーイング", "Boeing", "BOEING CO",
            "THE BOEING COMPANY", "BOEING COMPANY",
        },
        industry="aerospace", ticker="BA",
    ),
    Entity(
        "lockheed_martin", "Lockheed Martin Corporation", "US", "corporation",
        {
            "ロッキード・マーティン", "Lockheed Martin",
            "LOCKHEED MARTIN CORP", "LOCKHEED MARTIN CORPORATION",
            "LOCKHEED CORP",
        },
        industry="defense", ticker="LMT",
    ),
    Entity(
        "rtx", "RTX Corporation", "US", "corporation",
        {
            "レイセオン", "Raytheon", "RTX",
            "RTX CORP", "RTX CORPORATION",
            "RAYTHEON CO", "RAYTHEON COMPANY",
            "RAYTHEON TECHNOLOGIES CORP",
            "UNITED TECHNOLOGIES CORP",
            "United Technologies",
        },
        industry="defense", ticker="RTX",
    ),
    Entity(
        "northrop_grumman", "Northrop Grumman Corporation", "US", "corporation",
        {
            "ノースロップ・グラマン", "Northrop Grumman",
            "NORTHROP GRUMMAN CORP", "NORTHROP GRUMMAN CORPORATION",
        },
        industry="defense", ticker="NOC",
    ),
    Entity(
        "general_dynamics", "General Dynamics Corporation", "US", "corporation",
        {
            "ゼネラル・ダイナミクス", "General Dynamics",
            "GENERAL DYNAMICS CORP", "GENERAL DYNAMICS CORPORATION",
        },
        industry="defense", ticker="GD",
    ),
    Entity(
        "l3harris", "L3Harris Technologies, Inc.", "US", "corporation",
        {
            "エルスリーハリス", "L3Harris", "L3Harris Technologies",
            "L3HARRIS TECHNOLOGIES INC",
            "HARRIS CORP",
        },
        industry="defense", ticker="LHX",
    ),

    # =========================================================================
    # Telecommunications
    # =========================================================================
    Entity(
        "att", "AT&T Inc.", "US", "corporation",
        {
            "エーティーアンドティー", "AT&T",
            "AT&T INC", "AT & T INC",
            "AT&T Corp",
        },
        industry="telecommunications", ticker="T",
    ),
    Entity(
        "verizon", "Verizon Communications Inc.", "US", "corporation",
        {
            "ベライゾン", "Verizon",
            "VERIZON COMMUNICATIONS INC",
            "Verizon Communications",
        },
        industry="telecommunications", ticker="VZ",
    ),
    Entity(
        "t_mobile", "T-Mobile US, Inc.", "US", "corporation",
        {
            "ティーモバイル", "T-Mobile",
            "T-MOBILE US INC", "T MOBILE US INC",
        },
        industry="telecommunications", ticker="TMUS",
    ),

    # =========================================================================
    # Consumer Products
    # =========================================================================
    Entity(
        "procter_gamble", "The Procter & Gamble Company", "US", "corporation",
        {
            "プロクター・アンド・ギャンブル", "P&G",
            "Procter & Gamble", "PROCTER & GAMBLE CO",
            "THE PROCTER & GAMBLE COMPANY",
        },
        industry="consumer", ticker="PG",
    ),
    Entity(
        "colgate", "Colgate-Palmolive Company", "US", "corporation",
        {
            "コルゲート", "コルゲート・パーモリーブ",
            "Colgate-Palmolive", "Colgate",
            "COLGATE PALMOLIVE CO",
        },
        industry="consumer", ticker="CL",
    ),
    Entity(
        "kimberly_clark", "Kimberly-Clark Corporation", "US", "corporation",
        {
            "キンバリー・クラーク", "Kimberly-Clark",
            "KIMBERLY CLARK CORP",
        },
        industry="consumer", ticker="KMB",
    ),

    # =========================================================================
    # Energy / Oilfield Services
    # =========================================================================
    Entity(
        "exxonmobil", "Exxon Mobil Corporation", "US", "corporation",
        {
            "エクソンモービル", "ExxonMobil", "Exxon Mobil",
            "EXXON MOBIL CORP", "EXXONMOBIL",
            "EXXON CHEMICAL PATENTS INC",
            "EXXON RESEARCH & ENGINEERING CO",
        },
        industry="energy", ticker="XOM",
    ),
    Entity(
        "chevron", "Chevron Corporation", "US", "corporation",
        {
            "シェブロン", "Chevron", "CHEVRON CORP",
            "CHEVRON CORPORATION", "CHEVRON USA INC",
        },
        industry="energy", ticker="CVX",
    ),
    Entity(
        "slb", "SLB (Schlumberger)", "US", "corporation",
        {
            "シュルンベルジェ", "Schlumberger", "SLB",
            "SCHLUMBERGER TECHNOLOGY CORP",
            "SCHLUMBERGER LTD",
        },
        industry="energy_services", ticker="SLB",
    ),
    Entity(
        "baker_hughes", "Baker Hughes Company", "US", "corporation",
        {
            "ベーカー・ヒューズ", "Baker Hughes",
            "BAKER HUGHES CO", "BAKER HUGHES INC",
            "BAKER HUGHES A GE CO LLC",
        },
        industry="energy_services", ticker="BKR",
    ),
    Entity(
        "halliburton", "Halliburton Company", "US", "corporation",
        {
            "ハリバートン", "Halliburton",
            "HALLIBURTON CO", "HALLIBURTON COMPANY",
            "HALLIBURTON ENERGY SERVICES INC",
        },
        industry="energy_services", ticker="HAL",
    ),
    Entity(
        "conocophillips", "ConocoPhillips", "US", "corporation",
        {
            "コノコフィリップス", "ConocoPhillips",
            "CONOCOPHILLIPS", "CONOCOPHILLIPS CO",
        },
        industry="energy", ticker="COP",
    ),

    # =========================================================================
    # Financial Technology / Data Analytics (patent-active)
    # =========================================================================
    Entity(
        "visa", "Visa Inc.", "US", "corporation",
        {
            "ビザ", "Visa", "VISA INC",
        },
        industry="fintech", ticker="V",
    ),
    Entity(
        "mastercard", "Mastercard Incorporated", "US", "corporation",
        {
            "マスターカード", "Mastercard", "MASTERCARD INC",
            "MASTERCARD INCORPORATED",
        },
        industry="fintech", ticker="MA",
    ),

    # =========================================================================
    # Media / Entertainment (patent-active)
    # =========================================================================
    Entity(
        "disney", "The Walt Disney Company", "US", "corporation",
        {
            "ディズニー", "ウォルト・ディズニー",
            "Disney", "Walt Disney",
            "WALT DISNEY CO", "THE WALT DISNEY COMPANY",
        },
        industry="entertainment", ticker="DIS",
    ),
    Entity(
        "netflix", "Netflix, Inc.", "US", "corporation",
        {
            "ネットフリックス", "Netflix", "NETFLIX INC",
        },
        industry="entertainment", ticker="NFLX",
    ),

    # =========================================================================
    # Cloud / Enterprise (patent-active)
    # =========================================================================
    Entity(
        "snowflake", "Snowflake Inc.", "US", "corporation",
        {
            "スノーフレーク", "Snowflake", "SNOWFLAKE INC",
        },
        industry="technology", ticker="SNOW",
    ),
    Entity(
        "palo_alto_networks", "Palo Alto Networks, Inc.", "US", "corporation",
        {
            "パロアルトネットワークス", "Palo Alto Networks",
            "PALO ALTO NETWORKS INC",
        },
        industry="cybersecurity", ticker="PANW",
    ),
    Entity(
        "crowdstrike", "CrowdStrike Holdings, Inc.", "US", "corporation",
        {
            "クラウドストライク", "CrowdStrike",
            "CROWDSTRIKE HOLDINGS INC", "CROWDSTRIKE INC",
        },
        industry="cybersecurity", ticker="CRWD",
    ),
    Entity(
        "intuit", "Intuit Inc.", "US", "corporation",
        {
            "インテュイット", "Intuit", "INTUIT INC",
        },
        industry="technology", ticker="INTU",
    ),

    # =========================================================================
    # Diversified Technology / Imaging
    # =========================================================================
    Entity(
        "ge_healthcare", "GE HealthCare Technologies Inc.", "US", "corporation",
        {
            "GEヘルスケア", "GE HealthCare",
            "GE HEALTHCARE TECHNOLOGIES INC",
            "GE HEALTHCARE",
        },
        parent_id="ge",
        industry="medical_devices", ticker="GEHC",
    ),
    Entity(
        "danaher", "Danaher Corporation", "US", "corporation",
        {
            "ダナハー", "Danaher", "DANAHER CORP",
        },
        industry="life_sciences", ticker="DHR",
    ),
    Entity(
        "te_connectivity", "TE Connectivity Ltd.", "US", "corporation",
        {
            "ティーイーコネクティビティ", "TE Connectivity",
            "TE CONNECTIVITY LTD", "TYCO ELECTRONICS",
        },
        industry="electronics", ticker="TEL",
    ),
    Entity(
        "amphenol", "Amphenol Corporation", "US", "corporation",
        {
            "アンフェノール", "Amphenol", "AMPHENOL CORP",
        },
        industry="electronics", ticker="APH",
    ),

    # =========================================================================
    # Additional patent-heavy S&P 500 companies
    # =========================================================================
    Entity(
        "ge_vernova", "GE Vernova Inc.", "US", "corporation",
        {
            "GEベルノバ", "GE Vernova", "GE VERNOVA INC",
        },
        parent_id="ge",
        industry="energy", ticker="GEV",
    ),
    Entity(
        "motorola_solutions", "Motorola Solutions, Inc.", "US", "corporation",
        {
            "モトローラ・ソリューションズ", "Motorola Solutions",
            "MOTOROLA SOLUTIONS INC", "MOTOROLA INC",
            "Motorola",
        },
        industry="technology", ticker="MSI",
    ),
    Entity(
        "fortive", "Fortive Corporation", "US", "corporation",
        {
            "フォーティブ", "Fortive", "FORTIVE CORP",
        },
        industry="industrials", ticker="FTV",
    ),
    Entity(
        "rockwell_automation", "Rockwell Automation, Inc.", "US", "corporation",
        {
            "ロックウェル・オートメーション", "Rockwell Automation",
            "ROCKWELL AUTOMATION INC",
        },
        industry="industrials", ticker="ROK",
    ),
    Entity(
        "zebra", "Zebra Technologies Corporation", "US", "corporation",
        {
            "ゼブラ・テクノロジーズ", "Zebra Technologies",
            "ZEBRA TECHNOLOGIES CORP",
        },
        industry="technology", ticker="ZBRA",
    ),
]
