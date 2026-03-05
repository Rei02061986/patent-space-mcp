"""Global top patent filers — non-US, non-JP companies.

Covers major patent filers from Korea, China, Taiwan, Germany,
and other countries that are not in TSE or S&P 500 seeds.
BigQuery harmonized names (UPPERCASE) are included as aliases.
"""
from entity.registry import Entity

GLOBAL_ENTITIES: list[Entity] = [
    # =========================================================================
    # Korea
    # =========================================================================
    Entity(
        "samsung_electronics", "Samsung Electronics Co., Ltd.", "KR", "corporation",
        {
            "サムスン電子", "サムスン", "Samsung Electronics", "Samsung",
            "SAMSUNG ELECTRONICS CO LTD", "SAMSUNG ELECTRONICS",
            "Samsung Electronics Co Ltd", "삼성전자",
            "SAMSUNG ELEC CO LTD",
        },
        industry="technology", ticker="005930",
    ),
    Entity(
        "samsung_sdi", "Samsung SDI Co., Ltd.", "KR", "corporation",
        {
            "サムスンSDI", "Samsung SDI", "SAMSUNG SDI CO LTD",
            "SAMSUNG SDI", "삼성SDI",
        },
        industry="battery", ticker="006400",
    ),
    Entity(
        "samsung_display", "Samsung Display Co., Ltd.", "KR", "corporation",
        {
            "サムスンディスプレイ", "Samsung Display", "SAMSUNG DISPLAY CO LTD",
            "SAMSUNG DISPLAY",
        },
        industry="display",
    ),
    Entity(
        "lg_electronics", "LG Electronics Inc.", "KR", "corporation",
        {
            "LGエレクトロニクス", "LG Electronics", "LG",
            "LG ELECTRONICS INC", "LG ELECTRONICS",
            "LG Electronics Inc", "엘지전자",
        },
        industry="electronics", ticker="066570",
    ),
    Entity(
        "lg_chem", "LG Chem, Ltd.", "KR", "corporation",
        {
            "LG化学", "LG Chem", "LG CHEM LTD", "LG CHEM",
            "엘지화학",
        },
        industry="chemicals", ticker="051910",
    ),
    Entity(
        "lg_energy_solution", "LG Energy Solution, Ltd.", "KR", "corporation",
        {
            "LGエナジーソリューション", "LG Energy Solution",
            "LG ENERGY SOLUTION LTD", "LG ENERGY SOLUTION",
            "LGES",
        },
        industry="battery", ticker="373220",
    ),
    Entity(
        "lg_display", "LG Display Co., Ltd.", "KR", "corporation",
        {
            "LGディスプレイ", "LG Display", "LG DISPLAY CO LTD",
            "LG DISPLAY",
        },
        industry="display", ticker="034220",
    ),
    Entity(
        "sk_hynix", "SK hynix Inc.", "KR", "corporation",
        {
            "SKハイニックス", "SK Hynix", "SK hynix",
            "SK HYNIX INC", "SK HYNIX",
            "에스케이하이닉스",
        },
        industry="semiconductor", ticker="000660",
    ),
    Entity(
        "hyundai_motor", "Hyundai Motor Company", "KR", "corporation",
        {
            "現代自動車", "ヒュンダイ", "Hyundai Motor", "Hyundai",
            "HYUNDAI MOTOR CO LTD", "HYUNDAI MOTOR COMPANY",
            "HYUNDAI MOTOR CO", "현대자동차",
        },
        industry="automotive", ticker="005380",
    ),
    Entity(
        "kia", "Kia Corporation", "KR", "corporation",
        {
            "起亜", "キア", "Kia", "KIA CORP", "KIA MOTORS CORP",
            "KIA MOTORS", "기아",
        },
        industry="automotive", ticker="000270",
    ),
    Entity(
        "sk_innovation", "SK Innovation Co., Ltd.", "KR", "corporation",
        {
            "SKイノベーション", "SK Innovation",
            "SK INNOVATION CO LTD", "SK INNOVATION",
        },
        industry="energy", ticker="096770",
    ),
    Entity(
        "posco", "POSCO Holdings Inc.", "KR", "corporation",
        {
            "ポスコ", "POSCO", "POSCO HOLDINGS",
            "POSCO HOLDINGS INC",
        },
        industry="steel", ticker="005490",
    ),
    # =========================================================================
    # China
    # =========================================================================
    Entity(
        "huawei", "Huawei Technologies Co., Ltd.", "CN", "corporation",
        {
            "ファーウェイ", "華為", "Huawei", "HUAWEI TECHNOLOGIES CO LTD",
            "HUAWEI TECHNOLOGIES", "HUAWEI DEVICE CO LTD",
            "HUAWEI TECH CO LTD", "华为技术有限公司",
        },
        industry="technology",
    ),
    Entity(
        "catl", "Contemporary Amperex Technology Co., Limited", "CN", "corporation",
        {
            "CATL", "寧徳時代", "宁德时代",
            "CONTEMPORARY AMPEREX TECHNOLOGY CO LTD",
            "CONTEMPORARY AMPEREX TECHNOLOGY",
            "Contemporary Amperex Technology",
        },
        industry="battery", ticker="300750",
    ),
    Entity(
        "byd", "BYD Company Limited", "CN", "corporation",
        {
            "BYD", "比亜迪", "比亚迪",
            "BYD CO LTD", "BYD COMPANY LIMITED",
        },
        industry="automotive", ticker="002594",
    ),
    Entity(
        "boe", "BOE Technology Group Co., Ltd.", "CN", "corporation",
        {
            "BOE", "京東方", "京东方",
            "BOE TECHNOLOGY GROUP CO LTD", "BOE TECHNOLOGY",
        },
        industry="display", ticker="000725",
    ),
    Entity(
        "xiaomi", "Xiaomi Corporation", "CN", "corporation",
        {
            "シャオミ", "小米", "Xiaomi",
            "XIAOMI CORP", "XIAOMI INC",
            "XIAOMI COMMUNICATIONS CO LTD",
        },
        industry="technology", ticker="1810",
    ),
    Entity(
        "oppo", "OPPO Electronics Corp.", "CN", "corporation",
        {
            "OPPO", "オッポ",
            "GUANGDONG OPPO MOBILE TELECOMMUNICATIONS",
            "OPPO MOBILE TELECOMMUNICATIONS",
        },
        industry="technology",
    ),
    Entity(
        "zte", "ZTE Corporation", "CN", "corporation",
        {
            "ZTE", "中興通訊", "中兴通讯",
            "ZTE CORP", "ZTE CORPORATION",
        },
        industry="telecom", ticker="000063",
    ),
    Entity(
        "lenovo", "Lenovo Group Limited", "CN", "corporation",
        {
            "レノボ", "聯想", "联想", "Lenovo",
            "LENOVO", "LENOVO GROUP",
            "LENOVO BEIJING LTD",
        },
        industry="technology", ticker="0992",
    ),
    Entity(
        "tencent", "Tencent Holdings Limited", "CN", "corporation",
        {
            "テンセント", "騰訊", "腾讯", "Tencent",
            "TENCENT HOLDINGS", "TENCENT TECHNOLOGY",
        },
        industry="technology", ticker="0700",
    ),
    Entity(
        "baidu", "Baidu, Inc.", "CN", "corporation",
        {
            "バイドゥ", "百度", "Baidu",
            "BAIDU INC", "BAIDU ONLINE NETWORK TECHNOLOGY",
            "BAIDU COM TIMES TECHNOLOGY",
        },
        industry="technology", ticker="BIDU",
    ),
    Entity(
        "alibaba", "Alibaba Group Holding Limited", "CN", "corporation",
        {
            "アリババ", "阿里巴巴", "Alibaba",
            "ALIBABA GROUP", "ALIBABA GROUP HOLDING",
        },
        industry="technology", ticker="BABA",
    ),
    # =========================================================================
    # Taiwan
    # =========================================================================
    Entity(
        "tsmc", "Taiwan Semiconductor Manufacturing Company", "TW", "corporation",
        {
            "TSMC", "台湾積体電路", "台积电",
            "TAIWAN SEMICONDUCTOR", "TAIWAN SEMICONDUCTOR MFG",
            "TAIWAN SEMICONDUCTOR MANUFACTURING",
            "TAIWAN SEMICONDUCTOR MFG CO LTD",
        },
        industry="semiconductor", ticker="TSM",
    ),
    Entity(
        "foxconn", "Hon Hai Precision Industry Co., Ltd.", "TW", "corporation",
        {
            "フォックスコン", "鴻海", "鸿海", "Foxconn", "Hon Hai",
            "HON HAI PRECISION IND CO LTD", "HON HAI PRECISION INDUSTRY",
            "FOXCONN TECHNOLOGY",
        },
        industry="electronics", ticker="2317",
    ),
    Entity(
        "mediatek", "MediaTek Inc.", "TW", "corporation",
        {
            "メディアテック", "聯發科", "联发科", "MediaTek",
            "MEDIATEK INC", "MEDIATEK",
        },
        industry="semiconductor", ticker="2454",
    ),
    Entity(
        "au_optronics", "AU Optronics Corp.", "TW", "corporation",
        {
            "AUO", "友達光電", "AU Optronics",
            "AU OPTRONICS CORP",
        },
        industry="display", ticker="2409",
    ),
    # =========================================================================
    # Germany
    # =========================================================================
    Entity(
        "bosch", "Robert Bosch GmbH", "DE", "corporation",
        {
            "ボッシュ", "Bosch", "Robert Bosch",
            "ROBERT BOSCH GMBH", "ROBERT BOSCH",
            "BOSCH GMBH",
        },
        industry="automotive",
    ),
    Entity(
        "siemens", "Siemens AG", "DE", "corporation",
        {
            "シーメンス", "Siemens", "SIEMENS AG", "SIEMENS",
            "SIEMENS AKTIENGESELLSCHAFT",
        },
        industry="industrial", ticker="SIE",
    ),
    Entity(
        "basf", "BASF SE", "DE", "corporation",
        {
            "BASF", "ビーエーエスエフ",
            "BASF SE", "BASF AG",
        },
        industry="chemicals", ticker="BAS",
    ),
    Entity(
        "continental", "Continental AG", "DE", "corporation",
        {
            "コンチネンタル", "Continental",
            "CONTINENTAL AG", "CONTINENTAL AUTOMOTIVE",
        },
        industry="automotive", ticker="CON",
    ),
    Entity(
        "bayer", "Bayer AG", "DE", "corporation",
        {
            "バイエル", "Bayer", "BAYER AG", "BAYER",
            "BAYER INTELLECTUAL PROPERTY",
        },
        industry="pharma", ticker="BAYN",
    ),
    Entity(
        "volkswagen", "Volkswagen AG", "DE", "corporation",
        {
            "フォルクスワーゲン", "VW", "Volkswagen",
            "VOLKSWAGEN AG", "VOLKSWAGEN",
        },
        industry="automotive", ticker="VOW3",
    ),
    Entity(
        "bmw", "Bayerische Motoren Werke AG", "DE", "corporation",
        {
            "BMW", "ビーエムダブリュー",
            "BAYERISCHE MOTOREN WERKE AG", "BMW AG",
        },
        industry="automotive", ticker="BMW",
    ),
    Entity(
        "daimler", "Mercedes-Benz Group AG", "DE", "corporation",
        {
            "メルセデス・ベンツ", "ダイムラー", "Mercedes-Benz", "Daimler",
            "DAIMLER AG", "MERCEDES BENZ GROUP AG",
            "MERCEDES-BENZ GROUP AG",
        },
        industry="automotive", ticker="MBG",
    ),
    Entity(
        "sap", "SAP SE", "DE", "corporation",
        {
            "SAP", "エスエーピー",
            "SAP SE", "SAP AG",
        },
        industry="technology", ticker="SAP",
    ),
    # =========================================================================
    # Other European
    # =========================================================================
    Entity(
        "abb", "ABB Ltd", "CH", "corporation",
        {
            "ABB", "エービービー",
            "ABB LTD", "ABB SCHWEIZ AG",
        },
        industry="industrial", ticker="ABBN",
    ),
    Entity(
        "philips", "Koninklijke Philips N.V.", "NL", "corporation",
        {
            "フィリップス", "Philips",
            "KONINKLIJKE PHILIPS", "KONINKLIJKE PHILIPS NV",
            "PHILIPS ELECTRONICS", "PHILIPS",
        },
        industry="healthcare", ticker="PHIA",
    ),
    Entity(
        "ericsson", "Telefonaktiebolaget LM Ericsson", "SE", "corporation",
        {
            "エリクソン", "Ericsson",
            "ERICSSON", "TELEFONAKTIEBOLAGET LM ERICSSON",
        },
        industry="telecom", ticker="ERIC",
    ),
    Entity(
        "nokia", "Nokia Corporation", "FI", "corporation",
        {
            "ノキア", "Nokia", "NOKIA CORP", "NOKIA",
            "NOKIA TECHNOLOGIES",
        },
        industry="telecom", ticker="NOKIA",
    ),
    Entity(
        "stmicroelectronics", "STMicroelectronics N.V.", "CH", "corporation",
        {
            "STマイクロ", "STMicroelectronics", "ST Micro",
            "STMICROELECTRONICS", "STMICROELECTRONICS NV",
        },
        industry="semiconductor", ticker="STM",
    ),
    Entity(
        "nxp", "NXP Semiconductors N.V.", "NL", "corporation",
        {
            "NXP", "エヌエックスピー",
            "NXP SEMICONDUCTORS", "NXP BV",
        },
        industry="semiconductor", ticker="NXPI",
    ),
]
