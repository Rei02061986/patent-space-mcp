"""TSE Prime market companies with patent filing name aliases.

Initial seed: top 50 JP patent filers (manufacturing, electronics, automotive, pharma).
BigQuery assignee_harmonized names are UPPERCASE; raw assignee includes Japanese forms.
"""
from entity.registry import Entity

TSE_PRIME_ENTITIES: list[Entity] = [
    # === Automotive ===
    Entity(
        "toyota", "Toyota Motor Corporation", "JP", "corporation",
        {
            "トヨタ自動車株式会社", "トヨタ自動車", "トヨタ",
            "Toyota", "Toyota Motor", "TOYOTA MOTOR CORP",
            "TOYOTA MOTOR CO LTD", "TOYOTA MOTOR CORPORATION",
            "TOYOTA JIDOSHA KK", "TOYOTA JIDOSHA KABUSHIKI KAISHA",
        },
        industry="automotive", edinet_code="E02144", ticker="7203",
        tse_section="Prime",
    ),
    Entity(
        "honda", "Honda Motor Co., Ltd.", "JP", "corporation",
        {
            "本田技研工業株式会社", "本田技研工業", "ホンダ",
            "Honda", "Honda Motor", "HONDA MOTOR CO LTD",
            "HONDA GIKEN KOGYO KK",
        },
        industry="automotive", edinet_code="E02166", ticker="7267",
        tse_section="Prime",
    ),
    Entity(
        "nissan", "Nissan Motor Co., Ltd.", "JP", "corporation",
        {
            "日産自動車株式会社", "日産自動車", "日産",
            "Nissan", "Nissan Motor", "NISSAN MOTOR CO LTD",
        },
        industry="automotive", edinet_code="E02142", ticker="7201",
        tse_section="Prime",
    ),
    Entity(
        "denso", "DENSO Corporation", "JP", "corporation",
        {
            "株式会社デンソー", "デンソー",
            "Denso", "DENSO CORP", "NIPPONDENSO CO LTD",
        },
        industry="automotive", edinet_code="E01843", ticker="6902",
        tse_section="Prime",
    ),
    Entity(
        "mazda", "Mazda Motor Corporation", "JP", "corporation",
        {
            "マツダ株式会社", "マツダ",
            "Mazda", "MAZDA MOTOR CORP",
        },
        industry="automotive", ticker="7261", tse_section="Prime",
    ),
    Entity(
        "subaru", "SUBARU Corporation", "JP", "corporation",
        {
            "株式会社SUBARU", "スバル", "富士重工業株式会社",
            "Subaru", "SUBARU CORP", "FUJI HEAVY IND LTD",
            "Fuji Heavy Industries",
        },
        industry="automotive", ticker="7270", tse_section="Prime",
    ),
    Entity(
        "suzuki", "Suzuki Motor Corporation", "JP", "corporation",
        {
            "スズキ株式会社", "スズキ",
            "Suzuki", "SUZUKI MOTOR CORP",
        },
        industry="automotive", ticker="7269", tse_section="Prime",
    ),

    # === Electronics / Electrical ===
    Entity(
        "sony", "Sony Group Corporation", "JP", "corporation",
        {
            "ソニーグループ株式会社", "ソニー株式会社", "ソニー",
            "Sony", "Sony Group", "SONY GROUP CORP",
            "SONY CORP", "SONY CORPORATION", "SONY KK",
        },
        industry="electronics", edinet_code="E01777", ticker="6758",
        tse_section="Prime",
    ),
    Entity(
        "panasonic", "Panasonic Holdings Corporation", "JP", "corporation",
        {
            "パナソニックホールディングス株式会社", "パナソニック株式会社",
            "パナソニック", "松下電器産業株式会社", "松下電器",
            "Panasonic", "PANASONIC HOLDINGS CORP", "PANASONIC CORP",
            "MATSUSHITA ELECTRIC IND CO LTD", "MATSUSHITA DENKI SANGYO KK",
        },
        industry="electronics", edinet_code="E01772", ticker="6752",
        tse_section="Prime",
    ),
    Entity(
        "hitachi", "Hitachi, Ltd.", "JP", "corporation",
        {
            "株式会社日立製作所", "日立製作所", "日立",
            "Hitachi", "HITACHI LTD", "HITACHI SEISAKUSHO KK",
        },
        industry="electronics", edinet_code="E01737", ticker="6501",
        tse_section="Prime",
    ),
    Entity(
        "toshiba", "Toshiba Corporation", "JP", "corporation",
        {
            "株式会社東芝", "東芝",
            "Toshiba", "TOSHIBA CORP", "TOSHIBA KK",
        },
        industry="electronics", edinet_code="E01738", ticker="6502",
        tse_section="Prime",
    ),
    Entity(
        "mitsubishi_electric", "Mitsubishi Electric Corporation", "JP", "corporation",
        {
            "三菱電機株式会社", "三菱電機",
            "Mitsubishi Electric", "MITSUBISHI ELECTRIC CORP",
            "MITSUBISHI DENKI KK",
        },
        industry="electronics", edinet_code="E01739", ticker="6503",
        tse_section="Prime",
    ),
    Entity(
        "sharp", "Sharp Corporation", "JP", "corporation",
        {
            "シャープ株式会社", "シャープ",
            "Sharp", "SHARP CORP", "SHARP KK",
        },
        industry="electronics", ticker="6753", tse_section="Prime",
    ),
    Entity(
        "nec", "NEC Corporation", "JP", "corporation",
        {
            "日本電気株式会社", "NEC", "エヌ・イー・シー",
            "NEC Corp", "NEC CORP", "NIPPON ELECTRIC CO LTD",
            "NIPPON DENKI KK",
        },
        industry="electronics", edinet_code="E01765", ticker="6701",
        tse_section="Prime",
    ),
    Entity(
        "fujitsu", "Fujitsu Limited", "JP", "corporation",
        {
            "富士通株式会社", "富士通",
            "Fujitsu", "FUJITSU LTD", "FUJITSU KK",
        },
        industry="electronics", edinet_code="E01766", ticker="6702",
        tse_section="Prime",
    ),
    Entity(
        "kyocera", "Kyocera Corporation", "JP", "corporation",
        {
            "京セラ株式会社", "京セラ",
            "Kyocera", "KYOCERA CORP",
        },
        industry="electronics", ticker="6971", tse_section="Prime",
    ),
    Entity(
        "murata", "Murata Manufacturing Co., Ltd.", "JP", "corporation",
        {
            "株式会社村田製作所", "村田製作所",
            "Murata", "MURATA MFG CO LTD",
        },
        industry="electronics", ticker="6981", tse_section="Prime",
    ),
    Entity(
        "tdk", "TDK Corporation", "JP", "corporation",
        {
            "TDK株式会社", "TDK",
            "TDK Corp", "TDK CORP",
        },
        industry="electronics", ticker="6762", tse_section="Prime",
    ),
    Entity(
        "nidec", "Nidec Corporation", "JP", "corporation",
        {
            "日本電産株式会社", "日本電産", "ニデック",
            "Nidec", "NIDEC CORP", "NIHON DENSAN KK",
            "Nippon Densan",
        },
        industry="electronics", ticker="6594", tse_section="Prime",
    ),
    Entity(
        "omron", "OMRON Corporation", "JP", "corporation",
        {
            "オムロン株式会社", "オムロン",
            "Omron", "OMRON CORP",
        },
        industry="electronics", ticker="6645", tse_section="Prime",
    ),
    Entity(
        "keyence", "KEYENCE Corporation", "JP", "corporation",
        {
            "株式会社キーエンス", "キーエンス",
            "Keyence", "KEYENCE CORP",
        },
        industry="electronics", ticker="6861", tse_section="Prime",
    ),
    Entity(
        "rohm", "ROHM Co., Ltd.", "JP", "corporation",
        {
            "ローム株式会社", "ローム",
            "Rohm", "ROHM CO LTD",
        },
        industry="semiconductors", ticker="6963", tse_section="Prime",
    ),
    Entity(
        "renesas", "Renesas Electronics Corporation", "JP", "corporation",
        {
            "ルネサスエレクトロニクス株式会社", "ルネサス",
            "Renesas", "RENESAS ELECTRONICS CORP",
        },
        industry="semiconductors", ticker="6723", tse_section="Prime",
    ),

    # === Telecommunications ===
    Entity(
        "ntt", "Nippon Telegraph and Telephone Corporation", "JP", "corporation",
        {
            "日本電信電話株式会社", "NTT", "エヌ・ティ・ティ",
            "Nippon Telegraph", "NTT CORP", "NIPPON TELEGRAPH & TELEPHONE CORP",
        },
        industry="telecommunications", edinet_code="E04430", ticker="9432",
        tse_section="Prime",
    ),
    Entity(
        "kddi", "KDDI Corporation", "JP", "corporation",
        {
            "KDDI株式会社", "KDDI",
            "KDDI Corp", "KDDI CORP",
        },
        industry="telecommunications", ticker="9433", tse_section="Prime",
    ),
    Entity(
        "softbank", "SoftBank Group Corp.", "JP", "corporation",
        {
            "ソフトバンクグループ株式会社", "ソフトバンク",
            "SoftBank", "SOFTBANK GROUP CORP", "SOFTBANK CORP",
        },
        industry="telecommunications", ticker="9984", tse_section="Prime",
    ),

    # === Chemical / Materials ===
    Entity(
        "shin_etsu", "Shin-Etsu Chemical Co., Ltd.", "JP", "corporation",
        {
            "信越化学工業株式会社", "信越化学",
            "Shin-Etsu Chemical", "SHIN ETSU CHEM CO LTD",
        },
        industry="chemicals", ticker="4063", tse_section="Prime",
    ),
    Entity(
        "sumitomo_chemical", "Sumitomo Chemical Co., Ltd.", "JP", "corporation",
        {
            "住友化学株式会社", "住友化学",
            "Sumitomo Chemical", "SUMITOMO CHEM CO LTD",
        },
        industry="chemicals", ticker="4005", tse_section="Prime",
    ),
    Entity(
        "toray", "Toray Industries, Inc.", "JP", "corporation",
        {
            "東レ株式会社", "東レ",
            "Toray", "TORAY IND INC", "TORAY INDUSTRIES INC",
        },
        industry="chemicals", ticker="3402", tse_section="Prime",
    ),
    Entity(
        "asahi_kasei", "Asahi Kasei Corporation", "JP", "corporation",
        {
            "旭化成株式会社", "旭化成",
            "Asahi Kasei", "ASAHI KASEI CORP",
        },
        industry="chemicals", ticker="3407", tse_section="Prime",
    ),

    # === Pharmaceutical ===
    Entity(
        "takeda", "Takeda Pharmaceutical Company Limited", "JP", "corporation",
        {
            "武田薬品工業株式会社", "武田薬品", "タケダ",
            "Takeda", "Takeda Pharmaceutical", "TAKEDA PHARMACEUTICAL CO LTD",
            "TAKEDA YAKUHIN KOGYO KK",
        },
        industry="pharmaceutical", edinet_code="E00919", ticker="4502",
        tse_section="Prime",
    ),
    Entity(
        "astellas", "Astellas Pharma Inc.", "JP", "corporation",
        {
            "アステラス製薬株式会社", "アステラス",
            "Astellas", "ASTELLAS PHARMA INC",
        },
        industry="pharmaceutical", ticker="4503", tse_section="Prime",
    ),
    Entity(
        "daiichi_sankyo", "Daiichi Sankyo Company, Limited", "JP", "corporation",
        {
            "第一三共株式会社", "第一三共",
            "Daiichi Sankyo", "DAIICHI SANKYO CO LTD",
        },
        industry="pharmaceutical", ticker="4568", tse_section="Prime",
    ),
    Entity(
        "eisai", "Eisai Co., Ltd.", "JP", "corporation",
        {
            "エーザイ株式会社", "エーザイ",
            "Eisai", "EISAI CO LTD",
        },
        industry="pharmaceutical", ticker="4523", tse_section="Prime",
    ),
    Entity(
        "otsuka", "Otsuka Holdings Co., Ltd.", "JP", "corporation",
        {
            "大塚ホールディングス株式会社", "大塚製薬株式会社", "大塚製薬",
            "Otsuka", "OTSUKA HOLDINGS CO LTD", "OTSUKA PHARMACEUTICAL CO LTD",
        },
        industry="pharmaceutical", ticker="4578", tse_section="Prime",
    ),
    Entity(
        "shionogi", "Shionogi & Co., Ltd.", "JP", "corporation",
        {
            "塩野義製薬株式会社", "塩野義",
            "Shionogi", "SHIONOGI & CO LTD",
        },
        industry="pharmaceutical", ticker="4507", tse_section="Prime",
    ),

    # === Heavy Industry / Machinery ===
    Entity(
        "mitsubishi_heavy", "Mitsubishi Heavy Industries, Ltd.", "JP", "corporation",
        {
            "三菱重工業株式会社", "三菱重工",
            "Mitsubishi Heavy Industries", "MITSUBISHI HEAVY IND LTD",
            "MHI", "MITSUBISHI JUKOGYO KK",
        },
        industry="machinery", edinet_code="E02126", ticker="7011",
        tse_section="Prime",
    ),
    Entity(
        "ihi", "IHI Corporation", "JP", "corporation",
        {
            "株式会社IHI", "IHI",
            "IHI Corp", "IHI CORP",
            "ISHIKAWAJIMA HARIMA HEAVY IND",
        },
        industry="machinery", ticker="7013", tse_section="Prime",
    ),
    Entity(
        "komatsu", "Komatsu Ltd.", "JP", "corporation",
        {
            "株式会社小松製作所", "小松製作所", "コマツ",
            "Komatsu", "KOMATSU LTD",
        },
        industry="machinery", ticker="6301", tse_section="Prime",
    ),
    Entity(
        "fanuc", "FANUC Corporation", "JP", "corporation",
        {
            "ファナック株式会社", "ファナック",
            "Fanuc", "FANUC CORP",
        },
        industry="machinery", ticker="6954", tse_section="Prime",
    ),

    # === Steel / Mining ===
    Entity(
        "nippon_steel", "Nippon Steel Corporation", "JP", "corporation",
        {
            "日本製鉄株式会社", "日本製鉄", "新日鐵住金株式会社",
            "Nippon Steel", "NIPPON STEEL CORP",
            "NIPPON STEEL & SUMITOMO METAL CORP",
            "SHIN NIPPON SEITETSU KK",
        },
        industry="steel", ticker="5401", tse_section="Prime",
    ),
    Entity(
        "jfe", "JFE Holdings, Inc.", "JP", "corporation",
        {
            "JFEホールディングス株式会社", "JFEスチール株式会社", "JFE",
            "JFE Holdings", "JFE HOLDINGS INC", "JFE STEEL CORP",
        },
        industry="steel", ticker="5411", tse_section="Prime",
    ),

    # === Printing / Imaging ===
    Entity(
        "canon", "Canon Inc.", "JP", "corporation",
        {
            "キヤノン株式会社", "キヤノン", "キャノン",
            "Canon", "CANON INC", "CANON KK",
        },
        industry="imaging", edinet_code="E02130", ticker="7751",
        tse_section="Prime",
    ),
    Entity(
        "ricoh", "Ricoh Company, Ltd.", "JP", "corporation",
        {
            "株式会社リコー", "リコー",
            "Ricoh", "RICOH CO LTD",
        },
        industry="imaging", ticker="7752", tse_section="Prime",
    ),
    Entity(
        "fujifilm", "FUJIFILM Holdings Corporation", "JP", "corporation",
        {
            "富士フイルムホールディングス株式会社", "富士フイルム株式会社",
            "富士フイルム", "富士写真フイルム",
            "Fujifilm", "FUJIFILM HOLDINGS CORP", "FUJIFILM CORP",
            "FUJI PHOTO FILM CO LTD",
        },
        industry="imaging", ticker="4901", tse_section="Prime",
    ),
    Entity(
        "konica_minolta", "Konica Minolta, Inc.", "JP", "corporation",
        {
            "コニカミノルタ株式会社", "コニカミノルタ",
            "Konica Minolta", "KONICA MINOLTA INC",
        },
        industry="imaging", ticker="4902", tse_section="Prime",
    ),
    Entity(
        "seiko_epson", "Seiko Epson Corporation", "JP", "corporation",
        {
            "セイコーエプソン株式会社", "エプソン",
            "Seiko Epson", "Epson", "SEIKO EPSON CORP",
        },
        industry="imaging", ticker="6724", tse_section="Prime",
    ),
    Entity(
        "brother", "Brother Industries, Ltd.", "JP", "corporation",
        {
            "ブラザー工業株式会社", "ブラザー",
            "Brother", "BROTHER IND LTD", "BROTHER INDUSTRIES LTD",
        },
        industry="imaging", ticker="6448", tse_section="Prime",
    ),

    # === IT / Software ===
    Entity(
        "ntt_data", "NTT DATA Group Corporation", "JP", "corporation",
        {
            "株式会社NTTデータグループ", "NTTデータ",
            "NTT Data", "NTT DATA GROUP CORP", "NTT DATA CORP",
        },
        parent_id="ntt",
        industry="it_services", ticker="9613", tse_section="Prime",
    ),

    # === Glass / Ceramics ===
    Entity(
        "agc", "AGC Inc.", "JP", "corporation",
        {
            "AGC株式会社", "AGC", "旭硝子株式会社", "旭硝子",
            "AGC Inc", "AGC INC", "ASAHI GLASS CO LTD",
        },
        industry="glass", ticker="5201", tse_section="Prime",
    ),

    # === Rubber / Tires ===
    Entity(
        "bridgestone", "Bridgestone Corporation", "JP", "corporation",
        {
            "株式会社ブリヂストン", "ブリヂストン",
            "Bridgestone", "BRIDGESTONE CORP",
        },
        industry="rubber", ticker="5108", tse_section="Prime",
    ),

    # === Trading / Conglomerate ===
    Entity(
        "mitsubishi_corp", "Mitsubishi Corporation", "JP", "corporation",
        {
            "三菱商事株式会社", "三菱商事",
            "Mitsubishi Corporation", "MITSUBISHI CORP",
        },
        industry="trading", ticker="8058", tse_section="Prime",
    ),
]
