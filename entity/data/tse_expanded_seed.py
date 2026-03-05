"""Expanded JP entity seed from top assignee variants not covered in TSE_PRIME_ENTITIES."""
from entity.registry import Entity

TSE_EXPANDED_ENTITIES: list[Entity] = [
    # === Printing / Consumer ===
    Entity(
        "dnp", "Dai Nippon Printing Co., Ltd.", "JP", "corporation",
        {
            "大日本印刷株式会社", "大日本印刷", "DNP",
            "Dai Nippon Printing", "Dainippon Printing Co Ltd", "DAINIPPON PRINTING CO LTD",
        },
        industry="printing", ticker="7912", tse_section="Prime",
    ),
    Entity(
        "toppan", "TOPPAN Holdings Inc.", "JP", "corporation",
        {
            "凸版印刷株式会社", "凸版印刷", "TOPPAN",
            "Toppan Printing", "Toppan Printing Co Ltd", "TOPPAN PRINTING CO LTD",
            "TOPPAN Holdings", "TOPPAN HOLDINGS INC",
        },
        industry="printing", ticker="7911", tse_section="Prime",
    ),
    Entity(
        "kao", "Kao Corporation", "JP", "corporation",
        {
            "花王株式会社", "花王",
            "Kao", "Kao Corp", "KAO CORP",
        },
        industry="consumer_goods", ticker="4452", tse_section="Prime",
    ),

    # === Electronics / Components ===
    Entity(
        "sumitomo_electric", "Sumitomo Electric Industries, Ltd.", "JP", "corporation",
        {
            "住友電気工業株式会社", "住友電気工業",
            "Sumitomo Electric", "Sumitomo Electric Ind Ltd", "SUMITOMO ELECTRIC INDUSTRIES",
        },
        industry="electronics", ticker="5802", tse_section="Prime",
    ),
    Entity(
        "yazaki", "Yazaki Corporation", "JP", "corporation",
        {
            "矢崎総業株式会社", "矢崎総業",
            "Yazaki", "Yazaki Corp", "YAZAKI CORP",
        },
        industry="automotive_components", ticker=None, tse_section=None,
    ),
    Entity(
        "semiconductor_energy_laboratory", "Semiconductor Energy Laboratory Co., Ltd.", "JP", "corporation",
        {
            "株式会社半導体エネルギー研究所", "半導体エネルギー研究所",
            "Semiconductor Energy Laboratory", "Semiconductor Energy Lab Co Ltd", "SEL",
        },
        industry="semiconductors", ticker=None, tse_section=None,
    ),
    Entity(
        "sekisui_chemical", "Sekisui Chemical Co., Ltd.", "JP", "corporation",
        {
            "積水化学工業株式会社", "積水化学工業", "積水化学",
            "Sekisui Chemical", "Sekisui Chem Co Ltd", "SEKISUI CHEMICAL CO LTD",
        },
        industry="chemicals", ticker="4204", tse_section="Prime",
    ),
    Entity(
        "sumitomo_wiring_systems", "Sumitomo Wiring Systems, Ltd.", "JP", "corporation",
        {
            "住友電装株式会社", "住友電装",
            "Sumitomo Wiring Systems", "Sumitomo Wiring Syst Ltd", "SUMITOMO WIRING SYSTEMS",
        },
        parent_id="sumitomo_electric",
        industry="automotive_components", ticker=None, tse_section=None,
    ),
    Entity(
        "nsk", "NSK Ltd.", "JP", "corporation",
        {
            "日本精工株式会社", "日本精工",
            "NSK", "Nsk Ltd", "NSK LTD",
        },
        industry="machinery", ticker="6471", tse_section="Prime",
    ),
    Entity(
        "toshiba_tec", "Toshiba Tec Corporation", "JP", "corporation",
        {
            "東芝テック株式会社", "東芝テック",
            "Toshiba Tec", "Toshiba Tec Corp", "TOSHIBA TEC KK",
        },
        parent_id="toshiba",
        industry="electronics", ticker="6588", tse_section="Prime",
    ),
    Entity(
        "kobe_steel", "Kobe Steel, Ltd.", "JP", "corporation",
        {
            "株式会社神戸製鋼所", "神戸製鋼所",
            "Kobe Steel", "Kobe Steel Ltd", "KOBE STEEL LTD",
        },
        industry="steel", ticker="5406", tse_section="Prime",
    ),
    Entity(
        "nitto_denko", "Nitto Denko Corporation", "JP", "corporation",
        {
            "日東電工株式会社", "日東電工",
            "Nitto Denko", "Nitto Denko Corp", "NITTO DENKO CORP",
        },
        industry="chemicals", ticker="6988", tse_section="Prime",
    ),
    Entity(
        "fuji_electric", "Fuji Electric Co., Ltd.", "JP", "corporation",
        {
            "富士電機株式会社", "富士電機",
            "Fuji Electric", "Fuji Electric Co Ltd", "FUJI ELECTRIC CO LTD",
        },
        industry="electronics", ticker="6504", tse_section="Prime",
    ),
    Entity(
        "yamaha", "Yamaha Corporation", "JP", "corporation",
        {
            "ヤマハ株式会社", "ヤマハ",
            "Yamaha", "Yamaha Corp", "YAMAHA CORP",
        },
        industry="electronics", ticker="7951", tse_section="Prime",
    ),
    Entity(
        "ntn", "NTN Corporation", "JP", "corporation",
        {
            "Ｎｔｎ株式会社", "NTN株式会社", "NTN",
            "Ntn Corp", "NTN TOYO BEARING CO LTD", "NTN CORP",
        },
        industry="automotive_components", ticker="6472", tse_section="Prime",
    ),
    Entity(
        "furukawa_electric", "Furukawa Electric Co., Ltd.", "JP", "corporation",
        {
            "古河電気工業株式会社", "古河電気工業",
            "Furukawa Electric", "Furukawa Electric Co Ltd:The", "FURUKAWA ELECTRIC CO LTD",
        },
        industry="electronics", ticker="5801", tse_section="Prime",
    ),
    Entity(
        "shimadzu", "Shimadzu Corporation", "JP", "corporation",
        {
            "株式会社島津製作所", "島津製作所",
            "Shimadzu", "Shimadzu Corp", "SHIMADZU CORP",
        },
        industry="precision_instruments", ticker="7701", tse_section="Prime",
    ),
    Entity(
        "mitsubishi_materials", "Mitsubishi Materials Corporation", "JP", "corporation",
        {
            "三菱マテリアル株式会社", "三菱マテリアル",
            "Mitsubishi Materials", "Mitsubishi Materials Corp", "MITSUBISHI MATERIALS CORP",
        },
        industry="materials", ticker="5711", tse_section="Prime",
    ),
    Entity(
        "pioneer", "Pioneer Corporation", "JP", "corporation",
        {
            "パイオニア株式会社", "パイオニア",
            "Pioneer", "PIONEER CORP", "PIONEER CORPORATION",
        },
        industry="electronics", ticker="6773", tse_section=None,
    ),
    Entity(
        "iseki", "ISEKI & CO., LTD.", "JP", "corporation",
        {
            "井関農機株式会社", "井関農機",
            "Iseki", "ISEKI", "ISEKI & CO LTD",
        },
        industry="machinery", ticker="6310", tse_section="Prime",
    ),
    Entity(
        "fujikura", "Fujikura Ltd.", "JP", "corporation",
        {
            "株式会社フジクラ", "フジクラ",
            "Fujikura", "FUJIKURA LTD",
        },
        industry="electronics", ticker="5803", tse_section="Prime",
    ),
    Entity(
        "mitsubishi_motors", "Mitsubishi Motors Corporation", "JP", "corporation",
        {
            "三菱自動車工業株式会社", "三菱自動車工業", "三菱自動車",
            "Mitsubishi Motors", "MITSUBISHI MOTORS CORP",
        },
        industry="automotive", ticker="7211", tse_section="Prime",
    ),
    Entity(
        "daihatsu", "Daihatsu Motor Co., Ltd.", "JP", "corporation",
        {
            "ダイハツ工業株式会社", "ダイハツ工業", "ダイハツ",
            "Daihatsu", "DAIHATSU MOTOR CO LTD",
        },
        parent_id="toyota",
        industry="automotive", ticker=None, tse_section=None,
    ),
    Entity(
        "oki", "OKI Electric Industry Co., Ltd.", "JP", "corporation",
        {
            "沖電気工業株式会社", "沖電気工業", "OKI",
            "Oki Electric", "Oki Electric Ind Co Ltd", "OKI ELECTRIC IND CO LTD",
        },
        industry="electronics", ticker="6703", tse_section="Prime",
    ),
    Entity(
        "murata_machinery", "Murata Machinery, Ltd.", "JP", "corporation",
        {
            "村田機械株式会社", "村田機械",
            "Murata Machinery", "MURATA MACHINERY LTD",
        },
        industry="machinery", ticker=None, tse_section=None,
    ),
    Entity(
        "funai_electric", "Funai Electric Co., Ltd.", "JP", "corporation",
        {
            "船井電機株式会社", "船井電機",
            "Funai Electric", "Funai Electric Co Ltd", "FUNAI ELECTRIC CO",
        },
        industry="electronics", ticker=None, tse_section=None,
    ),
    Entity(
        "niterra", "Niterra Co., Ltd.", "JP", "corporation",
        {
            "日本特殊陶業株式会社", "日本特殊陶業",
            "Niterra", "NGK Spark Plug", "NGK SPARK PLUG CO LTD",
        },
        industry="automotive_components", ticker="5334", tse_section="Prime",
    ),
    Entity(
        "disco", "DISCO Corporation", "JP", "corporation",
        {
            "株式会社ディスコ", "ディスコ",
            "Disco", "Disco Corp", "DISCO CORP",
        },
        industry="semiconductors", ticker="6146", tse_section="Prime",
    ),
    Entity(
        "ngk_insulators", "NGK Insulators, Ltd.", "JP", "corporation",
        {
            "日本碍子株式会社", "日本碍子",
            "NGK Insulators", "NGK INSULATORS LTD",
        },
        industry="ceramics", ticker="5333", tse_section="Prime",
    ),
    Entity(
        "sumitomo_bakelite", "Sumitomo Bakelite Co., Ltd.", "JP", "corporation",
        {
            "住友ベークライト株式会社", "住友ベークライト",
            "Sumitomo Bakelite", "SUMITOMO BAKELITE CO LTD",
        },
        industry="chemicals", ticker="4203", tse_section="Prime",
    ),
    Entity(
        "kaneka", "Kaneka Corporation", "JP", "corporation",
        {
            "株式会社カネカ", "カネカ",
            "Kaneka", "KANEKA CORP", "KANEKA CORPORATION",
        },
        industry="chemicals", ticker="4118", tse_section="Prime",
    ),
    Entity(
        "sumitomo_heavy", "Sumitomo Heavy Industries, Ltd.", "JP", "corporation",
        {
            "住友重機械工業株式会社", "住友重機械工業",
            "Sumitomo Heavy", "SUMITOMO HEAVY INDUSTRIES LTD",
        },
        industry="machinery", ticker="6302", tse_section="Prime",
    ),
    Entity(
        "isuzu", "Isuzu Motors Limited", "JP", "corporation",
        {
            "いすゞ自動車株式会社", "いすゞ自動車",
            "Isuzu", "Isuzu Motors", "ISUZU MOTORS LTD",
        },
        industry="automotive", ticker="7202", tse_section="Prime",
    ),
    Entity(
        "tokyo_electron", "Tokyo Electron Limited", "JP", "corporation",
        {
            "東京エレクトロン株式会社", "東京エレクトロン",
            "Tokyo Electron", "Tokyo Electron Ltd", "TOKYO ELECTRON LTD",
        },
        industry="semiconductors", ticker="8035", tse_section="Prime",
    ),
    Entity(
        "proterial", "Proterial, Ltd.", "JP", "corporation",
        {
            "日立金属株式会社", "日立金属", "株式会社プロテリアル", "プロテリアル",
            "Proterial", "Hitachi Metals", "HITACHI METALS LTD",
        },
        industry="materials", ticker=None, tse_section=None,
    ),
    Entity(
        "ntt_docomo", "NTT DOCOMO, INC.", "JP", "corporation",
        {
            "株式会社エヌ・ティ・ティ・ドコモ", "NTTドコモ", "ドコモ",
            "Ntt Docomo Inc", "NTT DOCOMO INC", "NTT DOCOMO",
        },
        parent_id="ntt",
        industry="telecommunications", ticker="9437", tse_section=None,
    ),
    Entity(
        "yamaha_motor", "Yamaha Motor Co., Ltd.", "JP", "corporation",
        {
            "ヤマハ発動機株式会社", "ヤマハ発動機",
            "Yamaha Motor", "YAMAHA MOTOR CO LTD",
        },
        industry="automotive", ticker="7272", tse_section="Prime",
    ),
    Entity(
        "hitachi_construction", "Hitachi Construction Machinery Co., Ltd.", "JP", "corporation",
        {
            "日立建機株式会社", "日立建機",
            "Hitachi Construction Machinery", "HITACHI CONSTRUCTION MACHINERY CO LTD",
        },
        industry="machinery", ticker="6305", tse_section="Prime",
    ),
    Entity(
        "sumitomo_metal_mining", "Sumitomo Metal Mining Co., Ltd.", "JP", "corporation",
        {
            "住友金属鉱山株式会社", "住友金属鉱山",
            "Sumitomo Metal Mining", "SUMITOMO METAL MINING CO LTD",
        },
        industry="materials", ticker="5713", tse_section="Prime",
    ),
    Entity(
        "toyota_boshoku", "Toyota Boshoku Corporation", "JP", "corporation",
        {
            "トヨタ紡織株式会社", "トヨタ紡織",
            "Toyota Boshoku", "TOYOTA BOSHOKU CORP",
        },
        industry="automotive_components", ticker="3116", tse_section="Prime",
    ),
    Entity(
        "shimizu", "Shimizu Corporation", "JP", "corporation",
        {
            "清水建設株式会社", "清水建設",
            "Shimizu", "Shimizu Corp", "SHIMIZU CORP",
        },
        industry="construction", ticker="1803", tse_section="Prime",
    ),
    Entity(
        "terumo", "Terumo Corporation", "JP", "corporation",
        {
            "テルモ株式会社", "テルモ",
            "Terumo", "TERUMO CORP",
        },
        industry="medical_devices", ticker="4543", tse_section="Prime",
    ),
    Entity(
        "hoya", "HOYA Corporation", "JP", "corporation",
        {
            "Ｈｏｙａ株式会社", "HOYA株式会社", "HOYA",
            "Hoya", "HOYA CORP",
        },
        industry="precision_instruments", ticker="7741", tse_section="Prime",
    ),
    Entity(
        "resonac", "Resonac Holdings Corporation", "JP", "corporation",
        {
            "昭和電工株式会社", "昭和電工", "レゾナック・ホールディングス株式会社", "レゾナック",
            "Resonac", "RESONAC HOLDINGS CORP", "Showa Denko", "SHOWA DENKO KK",
        },
        industry="chemicals", ticker="4004", tse_section="Prime",
    ),
    Entity(
        "kuraray", "Kuraray Co., Ltd.", "JP", "corporation",
        {
            "株式会社クラレ", "クラレ",
            "Kuraray", "KURARAY CO LTD",
        },
        industry="chemicals", ticker="3405", tse_section="Prime",
    ),
    Entity(
        "tokai_rika", "Tokai Rika Co., Ltd.", "JP", "corporation",
        {
            "株式会社東海理化電機製作所", "東海理化電機製作所",
            "Tokai Rika", "TOKAI RIKA CO LTD",
        },
        industry="automotive_components", ticker="6995", tse_section="Prime",
    ),
    Entity(
        "ebara", "Ebara Corporation", "JP", "corporation",
        {
            "株式会社荏原製作所", "荏原製作所",
            "Ebara", "EBARA CORP",
        },
        industry="machinery", ticker="6361", tse_section="Prime",
    ),
    Entity(
        "nippon_shokubai", "Nippon Shokubai Co., Ltd.", "JP", "corporation",
        {
            "株式会社日本触媒", "日本触媒",
            "Nippon Shokubai", "NIPPON SHOKUBAI CO LTD",
        },
        industry="chemicals", ticker="4114", tse_section="Prime",
    ),
    Entity(
        "dic", "DIC Corporation", "JP", "corporation",
        {
            "Ｄｉｃ株式会社", "DIC株式会社", "DIC",
            "Dic", "DIC Corp", "DIC CORPORATION",
        },
        industry="chemicals", ticker="4631", tse_section="Prime",
    ),
]
