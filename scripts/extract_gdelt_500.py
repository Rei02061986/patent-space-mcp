"""Extract GDELT 5-axis features for top 500 firms × recent quarters.

Expands from 46 → 500 firms. Handles duplicate firm_ids
(named IDs like 'panasonic' and ticker IDs like 'company_6752' that
refer to the same company) by only querying once per English name set.

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

# ──────────────────────────────────────────────────────────────────
# Comprehensive firm_id → GDELT English search names mapping
# Includes BOTH named IDs and ticker IDs for top 500 patent filers
# ──────────────────────────────────────────────────────────────────

FIRM_GDELT_NAMES: dict[str, list[str]] = {
    # ═══ Original 46 named firms (from extract_gdelt_features.py) ═══
    "panasonic": ["PANASONIC"],
    "canon": ["CANON"],
    "toyota": ["TOYOTA", "TOYOTA MOTOR"],
    "toshiba": ["TOSHIBA"],
    "mitsubishi_electric": ["MITSUBISHI ELECTRIC"],
    "hitachi": ["HITACHI"],
    "ricoh": ["RICOH"],
    "fujifilm": ["FUJIFILM", "FUJI FILM"],
    "denso": ["DENSO"],
    "sharp": ["SHARP CORPORATION"],
    "fujitsu": ["FUJITSU"],
    "honda": ["HONDA", "HONDA MOTOR"],
    "seiko_epson": ["SEIKO EPSON", "EPSON"],
    "kyocera": ["KYOCERA"],
    "sony": ["SONY"],
    "ntt": ["NTT", "NIPPON TELEGRAPH"],
    "dnp": ["DAI NIPPON PRINTING"],
    "nissan": ["NISSAN", "NISSAN MOTOR"],
    "konica_minolta": ["KONICA MINOLTA"],
    "nec": ["NEC CORPORATION"],
    "sumitomo_electric": ["SUMITOMO ELECTRIC"],
    "nippon_steel": ["NIPPON STEEL"],
    "toppan": ["TOPPAN"],
    "kao": ["KAO CORPORATION"],
    "jfe": ["JFE HOLDINGS", "JFE STEEL"],
    "yazaki": ["YAZAKI"],
    "murata": ["MURATA MANUFACTURING"],
    "toray": ["TORAY INDUSTRIES", "TORAY"],
    "bridgestone": ["BRIDGESTONE"],
    "sekisui_chemical": ["SEKISUI CHEMICAL", "SEKISUI"],
    "mitsubishi_heavy": ["MITSUBISHI HEAVY INDUSTRIES"],
    "sumitomo_chemical": ["SUMITOMO CHEMICAL"],
    "brother": ["BROTHER INDUSTRIES"],
    "resonac": ["RESONAC", "SHOWA DENKO"],
    "fuji_electric": ["FUJI ELECTRIC"],
    "daikin": ["DAIKIN INDUSTRIES", "DAIKIN"],
    "olympus": ["OLYMPUS CORPORATION"],
    "mazda": ["MAZDA MOTOR"],
    "suzuki": ["SUZUKI MOTOR"],
    "subaru": ["SUBARU", "FUJI HEAVY INDUSTRIES"],
    "asahi_kasei": ["ASAHI KASEI"],
    "nikon": ["NIKON CORPORATION"],
    "omron": ["OMRON CORPORATION"],
    "ihi": ["IHI CORPORATION"],
    "kobe_steel": ["KOBE STEEL"],
    "yaskawa": ["YASKAWA ELECTRIC"],
    "kubota": ["KUBOTA CORPORATION"],
    "mitsubishi_chemical": ["MITSUBISHI CHEMICAL"],
    "sumitomo_metal_mining": ["SUMITOMO METAL MINING"],
    "aisin": ["AISIN"],

    # ═══ Additional named firms (not in original 46) ═══
    "dainippon_screen": ["SCREEN HOLDINGS"],
    "nidec": ["NIDEC"],
    "rohm": ["ROHM"],
    "renesas": ["RENESAS ELECTRONICS"],
    "alps_alpine": ["ALPS ALPINE"],
    "tdk": ["TDK CORPORATION"],
    "mitsui_chemicals": ["MITSUI CHEMICALS"],
    "shin_etsu": ["SHIN-ETSU CHEMICAL"],
    "nippon_kayaku": ["NIPPON KAYAKU"],
    "daicel": ["DAICEL CORPORATION"],
    "ube": ["UBE CORPORATION"],
    "tosoh": ["TOSOH CORPORATION"],
    "nitto_denko": ["NITTO DENKO"],
    "ngk_insulators": ["NGK INSULATORS"],
    "ngk_spark_plug": ["NGK SPARK PLUG"],
    "shimadzu": ["SHIMADZU CORPORATION"],
    "horiba": ["HORIBA"],
    "keyence": ["KEYENCE"],
    "disco": ["DISCO CORPORATION"],
    "hamamatsu_photonics": ["HAMAMATSU PHOTONICS"],
    "yokogawa": ["YOKOGAWA ELECTRIC"],
    "casio": ["CASIO"],
    "citizen": ["CITIZEN WATCH"],
    "koito_mfg": ["KOITO MANUFACTURING"],
    "stanley_electric": ["STANLEY ELECTRIC"],
    "mitsuba": ["MITSUBA CORPORATION"],
    "toyoda_gosei": ["TOYODA GOSEI"],
    "jtekt": ["JTEKT CORPORATION"],
    "nippon_seiki": ["NIPPON SEIKI"],
    "hino_motors": ["HINO MOTORS"],
    "idemitsu": ["IDEMITSU KOSAN"],
    "taiheiyo_cement": ["TAIHEIYO CEMENT"],
    "lixil": ["LIXIL GROUP", "LIXIL"],
    "unicharm": ["UNICHARM"],
    "rinnai": ["RINNAI CORPORATION"],
    "meidensha": ["MEIDENSHA"],
    "lintec": ["LINTEC CORPORATION"],
    "teijin": ["TEIJIN"],
    "shiseido": ["SHISEIDO"],
    "shimano": ["SHIMANO"],
    "makita": ["MAKITA"],
    "ulvac": ["ULVAC"],
    "daifuku": ["DAIFUKU"],
    "ushio": ["USHIO"],
    "sekisui_house": ["SEKISUI HOUSE"],
    "unitika": ["UNITIKA"],
    "taiyo_yuden": ["TAIYO YUDEN"],
    "amada": ["AMADA"],
    "toyobo": ["TOYOBO"],
    "ts_tech": ["TS TECH"],
    "shin_etsu_polymer": ["SHIN-ETSU POLYMER"],
    "tokyo_ohka": ["TOKYO OHKA KOGYO"],
    "nippon_paper": ["NIPPON PAPER"],
    "jae": ["JAPAN AVIATION ELECTRONICS"],
    "sumitomo_osaka_cement": ["SUMITOMO OSAKA CEMENT"],
    "glory": ["GLORY"],
    "sanden": ["SANDEN HOLDINGS"],
    "azbil": ["AZBIL CORPORATION"],
    "tokuyama": ["TOKUYAMA CORPORATION"],
    "kokuyo": ["KOKUYO"],
    "daihen": ["DAIHEN CORPORATION"],
    "nohmi_bosai": ["NOHMI BOSAI"],
    "daido_steel": ["DAIDO STEEL"],
    "advantest": ["ADVANTEST"],
    "japan_steel_works": ["JAPAN STEEL WORKS"],
    "nippon_signal": ["NIPPON SIGNAL"],
    "anritsu": ["ANRITSU"],
    "fuji_corp": ["FUJI CORPORATION"],
    "max_co": ["MAX CO"],
    "central_glass": ["CENTRAL GLASS"],
    "hochiki": ["HOCHIKI"],
    "miura": ["MIURA CO"],
    "nintendo": ["NINTENDO"],
    "aisan": ["AISAN INDUSTRY"],
    "nippon_sheet_glass": ["NIPPON SHEET GLASS"],
    "mitsubishi_pencil": ["MITSUBISHI PENCIL"],
    "juki": ["JUKI CORPORATION"],
    "maxell": ["MAXELL"],
    "kansai_paint": ["KANSAI PAINT"],
    "aisin_corp": ["AISIN CORPORATION"],
    "sato": ["SATO HOLDINGS"],
    "jt": ["JAPAN TOBACCO"],
    "adeka": ["ADEKA CORPORATION"],
    "nhk_spring": ["NHK SPRING"],
    "tokyo_seimitsu": ["TOKYO SEIMITSU"],
    "jeol": ["JEOL"],
    "shindengen": ["SHINDENGEN ELECTRIC"],
    "shibaura": ["SHIBAURA MECHATRONICS"],
    "shinmaywa": ["SHINMAYWA INDUSTRIES"],
    "nifco": ["NIFCO"],
    "rakuten": ["RAKUTEN"],
    "bunka_shutter": ["BUNKA SHUTTER"],
    "organo": ["ORGANO CORPORATION"],
    "gunze": ["GUNZE"],
    "hioki": ["HIOKI"],
    "kobayashi": ["KOBAYASHI PHARMACEUTICAL"],
    "sumco": ["SUMCO"],
    "toagosei": ["TOAGOSEI"],
    "nippon_sharyo": ["NIPPON SHARYO"],
    "ichikoh": ["ICHIKOH INDUSTRIES"],
    "denka": ["DENKA"],
    "nabtesco": ["NABTESCO"],
    "daicel_corp": ["DAICEL"],
    "nichiyu": ["NICHIYU"],
    "nipro": ["NIPRO"],
    "sysmex": ["SYSMEX"],
    "kurimoto": ["KURIMOTO"],
    "corona_corp": ["CORONA CORPORATION"],
    "kose": ["KOSE CORPORATION"],
    "nippon_light_metal": ["NIPPON LIGHT METAL"],
    "horiba_corp": ["HORIBA"],
    "sanken_electric": ["SANKEN ELECTRIC"],
    "takeda": ["TAKEDA PHARMACEUTICAL", "TAKEDA"],
    "yokogawa_corp": ["YOKOGAWA ELECTRIC"],

    # ═══ Ticker-based firms (company_XXXX) ═══
    # Duplicates of named firms — will be queried once, stored under both IDs
    "company_6724": ["SEIKO EPSON", "EPSON"],           # = seiko_epson
    "company_6758": ["SONY"],                            # = sony
    "company_6701": ["NEC CORPORATION"],                 # = nec
    "company_6752": ["PANASONIC"],                       # = panasonic
    "company_4901": ["FUJIFILM"],                        # = fujifilm
    "company_6448": ["BROTHER INDUSTRIES"],              # = brother
    "company_7011": ["MITSUBISHI HEAVY INDUSTRIES"],     # = mitsubishi_heavy
    "company_7731": ["NIKON CORPORATION"],               # = nikon
    "company_6952": ["CASIO"],                           # = casio
    "company_7733": ["OLYMPUS CORPORATION"],             # = olympus
    "company_6367": ["DAIKIN INDUSTRIES"],               # = daikin
    "company_6326": ["KUBOTA CORPORATION"],              # = kubota
    "company_6506": ["YASKAWA ELECTRIC"],                # = yaskawa
    "company_4188": ["MITSUBISHI CHEMICAL"],             # = mitsubishi_chemical
    "company_7259": ["AISIN CORPORATION"],               # = aisin

    # Major companies (new)
    "company_6417": ["SANKYO"],                          # SANKYO (pachinko)
    "company_6201": ["TOYOTA INDUSTRIES"],               # 豊田自動織機
    "company_5110": ["SUMITOMO RUBBER"],                 # 住友ゴム工業
    "company_6473": ["JTEKT"],                           # ジェイテクト
    "company_5101": ["YOKOHAMA RUBBER"],                 # 横浜ゴム
    "company_4063": ["SHIN-ETSU CHEMICAL"],              # 信越化学工業
    "company_4183": ["MITSUI CHEMICALS"],                # 三井化学
    "company_6425": ["UNIVERSAL ENTERTAINMENT"],         # ユニバーサルエンターテインメント
    "company_9504": ["CHUGOKU ELECTRIC POWER"],          # 中国電力
    "company_7012": ["KAWASAKI HEAVY INDUSTRIES"],       # 川崎重工業
    "company_9532": ["OSAKA GAS"],                       # 大阪瓦斯
    "company_6723": ["RENESAS ELECTRONICS"],             # ルネサスエレクトロニクス
    "company_7282": ["TOYODA GOSEI"],                    # 豊田合成
    "company_6412": ["HEIWA CORPORATION"],               # 平和
    "company_6841": ["YOKOGAWA ELECTRIC"],               # 横河電機
    "company_9984": ["SOFTBANK GROUP"],                  # ソフトバンクグループ
    "company_5332": ["TOTO"],                            # TOTO
    "company_1801": ["TAISEI CORPORATION"],              # 大成建設
    "company_4042": ["TOSOH CORPORATION"],               # 東ソー
    "company_4205": ["ZEON CORPORATION"],                # 日本ゼオン
    "company_1802": ["OBAYASHI CORPORATION"],            # 大林組
    "company_1812": ["KAJIMA CORPORATION"],              # 鹿島建設
    "company_6923": ["STANLEY ELECTRIC"],                # スタンレー電気
    "company_9531": ["TOKYO GAS"],                       # 東京瓦斯
    "company_6632": ["JVC KENWOOD"],                     # JVCケンウッド
    "company_6257": ["FUJISHOJI"],                       # 藤商事
    "company_3880": ["DAIO PAPER"],                      # 大王製紙
    "company_5019": ["IDEMITSU KOSAN"],                  # 出光興産
    "company_8113": ["UNICHARM"],                        # ユニ・チャーム
    "company_5947": ["RINNAI"],                          # リンナイ
    "company_6508": ["MEIDENSHA"],                       # 明電舎
    "company_7966": ["LINTEC"],                          # リンテック
    "company_3401": ["TEIJIN"],                          # 帝人
    "company_6965": ["HAMAMATSU PHOTONICS"],             # 浜松ホトニクス
    "company_5938": ["LIXIL"],                           # LIXIL
    "company_4182": ["MITSUBISHI GAS CHEMICAL"],         # 三菱瓦斯化学
    "company_4912": ["LION CORPORATION"],                # ライオン
    "company_7276": ["KOITO MANUFACTURING"],             # 小糸製作所
    "company_6370": ["KURITA WATER INDUSTRIES"],         # 栗田工業
    "company_5214": ["NIPPON ELECTRIC GLASS"],           # 日本電気硝子
    "company_9501": ["TEPCO", "TOKYO ELECTRIC POWER"],   # 東京電力
    "company_6740": ["JAPAN DISPLAY"],                   # ジャパンディスプレイ
    "company_5233": ["TAIHEIYO CEMENT"],                 # 太平洋セメント
    "company_5943": ["NORITZ"],                          # ノーリツ
    "company_7240": ["NOK CORPORATION"],                 # NOK
    "company_4062": ["IBIDEN"],                          # イビデン
    "company_7287": ["NIPPON SEIKI"],                    # 日本精機
    "company_4911": ["SHISEIDO"],                        # 資生堂
    "company_7309": ["SHIMANO"],                         # シマノ
    "company_1925": ["DAIWA HOUSE"],                     # 大和ハウス工業
    "company_6586": ["MAKITA"],                          # マキタ
    "company_3864": ["MITSUBISHI PAPER MILLS"],          # 三菱製紙
    "company_6728": ["ULVAC"],                           # アルバック
    "company_7205": ["HINO MOTORS"],                     # 日野自動車
    "company_4471": ["SANYO CHEMICAL INDUSTRIES"],       # 三洋化成工業
    "company_4272": ["NIPPON KAYAKU"],                   # 日本化薬
    "company_2802": ["AJINOMOTO"],                       # 味の素
    "company_5901": ["TOYO SEIKAN"],                     # 東洋製罐グループ
    "company_7735": ["SCREEN HOLDINGS"],                 # SCREENホールディングス
    "company_3861": ["OJI HOLDINGS"],                    # 王子ホールディングス
    "company_6383": ["DAIFUKU"],                         # ダイフク
    "company_6925": ["USHIO"],                           # ウシオ電機
    "company_1928": ["SEKISUI HOUSE"],                   # 積水ハウス
    "company_7280": ["MITSUBA"],                         # ミツバ
    "company_7762": ["CITIZEN WATCH"],                   # シチズン時計
    "company_3103": ["UNITIKA"],                         # ユニチカ
    "company_6976": ["TAIYO YUDEN"],                     # 太陽誘電
    "company_2654": ["ASMO"],                            # アスモ
    "company_6113": ["AMADA"],                           # アマダ
    "company_6413": ["RISO KAGAKU"],                     # 理想科学工業
    "company_3101": ["TOYOBO"],                          # 東洋紡
    "company_7313": ["TS TECH"],                         # テイ・エス テック
    "company_7970": ["SHIN-ETSU POLYMER"],               # 信越ポリマー
    "company_4186": ["TOKYO OHKA KOGYO"],                # 東京応化工業
    "company_3863": ["NIPPON PAPER"],                    # 日本製紙
    "company_6807": ["JAPAN AVIATION ELECTRONICS"],      # 日本航空電子工業
    "company_5232": ["SUMITOMO OSAKA CEMENT"],           # 住友大阪セメント
    "company_6457": ["GLORY"],                           # グローリー
    "company_6444": ["SANDEN HOLDINGS"],                 # サンデン
    "company_6845": ["AZBIL"],                           # アズビル
    "company_4043": ["TOKUYAMA"],                        # トクヤマ
    "company_7984": ["KOKUYO"],                          # コクヨ
    "company_6622": ["DAIHEN"],                          # ダイヘン
    "company_6744": ["NOHMI BOSAI"],                     # 能美防災
    "company_7744": ["NORITSU KOKI"],                    # ノーリツ鋼機
    "company_4228": ["SEKISUI KASEI"],                   # 積水化成品工業
    "company_5471": ["DAIDO STEEL"],                     # 大同特殊鋼
    "company_8060": ["CANON MARKETING JAPAN"],           # キヤノンMJ
    "company_6857": ["ADVANTEST"],                       # アドバンテスト
    "company_9020": ["JR EAST", "EAST JAPAN RAILWAY"],   # 東日本旅客鉄道
    "company_5631": ["JAPAN STEEL WORKS"],               # 日本製鋼所
    "company_6741": ["NIPPON SIGNAL"],                   # 日本信号
    "company_6754": ["ANRITSU"],                         # アンリツ
    "company_6134": ["FUJI CORPORATION"],                # FUJI
    "company_6454": ["MAX CO"],                          # マックス
    "company_4044": ["CENTRAL GLASS"],                   # セントラル硝子
    "company_6745": ["HOCHIKI"],                         # ホーチキ
    "company_6005": ["MIURA CO"],                        # 三浦工業
    "company_7974": ["NINTENDO"],                        # 任天堂
    "company_7283": ["AISAN INDUSTRY"],                  # 愛三工業
    "company_5202": ["NIPPON SHEET GLASS"],              # 日本板硝子
    "company_7976": ["MITSUBISHI PENCIL"],               # 三菱鉛筆
    "company_6440": ["JUKI"],                            # JUKI
    "company_6810": ["MAXELL"],                          # マクセル
    "company_9503": ["KANSAI ELECTRIC POWER"],           # 関西電力
    "company_4613": ["KANSAI PAINT"],                    # 関西ペイント
    "company_6287": ["SATO HOLDINGS"],                   # サトー
    "company_2914": ["JAPAN TOBACCO", "JT"],             # 日本たばこ産業
    "company_4401": ["ADEKA"],                           # ADEKA
    "company_5991": ["NHK SPRING"],                      # 日本発條
    "company_7729": ["TOKYO SEIMITSU"],                  # 東京精密
    "company_6951": ["JEOL"],                            # 日本電子
    "company_6844": ["SHINDENGEN ELECTRIC"],             # 新電元工業
    "company_6590": ["SHIBAURA MECHATRONICS"],           # 芝浦メカトロニクス
    "company_7224": ["SHINMAYWA INDUSTRIES"],            # 新明和工業
    "company_7988": ["NIFCO"],                           # ニフコ
    "company_4755": ["RAKUTEN"],                         # 楽天グループ
    "company_5930": ["BUNKA SHUTTER"],                   # 文化シヤッター
    "company_6368": ["ORGANO"],                          # オルガノ
    "company_3002": ["GUNZE"],                           # グンゼ
    "company_6866": ["HIOKI"],                           # 日置電機
    "company_4967": ["KOBAYASHI PHARMACEUTICAL"],        # 小林製薬
    "company_3436": ["SUMCO"],                           # SUMCO
    "company_4045": ["TOAGOSEI"],                        # 東亞合成
    "company_7102": ["NIPPON SHARYO"],                   # 日本車輌製造
    "company_7244": ["ICHIKOH INDUSTRIES"],              # 市光工業
    "company_4061": ["DENKA"],                           # デンカ
    "company_6268": ["NABTESCO"],                        # ナブテスコ
    "company_4202": ["DAICEL"],                          # ダイセル
    "company_4403": ["NOF CORPORATION"],                 # 日油
    "company_8086": ["NIPRO"],                           # ニプロ
    "company_6869": ["SYSMEX"],                          # シスメックス
    "company_5602": ["KURIMOTO"],                        # 栗本鐵工所
    "company_5909": ["CORONA CORPORATION"],              # コロナ
    "company_4922": ["KOSE"],                            # コーセー
    "company_3632": ["GREE"],                            # グリー
    "company_5703": ["NIPPON LIGHT METAL"],              # 日本軽金属
    "company_6856": ["HORIBA"],                          # 堀場製作所
    "company_6707": ["SANKEN ELECTRIC"],                 # サンケン電気
    "company_4502": ["TAKEDA PHARMACEUTICAL", "TAKEDA"], # 武田薬品工業
    "company_6942": ["SOPHIA HOLDINGS"],                 # ソフィアHD
    "company_7972": ["ITOKI"],                           # イトーキ
    "company_6779": ["NDK"],                             # 日本電波工業
    "company_7911": ["TOPPAN HOLDINGS"],                 # TOPPANホールディングス
    "company_9502": ["CHUBU ELECTRIC POWER"],            # 中部電力
    "company_7739": ["CANON ELECTRONICS"],               # キヤノン電子
    "company_6371": ["TSUBAKIMOTO CHAIN"],               # 椿本チエイン
    "company_6430": ["DAIKOKU DENKI"],                   # ダイコク電機
    "company_5192": ["MITSUBOSHI BELTING"],              # 三ツ星ベルト
    "company_4980": ["DEXERIALS"],                       # デクセリアルズ
    "company_6479": ["MINEBEA MITSUMI"],                 # ミネベアミツミ
    "company_5105": ["TOYO TIRE"],                       # TOYO TIRE
    "company_6768": ["TAMURA CORPORATION"],              # タムラ製作所
    "company_1833": ["OKUMURA CORPORATION"],             # 奥村組
    "company_6718": ["AIPHONE"],                         # アイホン
    "company_4307": ["NOMURA RESEARCH INSTITUTE", "NRI"],# 野村総合研究所
    "company_6406": ["FUJITEC"],                         # フジテック
    "company_1861": ["KUMAGAI GUMI"],                    # 熊谷組
    "company_6339": ["SINTOKOGIO"],                      # 新東工業
    "company_9735": ["SECOM"],                           # セコム
    "company_7931": ["MIRAI INDUSTRIES"],                # 未来工業
    "company_7846": ["PILOT CORPORATION"],               # パイロットコーポレーション
    "company_5195": ["BANDO CHEMICAL"],                  # バンドー化学
    "company_4041": ["NIPPON SODA"],                     # 日本曹達
    "company_6340": ["SHIBUYA CORPORATION"],             # 澁谷工業
    "company_5932": ["SANKYO TATEYAMA"],                 # 三協立山
    "company_4212": ["SEKISUI JUSHI"],                   # 積水樹脂
    "company_7291": ["NIHON PLAST"],                     # 日本プラスト
    "company_6395": ["TADANO"],                          # タダノ
    "company_6376": ["NIKKISO"],                         # 日機装
    "company_6996": ["NICHICON"],                        # ニチコン
    "company_6103": ["OKUMA CORPORATION"],               # オークマ
    "company_7990": ["GLOBERIDE"],                       # グローブライド
    "company_6961": ["ENPLAS"],                          # エンプラス
    "company_7965": ["ZOJIRUSHI"],                       # 象印マホービン
    "company_4612": ["NIPPON PAINT"],                    # 日本ペイント
    "company_3941": ["RENGO"],                           # レンゴー
    "company_5016": ["JX METALS"],                       # JX金属
    "company_7239": ["TACHI-S"],                         # タチエス
    "company_2809": ["KEWPIE"],                          # キユーピー
    "company_4684": ["OBIC"],                            # オービック
    "company_6407": ["CKD CORPORATION"],                 # CKD
    "company_1893": ["PENTA-OCEAN CONSTRUCTION"],        # 五洋建設
    "company_6474": ["NACHI-FUJIKOSHI"],                 # 不二越
    "company_6651": ["NITTO KOGYO"],                     # 日東工業
    "company_9022": ["JR CENTRAL", "CENTRAL JAPAN RAILWAY"],  # 東海旅客鉄道
    "company_3668": ["COLOPL"],                          # コロプラ
    "company_2607": ["FUJI OIL"],                        # 不二製油
    "company_4116": ["DAINICHISEIKA"],                   # 大日精化工業
    "company_6997": ["NIPPON CHEMI-CON"],                # 日本ケミコン
    "company_7458": ["DAIICHIKOSHO"],                    # 第一興商
    "company_7952": ["KAWAI MUSICAL INSTRUMENTS"],       # 河合楽器製作所
    "company_6507": ["SYMPHONIA TECHNOLOGY"],            # シンフォニアテクノロジー
    "company_9533": ["TOHO GAS"],                        # 東邦瓦斯
    "company_4461": ["DKS CO"],                          # 第一工業製薬
    "company_9697": ["CAPCOM"],                          # カプコン
    "company_6638": ["MIMAKI ENGINEERING"],              # ミマキエンジニアリング
    "company_6814": ["FURUNO ELECTRIC"],                 # 古野電気
    "company_6273": ["SMC CORPORATION"],                 # SMC
    "company_6481": ["THK"],                             # THK
    "company_5161": ["NISHIKAWA RUBBER"],                # 西川ゴム工業
    "company_6955": ["FDK CORPORATION"],                 # FDK
    "company_6770": ["ALPS ALPINE"],                     # アルプスアルパイン
    "company_4021": ["NISSAN CHEMICAL"],                 # 日産化学
    "company_6675": ["SAXA"],                            # サクサ
    "company_1860": ["TODA CORPORATION"],                # 戸田建設
    "company_6282": ["OILES CORPORATION"],               # オイレス工業
    "company_4527": ["ROHTO PHARMACEUTICAL"],            # ロート製薬
    "company_5142": ["ACHILLES CORPORATION"],            # アキレス
    "company_4519": ["CHUGAI PHARMACEUTICAL"],           # 中外製薬
    "company_6804": ["HOSIDEN"],                         # ホシデン
    "company_7914": ["KYODO PRINTING"],                  # 共同印刷
    "company_4023": ["KUREHA"],                          # クレハ
    "company_7256": ["KASAI KOGYO"],                     # 河西工業
    "company_5393": ["NICHIAS"],                         # ニチアス
    "company_1820": ["NISHIMATSU CONSTRUCTION"],         # 西松建設
    "company_6806": ["HIROSE ELECTRIC"],                 # ヒロセ電機
    "company_2602": ["NISSHIN OILLIO"],                  # 日清オイリオ
    "company_5988": ["PIOLAX"],                          # パイオラックス
    "company_4008": ["SUMITOMO SEIKA CHEMICALS"],        # 住友精化
    "company_4206": ["AICA KOGYO"],                      # アイカ工業
    "company_5186": ["NITTA CORPORATION"],               # ニッタ
    "company_6798": ["SMK CORPORATION"],                 # SMK
    "company_6875": ["MEGACHIPS"],                       # メガチップス
    "company_1911": ["SUMITOMO FORESTRY"],               # 住友林業
    "company_7278": ["EXEDY"],                           # エクセディ
    "company_7994": ["OKAMURA CORPORATION"],             # オカムラ
    "company_4968": ["ARAKAWA CHEMICAL"],                # 荒川化学工業
    "company_3569": ["SEIREN"],                          # セーレン
    "company_3526": ["ASHIMORI INDUSTRY"],               # 芦森工業
    "company_7989": ["TACHIKAWA CORPORATION"],           # 立川ブラインド工業
    "company_8022": ["MIZUNO"],                          # 美津濃
    "company_5851": ["RYOBI"],                           # リョービ
    "company_7864": ["FUJI SEAL INTERNATIONAL"],         # フジシールインターナショナル
    "company_2264": ["MORINAGA MILK"],                   # 森永乳業
    "company_7238": ["AKEBONO BRAKE"],                   # 曙ブレーキ工業
    "company_7817": ["PARAMOUNT BED"],                   # パラマウントベッド
    "company_6525": ["KOKUSAI ELECTRIC"],                # KOKUSAI ELECTRIC
    "company_6742": ["KYOSAN ELECTRIC"],                 # 京三製作所
    "company_6486": ["EAGLE INDUSTRY"],                  # イーグル工業
    "company_2432": ["DENA"],                            # ディー・エヌ・エー
    "company_1961": ["SANKI ENGINEERING"],               # 三機工業
    "company_9766": ["KONAMI"],                          # コナミグループ
    "company_6505": ["TOYO DENKI SEIZO"],                # 東洋電機製造
    "company_6013": ["TAKUMA"],                          # タクマ
    "company_6470": ["TAIHO KOGYO"],                     # 大豊工業
    "company_4760": ["ALPHA CORPORATION"],               # アルファ
    "company_4100": ["TODA KOGYO"],                      # 戸田工業
    "company_1969": ["TAKASAGO THERMAL"],                # 高砂熱学工業
    "company_3106": ["KURABO INDUSTRIES"],               # 倉敷紡績
    "company_7226": ["KYOKUTO KAIHATSU KOGYO"],          # 極東開発工業
    "company_5715": ["FURUKAWA MACHINERY"],              # 古河機械金属
    "company_6849": ["NIHON KOHDEN"],                    # 日本光電工業
    "company_6222": ["SHIMA SEIKI"],                     # 島精機製作所
    "company_6966": ["MITSUI HIGH-TEC"],                 # 三井ハイテック
    "company_5352": ["KROSAKI HARIMA"],                  # 黒崎播磨
    "company_2269": ["MEIJI HOLDINGS"],                  # 明治ホールディングス
    "company_5741": ["UACJ"],                            # UACJ
    "company_4611": ["DAI NIPPON TORYO"],                # 大日本塗料
    "company_6986": ["FUTABA ELECTRONICS"],              # 双葉電子工業
    "company_2897": ["NISSIN FOODS"],                    # 日清食品
    "company_6141": ["DMG MORI"],                        # DMG森精機
    "company_4917": ["MANDOM"],                          # マンダム
    "company_6803": ["TEAC"],                            # ティアック
    "company_4985": ["EARTH CORPORATION"],               # アース製薬
    "company_4507": ["SHIONOGI"],                        # 塩野義製薬
    "company_6315": ["TOWA CORPORATION"],                # TOWA
    "company_4246": ["DAIKYO NISHIKAWA"],                # ダイキョーニシカワ
    "company_2121": ["MIXI"],                            # MIXI
    "company_6293": ["NISSEI PLASTIC"],                  # 日精樹脂工業
    "company_7943": ["NICHIHA"],                         # ニチハ
    "company_5218": ["OHARA"],                           # オハラ
    "company_5976": ["NETUREN"],                         # 高周波熱錬
    "company_2801": ["KIKKOMAN"],                        # キッコーマン
    "company_6349": ["KOMORI CORPORATION"],              # 小森コーポレーション
    "company_7740": ["TAMRON"],                          # タムロン
    "company_4092": ["NIPPON CHEMICAL INDUSTRIAL"],      # 日本化学工業
    "company_6436": ["AMANO"],                           # アマノ
    "company_6465": ["HOSHIZAKI"],                       # ホシザキ
    "company_7247": ["MIKUNI"],                          # ミクニ
    "company_7885": ["TAKANO"],                          # タカノ
    "company_6960": ["FUKUDA DENSHI"],                   # フクダ電子
    "company_5384": ["FUJIMI INCORPORATED"],             # フジミインコーポレーテッド
    "company_9551": ["METAWATER"],                       # メタウォーター
    "company_6727": ["WACOM"],                           # ワコム
    "company_4928": ["NOEVIR HOLDINGS"],                 # ノエビア
    "company_6217": ["TSUDAKOMA"],                       # 津田駒工業
    "company_7747": ["ASAHI INTECC"],                    # 朝日インテック
    "company_4633": ["SAKATA INX"],                      # サカタインクス
    "company_9506": ["TOHOKU ELECTRIC POWER"],           # 東北電力
    "company_3110": ["NITTO BOSEKI"],                    # 日東紡績
    "company_5727": ["TOHO TITANIUM"],                   # 東邦チタニウム
    "company_7241": ["FUTABA INDUSTRIAL"],               # フタバ産業
    "company_6238": ["FURYU CORPORATION"],               # フリュー
    "company_6871": ["JAPAN MICRONICS"],                 # 日本マイクロニクス
    "company_6824": ["NEW COSMOS ELECTRIC"],             # 新コスモス電機
    "company_6962": ["DAISHINKU"],                       # 大真空
    "company_5957": ["NITTO SEIKO"],                     # 日東精工
    "company_1964": ["CHUGAI RO"],                       # 中外炉工業
    "company_6859": ["ESPEC"],                           # エスペック
    "company_1720": ["TOKYU CONSTRUCTION"],              # 東急建設
    "company_7871": ["FUKUVI CHEMICAL"],                 # フクビ化学工業
    "company_6820": ["ICOM"],                            # アイコム
    "company_6999": ["KOA CORPORATION"],                 # KOA
    "company_4095": ["NIHON PARKERIZING"],               # 日本パーカライジング
    "company_3352": ["BUFFALO"],                         # バッファロー
    "company_6143": ["SODICK"],                          # ソディック
    "company_7944": ["ROLAND"],                          # ローランド
    "company_4689": ["LINE YAHOO"],                      # LINEヤフー
    "company_4064": ["NIPPON CARBIDE INDUSTRIES"],       # 日本カーバイド工業
    "company_6800": ["YOKOWO"],                          # ヨコオ
    "company_4220": ["RIKEN TECHNOS"],                   # リケンテクノス
    "company_7822": ["EIDAI"],                           # 永大産業
    "company_4028": ["ISHIHARA SANGYO KAISHA"],          # 石原産業
    "company_6526": ["SOCIONEXT"],                       # ソシオネクスト
    "company_2503": ["KIRIN HOLDINGS"],                  # キリン
    "company_4088": ["AIR WATER"],                       # エア・ウォーター
    "company_1885": ["TOA CORPORATION"],                 # 東亜建設工業
    "company_4463": ["NICCA CHEMICAL"],                  # 日華化学
    "company_5482": ["AICHI STEEL"],                     # 愛知製鋼
    "company_4503": ["ASTELLAS PHARMA"],                 # アステラス製薬
    "company_4914": ["TAKASAGO INTERNATIONAL"],          # 高砂香料工業
    "company_9021": ["JR WEST", "WEST JAPAN RAILWAY"],   # 西日本旅客鉄道
    "company_4530": ["HISAMITSU PHARMACEUTICAL"],        # 久光製薬
    "company_7279": ["HI-LEX CONTROLS"],                 # ハイレックスコーポレーション
    "company_6203": ["HOWA MACHINERY"],                  # 豊和工業
    "company_7231": ["TOPY INDUSTRIES"],                 # トピー工業
    "company_5959": ["OKABE"],                           # 岡部
    "company_9508": ["KYUSHU ELECTRIC POWER"],           # 九州電力
    "company_6366": ["CHIYODA CORPORATION"],             # 千代田化工建設
    "company_7702": ["JMS"],                             # JMS
    "company_2267": ["YAKULT"],                          # ヤクルト本社
    "company_7717": ["V TECHNOLOGY"],                    # ブイ・テクノロジー
    "company_7955": ["CLEANUP"],                         # クリナップ
    "company_4112": ["HODOGAYA CHEMICAL"],               # 保土谷化学工業
    "company_6135": ["MAKINO MILLING MACHINE"],          # 牧野フライス製作所
    "company_7718": ["STAR MICRONICS"],                  # スター精密
    "company_4958": ["HASEGAWA FRAGRANCE"],              # 長谷川香料
    "company_6498": ["KITZ CORPORATION"],                # キッツ
    "company_7942": ["JSP CORPORATION"],                 # JSP
    "company_4404": ["MIYOSHI OIL AND FAT"],             # ミヨシ油脂
    "company_6941": ["YAMAICHI ELECTRONICS"],            # 山一電機
    "company_6516": ["SANYO DENKI"],                     # 山洋電気
    "company_7245": ["DAIDO METAL"],                     # 大同メタル工業
    "company_2593": ["ITO EN"],                          # 伊藤園
    "company_3104": ["FUJIBO HOLDINGS"],                 # 富士紡ホールディングス
    "company_7266": ["IMASEN ELECTRIC"],                 # 今仙電機製作所
    "company_5363": ["TYK CORPORATION"],                 # 東京窯業
    "company_6151": ["NITTO KOHKI"],                     # 日東工器
    "company_6817": ["SUMIDA CORPORATION"],              # スミダコーポレーション
    "company_4626": ["TAIYO HOLDINGS"],                  # 太陽ホールディングス
    "company_4221": ["OKURA INDUSTRIAL"],                # 大倉工業
    "company_7250": ["PACIFIC INDUSTRIAL"],              # 太平洋工業
    "company_4628": ["SK KAKEN"],                        # エスケー化研
    "company_9837": ["MORITO"],                          # モリト
    "company_4634": ["ARTIENCE"],                        # artience
    "company_6118": ["AIDA ENGINEERING"],                # アイダエンジニアリング
    "company_7236": ["TRAD"],                            # ティラド
    "company_4998": ["FUMAKILLA"],                       # フマキラー
    "company_6763": ["TEIKOKU TSUSHIN KOGYO"],           # 帝国通信工業
    "company_6316": ["MARUYAMA MFG"],                    # 丸山製作所
    "company_7956": ["PIGEON"],                          # ピジョン
    "company_7936": ["ASICS"],                           # アシックス
    "company_4365": ["MATSUMOTO YUSHI-SEIYAKU"],         # 松本油脂製薬
    "company_5933": ["ALINCO"],                          # アルインコ
    "company_6946": ["JAPAN AVIONICS"],                  # 日本アビオニクス
    "company_4528": ["ONO PHARMACEUTICAL"],              # 小野薬品工業
    "company_7908": ["KIMOTO"],                          # きもと
    "company_5992": ["CHUHATSU SPRING"],                 # 中央発條
    "company_4526": ["RIKEN VITAMIN"],                   # 理研ビタミン
    "company_5020": ["ENEOS HOLDINGS"],                  # ENEOSホールディングス
    "company_6858": ["ONO SOKKI"],                       # 小野測器
    "company_6489": ["MAEZAWA INDUSTRIES"],              # 前澤工業
    "company_9513": ["J-POWER"],                         # 電源開発
    "company_5210": ["NIHON YAMAMURA GLASS"],            # 日本山村硝子
    "company_8012": ["NAGASE"],                          # 長瀬産業
    "company_5906": ["MK SEIKO"],                        # エムケー精工
    "company_7979": ["SHOFU"],                           # 松風
    "company_7105": ["MITSUBISHI LOGISNEXT"],            # 三菱ロジスネクスト
    "company_6351": ["TSURUMI MANUFACTURING"],           # 鶴見製作所
    "company_1719": ["HAZAMA ANDO"],                     # 安藤・間
    "company_4536": ["SANTEN PHARMACEUTICAL"],           # 参天製薬
    "company_7867": ["TAKARA TOMY"],                     # タカラトミー
    "company_7723": ["AICHI TOKEI DENKI"],               # 愛知時計電機
    "company_4078": ["SAKAI CHEMICAL"],                  # 堺化学工業
    "company_7780": ["MENICON"],                         # メニコン
    "company_2270": ["MEGMILK SNOW BRAND"],              # 雪印メグミルク
    "company_7292": ["MURAKAMI CORPORATION"],             # 村上開明堂
    "company_5714": ["DOWA HOLDINGS"],                   # DOWAホールディングス
}

# ──────────────────────────────────────────────────────────────────
# Duplicate mapping: share query results across IDs for same company
# Key = canonical named firm, Value = list of ticker-based aliases
# ──────────────────────────────────────────────────────────────────
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

# Reverse map: ticker → canonical
_TICKER_TO_CANONICAL = {}
for canon, tickers in DUPLICATE_MAP.items():
    for t in tickers:
        _TICKER_TO_CANONICAL[t] = canon


def run_extraction(
    db_path: str = "data/patents.db",
    year_from: int = 2020,
    year_to: int = 2024,
    max_firms: int = 500,
    skip_existing: bool = True,
) -> int:
    from sources.gdelt_bigquery import GDELTBigQuerySource

    gdelt = GDELTBigQuerySource()

    conn = sqlite3.connect(db_path, timeout=60)
    conn.executescript(SCHEMA_SQL)
    conn.execute("PRAGMA journal_mode=WAL")

    quarters = [(y, q) for y in range(year_from, year_to + 1) for q in range(1, 5)]

    # Get existing firm_ids if skipping
    existing_firms = set()
    if skip_existing:
        rows = conn.execute(
            "SELECT DISTINCT firm_id FROM gdelt_company_features"
        ).fetchall()
        existing_firms = {r[0] for r in rows}
        print(f"Existing firms in DB: {len(existing_firms)}")

    # Build work list: skip duplicates (use canonical's data instead)
    firms_to_query = []
    firms_to_copy = []  # (source_firm, dest_firm) pairs

    all_firms = list(FIRM_GDELT_NAMES.items())[:max_firms]

    for firm_id, search_names in all_firms:
        canonical = _TICKER_TO_CANONICAL.get(firm_id)
        if canonical:
            # This is a duplicate; copy from canonical after it's done
            firms_to_copy.append((canonical, firm_id))
            continue

        if skip_existing and firm_id in existing_firms:
            print(f"  SKIP (exists): {firm_id}")
            continue

        firms_to_query.append((firm_id, search_names))

    print(f"\nWork plan:")
    print(f"  Firms to query GDELT: {len(firms_to_query)}")
    print(f"  Firms to copy (duplicates): {len(firms_to_copy)}")
    print(f"  Quarters per firm: {len(quarters)}")
    print(f"  Total GDELT queries: {len(firms_to_query) * len(quarters)}")
    sys.stdout.flush()

    inserted = 0
    errors = 0
    start_time = time.time()

    # Phase 1: Query GDELT for non-duplicate firms
    for fi, (firm_id, search_names) in enumerate(firms_to_query):
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
        remaining = len(firms_to_query) - (fi + 1)
        eta_min = (total_elapsed / (fi + 1) * remaining) / 60 if fi > 0 else 0
        print(
            f"  [{fi+1}/{len(firms_to_query)}] {firm_id}: {len(quarters)}q in {firm_elapsed:.1f}s "
            f"(total: {inserted} ok, {errors} err, {total_elapsed:.0f}s, ETA: {eta_min:.0f}m)"
        )
        sys.stdout.flush()

    # Phase 2: Copy data for duplicate firms
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
                """
                INSERT OR REPLACE INTO gdelt_company_features (
                    firm_id, year, quarter,
                    direction_score, openness_score, investment_score,
                    governance_friction_score, leadership_score,
                    total_mentions, total_sources, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (dest_firm, *row),
            )
            copied += 1

        conn.commit()
        if rows:
            print(f"  COPIED {source_firm} → {dest_firm}: {len(rows)} quarters")

    conn.close()
    elapsed = time.time() - start_time

    print(f"\n{'='*60}")
    print(f"GDELT extraction complete.")
    print(f"Queried: {inserted} (errors: {errors})")
    print(f"Copied:  {copied} (duplicates)")
    print(f"Elapsed: {elapsed:.1f}s ({elapsed/60:.1f}m)")
    print(f"{'='*60}")
    sys.stdout.flush()

    return inserted + copied


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract GDELT 5-axis features for top 500 firms"
    )
    parser.add_argument("--db", default="data/patents.db", help="SQLite DB path")
    parser.add_argument("--year-from", type=int, default=2020, help="Start year")
    parser.add_argument("--year-to", type=int, default=2024, help="End year")
    parser.add_argument("--max-firms", type=int, default=500, help="Max firms to process")
    parser.add_argument("--no-skip", action="store_true", help="Re-extract even if exists")
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
