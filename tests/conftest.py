"""Shared test fixtures."""
import sys
from pathlib import Path

import pytest

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.sqlite_store import PatentStore
from entity.registry import Entity, EntityRegistry
from entity.resolver import EntityResolver


@pytest.fixture
def tmp_store(tmp_path):
    """Create a temporary patent database with sample data."""
    db_path = tmp_path / "test_patents.db"
    store = PatentStore(db_path)

    sample_patents = [
        {
            "publication_number": "JP-2020123456-A",
            "application_number": "JP-2020-001234",
            "family_id": "FAM001",
            "country_code": "JP",
            "kind_code": "A",
            "title_ja": "人工知能による画像認識装置",
            "title_en": "Image Recognition Device Using AI",
            "abstract_ja": "本発明は深層学習を用いた画像認識の改良に関する",
            "abstract_en": "An image recognition device using deep learning",
            "filing_date": 20200115,
            "publication_date": 20200715,
            "entity_status": "GRANT",
            "cpc_codes": [
                {"code": "G06N3/08", "inventive": True, "first": True},
                {"code": "G06V10/82", "inventive": False, "first": False},
            ],
            "applicants": [
                {
                    "raw_name": "TOYOTA MOTOR CORP",
                    "harmonized_name": "TOYOTA MOTOR CORP",
                    "country_code": "JP",
                    "firm_id": "toyota",
                },
            ],
            "inventors": ["田中太郎", "Smith John"],
            "citations_backward": ["US-10000001-B2", "JP-2019111111-A"],
            "source": "bigquery",
        },
        {
            "publication_number": "JP-2021234567-A",
            "family_id": "FAM002",
            "country_code": "JP",
            "kind_code": "A",
            "title_ja": "電池管理システム",
            "title_en": "Battery Management System",
            "abstract_ja": "リチウムイオン電池の劣化予測システム",
            "filing_date": 20210301,
            "publication_date": 20210901,
            "cpc_codes": [
                {"code": "H01M10/48", "inventive": True, "first": True},
            ],
            "applicants": [
                {
                    "raw_name": "TOYOTA MOTOR CORP",
                    "harmonized_name": "TOYOTA MOTOR CORP",
                    "country_code": "JP",
                    "firm_id": "toyota",
                },
                {
                    "raw_name": "PANASONIC CORP",
                    "harmonized_name": "PANASONIC CORP",
                    "country_code": "JP",
                    "firm_id": "panasonic",
                },
            ],
            "inventors": ["佐藤花子"],
            "source": "bigquery",
        },
        {
            "publication_number": "JP-2022345678-A",
            "family_id": "FAM003",
            "country_code": "JP",
            "kind_code": "A",
            "title_ja": "自動運転制御方法",
            "title_en": "Autonomous Driving Control Method",
            "abstract_ja": "LIDARとカメラの融合による自動運転制御",
            "filing_date": 20220510,
            "publication_date": 20221110,
            "cpc_codes": [
                {"code": "G05D1/02", "inventive": True, "first": True},
                {"code": "B60W60/00", "inventive": False, "first": False},
            ],
            "applicants": [
                {
                    "raw_name": "HONDA MOTOR CO LTD",
                    "harmonized_name": "HONDA MOTOR CO LTD",
                    "country_code": "JP",
                    "firm_id": "honda",
                },
            ],
            "inventors": ["高橋一郎"],
            "source": "bigquery",
        },
    ]

    for p in sample_patents:
        store.upsert_patent(p)

    return store


@pytest.fixture
def entity_registry():
    """Create a test entity registry with a few entities."""
    registry = EntityRegistry()
    registry.register(
        Entity(
            "toyota",
            "Toyota Motor Corporation",
            "JP",
            "corporation",
            {
                "トヨタ自動車株式会社",
                "Toyota",
                "TOYOTA MOTOR CORP",
                "トヨタ",
                "TOYOTA JIDOSHA KK",
            },
            industry="automotive",
            edinet_code="E02144",
            ticker="7203",
            tse_section="Prime",
        )
    )
    registry.register(
        Entity(
            "sony",
            "Sony Group Corporation",
            "JP",
            "corporation",
            {
                "ソニーグループ株式会社",
                "Sony",
                "SONY GROUP CORP",
                "ソニー",
                "SONY CORP",
            },
            industry="electronics",
            tse_section="Prime",
        )
    )
    registry.register(
        Entity(
            "panasonic",
            "Panasonic Holdings Corporation",
            "JP",
            "corporation",
            {
                "パナソニック株式会社",
                "Panasonic",
                "PANASONIC CORP",
                "松下電器産業株式会社",
            },
            industry="electronics",
            tse_section="Prime",
        )
    )
    registry.register(
        Entity(
            "honda",
            "Honda Motor Co., Ltd.",
            "JP",
            "corporation",
            {
                "本田技研工業株式会社",
                "Honda",
                "HONDA MOTOR CO LTD",
            },
            industry="automotive",
            tse_section="Prime",
        )
    )
    return registry
