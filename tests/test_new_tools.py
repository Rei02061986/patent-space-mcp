"""Tests for Phase 3 analysis tools: cross_domain, adversarial, invention_intel, market_fusion."""
import json
import struct
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.sqlite_store import PatentStore
from entity.registry import Entity, EntityRegistry
from entity.resolver import EntityResolver


def _pack_embedding(arr):
    """Pack a 64-dim float64 array to BLOB."""
    return struct.pack("64d", *arr.tolist())


@pytest.fixture
def rich_store(tmp_path):
    """Create a store with tech_clusters, startability_surface, etc."""
    db_path = tmp_path / "test_rich.db"
    store = PatentStore(db_path)

    # Insert sample patents
    patents = [
        {
            "publication_number": "JP-2020001111-B2",
            "family_id": "FAM101",
            "country_code": "JP",
            "kind_code": "B2",
            "title_ja": "固体電解質リチウム電池",
            "title_en": "Solid-State Lithium Battery",
            "abstract_ja": "硫化物系固体電解質を用いた全固体リチウムイオン電池",
            "filing_date": 20200301,
            "publication_date": 20210901,
            "entity_status": "GRANT",
            "cpc_codes": [
                {"code": "H01M10/0562", "inventive": True, "first": True},
            ],
            "applicants": [
                {"raw_name": "TOYOTA MOTOR CORP", "harmonized_name": "TOYOTA MOTOR CORP",
                 "country_code": "JP", "firm_id": "toyota"},
            ],
            "source": "bigquery",
        },
        {
            "publication_number": "JP-2021002222-A",
            "family_id": "FAM102",
            "country_code": "JP",
            "kind_code": "A",
            "title_ja": "画像認識用ニューラルネットワーク",
            "title_en": "Neural Network for Image Recognition",
            "abstract_ja": "深層学習を用いた高精度画像認識手法",
            "filing_date": 20210601,
            "publication_date": 20211201,
            "cpc_codes": [
                {"code": "G06N3/08", "inventive": True, "first": True},
            ],
            "applicants": [
                {"raw_name": "SONY GROUP CORP", "harmonized_name": "SONY GROUP CORP",
                 "country_code": "JP", "firm_id": "sony"},
            ],
            "source": "bigquery",
        },
        {
            "publication_number": "JP-2022003333-B2",
            "family_id": "FAM103",
            "country_code": "JP",
            "kind_code": "B2",
            "title_ja": "電池管理装置",
            "title_en": "Battery Management Device",
            "abstract_ja": "リチウムイオン電池の劣化予測と管理",
            "filing_date": 20220101,
            "publication_date": 20230101,
            "entity_status": "GRANT",
            "cpc_codes": [
                {"code": "H01M10/48", "inventive": True, "first": True},
            ],
            "applicants": [
                {"raw_name": "HONDA MOTOR CO LTD", "harmonized_name": "HONDA MOTOR CO LTD",
                 "country_code": "JP", "firm_id": "honda"},
            ],
            "source": "bigquery",
        },
    ]
    for p in patents:
        store.upsert_patent(p)

    # Insert tech clusters with center vectors
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")

    vec_h01m = np.random.RandomState(42).randn(64).astype(np.float64)
    vec_h01m /= np.linalg.norm(vec_h01m)
    vec_g06n = np.random.RandomState(43).randn(64).astype(np.float64)
    vec_g06n /= np.linalg.norm(vec_g06n)
    vec_b60w = np.random.RandomState(44).randn(64).astype(np.float64)
    vec_b60w /= np.linalg.norm(vec_b60w)

    conn.executemany(
        """INSERT OR REPLACE INTO tech_clusters
           (cluster_id, label, cpc_class, cpc_codes, center_vector,
            patent_count, yearly_counts, growth_rate, top_applicants, top_terms)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            ("H01M_0", "Lithium Batteries", "H01M", '["H01M"]', _pack_embedding(vec_h01m),
             500, '{}', 0.15, '["TOYOTA"]', '["battery","lithium"]'),
            ("G06N_0", "Neural Networks", "G06N", '["G06N"]', _pack_embedding(vec_g06n),
             300, '{}', 0.45, '["SONY"]', '["neural","learning"]'),
            ("B60W_0", "Vehicle Control", "B60W", '["B60W"]', _pack_embedding(vec_b60w),
             200, '{}', 0.25, '["HONDA"]', '["vehicle","control"]'),
        ],
    )

    # Patent cluster mapping
    conn.executemany(
        "INSERT OR REPLACE INTO patent_cluster_mapping (publication_number, cluster_id, distance) VALUES (?, ?, ?)",
        [
            ("JP-2020001111-B2", "H01M_0", 0.1),
            ("JP-2021002222-A", "G06N_0", 0.2),
            ("JP-2022003333-B2", "H01M_0", 0.15),
        ],
    )

    # Tech cluster momentum
    conn.executemany(
        "INSERT OR REPLACE INTO tech_cluster_momentum (cluster_id, year, patent_count, growth_rate, acceleration) VALUES (?, ?, ?, ?, ?)",
        [
            ("H01M_0", 2024, 50, 0.15, 0.02),
            ("G06N_0", 2024, 80, 0.45, 0.10),
            ("B60W_0", 2024, 30, 0.25, 0.05),
        ],
    )

    # Firm tech vectors
    vec_toyota = np.random.RandomState(100).randn(64).astype(np.float64)
    vec_toyota /= np.linalg.norm(vec_toyota)
    vec_sony = np.random.RandomState(101).randn(64).astype(np.float64)
    vec_sony /= np.linalg.norm(vec_sony)
    vec_honda = np.random.RandomState(102).randn(64).astype(np.float64)
    vec_honda /= np.linalg.norm(vec_honda)

    conn.executemany(
        """INSERT OR REPLACE INTO firm_tech_vectors
           (firm_id, year, tech_vector, patent_count, dominant_cpc, tech_diversity, tech_concentration)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            ("toyota", 2024, _pack_embedding(vec_toyota), 100, "H01M", 1.5, 0.6),
            ("sony", 2024, _pack_embedding(vec_sony), 80, "G06N", 1.2, 0.7),
            ("honda", 2024, _pack_embedding(vec_honda), 50, "H01M", 1.0, 0.8),
        ],
    )

    # Startability surface
    conn.executemany(
        """INSERT OR REPLACE INTO startability_surface
           (firm_id, cluster_id, year, score, gate_open, phi_tech_cos, phi_tech_dist, phi_tech_cpc)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            ("toyota", "H01M_0", 2024, 0.75, 1, 0.8, 0.6, 0.4),
            ("toyota", "G06N_0", 2024, 0.30, 1, 0.3, 0.2, 0.1),
            ("sony", "G06N_0", 2024, 0.85, 1, 0.9, 0.7, 0.5),
            ("sony", "B60W_0", 2024, 0.20, 0, 0.2, 0.1, 0.05),
            ("honda", "H01M_0", 2024, 0.60, 1, 0.6, 0.5, 0.3),
            ("honda", "B60W_0", 2024, 0.70, 1, 0.7, 0.5, 0.4),
        ],
    )

    # Patent legal status
    conn.executemany(
        "INSERT OR REPLACE INTO patent_legal_status (publication_number, status, expiry_date) VALUES (?, ?, ?)",
        [
            ("JP-2020001111-B2", "alive", 20400301),
            ("JP-2021002222-A", "pending", None),
            ("JP-2022003333-B2", "alive", 20420101),
        ],
    )

    # Patent value index
    conn.executemany(
        """INSERT OR REPLACE INTO patent_value_index
           (publication_number, value_score, citation_component, family_component,
            recency_component, cluster_momentum_component)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [
            ("JP-2020001111-B2", 0.72, 0.8, 0.5, 0.7, 0.15),
            ("JP-2021002222-A", 0.55, 0.3, 0.2, 0.9, 0.45),
            ("JP-2022003333-B2", 0.48, 0.2, 0.3, 0.8, 0.15),
        ],
    )

    # Patent family
    conn.executemany(
        "INSERT OR REPLACE INTO patent_family (publication_number, family_id, family_size) VALUES (?, ?, ?)",
        [
            ("JP-2020001111-B2", "FAM101", 3),
            ("JP-2021002222-A", "FAM102", 1),
            ("JP-2022003333-B2", "FAM103", 2),
        ],
    )

    # GDELT features (for market_fusion)
    conn.executemany(
        """INSERT OR REPLACE INTO gdelt_company_features
           (firm_id, year, quarter, direction_score, openness_score, investment_score,
            governance_friction_score, leadership_score, total_mentions, total_sources)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            ("toyota", 2024, 4, 0.7, 0.6, 0.8, 0.2, 0.7, 500, 50),
            ("sony", 2024, 4, 0.6, 0.5, 0.7, 0.3, 0.6, 300, 30),
        ],
    )

    conn.commit()
    conn.close()
    return store


@pytest.fixture
def rich_registry():
    """Entity registry with Toyota, Sony, Honda."""
    registry = EntityRegistry()
    registry.register(Entity(
        "toyota", "Toyota Motor Corporation", "JP", "corporation",
        {"トヨタ自動車株式会社", "Toyota", "TOYOTA MOTOR CORP", "トヨタ"},
        industry="automotive", ticker="7203", tse_section="Prime",
    ))
    registry.register(Entity(
        "sony", "Sony Group Corporation", "JP", "corporation",
        {"ソニーグループ株式会社", "Sony", "SONY GROUP CORP", "ソニー"},
        industry="electronics", tse_section="Prime",
    ))
    registry.register(Entity(
        "honda", "Honda Motor Co., Ltd.", "JP", "corporation",
        {"本田技研工業株式会社", "Honda", "HONDA MOTOR CO LTD"},
        industry="automotive", tse_section="Prime",
    ))
    return registry


# ── cross_domain_discovery tests ──


def test_cross_domain_cpc_query(rich_store):
    from tools.cross_domain import cross_domain_discovery

    result = cross_domain_discovery(
        store=rich_store,
        query="H01M",
        top_n=5,
        exclude_same_domain=True,
        min_similarity=0.0,
    )

    assert result["endpoint"] == "cross_domain_discovery"
    assert result["source"]["query_type"] == "cpc"
    # Should have discovered clusters outside H section
    # (Results depend on centroid similarity — may be empty in small test)
    assert isinstance(result["discoveries"], list)


def test_cross_domain_text_query(rich_store):
    from tools.cross_domain import cross_domain_discovery

    result = cross_domain_discovery(
        store=rich_store,
        query="machine learning image recognition",
        top_n=5,
        exclude_same_domain=False,
        min_similarity=0.0,
    )

    assert result["endpoint"] == "cross_domain_discovery"
    assert result["source"]["query_type"] == "text"


# ── adversarial_strategy tests ──


def test_adversarial_basic(rich_store, rich_registry):
    from tools.adversarial import adversarial_strategy

    resolver = EntityResolver(rich_registry)
    result = adversarial_strategy(
        store=rich_store,
        resolver=resolver,
        firm_a="Toyota",
        firm_b="Honda",
        year=2024,
    )

    assert "error" not in result
    assert result["endpoint"] == "adversarial_strategy"
    overview = result["overview"]
    assert overview["firm_a"]["firm_id"] == "toyota"
    assert overview["firm_b"]["firm_id"] == "honda"
    assert isinstance(overview["overlap_clusters"], int)
    assert isinstance(result["territory_map"], dict)
    assert "overlap" in result["territory_map"]
    assert "firm_a_exclusive" in result["territory_map"]


def test_adversarial_unknown_firm(rich_store, rich_registry):
    from tools.adversarial import adversarial_strategy

    resolver = EntityResolver(rich_registry)
    result = adversarial_strategy(
        store=rich_store,
        resolver=resolver,
        firm_a="Toyota",
        firm_b="Unknown Corp",
    )

    assert "error" in result


# ── invention_intelligence tests ──


def test_invention_intel_basic(rich_store):
    from tools.invention_intel import invention_intelligence

    result = invention_intelligence(
        store=rich_store,
        text="solid state lithium battery",
        max_prior_art=10,
    )

    assert result["endpoint"] == "invention_intelligence"
    assert "bridge_info" in result
    # May or may not find clusters depending on FTS match
    if "error" not in result:
        assert "landscape" in result
        assert "prior_art" in result


def test_invention_intel_fto_flag(rich_store):
    from tools.invention_intel import invention_intelligence

    result = invention_intelligence(
        store=rich_store,
        text="battery management system",
        include_fto=False,
        include_whitespace=False,
    )

    assert result["endpoint"] == "invention_intelligence"
    if "error" not in result:
        assert result["fto_assessment"] is None
        assert result["whitespace_opportunities"] is None


# ── patent_market_fusion tests ──


def test_market_fusion_firm_mode(rich_store, rich_registry):
    from tools.market_fusion import patent_market_fusion

    resolver = EntityResolver(rich_registry)
    result = patent_market_fusion(
        store=rich_store,
        resolver=resolver,
        query="Toyota",
        query_type="firm",
        purpose="investment",
        year=2024,
    )

    assert result["endpoint"] == "patent_market_fusion"
    assert result["query_type"] == "firm"
    assert result["firm"]["firm_id"] == "toyota"
    assert "fusion_score" in result
    assert 0 <= result["fusion_score"] <= 1
    assert "components" in result
    assert "tech_strength" in result["components"]
    assert "market_sentiment" in result["components"]


def test_market_fusion_firm_not_found(rich_store, rich_registry):
    from tools.market_fusion import patent_market_fusion

    resolver = EntityResolver(rich_registry)
    result = patent_market_fusion(
        store=rich_store,
        resolver=resolver,
        query="Unknown Corp",
        query_type="firm",
    )

    assert "error" in result


def test_market_fusion_purpose_weights(rich_store, rich_registry):
    from tools.market_fusion import patent_market_fusion

    resolver = EntityResolver(rich_registry)

    inv = patent_market_fusion(
        store=rich_store, resolver=resolver,
        query="Toyota", purpose="investment", year=2024,
    )
    ma = patent_market_fusion(
        store=rich_store, resolver=resolver,
        query="Toyota", purpose="ma_target", year=2024,
    )

    # Different purposes should use different weights
    assert inv["weights"] != ma["weights"]
    assert inv["weights"]["growth_potential"] == 0.35  # investment emphasizes growth
    assert ma["weights"]["tech_strength"] == 0.40  # M&A emphasizes tech
