# Architecture

## Overview

Patent Space MCP is a three-layer system:

```
┌─────────────────────────────────────────────┐
│              MCP Tools (18)                  │  ← Claude / LLM interface
├─────────────────────────────────────────────┤
│  Space Layer    │  Entity Layer             │  ← Computation
│  - embedding    │  - registry (2,785+ firms)│
│  - startability │  - JP TSE + US S&P 500    │
│  - clustering   │  - fuzzy resolver         │
├─────────────────────────────────────────────┤
│           SQLite Store (56 GB)              │  ← Storage
│  20+ tables, FTS5, WAL mode                 │
└─────────────────────────────────────────────┘
```

## Data Flow

### Ingestion Pipeline

```
Google Patents BigQuery ──→ ingest scripts ──→ SQLite
                              │
                              ├── patents (13.7M)
                              ├── patent_cpc (22M+)
                              ├── patent_assignees (15M+)
                              ├── patent_research_data (6.4M embeddings)
                              └── citation_counts, inventors, ...
```

### Computation Pipeline

```
Raw Data ──→ compute scripts ──→ Derived Tables
                │
                ├── firm_tech_vectors (h_{f,t})
                │     weighted avg of patent embeddings per firm/year
                │     w_p = sqrt(citations) × exp(-0.1 × age)
                │
                ├── tech_clusters (607)
                │     CPC-based grouping + k-means subdivision
                │     center_vector, growth_rate, top_applicants
                │
                ├── startability_surface S(v,f,t)
                │     sigmoid(α + β_tech · φ_tech)
                │     φ_tech = [cosine, distance, cpc_jaccard, citation_prox]
                │     Gate function filters ~75% of pairs
                │
                ├── patent_legal_status
                │     Derived from JP kind_code + filing_date
                │
                ├── patent_value_index
                │     normalize(citations × family × recency × momentum)
                │
                └── tech_cluster_momentum
                      Year-over-year growth rate + acceleration
```

## Embedding Bridge

Google Patents Research embeddings (64-dim) are proprietary — there is no public model to generate compatible vectors from arbitrary text. The embedding bridge solves this:

```
User text
    │
    ▼
FTS5 search (trigram) ──→ candidate patents (top 200)
    │
    ▼
Retrieve existing 64-dim embeddings for matched patents
    │
    ▼
Weighted centroid (1/rank weighting) ──→ proxy embedding (64-dim)
    │
    ▼
Cosine similarity vs 607 cluster centroids ──→ matched clusters
```

This enables all text-based tools (cross_domain_discovery, invention_intelligence, patent_market_fusion) to work without an external embedding API.

## Startability Model

The startability score S(v,f,t) measures how well firm f can enter technology cluster v at time t:

```
S(v,f,t) = σ(α + β_tech · φ_tech(v,f,t))

where φ_tech = [
    cos_sim(y_v, h_{f,t}),        # embedding cosine similarity
    1/(1 + ||y_v - h_{f,t}||),    # normalized Euclidean distance
    Jaccard(CPC_v, CPC_f),        # CPC code overlap
    |cited_v ∩ patents_f| / |v|   # citation proximity
]

Gate: compute S only if cos_sim > 0.3 OR cpc_overlap > 0.01 OR cite_prox > 0
```

## Entity Resolution

Three-level matching for company name resolution:

1. **Exact match**: canonical name, alias, ticker, or EDINET code lookup (O(1) hash)
2. **Normalized match**: strip suffixes (株式会社, Corp, Ltd, Inc.), normalize encoding
3. **Fuzzy match**: Levenshtein ratio > 0.80, with country hint bonus

Registry covers:
- **2,785 Japanese firms** (TSE Prime + Standard + Growth)
- **~100 US S&P 500 companies** (top patent filers)
- Manual overrides for ambiguous cases (e.g., "日立" group)

## Key Design Decisions

1. **SQLite over PostgreSQL**: Single-file deployment, no server process. WAL mode enables concurrent reads. 56 GB is manageable for JP-only data.

2. **No external vector DB**: 607 cluster centroids fit in ~300 KB of RAM. Tier 2 similarity search uses SQL pre-filtering + numpy computation.

3. **Pre-computed surfaces**: Startability scores are computed offline for all firm-cluster pairs. MCP tools do lookups, not computation — response time is O(1).

4. **FTS5 trigram tokenizer**: Handles Japanese text without MeCab dependency. English queries use multi-strategy fallback (FTS → LIKE on title_en) for improved coverage.

5. **Gate function**: Reduces startability computation from N×M to ~25% of pairs, saving storage and compute time.

6. **Multi-jurisdiction support**: EPO OPS connector for European patent data and legal status. BigQuery ingestion scripts for US/EP/WO/CN/KR metadata.
