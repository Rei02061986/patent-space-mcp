# Patent Space MCP

A Model Context Protocol (MCP) server providing **38 tools** for patent intelligence — search, portfolio analysis, technology landscape mapping, startability scoring, citation networks, patent finance, cross-domain discovery, adversarial strategy, and patent-market fusion.

Built on **13.6M+ patent records** from Google Patents Public Data (BigQuery), with 607 technology clusters, 4,300+ resolved entities (JP TSE + US S&P 500), and pre-computed startability surfaces.

## Demo

### Basic: Ask AI about patents
https://github.com/Rei02061986/patent-space-mcp/raw/main/demo/demo_ja_basic.mp4

### Advanced: Simulate patent wars
https://github.com/Rei02061986/patent-space-mcp/raw/main/demo/demo_ja_strategy.mp4

## Quick Start

### Option 1: Claude Desktop (stdio)

```bash
# Clone and install
git clone https://github.com/Rei02061986/patent-space-mcp.git
cd patent-space-mcp
python -m venv .venv && source .venv/bin/activate
pip install .

# Place your patents.db in data/
# Configure Claude Desktop (see below)
```

Add to `~/.config/claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "patent-space": {
      "command": "/path/to/patent-space-mcp/.venv/bin/python",
      "args": ["server.py"],
      "cwd": "/path/to/patent-space-mcp",
      "env": {
        "PATENT_DB_PATH": "/path/to/patent-space-mcp/data/patents.db"
      }
    }
  }
}
```

### Option 2: Docker Compose (HTTP)

```bash
# Place patents.db in data/
docker compose up -d

# Server available at http://localhost:8001
```

### Option 3: HTTP server (no Docker)

```bash
python server.py --transport http --host 0.0.0.0 --port 8001
```

### Option 4: Remote / Hosted MCP (HTTP)

If running on a remote server, configure Claude Desktop to connect via HTTP:

```json
{
  "mcpServers": {
    "patent-space": {
      "url": "http://your-server-ip:8001/mcp"
    }
  }
}
```

Verify the server is running:

```bash
curl http://your-server-ip:8001/health
# Expected: {"status": "ok", "tools": 38, ...}
```

**Endpoints**:
- `POST /mcp` — MCP protocol endpoint (streamable HTTP transport)
- `GET /health` — Health check (returns tool count, DB status)

## Tools (38)

### Search & Retrieval (3)
| Tool | Description |
|------|-------------|
| `patent_search` | Full-text + multi-CPC + applicant + date search with English fallback |
| `patent_detail` | Single patent full record with claims and full text |
| `entity_resolve` | Company name resolution (any language, ticker, EDINET code) |

### Portfolio & Comparison (6)
| Tool | Description |
|------|-------------|
| `firm_patent_portfolio` | Patent portfolio analysis — CPC distribution, filing trend, co-applicants |
| `patent_compare` | Side-by-side comparison of multiple firms' portfolios |
| `applicant_network` | Co-applicant graph with shared patent counts |
| `firm_tech_vector` | 64-dim technology vector + diversity/concentration metrics |
| `similar_firms` | Find firms with similar patent portfolios (cosine + Jaccard) |
| `portfolio_evolution` | Track how a firm's technology portfolio evolved over time |

### Technology Landscape & Trends (6)
| Tool | Description |
|------|-------------|
| `tech_landscape` | Filing trends, top applicants, growth areas by CPC |
| `tech_clusters_list` | Browse 607 technology clusters with labels and top players |
| `tech_fit` | Technology fit components (CPC overlap, citation proximity, co-inventor) |
| `tech_trend` | Time-series technology trends with growth rates and new entrants |
| `tech_trend_alert` | Detect hot and cooling technology trends automatically |
| `tech_entropy` | Technology maturity via Shannon entropy of applicant diversity |

### Startability Scoring (3)
| Tool | Description |
|------|-------------|
| `startability` | S(v,f,t) score for a firm-technology pair |
| `startability_ranking` | Rank firms for a cluster or clusters for a firm |
| `startability_delta` | Track startability changes over time (gainers/losers) |

### Strategy & Competition (5)
| Tool | Description |
|------|-------------|
| `adversarial_strategy` | Game-theoretic portfolio comparison with attack/defend/preempt scenarios |
| `tech_gap` | Technology gap and synergy analysis between two firms |
| `cross_domain_discovery` | Find cross-section technology connections via embedding similarity |
| `invention_intelligence` | Prior art analysis, FTO risk assessment, whitespace opportunities |
| `cross_border_similarity` | Detect similar patent filings across international jurisdictions |

### Market Intelligence (4)
| Tool | Description |
|------|-------------|
| `patent_market_fusion` | Combined tech-strength + GDELT market signal scoring |
| `gdelt_company_events` | GDELT news events and 5-axis media features |
| `sales_prospect` | Identify and rank patent licensing sales targets |
| `ma_target` | Recommend M&A acquisition targets based on patent portfolio analysis |

### Citation Network & Topology (5)
| Tool | Description |
|------|-------------|
| `citation_network` | Build citation network around a patent or firm's top patents |
| `network_topology` | Citation network topology — scale-free, small-world, hubs |
| `network_resilience` | Patent network resilience via percolation theory |
| `knowledge_flow` | Cross-CPC knowledge transfer analysis via citation patterns |
| `tech_fusion_detector` | Detect technology convergence via co-citation analysis |

### Patent Finance & Valuation (6)
| Tool | Description |
|------|-------------|
| `patent_option_value` | Black-Scholes real option valuation for patents |
| `patent_valuation` | Patent/portfolio/technology value scoring with royalty rate benchmarks |
| `portfolio_var` | Portfolio Value-at-Risk for patent expiration risk |
| `tech_volatility` | Technology volatility with decay curve and half-life |
| `tech_beta` | CAPM-style technology beta: market sensitivity analysis |
| `bayesian_scenario` | Bayesian patent investment simulation with data-driven priors |

## Database

The server uses a single SQLite database (`data/patents.db`) containing:

| Table | Rows | Description |
|-------|------|-------------|
| `patents` | 13.7M | Core patent metadata (title, abstract, dates, jurisdiction) |
| `patent_cpc` | 44.8M | CPC classification codes |
| `patent_assignees` | 30.4M | Assignee records with resolved firm_id |
| `patent_research_data` | 171.5M | Embeddings + citation data (Google Patents Research) |
| `tech_clusters` | 607 | Technology clusters with labels, top applicants, top terms |
| `patent_cluster_mapping` | 76K | Patent-to-cluster assignments |
| `firm_tech_vectors` | 27.8K | Per-firm-per-year 64-dim technology vectors |
| `startability_surface` | 10.3M | Pre-computed S(v,f,t) scores (600+ clusters, 4300+ firms) |
| `patent_citations` | - | Forward/backward citation links |
| `citation_counts` | - | Pre-computed forward citation counts |
| `gdelt_company_features` | 3K+ | GDELT 5-axis market features (46 firms, 2020-2024) |

## Building the Database

If you want to build the database from scratch using Google BigQuery:

```bash
# 1. Set up GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
export BIGQUERY_PROJECT=your-project-id

# 2. Extract patent data (cost: ~$56 for full extract)
bq extract --destination_format=PARQUET --compression=SNAPPY \
  'patents-public-data:patents.publications' \
  'gs://your-bucket/patents/publications-*.parquet'

# 3. Download to local
gsutil -m cp -r gs://your-bucket/patents/ ./exports/

# 4. Ingest into SQLite
python scripts/ingest_global_v2.py --parquet-dir ./exports/patents --db data/patents.db

# 5. Build upper layers
python scripts/compute_firm_tech_vectors.py --db data/patents.db
python scripts/build_tech_clusters.py --db data/patents.db
python scripts/compute_startability_surface.py --db data/patents.db
```

## Entity Resolution

The server resolves company names to canonical entities using 3-level matching:

| Level | Method | Example |
|-------|--------|---------|
| Exact | Alias/ticker/EDINET lookup | "7203" → Toyota Motor Corporation |
| Normalized | Suffix stripping + NFKC | "トヨタ自動車株式会社" → Toyota |
| Fuzzy | Levenshtein ratio > 0.80 | "Toshiba Corp" → Toshiba Corporation |

**Coverage:**
- 2,785 Japanese firms (TSE Prime + Standard + Growth)
- ~100 US S&P 500 companies (top patent filers)
- 4,300+ total resolved entities
- Stock ticker and EDINET code resolution

## Project Structure

```
patent-space-mcp/
├── server.py                    # MCP server (38 tools, FastMCP)
├── db/
│   └── sqlite_store.py          # Database access layer (connection pooling, query timeout)
├── entity/
│   ├── registry.py              # Entity registry
│   ├── resolver.py              # Fuzzy name resolution
│   └── data/                    # Entity seed data (4,300+ firms)
├── tools/
│   ├── search.py                # patent_search, patent_detail
│   ├── portfolio.py             # firm_patent_portfolio
│   ├── compare.py               # patent_compare
│   ├── vectors.py               # firm_tech_vector
│   ├── similar_firms.py         # similar_firms
│   ├── landscape.py             # tech_landscape
│   ├── clusters.py              # tech_clusters_list
│   ├── tech_fit.py              # tech_fit
│   ├── network.py               # applicant_network
│   ├── startability_tool.py     # startability, startability_ranking
│   ├── startability_delta.py    # startability_delta
│   ├── adversarial.py           # adversarial_strategy
│   ├── tech_gap.py              # tech_gap
│   ├── cross_domain.py          # cross_domain_discovery
│   ├── invention_intel.py       # invention_intelligence
│   ├── cross_border.py          # cross_border_similarity
│   ├── market_fusion.py         # patent_market_fusion
│   ├── gdelt_tool.py            # gdelt_company_events
│   ├── sales_prospect.py        # sales_prospect
│   ├── ma_target.py             # ma_target
│   ├── citation_network.py      # citation_network
│   ├── network_topology.py      # network_topology
│   ├── network_resilience.py    # network_resilience
│   ├── knowledge_flow.py        # knowledge_flow
│   ├── tech_fusion.py           # tech_fusion_detector
│   ├── patent_option.py         # patent_option_value
│   ├── patent_valuation.py      # patent_valuation
│   ├── portfolio_var.py         # portfolio_var
│   ├── portfolio_evolution.py   # portfolio_evolution
│   ├── tech_volatility.py       # tech_volatility
│   ├── tech_beta.py             # tech_beta
│   ├── tech_entropy.py          # tech_entropy
│   ├── tech_trend.py            # tech_trend
│   ├── tech_trend_alert.py      # tech_trend_alert
│   ├── bayesian_scenario.py     # bayesian_scenario
│   └── pagination.py            # Shared pagination helper
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── ATTRIBUTION.md               # Data source attribution
└── smithery.yaml                # Smithery marketplace config
```

## Known Limitations

| Area | Limitation | Impact |
|------|-----------|--------|
| **Data scope** | 13.7M JP patents (global ingestion in progress) | Non-JP patents not yet searchable |
| **Embedding coverage** | ~46.6% of JP patents have embeddings (2000-2018) | Recent patents lack some embeddings |
| **GDELT coverage** | 46 of 50 target firms have market signal data | `patent_market_fusion` uses neutral fallback for uncovered firms |
| **Citation scope** | JP-internal citations only | Forward citation counts understate true impact |
| **Legal status** | Derived heuristically from filing date + 20 years | May not reflect maintenance fee lapses |
| **HDD performance** | SQLite on spinning disk is slow for cold queries | NVMe SSD recommended for production |

## Requirements

- Python >= 3.11
- SQLite database with patent data
- Recommended: NVMe SSD for database storage
- ~500 GB disk for full JP patent database with WAL

## License

MIT License

See [ATTRIBUTION.md](ATTRIBUTION.md) for data source licenses.
