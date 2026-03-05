# Changelog

## v0.3.0 (2026-02-27)

### Added
- **S&P 500 entity resolution**: ~100 major US companies added to entity registry with full alias coverage (English, Japanese, UPPERCASE BigQuery forms)
- **EPO OPS connector** (`sources/epo_ops.py`): European Patent Office API integration for legal status, patent families, and EP/WO metadata
- **Global patent ingestion** (`scripts/ingest_global_patents.py`): BigQuery-based ingestion for US, EP, WO, CN, KR patent metadata with batch processing, resume support, and cost estimation
- **Paper data export** (`scripts/export_paper_data.py`): Comprehensive export of paper-ready datasets including startability distributions, cluster momentum, phi_tech components, and cross-sector analysis
- **English query fallback**: `patent_search` now falls back to LIKE search on title_en when FTS5 returns few results for English queries
- **Multi-CPC search**: `patent_search` supports filtering by multiple CPC codes simultaneously
- **Ticker resolution**: Entity registry now registers stock tickers and EDINET codes as aliases (fixes "7203" not resolving to Toyota)
- **Improved embedding bridge**: Multi-strategy FTS query generation for better English text → proxy embedding matching

### Changed
- Entity resolver fuzzy match threshold lowered from 0.92 to 0.84 for better cross-language matching while preventing false positives (e.g., Hyundai/Honda)
- Updated ARCHITECTURE.md to reflect multi-jurisdiction support
- Updated README.md with comprehensive documentation
- Version bumped to 0.3.0

### Fixed
- Ticker-based entity resolution (e.g., "7203" for Toyota, "AAPL" for Apple)
- English queries returning empty results in embedding bridge

## v0.2.0 (2026-02-24)

### Added
- 5 advanced analysis tools: `cross_domain_discovery`, `adversarial_strategy`, `invention_intelligence`, `patent_market_fusion`, `gdelt_company_events`
- `startability_delta` tool for time-series analysis
- `tech_fit` tool for phi_tech component inspection
- `firm_tech_vector` tool
- `tech_clusters_list` tool
- Patent value index computation
- Patent legal status derivation
- GDELT 5-axis market features integration
- Patent litigation data ingestion
- Overnight pipeline scripts for autonomous data updates

### Changed
- Tool count: 10 -> 18
- Startability model calibrated (alpha=-4.0, beta=[6,3,2,1])
- Entity registry expanded to 2,785 firms (TSE Prime + Standard + Growth)

## v0.1.0 (2026-02-22)

### Added
- Initial MCP server with 10 core tools
- BigQuery data ingestion pipeline
- SQLite database with 13.7M JP patents
- Entity resolution (TSE Prime, 50 firms)
- Startability surface computation
- 607 technology clusters
- Docker deployment support
