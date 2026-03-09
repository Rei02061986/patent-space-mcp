"""SQLite schema for patent metadata cache."""

SCHEMA_SQL = """
-- Main patent table (one row per publication)
CREATE TABLE IF NOT EXISTS patents (
    publication_number TEXT PRIMARY KEY,
    application_number TEXT,
    family_id TEXT,
    country_code TEXT NOT NULL,
    kind_code TEXT,
    title_ja TEXT,
    title_en TEXT,
    abstract_ja TEXT,
    abstract_en TEXT,
    filing_date INTEGER,
    publication_date INTEGER,
    grant_date INTEGER,
    entity_status TEXT,
    citation_count_forward INTEGER DEFAULT 0,
    source TEXT DEFAULT 'bigquery',
    ingested_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_patents_family ON patents(family_id);
CREATE INDEX IF NOT EXISTS idx_patents_country ON patents(country_code);
CREATE INDEX IF NOT EXISTS idx_patents_filing_date ON patents(filing_date);
CREATE INDEX IF NOT EXISTS idx_patents_pub_date ON patents(publication_date);

-- CPC classifications (many per patent)
CREATE TABLE IF NOT EXISTS patent_cpc (
    publication_number TEXT NOT NULL,
    cpc_code TEXT NOT NULL,
    is_inventive INTEGER DEFAULT 0,
    is_first INTEGER DEFAULT 0,
    PRIMARY KEY (publication_number, cpc_code),
    FOREIGN KEY (publication_number) REFERENCES patents(publication_number)
);

CREATE INDEX IF NOT EXISTS idx_cpc_code ON patent_cpc(cpc_code);
CREATE INDEX IF NOT EXISTS idx_cpc_class ON patent_cpc(substr(cpc_code, 1, 4));

-- Assignees/applicants (many per patent)
CREATE TABLE IF NOT EXISTS patent_assignees (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    publication_number TEXT NOT NULL,
    raw_name TEXT NOT NULL,
    harmonized_name TEXT,
    country_code TEXT,
    firm_id TEXT,
    FOREIGN KEY (publication_number) REFERENCES patents(publication_number)
);

CREATE INDEX IF NOT EXISTS idx_assignee_pub ON patent_assignees(publication_number);
CREATE INDEX IF NOT EXISTS idx_assignee_harmonized ON patent_assignees(harmonized_name);
CREATE INDEX IF NOT EXISTS idx_assignee_firm ON patent_assignees(firm_id);

-- Inventors (many per patent)
CREATE TABLE IF NOT EXISTS patent_inventors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    publication_number TEXT NOT NULL,
    name TEXT NOT NULL,
    country_code TEXT,
    FOREIGN KEY (publication_number) REFERENCES patents(publication_number)
);

CREATE INDEX IF NOT EXISTS idx_inventor_pub ON patent_inventors(publication_number);

-- Citations (forward and backward)
CREATE TABLE IF NOT EXISTS patent_citations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    citing_publication TEXT NOT NULL,
    cited_publication TEXT NOT NULL,
    citation_type TEXT,
    FOREIGN KEY (citing_publication) REFERENCES patents(publication_number)
);

CREATE INDEX IF NOT EXISTS idx_citation_citing ON patent_citations(citing_publication);
CREATE INDEX IF NOT EXISTS idx_citation_cited ON patent_citations(cited_publication);

-- Research metadata and embeddings from google_patents_research.publications
CREATE TABLE IF NOT EXISTS patent_research_data (
    publication_number TEXT PRIMARY KEY,
    title_en TEXT,
    abstract_en TEXT,
    top_terms TEXT,        -- JSON array of strings
    embedding_v1 BLOB,     -- Float64 x 64 as binary
    FOREIGN KEY (publication_number) REFERENCES patents(publication_number)
);

CREATE TABLE IF NOT EXISTS citation_counts (
    publication_number TEXT PRIMARY KEY,
    forward_citations INTEGER NOT NULL,
    FOREIGN KEY (publication_number) REFERENCES patents(publication_number)
);

CREATE INDEX IF NOT EXISTS idx_cc_pub ON citation_counts(publication_number);

CREATE TABLE IF NOT EXISTS gdelt_company_features (
    firm_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    direction_score REAL,
    openness_score REAL,
    investment_score REAL,
    governance_friction_score REAL,
    leadership_score REAL,
    total_mentions INTEGER,
    total_sources INTEGER,
    raw_data TEXT,
    PRIMARY KEY (firm_id, year, quarter)
);

CREATE TABLE IF NOT EXISTS firm_tech_vectors (
    firm_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    tech_vector BLOB,
    patent_count INTEGER,
    dominant_cpc TEXT,
    tech_diversity REAL,
    tech_concentration REAL,
    PRIMARY KEY (firm_id, year)
);

CREATE TABLE IF NOT EXISTS tech_clusters (
    cluster_id TEXT PRIMARY KEY,
    label TEXT,
    cpc_class TEXT,
    cpc_codes TEXT,
    center_vector BLOB,
    patent_count INTEGER,
    yearly_counts TEXT,
    growth_rate REAL,
    top_applicants TEXT,
    top_terms TEXT
);

CREATE TABLE IF NOT EXISTS patent_cluster_mapping (
    publication_number TEXT PRIMARY KEY,
    cluster_id TEXT NOT NULL,
    distance REAL,
    FOREIGN KEY (cluster_id) REFERENCES tech_clusters(cluster_id)
);

CREATE INDEX IF NOT EXISTS idx_pcm_cluster ON patent_cluster_mapping(cluster_id);

CREATE TABLE IF NOT EXISTS startability_surface (
    cluster_id TEXT NOT NULL,
    firm_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    score REAL,
    gate_open INTEGER,
    phi_tech_cos REAL,
    phi_tech_dist REAL,
    phi_tech_cpc REAL,
    phi_tech_cite REAL,
    phi_org REAL,
    phi_dyn REAL,
    PRIMARY KEY (cluster_id, firm_id, year)
);

CREATE TABLE IF NOT EXISTS tech_cluster_momentum (
    cluster_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    patent_count INTEGER,
    growth_rate REAL,
    acceleration REAL,
    PRIMARY KEY (cluster_id, year)
);

-- Patent legal status (derived from entity_status + filing_date + 20yr rule)
CREATE TABLE IF NOT EXISTS patent_legal_status (
    publication_number TEXT PRIMARY KEY,
    status TEXT NOT NULL,        -- 'alive' | 'expired' | 'abandoned' | 'pending'
    expiry_date INTEGER,         -- estimated YYYYMMDD (filing_date + 20 years)
    FOREIGN KEY (publication_number) REFERENCES patents(publication_number)
);

CREATE INDEX IF NOT EXISTS idx_pls_status ON patent_legal_status(status);

-- Patent value index (composite score from citations, family, recency, momentum)
CREATE TABLE IF NOT EXISTS patent_value_index (
    publication_number TEXT PRIMARY KEY,
    value_score REAL NOT NULL,   -- normalized 0-1
    citation_component REAL,
    family_component REAL,
    recency_component REAL,
    cluster_momentum_component REAL,
    FOREIGN KEY (publication_number) REFERENCES patents(publication_number)
);

CREATE INDEX IF NOT EXISTS idx_pvi_score ON patent_value_index(value_score DESC);

-- Patent family sizes
CREATE TABLE IF NOT EXISTS patent_family (
    publication_number TEXT PRIMARY KEY,
    family_id TEXT NOT NULL,
    family_size INTEGER DEFAULT 1,
    FOREIGN KEY (publication_number) REFERENCES patents(publication_number)
);

CREATE INDEX IF NOT EXISTS idx_pf_family ON patent_family(family_id);

-- Patent litigation data (from USPTO Research Datasets)
CREATE TABLE IF NOT EXISTS patent_litigation (
    case_id TEXT PRIMARY KEY,
    patent_number TEXT,
    plaintiff TEXT,
    defendant TEXT,
    filing_date TEXT,            -- ISO date string
    court TEXT,
    outcome TEXT,
    damages_amount REAL
);

CREATE INDEX IF NOT EXISTS idx_pl_patent ON patent_litigation(patent_number);
CREATE INDEX IF NOT EXISTS idx_pl_plaintiff ON patent_litigation(plaintiff);
CREATE INDEX IF NOT EXISTS idx_pl_defendant ON patent_litigation(defendant);

-- Full-text search index (trigram tokenizer for CJK support, titles only to limit size)
CREATE VIRTUAL TABLE IF NOT EXISTS patents_fts USING fts5(
    publication_number,
    title_ja,
    title_en,
    content='patents',
    content_rowid='rowid',
    tokenize='trigram'
);

-- Triggers to keep FTS in sync
CREATE TRIGGER IF NOT EXISTS patents_ai AFTER INSERT ON patents BEGIN
    INSERT INTO patents_fts(rowid, publication_number, title_ja, title_en)
    VALUES (new.rowid, new.publication_number, new.title_ja, new.title_en);
END;

CREATE TRIGGER IF NOT EXISTS patents_ad AFTER DELETE ON patents BEGIN
    INSERT INTO patents_fts(patents_fts, rowid, publication_number, title_ja, title_en)
    VALUES ('delete', old.rowid, old.publication_number, old.title_ja, old.title_en);
END;

CREATE TRIGGER IF NOT EXISTS patents_au AFTER UPDATE ON patents BEGIN
    INSERT INTO patents_fts(patents_fts, rowid, publication_number, title_ja, title_en)
    VALUES ('delete', old.rowid, old.publication_number, old.title_ja, old.title_en);
    INSERT INTO patents_fts(rowid, publication_number, title_ja, title_en)
    VALUES (new.rowid, new.publication_number, new.title_ja, new.title_en);
END;

-- Ingestion progress tracking
CREATE TABLE IF NOT EXISTS ingestion_log (
    batch_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    country_code TEXT,
    started_at TEXT DEFAULT (datetime('now')),
    completed_at TEXT,
    records_fetched INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    last_publication_date INTEGER,
    status TEXT DEFAULT 'running'
);

-- Corporate hierarchy (parent-subsidiary relationships)
CREATE TABLE IF NOT EXISTS corporate_hierarchy (
    firm_id TEXT NOT NULL,
    parent_firm_id TEXT NOT NULL,
    relationship TEXT DEFAULT 'subsidiary',
    ownership_pct REAL,
    source TEXT DEFAULT 'manual',
    PRIMARY KEY (firm_id, parent_firm_id)
);

CREATE INDEX IF NOT EXISTS idx_hierarchy_parent ON corporate_hierarchy(parent_firm_id);

-- Custom technology categories (user-defined)
CREATE TABLE IF NOT EXISTS custom_categories (
    category_id TEXT PRIMARY KEY,
    category_name TEXT NOT NULL,
    description TEXT,
    cpc_patterns TEXT,
    keyword_patterns TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    patent_count INTEGER DEFAULT 0
);

-- Patent-to-category mapping
CREATE TABLE IF NOT EXISTS patent_category_mapping (
    publication_number TEXT NOT NULL,
    category_id TEXT NOT NULL,
    confidence REAL DEFAULT 1.0,
    method TEXT DEFAULT 'rule',
    PRIMARY KEY (publication_number, category_id)
);

CREATE INDEX IF NOT EXISTS idx_pcm_cat ON patent_category_mapping(category_id);

-- SEP (Standard Essential Patent) declarations
CREATE TABLE IF NOT EXISTS sep_declarations (
    declaration_id INTEGER PRIMARY KEY AUTOINCREMENT,
    patent_number TEXT,
    standard_name TEXT NOT NULL,
    standard_org TEXT DEFAULT 'ETSI',
    sso_project TEXT,
    declarant TEXT NOT NULL,
    declaration_date TEXT,
    technical_area TEXT,
    publication_number TEXT,
    UNIQUE(patent_number, standard_name, declarant)
);

CREATE INDEX IF NOT EXISTS idx_sep_standard ON sep_declarations(standard_name);
CREATE INDEX IF NOT EXISTS idx_sep_declarant ON sep_declarations(declarant);
CREATE INDEX IF NOT EXISTS idx_sep_pub ON sep_declarations(publication_number);

-- Monitoring watches (user-defined patent alerts)
CREATE TABLE IF NOT EXISTS monitoring_watches (
    watch_id TEXT PRIMARY KEY,
    watch_type TEXT NOT NULL,
    target TEXT NOT NULL,
    parameters TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    last_checked TEXT,
    alert_threshold INTEGER DEFAULT 5
);

CREATE TABLE IF NOT EXISTS monitoring_alerts (
    alert_id TEXT PRIMARY KEY,
    watch_id TEXT NOT NULL,
    detected_at TEXT DEFAULT (datetime('now')),
    new_patents_count INTEGER DEFAULT 0,
    patent_ids TEXT,
    summary TEXT,
    acknowledged INTEGER DEFAULT 0,
    FOREIGN KEY (watch_id) REFERENCES monitoring_watches(watch_id)
);

CREATE INDEX IF NOT EXISTS idx_alerts_watch ON monitoring_alerts(watch_id);
CREATE INDEX IF NOT EXISTS idx_alerts_ack ON monitoring_alerts(acknowledged);

-- PTAB trials
CREATE TABLE IF NOT EXISTS ptab_trials (
    trial_number TEXT PRIMARY KEY,
    patent_number TEXT,
    publication_number TEXT,
    filing_date TEXT,
    institution_decision_date TEXT,
    prosecution_status TEXT,
    accorded_filing_date TEXT,
    petitioner TEXT,
    patent_owner TEXT,
    inventor_name TEXT,
    application_number TEXT
);

CREATE INDEX IF NOT EXISTS idx_ptab_patent ON ptab_trials(patent_number);
CREATE INDEX IF NOT EXISTS idx_ptab_pub ON ptab_trials(publication_number);
CREATE INDEX IF NOT EXISTS idx_ptab_petitioner ON ptab_trials(petitioner);
CREATE INDEX IF NOT EXISTS idx_ptab_owner ON ptab_trials(patent_owner);
CREATE INDEX IF NOT EXISTS idx_ptab_status ON ptab_trials(prosecution_status);

-- Litigation cases (from USPTO OCE)
CREATE TABLE IF NOT EXISTS litigation_cases (
    case_id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_number TEXT,
    court TEXT,
    judge TEXT,
    date_filed TEXT,
    date_terminated TEXT,
    plaintiff TEXT,
    defendant TEXT,
    nature_of_suit TEXT,
    outcome TEXT
);

CREATE INDEX IF NOT EXISTS idx_lit_number ON litigation_cases(case_number);
CREATE INDEX IF NOT EXISTS idx_lit_plaintiff ON litigation_cases(plaintiff);
CREATE INDEX IF NOT EXISTS idx_lit_defendant ON litigation_cases(defendant);
CREATE INDEX IF NOT EXISTS idx_lit_date ON litigation_cases(date_filed);

-- Litigation-patent link table
CREATE TABLE IF NOT EXISTS litigation_patents (
    case_id INTEGER,
    patent_number TEXT,
    PRIMARY KEY (case_id, patent_number)
);

CREATE INDEX IF NOT EXISTS idx_litpat_patent ON litigation_patents(patent_number);

-- Citation index (pre-computed citation counts)
CREATE TABLE IF NOT EXISTS citation_index (
    publication_number TEXT PRIMARY KEY,
    citing_count INTEGER DEFAULT 0,
    cited_by_count INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_citidx_cited_by ON citation_index(cited_by_count DESC);
CREATE INDEX IF NOT EXISTS idx_citidx_citing ON citation_index(citing_count DESC);

-- Display names for global entity resolution
CREATE TABLE IF NOT EXISTS display_names (
    assignee_raw TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    country TEXT,
    sector TEXT
);

CREATE INDEX IF NOT EXISTS idx_dn_canonical ON display_names(canonical_name);
"""
