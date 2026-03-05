#!/bin/bash
# post_download.sh — Run after GCS download completes
# 1. Delete GCS bucket to avoid storage charges
# 2. Start Parquet → SQLite ingestion
# 3. Create indexes + FTS
# 4. Swap DB and restart MCP
#
# Usage: nohup bash ~/patent-space-mcp/scripts/post_download.sh > ~/post_download.log 2>&1 &

set -e

echo "$(date) === Post-download pipeline started ==="

# Step 1: Verify download is complete
EXPECTED=14216
ACTUAL=$(ls ~/exports/patents/patents/publications-*.parquet 2>/dev/null | wc -l)
echo "$(date) Downloaded files: $ACTUAL / $EXPECTED"

if [ "$ACTUAL" -lt "$EXPECTED" ]; then
    echo "$(date) ERROR: Download incomplete ($ACTUAL < $EXPECTED). Aborting."
    exit 1
fi

echo "$(date) Download verified: $ACTUAL files"

# Step 2: Delete GCS bucket
echo "$(date) === Deleting GCS bucket ==="
~/google-cloud-sdk/bin/gsutil -m rm -r gs://patent-mcp-exports/ 2>&1 || true
~/google-cloud-sdk/bin/gsutil rb gs://patent-mcp-exports/ 2>&1 || true
echo "$(date) GCS bucket deleted"

# Step 3: Ingest into a new SQLite database (separate from the running MCP)
echo "$(date) === Starting Parquet → SQLite ingestion ==="
NEW_DB="$HOME/patent-space-mcp/data/patents_global.db"
rm -f "$NEW_DB" "${NEW_DB}-wal" "${NEW_DB}-shm"

python3 ~/patent-space-mcp/scripts/ingest_parquet_patents.py \
    --parquet-dir ~/exports/patents/patents/ \
    --db "$NEW_DB" \
    --batch-size 10000 \
    --no-indexes \
    2>&1

echo "$(date) Ingestion complete (without indexes)"

# Step 4: Create indexes and FTS
echo "$(date) === Creating indexes and FTS ==="
python3 << 'PYEOF'
import sqlite3, time

DB = "$HOME/patent-space-mcp/data/patents_global.db".replace("$HOME", __import__("os").environ["HOME"])
conn = sqlite3.connect(DB, timeout=600)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA cache_size=-4000000")
conn.execute("PRAGMA mmap_size=8589934592")
conn.execute("PRAGMA synchronous=NORMAL")
conn.execute("PRAGMA temp_store=MEMORY")

# Create indexes
print(f"{time.strftime('%H:%M:%S')} Creating indexes...")
t0 = time.time()
conn.executescript("""
    CREATE INDEX IF NOT EXISTS idx_patents_family ON patents(family_id);
    CREATE INDEX IF NOT EXISTS idx_patents_country ON patents(country_code);
    CREATE INDEX IF NOT EXISTS idx_patents_filing_date ON patents(filing_date);
    CREATE INDEX IF NOT EXISTS idx_patents_pub_date ON patents(publication_date);
    CREATE INDEX IF NOT EXISTS idx_cpc_code ON patent_cpc(cpc_code);
    CREATE INDEX IF NOT EXISTS idx_cpc_class ON patent_cpc(substr(cpc_code, 1, 4));
    CREATE INDEX IF NOT EXISTS idx_assignee_pub ON patent_assignees(publication_number);
    CREATE INDEX IF NOT EXISTS idx_assignee_harmonized ON patent_assignees(harmonized_name);
    CREATE INDEX IF NOT EXISTS idx_assignee_firm ON patent_assignees(firm_id);
    CREATE INDEX IF NOT EXISTS idx_inventor_pub ON patent_inventors(publication_number);
    CREATE INDEX IF NOT EXISTS idx_citation_citing ON patent_citations(citing_publication);
    CREATE INDEX IF NOT EXISTS idx_citation_cited ON patent_citations(cited_publication);
""")
print(f"{time.strftime('%H:%M:%S')} Indexes created in {time.time()-t0:.0f}s")

# Create FTS5 table and populate it
print(f"{time.strftime('%H:%M:%S')} Creating FTS5 index...")
t0 = time.time()
conn.executescript("""
    CREATE VIRTUAL TABLE IF NOT EXISTS patents_fts USING fts5(
        publication_number,
        title_ja,
        title_en,
        content='patents',
        content_rowid='rowid',
        tokenize='trigram'
    );

    -- Populate FTS from existing data
    INSERT INTO patents_fts(patents_fts) VALUES('rebuild');
""")

# Create FTS triggers for future inserts
conn.executescript("""
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
""")
print(f"{time.strftime('%H:%M:%S')} FTS5 created in {time.time()-t0:.0f}s")

# Create remaining tables from MCP schema (empty but needed by MCP code)
conn.executescript("""
    CREATE TABLE IF NOT EXISTS patent_research_data (
        publication_number TEXT PRIMARY KEY,
        title_en TEXT,
        abstract_en TEXT,
        top_terms TEXT,
        embedding_v1 BLOB
    );

    CREATE TABLE IF NOT EXISTS citation_counts (
        publication_number TEXT PRIMARY KEY,
        forward_citations INTEGER NOT NULL
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
        distance REAL
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

    CREATE TABLE IF NOT EXISTS patent_legal_status (
        publication_number TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        expiry_date INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_pls_status ON patent_legal_status(status);

    CREATE TABLE IF NOT EXISTS patent_value_index (
        publication_number TEXT PRIMARY KEY,
        value_score REAL NOT NULL,
        citation_component REAL,
        family_component REAL,
        recency_component REAL,
        cluster_momentum_component REAL
    );
    CREATE INDEX IF NOT EXISTS idx_pvi_score ON patent_value_index(value_score DESC);

    CREATE TABLE IF NOT EXISTS patent_family (
        publication_number TEXT PRIMARY KEY,
        family_id TEXT NOT NULL,
        family_size INTEGER DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_pf_family ON patent_family(family_id);

    CREATE TABLE IF NOT EXISTS patent_litigation (
        case_id TEXT PRIMARY KEY,
        patent_number TEXT,
        plaintiff TEXT,
        defendant TEXT,
        filing_date TEXT,
        court TEXT,
        outcome TEXT,
        damages_amount REAL
    );
    CREATE INDEX IF NOT EXISTS idx_pl_patent ON patent_litigation(patent_number);

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
""")
print(f"{time.strftime('%H:%M:%S')} Additional MCP tables created")

# Final checkpoint
conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
total = conn.execute("SELECT COUNT(*) FROM patents").fetchone()[0]
print(f"{time.strftime('%H:%M:%S')} Total patents: {total}")
conn.close()
PYEOF

echo "$(date) Indexes, FTS, and MCP schema created"

# Step 5: Swap databases
echo "$(date) === Swapping databases ==="
OLD_DB="$HOME/patent-space-mcp/data/patents.db"
BACKUP_DB="$HOME/patent-space-mcp/data/patents_jp_only.db"

# Stop MCP container
cd ~/patent-space-mcp
docker compose stop mcp 2>/dev/null || true

# Backup old JP-only DB, swap in new global DB
if [ -f "$OLD_DB" ]; then
    mv "$OLD_DB" "$BACKUP_DB"
    mv "${OLD_DB}-wal" "${BACKUP_DB}-wal" 2>/dev/null || true
    mv "${OLD_DB}-shm" "${BACKUP_DB}-shm" 2>/dev/null || true
fi
mv "$NEW_DB" "$OLD_DB"
mv "${NEW_DB}-wal" "${OLD_DB}-wal" 2>/dev/null || true
mv "${NEW_DB}-shm" "${OLD_DB}-shm" 2>/dev/null || true

echo "$(date) Database swapped"

# Step 6: Rebuild and restart MCP
echo "$(date) === Rebuilding Docker image ==="
docker compose build 2>&1
docker compose up -d 2>&1

echo "$(date) === Waiting for health check ==="
sleep 15
curl -s http://localhost:8001/health | python3 -m json.tool 2>/dev/null || echo "Health check pending..."

echo "$(date) === Post-download pipeline complete ==="
echo "$(date) Old JP DB backed up to: $BACKUP_DB"
echo "$(date) New global DB at: $OLD_DB"
echo "$(date) To delete Parquet files: rm -rf ~/exports/patents/"
