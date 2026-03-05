#!/bin/bash
# Overnight autonomous pipeline: research ingestion → recompute → quality check
# Run on Hetzner as: nohup bash ~/patent-space-mcp/scripts/overnight_pipeline.sh > ~/overnight.log 2>&1 &

set -e
PYTHON=~/pyenv/bin/python3
DB=/home/deploy/patent-space-mcp/data/patents.db
SCRIPTS=/home/deploy/patent-space-mcp/scripts
EXPORTS=~/exports

echo "=== OVERNIGHT PIPELINE START: $(date) ==="

# Step 0: Wait for research download to complete
echo "[Step 0] Waiting for research download..."
while true; do
    if ! pgrep -f "gsutil.*research" > /dev/null 2>&1; then
        echo "  Download process finished at $(date)"
        break
    fi
    CURRENT=$(du -sb $EXPORTS/research/ 2>/dev/null | cut -f1)
    CURRENT_GB=$((CURRENT / 1073741824))
    echo "  $(date +%H:%M:%S) - ${CURRENT_GB}GB / 354GB downloaded"
    sleep 60
done

# Verify download
FILE_COUNT=$(ls $EXPORTS/research/*.parquet 2>/dev/null | wc -l)
TOTAL_SIZE=$(du -sh $EXPORTS/research/ 2>/dev/null | cut -f1)
echo "  Download complete: $FILE_COUNT files, $TOTAL_SIZE"

if [ "$FILE_COUNT" -lt 5000 ]; then
    echo "  WARNING: Expected ~5800 files, got $FILE_COUNT. Continuing anyway."
fi

# Step 1: Ingest research embeddings (ALL countries)
echo ""
echo "=== [Step 1] Ingest research embeddings: $(date) ==="
$PYTHON $SCRIPTS/ingest_research_parquet.py \
    --parquet-dir $EXPORTS/research/ \
    --db $DB

# Check embedding count
echo ""
echo "=== Embedding count check ==="
$PYTHON -c "
import sqlite3
conn = sqlite3.connect('$DB')
total = conn.execute('SELECT COUNT(*) FROM patent_research_data').fetchone()[0]
jp_count = conn.execute(\"SELECT COUNT(*) FROM patent_research_data WHERE publication_number LIKE 'JP-%'\").fetchone()[0]
us_count = conn.execute(\"SELECT COUNT(*) FROM patent_research_data WHERE publication_number LIKE 'US-%'\").fetchone()[0]
print(f'Total embeddings: {total:,}')
print(f'JP embeddings: {jp_count:,}')
print(f'US embeddings: {us_count:,}')

# Check JP 2019+ coverage
for year in range(2019, 2026):
    prefix = f'JP-{year}'
    c = conn.execute('SELECT COUNT(*) FROM patent_research_data WHERE publication_number LIKE ?', (prefix+'%',)).fetchone()[0]
    print(f'  {prefix}: {c:,}')
conn.close()
"

# Step 2: Ingest litigation data
echo ""
echo "=== [Step 2] Ingest litigation: $(date) ==="
$PYTHON $SCRIPTS/ingest_litigation_parquet.py \
    --parquet-dir $EXPORTS/litigation/ \
    --db $DB || echo "  Litigation ingestion failed (non-critical), continuing..."

# Step 3: Recompute firm tech vectors
echo ""
echo "=== [Step 3] Recompute firm_tech_vectors: $(date) ==="
$PYTHON $SCRIPTS/compute_firm_tech_vectors.py --db $DB

# Step 4: Rebuild tech clusters
echo ""
echo "=== [Step 4] Rebuild tech_clusters: $(date) ==="
$PYTHON $SCRIPTS/build_tech_clusters.py --db $DB

# Step 5: Recompute patent value index
echo ""
echo "=== [Step 5] Recompute patent_value_index: $(date) ==="
$PYTHON $SCRIPTS/compute_patent_value.py --db $DB

# Step 6: Recompute startability surface
echo ""
echo "=== [Step 6] Recompute startability_surface: $(date) ==="
$PYTHON $SCRIPTS/compute_startability_surface.py --db $DB

# Step 7: Quality checks
echo ""
echo "=== [Step 7] Quality checks: $(date) ==="
$PYTHON -c "
import sqlite3, struct, numpy as np
conn = sqlite3.connect('$DB')

print('=== Quality Check Results ===')
print()

# 1. Total patents
total = conn.execute('SELECT COUNT(*) FROM patents').fetchone()[0]
print(f'1. Total patents: {total:,}')

# 2. Embedding coverage
emb_total = conn.execute('SELECT COUNT(*) FROM patent_research_data').fetchone()[0]
print(f'2. Total embeddings: {emb_total:,}')
if total > 0:
    print(f'   Coverage: {emb_total/total*100:.1f}%')

# 3. JP 2019-2024 embedding coverage (THE KEY FIX)
for year in range(2016, 2025):
    prefix = f'JP-{year}'
    patents = conn.execute('SELECT COUNT(*) FROM patents WHERE publication_number LIKE ?', (prefix+'%',)).fetchone()[0]
    embeddings = conn.execute('SELECT COUNT(*) FROM patent_research_data WHERE publication_number LIKE ?', (prefix+'%',)).fetchone()[0]
    cov = embeddings/patents*100 if patents > 0 else 0
    flag = ' ← WAS 0%' if year >= 2019 else ''
    print(f'   {prefix}: {patents:,} patents, {embeddings:,} embeddings ({cov:.1f}%){flag}')

# 4. Firm tech vectors
ftv = conn.execute('SELECT COUNT(*) FROM firm_tech_vectors').fetchone()[0]
ftv_years = conn.execute('SELECT DISTINCT year FROM firm_tech_vectors ORDER BY year').fetchall()
print(f'3. Firm tech vectors: {ftv:,} (years: {[y[0] for y in ftv_years]})')

# 5. Tech clusters
clusters = conn.execute('SELECT COUNT(*) FROM tech_clusters').fetchone()[0]
print(f'4. Tech clusters: {clusters:,}')

# 6. Startability surface
surface = conn.execute('SELECT COUNT(*) FROM startability_surface').fetchone()[0]
print(f'5. Startability surface entries: {surface:,}')

# 7. ΔS check for 2019-2024
print(f'6. ΔS check (non-zero changes):')
for y1, y2 in [(2018,2019), (2019,2020), (2020,2021), (2021,2022), (2022,2023)]:
    delta_count = conn.execute('''
        SELECT COUNT(*) FROM startability_surface s1
        JOIN startability_surface s2 ON s1.cluster_id = s2.cluster_id AND s1.firm_id = s2.firm_id
        WHERE s1.year = ? AND s2.year = ? AND ABS(s1.score - s2.score) > 0.001
    ''', (y1, y2)).fetchone()[0]
    print(f'   {y1}→{y2}: {delta_count:,} non-zero deltas')

# 8. Sanity test: Toyota × automotive
toyota_check = conn.execute('''
    SELECT score FROM startability_surface
    WHERE firm_id = '7203' AND year = 2020
    ORDER BY score DESC LIMIT 5
''').fetchall()
if toyota_check:
    print(f'7. Toyota top-5 startability scores (2020): {[round(s[0],3) for s in toyota_check]}')

conn.close()
print()
print('=== Quality Check Complete ===')" || echo "Quality check had errors"

# Step 8: Restart MCP server with new data
echo ""
echo "=== [Step 8] Restart MCP server: $(date) ==="
cd /home/deploy/patent-space-mcp
docker compose restart || docker restart patent-mcp || echo "  Server restart failed - may need manual intervention"
sleep 10

# Health check
curl -s http://localhost:8001/health | python3 -m json.tool || echo "  Health check failed"

echo ""
echo "=== OVERNIGHT PIPELINE COMPLETE: $(date) ==="
echo "Next: Delete GCS bucket from local machine"
