#!/bin/bash
# watch_download.sh — Monitor GCS download and trigger post-processing
# Usage: nohup bash ~/patent-space-mcp/scripts/watch_download.sh > ~/watch_download.log 2>&1 &

EXPECTED=14216
CHECK_INTERVAL=300  # 5 minutes

echo "$(date) Watching for download completion ($EXPECTED files expected)..."

while true; do
    # Check if gsutil is still running
    if ! pgrep -f "gsutil.*cp.*patent-mcp-exports" > /dev/null 2>&1; then
        ACTUAL=$(ls ~/exports/patents/patents/publications-*.parquet 2>/dev/null | wc -l)
        echo "$(date) gsutil not running. Files: $ACTUAL / $EXPECTED"

        if [ "$ACTUAL" -ge "$EXPECTED" ]; then
            echo "$(date) Download COMPLETE! Starting post-download pipeline..."
            bash ~/patent-space-mcp/scripts/post_download.sh 2>&1
            echo "$(date) Post-download pipeline finished."
            exit 0
        else
            echo "$(date) WARNING: gsutil stopped but only $ACTUAL / $EXPECTED files. Possible error."
            echo "$(date) Check ~/exports/dl_publications.log for details."
            exit 1
        fi
    fi

    ACTUAL=$(ls ~/exports/patents/patents/publications-*.parquet 2>/dev/null | wc -l)
    SIZE=$(du -sh ~/exports/patents/ 2>/dev/null | cut -f1)
    echo "$(date) Download in progress: $ACTUAL / $EXPECTED files, $SIZE"

    sleep $CHECK_INTERVAL
done
