#!/usr/bin/env bash
# deploy_remote.sh — Deploy Patent Space MCP to a remote server
#
# Usage:
#   ./scripts/deploy_remote.sh <remote_host> [options]
#
# Examples:
#   ./scripts/deploy_remote.sh user@192.168.1.100
#   ./scripts/deploy_remote.sh user@mcp.example.com --db-only
#   ./scripts/deploy_remote.sh user@mcp.example.com --skip-db
#
# Prerequisites:
#   - SSH key-based auth configured for the remote host
#   - Local patents.db available at data/patents.db (or $LOCAL_DB_PATH)
#   - git, docker, docker compose available on remote (script installs if missing)

set -euo pipefail

# ─────────────────────────────────────────────
# Configuration (override via environment)
# ─────────────────────────────────────────────
REMOTE_HOST="${1:-}"
REMOTE_USER="${REMOTE_USER:-}"
REMOTE_DIR="${REMOTE_DIR:-/opt/patent-space-mcp}"
REMOTE_PORT="${REMOTE_PORT:-8001}"
LOCAL_DB_PATH="${LOCAL_DB_PATH:-data/patents.db}"
REPO_URL="${REPO_URL:-https://github.com/<your-org>/patent-space-mcp.git}"
REPO_BRANCH="${REPO_BRANCH:-main}"

# Flags
SKIP_DB=false
DB_ONLY=false
SKIP_DOCKER_INSTALL=false

shift || true
for arg in "$@"; do
  case "$arg" in
    --skip-db) SKIP_DB=true ;;
    --db-only) DB_ONLY=true ;;
    --skip-docker-install) SKIP_DOCKER_INSTALL=true ;;
    *) echo "Unknown option: $arg"; exit 1 ;;
  esac
done

if [ -z "$REMOTE_HOST" ]; then
  echo "Usage: $0 <user@host> [--skip-db] [--db-only] [--skip-docker-install]"
  echo ""
  echo "Environment variables:"
  echo "  REMOTE_DIR          Deploy directory (default: /opt/patent-space-mcp)"
  echo "  REMOTE_PORT         MCP HTTP port (default: 8001)"
  echo "  LOCAL_DB_PATH       Local DB path (default: data/patents.db)"
  echo "  REPO_URL            Git repo URL"
  echo "  REPO_BRANCH         Git branch (default: main)"
  exit 1
fi

log() { echo "$(date '+%H:%M:%S') [deploy] $*"; }
err() { echo "$(date '+%H:%M:%S') [ERROR] $*" >&2; exit 1; }

# ─────────────────────────────────────────────
# Step 1: Install Docker on remote (if needed)
# ─────────────────────────────────────────────
if [ "$SKIP_DOCKER_INSTALL" = false ] && [ "$DB_ONLY" = false ]; then
  log "Step 1: Checking Docker on $REMOTE_HOST ..."
  ssh "$REMOTE_HOST" bash -s <<'INSTALL_DOCKER'
    set -euo pipefail
    if command -v docker &>/dev/null && command -v docker compose &>/dev/null; then
      echo "Docker and docker compose already installed"
      docker --version
      docker compose version
    else
      echo "Installing Docker ..."
      if [ -f /etc/debian_version ]; then
        sudo apt-get update
        sudo apt-get install -y ca-certificates curl gnupg
        sudo install -m 0755 -d /etc/apt/keyrings
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
        sudo chmod a+r /etc/apt/keyrings/docker.gpg
        echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
          sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
        sudo apt-get update
        sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
      elif [ -f /etc/redhat-release ]; then
        sudo yum install -y yum-utils
        sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
        sudo yum install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
        sudo systemctl start docker
        sudo systemctl enable docker
      else
        echo "Unsupported OS. Please install Docker manually."
        exit 1
      fi
      sudo usermod -aG docker "$USER" || true
      echo "Docker installed. You may need to log out and back in for group changes."
    fi
INSTALL_DOCKER
  log "  Docker ready."
fi

# ─────────────────────────────────────────────
# Step 2: Clone / update repository
# ─────────────────────────────────────────────
if [ "$DB_ONLY" = false ]; then
  log "Step 2: Setting up repository on remote ..."
  ssh "$REMOTE_HOST" bash -s -- "$REMOTE_DIR" "$REPO_URL" "$REPO_BRANCH" <<'CLONE_REPO'
    set -euo pipefail
    REMOTE_DIR="$1"
    REPO_URL="$2"
    REPO_BRANCH="$3"

    sudo mkdir -p "$REMOTE_DIR"
    sudo chown "$USER:$USER" "$REMOTE_DIR"

    if [ -d "$REMOTE_DIR/.git" ]; then
      echo "Repo exists, pulling latest ..."
      cd "$REMOTE_DIR"
      git fetch origin
      git checkout "$REPO_BRANCH"
      git pull origin "$REPO_BRANCH"
    else
      echo "Cloning repo ..."
      git clone --branch "$REPO_BRANCH" "$REPO_URL" "$REMOTE_DIR"
    fi

    mkdir -p "$REMOTE_DIR/data"
    echo "Repository ready at $REMOTE_DIR"
CLONE_REPO
  log "  Repository ready."
fi

# ─────────────────────────────────────────────
# Step 3: Transfer database (compressed)
# ─────────────────────────────────────────────
if [ "$SKIP_DB" = false ]; then
  if [ ! -f "$LOCAL_DB_PATH" ]; then
    err "Local database not found: $LOCAL_DB_PATH"
  fi

  DB_SIZE=$(du -sh "$LOCAL_DB_PATH" | cut -f1)
  log "Step 3: Transferring database ($DB_SIZE) ..."
  log "  Compressing and streaming via ssh (pigz if available, else gzip) ..."

  COMPRESS_CMD="gzip -1"
  if command -v pigz &>/dev/null; then
    COMPRESS_CMD="pigz -1 -p 4"
  fi

  STARTED=$(date +%s)

  # Stream compress → ssh → decompress on remote
  $COMPRESS_CMD -c "$LOCAL_DB_PATH" | \
    ssh "$REMOTE_HOST" "mkdir -p ${REMOTE_DIR}/data && gunzip > ${REMOTE_DIR}/data/patents.db"

  ELAPSED=$(( $(date +%s) - STARTED ))
  log "  Database transferred in ${ELAPSED}s."

  # Verify remote DB
  ssh "$REMOTE_HOST" bash -s -- "$REMOTE_DIR" <<'VERIFY_DB'
    set -euo pipefail
    DB="$1/data/patents.db"
    if [ ! -f "$DB" ]; then
      echo "ERROR: DB not found at $DB"
      exit 1
    fi
    SIZE=$(du -sh "$DB" | cut -f1)
    TABLES=$(sqlite3 "$DB" "SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
    PATENTS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM patents" 2>/dev/null || echo "N/A")
    echo "DB verified: $SIZE, $TABLES tables, $PATENTS patents"
VERIFY_DB
  log "  Database verified."
fi

if [ "$DB_ONLY" = true ]; then
  log "DB-only mode complete."
  exit 0
fi

# ─────────────────────────────────────────────
# Step 4: Build and start MCP server
# ─────────────────────────────────────────────
log "Step 4: Building and starting MCP server ..."
ssh "$REMOTE_HOST" bash -s -- "$REMOTE_DIR" "$REMOTE_PORT" <<'START_MCP'
  set -euo pipefail
  cd "$1"
  PORT="$2"

  # Create .env if not exists
  if [ ! -f .env ]; then
    cp .env.example .env 2>/dev/null || echo "PATENT_DB_PATH=/app/data/patents.db" > .env
  fi

  # Build and start
  docker compose down 2>/dev/null || true
  docker compose build
  docker compose up -d

  echo "Waiting for server to start ..."
  sleep 5
START_MCP
log "  Server started."

# ─────────────────────────────────────────────
# Step 5: Health check
# ─────────────────────────────────────────────
log "Step 5: Health check ..."

MAX_RETRIES=6
RETRY_INTERVAL=5
for i in $(seq 1 $MAX_RETRIES); do
  HEALTH=$(ssh "$REMOTE_HOST" "curl -sf http://localhost:${REMOTE_PORT}/health 2>/dev/null" || echo "")
  if [ -n "$HEALTH" ]; then
    log "  Health check passed:"
    echo "  $HEALTH" | python3 -m json.tool 2>/dev/null || echo "  $HEALTH"
    break
  fi
  if [ "$i" -eq "$MAX_RETRIES" ]; then
    err "Health check failed after $MAX_RETRIES attempts. Check: ssh $REMOTE_HOST 'docker compose -f ${REMOTE_DIR}/docker-compose.yml logs'"
  fi
  log "  Attempt $i/$MAX_RETRIES failed, retrying in ${RETRY_INTERVAL}s ..."
  sleep "$RETRY_INTERVAL"
done

# ─────────────────────────────────────────────
# Done
# ─────────────────────────────────────────────
log ""
log "Deployment complete!"
log "  MCP endpoint: http://$REMOTE_HOST:$REMOTE_PORT/mcp"
log "  Health check: http://$REMOTE_HOST:$REMOTE_PORT/health"
log ""
log "Configure Claude Desktop:"
log '  {"mcpServers": {"patent-space": {"url": "http://'"$REMOTE_HOST"':'"$REMOTE_PORT"'/mcp"}}}'
