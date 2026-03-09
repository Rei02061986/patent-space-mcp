"""Patent monitoring and alerting tool.

Provides watch-based monitoring for patent activity — new filings by
applicant, CPC class, keyword, or competitor.  Watches are persisted in
SQLite and checked on demand via ``run_monitoring()``.

Tables used (created by migrations.py or _ensure_tables):
    monitoring_watches  — watch definitions (applicant / cpc / keyword / competitor)
    monitoring_alerts   — detected alerts with patent ID lists
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any

from db.sqlite_store import PatentStore
from tools.pagination import paginate

_log = logging.getLogger(__name__)

_DB_PATH = "/app/data/patents.db"

_VALID_WATCH_TYPES = {"applicant", "cpc", "keyword", "competitor"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_conn() -> sqlite3.Connection:
    """Open a separate connection for writes (WAL allows concurrent reads).

    Uses isolation_level=None (autocommit) so Python's implicit
    transaction management doesn't interfere with writes.
    """
    conn = sqlite3.connect(_DB_PATH, timeout=120, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _ensure_tables(conn: sqlite3.Connection) -> None:
    """Create monitoring tables if they do not already exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monitoring_watches (
            watch_id TEXT PRIMARY KEY,
            watch_type TEXT NOT NULL,
            target TEXT NOT NULL,
            parameters TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            last_checked TEXT,
            alert_threshold INTEGER DEFAULT 5
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monitoring_alerts (
            alert_id TEXT PRIMARY KEY,
            watch_id TEXT NOT NULL,
            detected_at TEXT DEFAULT (datetime('now')),
            new_patents_count INTEGER DEFAULT 0,
            patent_ids TEXT,
            summary TEXT,
            acknowledged INTEGER DEFAULT 0,
            FOREIGN KEY (watch_id) REFERENCES monitoring_watches(watch_id)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ma_watch ON monitoring_alerts(watch_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ma_ack ON monitoring_alerts(acknowledged)"
    )


def _now_iso() -> str:
    """Return current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_watch_id(watch_type: str, target: str) -> str:
    """Deterministic watch ID from type + target."""
    raw = f"{watch_type}:{target}".encode("utf-8")
    return f"w_{hashlib.sha256(raw).hexdigest()[:16]}"


def _make_alert_id(watch_id: str, detected_at: str) -> str:
    """Deterministic alert ID from watch + timestamp."""
    raw = f"{watch_id}:{detected_at}".encode("utf-8")
    return f"a_{hashlib.sha256(raw).hexdigest()[:16]}"


# ---------------------------------------------------------------------------
# 1. create_watch
# ---------------------------------------------------------------------------

def create_watch(
    store: PatentStore,
    watch_type: str,
    target: str,
    parameters: dict[str, Any] | None = None,
    alert_threshold: int = 5,
) -> dict[str, Any]:
    """Create a new monitoring watch.

    Args:
        store: PatentStore instance (used for read-path consistency).
        watch_type: One of ``applicant``, ``cpc``, ``keyword``, ``competitor``.
        target: The value to monitor — company name, CPC code, keyword, etc.
        parameters: Optional JSON-serializable dict with extra filters
                    (e.g. ``{"jurisdiction": "JP", "date_from": "2024-01-01"}``).
        alert_threshold: Minimum number of new patents to trigger an alert.

    Returns:
        Dict with ``endpoint``, ``watch_id``, and confirmation details.
    """
    watch_type = watch_type.strip().lower()
    target = target.strip()

    if not target:
        return {"endpoint": "create_watch", "error": "target must not be empty"}

    if watch_type not in _VALID_WATCH_TYPES:
        return {
            "endpoint": "create_watch",
            "error": f"Invalid watch_type '{watch_type}'. Must be one of: {', '.join(sorted(_VALID_WATCH_TYPES))}",
        }

    watch_id = _make_watch_id(watch_type, target)
    params_json = json.dumps(parameters, ensure_ascii=False) if parameters else None
    now = _now_iso()

    wconn = _write_conn()
    try:
        _ensure_tables(wconn)

        # Check for existing watch with same ID
        existing = wconn.execute(
            "SELECT watch_id, watch_type, target FROM monitoring_watches WHERE watch_id = ?",
            (watch_id,),
        ).fetchone()

        if existing:
            return {
                "endpoint": "create_watch",
                "status": "already_exists",
                "watch_id": watch_id,
                "watch_type": existing["watch_type"],
                "target": existing["target"],
                "message": f"Watch already exists for {watch_type}='{target}'",
            }

        wconn.execute(
            """INSERT INTO monitoring_watches
               (watch_id, watch_type, target, parameters, created_at, alert_threshold)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (watch_id, watch_type, target, params_json, now, alert_threshold),
        )

        return {
            "endpoint": "create_watch",
            "status": "created",
            "watch_id": watch_id,
            "watch_type": watch_type,
            "target": target,
            "parameters": parameters,
            "alert_threshold": alert_threshold,
            "created_at": now,
        }
    finally:
        wconn.close()


# ---------------------------------------------------------------------------
# 2. list_watches
# ---------------------------------------------------------------------------

def list_watches(
    store: PatentStore,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """List all active monitoring watches with alert counts.

    Args:
        store: PatentStore instance.
        page: Page number (1-based).
        page_size: Items per page (max 100).

    Returns:
        Paginated list of watches, each with unacknowledged alert count.
    """
    conn = store._conn()

    # Ensure tables exist on the read connection too (idempotent)
    try:
        rows = conn.execute(
            """SELECT w.watch_id, w.watch_type, w.target, w.parameters,
                      w.created_at, w.last_checked, w.alert_threshold,
                      COALESCE(a.alert_count, 0) AS unacked_alerts
               FROM monitoring_watches w
               LEFT JOIN (
                   SELECT watch_id, COUNT(*) AS alert_count
                   FROM monitoring_alerts
                   WHERE acknowledged = 0
                   GROUP BY watch_id
               ) a ON a.watch_id = w.watch_id
               ORDER BY w.created_at DESC"""
        ).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            # Tables not created yet — return empty
            return {
                "endpoint": "list_watches",
                "watches": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "pages": 1,
                "message": "No watches configured yet. Use create_watch to add one.",
            }
        raise

    watches = []
    for r in rows:
        w: dict[str, Any] = {
            "watch_id": r["watch_id"],
            "watch_type": r["watch_type"],
            "target": r["target"],
            "parameters": json.loads(r["parameters"]) if r["parameters"] else None,
            "created_at": r["created_at"],
            "last_checked": r["last_checked"],
            "alert_threshold": r["alert_threshold"],
            "unacked_alerts": r["unacked_alerts"],
        }
        watches.append(w)

    paged = paginate(watches, page=page, page_size=page_size)
    return {
        "endpoint": "list_watches",
        "watches": paged["results"],
        "total": paged["total"],
        "page": paged["page"],
        "page_size": paged["page_size"],
        "pages": paged["pages"],
    }


# ---------------------------------------------------------------------------
# 3. check_alerts
# ---------------------------------------------------------------------------

def check_alerts(
    store: PatentStore,
    watch_id: str | None = None,
    acknowledged: bool = False,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """Return alerts, optionally filtered by watch and acknowledgement status.

    Args:
        store: PatentStore instance.
        watch_id: If given, only return alerts for this watch.
        acknowledged: If False (default), show only unacknowledged alerts.
                      If True, show all alerts regardless of status.
        page: Page number (1-based).
        page_size: Items per page (max 100).

    Returns:
        Paginated list of alerts with patent details.
    """
    conn = store._conn()

    conditions: list[str] = []
    params: list[Any] = []

    if watch_id:
        conditions.append("a.watch_id = ?")
        params.append(watch_id)
    if not acknowledged:
        conditions.append("a.acknowledged = 0")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    try:
        rows = conn.execute(
            f"""SELECT a.alert_id, a.watch_id, a.detected_at,
                       a.new_patents_count, a.patent_ids, a.summary,
                       a.acknowledged,
                       w.watch_type, w.target
                FROM monitoring_alerts a
                LEFT JOIN monitoring_watches w ON w.watch_id = a.watch_id
                {where}
                ORDER BY a.detected_at DESC""",
            params,
        ).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return {
                "endpoint": "check_alerts",
                "alerts": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "pages": 1,
                "message": "No alerts table yet. Run create_watch first.",
            }
        raise

    alerts = []
    for r in rows:
        patent_ids_raw = r["patent_ids"]
        try:
            pid_list = json.loads(patent_ids_raw) if patent_ids_raw else []
        except (json.JSONDecodeError, TypeError):
            pid_list = []

        alerts.append({
            "alert_id": r["alert_id"],
            "watch_id": r["watch_id"],
            "watch_type": r["watch_type"],
            "target": r["target"],
            "detected_at": r["detected_at"],
            "new_patents_count": r["new_patents_count"],
            "patent_ids": pid_list,
            "summary": r["summary"],
            "acknowledged": bool(r["acknowledged"]),
        })

    paged = paginate(alerts, page=page, page_size=page_size)
    return {
        "endpoint": "check_alerts",
        "alerts": paged["results"],
        "total": paged["total"],
        "page": paged["page"],
        "page_size": paged["page_size"],
        "pages": paged["pages"],
    }


# ---------------------------------------------------------------------------
# 4. acknowledge_alerts
# ---------------------------------------------------------------------------

def acknowledge_alerts(
    store: PatentStore,
    alert_ids: list[str] | None = None,
    watch_id: str | None = None,
) -> dict[str, Any]:
    """Mark alerts as acknowledged.

    Args:
        store: PatentStore instance.
        alert_ids: Specific alert IDs to acknowledge.  If None and watch_id
                   is given, acknowledge all alerts for that watch.
        watch_id: Acknowledge all unacked alerts for this watch.

    Returns:
        Count of acknowledged alerts.
    """
    if not alert_ids and not watch_id:
        return {
            "endpoint": "acknowledge_alerts",
            "error": "Provide alert_ids or watch_id to acknowledge.",
        }

    wconn = _write_conn()
    try:
        _ensure_tables(wconn)

        if alert_ids:
            ph = ",".join("?" * len(alert_ids))
            cur = wconn.execute(
                f"UPDATE monitoring_alerts SET acknowledged = 1 "
                f"WHERE alert_id IN ({ph}) AND acknowledged = 0",
                alert_ids,
            )
        else:
            cur = wconn.execute(
                "UPDATE monitoring_alerts SET acknowledged = 1 "
                "WHERE watch_id = ? AND acknowledged = 0",
                (watch_id,),
            )

        count = cur.rowcount
        return {
            "endpoint": "acknowledge_alerts",
            "acknowledged_count": count,
        }
    finally:
        wconn.close()


# ---------------------------------------------------------------------------
# 5. delete_watch
# ---------------------------------------------------------------------------

def delete_watch(
    store: PatentStore,
    watch_id: str,
) -> dict[str, Any]:
    """Delete a monitoring watch and all its alerts.

    Args:
        store: PatentStore instance.
        watch_id: ID of the watch to delete.

    Returns:
        Confirmation with counts of deleted alerts.
    """
    wconn = _write_conn()
    try:
        _ensure_tables(wconn)

        existing = wconn.execute(
            "SELECT watch_type, target FROM monitoring_watches WHERE watch_id = ?",
            (watch_id,),
        ).fetchone()

        if not existing:
            return {
                "endpoint": "delete_watch",
                "error": f"Watch '{watch_id}' not found.",
            }

        alert_cur = wconn.execute(
            "DELETE FROM monitoring_alerts WHERE watch_id = ?", (watch_id,)
        )
        alerts_deleted = alert_cur.rowcount

        wconn.execute(
            "DELETE FROM monitoring_watches WHERE watch_id = ?", (watch_id,)
        )

        return {
            "endpoint": "delete_watch",
            "status": "deleted",
            "watch_id": watch_id,
            "watch_type": existing["watch_type"],
            "target": existing["target"],
            "alerts_deleted": alerts_deleted,
        }
    finally:
        wconn.close()


# ---------------------------------------------------------------------------
# 6. run_monitoring  (core)
# ---------------------------------------------------------------------------

_PATENT_RESULT_LIMIT = 500  # Max patent IDs stored per alert


def _find_new_patents_applicant(
    conn: sqlite3.Connection,
    target: str,
    since: str | None,
    params: dict[str, Any] | None,
) -> list[str]:
    """Find new patents by applicant name since *since*."""
    conditions = ["a.harmonized_name LIKE ?"]
    bind: list[Any] = [f"%{target}%"]

    if since:
        conditions.append("p.publication_date > ?")
        # publication_date is stored as integer YYYYMMDD
        bind.append(int(since.replace("-", "").replace("T", "").replace(":", "")[:8]))

    if params:
        if params.get("jurisdiction"):
            conditions.append("p.country_code = ?")
            bind.append(params["jurisdiction"].upper())

    where = " AND ".join(conditions)
    rows = conn.execute(
        f"""SELECT DISTINCT a.publication_number
            FROM patent_assignees a
            JOIN patents p ON p.publication_number = a.publication_number
            WHERE {where}
            ORDER BY p.publication_date DESC
            LIMIT ?""",
        [*bind, _PATENT_RESULT_LIMIT],
    ).fetchall()
    return [r["publication_number"] for r in rows]


def _find_new_patents_cpc(
    conn: sqlite3.Connection,
    target: str,
    since: str | None,
    params: dict[str, Any] | None,
) -> list[str]:
    """Find new patents by CPC code prefix since *since*."""
    target_upper = target.upper().strip()
    conditions = ["c.cpc_code LIKE ?"]
    bind: list[Any] = [f"{target_upper}%"]

    if since:
        conditions.append("p.publication_date > ?")
        bind.append(int(since.replace("-", "").replace("T", "").replace(":", "")[:8]))

    if params:
        if params.get("jurisdiction"):
            conditions.append("p.country_code = ?")
            bind.append(params["jurisdiction"].upper())

    where = " AND ".join(conditions)
    rows = conn.execute(
        f"""SELECT DISTINCT c.publication_number
            FROM patent_cpc c
            JOIN patents p ON p.publication_number = c.publication_number
            WHERE {where}
            ORDER BY p.publication_date DESC
            LIMIT ?""",
        [*bind, _PATENT_RESULT_LIMIT],
    ).fetchall()
    return [r["publication_number"] for r in rows]


def _find_new_patents_keyword(
    conn: sqlite3.Connection,
    target: str,
    since: str | None,
    params: dict[str, Any] | None,
) -> list[str]:
    """Find new patents matching keyword via LIKE (FTS5 is corrupted).

    Searches title_ja and title_en columns.
    """
    keyword = target.strip()
    if not keyword:
        return []

    conditions = ["(p.title_ja LIKE ? OR p.title_en LIKE ?)"]
    bind: list[Any] = [f"%{keyword}%", f"%{keyword}%"]

    if since:
        conditions.append("p.publication_date > ?")
        bind.append(int(since.replace("-", "").replace("T", "").replace(":", "")[:8]))

    if params:
        if params.get("jurisdiction"):
            conditions.append("p.country_code = ?")
            bind.append(params["jurisdiction"].upper())

    where = " AND ".join(conditions)
    rows = conn.execute(
        f"""SELECT p.publication_number
            FROM patents p
            WHERE {where}
            ORDER BY p.publication_date DESC
            LIMIT ?""",
        [*bind, _PATENT_RESULT_LIMIT],
    ).fetchall()
    return [r["publication_number"] for r in rows]


def _find_new_patents_competitor(
    conn: sqlite3.Connection,
    target: str,
    since: str | None,
    params: dict[str, Any] | None,
) -> list[str]:
    """Find new patents by a competitor firm.

    First tries firm_id match in patent_assignees, then falls back
    to harmonized_name LIKE.
    """
    conditions_firm = ["a.firm_id = ?"]
    bind_firm: list[Any] = [target]

    if since:
        conditions_firm.append("p.publication_date > ?")
        bind_firm.append(
            int(since.replace("-", "").replace("T", "").replace(":", "")[:8])
        )

    if params:
        if params.get("jurisdiction"):
            conditions_firm.append("p.country_code = ?")
            bind_firm.append(params["jurisdiction"].upper())

    where_firm = " AND ".join(conditions_firm)

    # Try firm_id first (indexed)
    rows = conn.execute(
        f"""SELECT DISTINCT a.publication_number
            FROM patent_assignees a
            JOIN patents p ON p.publication_number = a.publication_number
            WHERE {where_firm}
            ORDER BY p.publication_date DESC
            LIMIT ?""",
        [*bind_firm, _PATENT_RESULT_LIMIT],
    ).fetchall()

    if rows:
        return [r["publication_number"] for r in rows]

    # Fallback: name LIKE
    return _find_new_patents_applicant(conn, target, since, params)


_FINDERS = {
    "applicant": _find_new_patents_applicant,
    "cpc": _find_new_patents_cpc,
    "keyword": _find_new_patents_keyword,
    "competitor": _find_new_patents_competitor,
}


def run_monitoring(
    store: PatentStore,
    watch_id: str | None = None,
) -> dict[str, Any]:
    """Execute monitoring for one or all watches.

    For each watch, query the patents table for new patents published
    since ``last_checked``.  If the count meets or exceeds the watch's
    ``alert_threshold``, create a new alert.

    Args:
        store: PatentStore instance (read path).
        watch_id: If given, only check this single watch.
                  Otherwise check all watches.

    Returns:
        Summary dict with ``watches_checked``, ``alerts_created``, and
        per-watch details.
    """
    store._relax_timeout()
    read_conn = store._conn()

    # Fetch watches to process
    try:
        if watch_id:
            watch_rows = read_conn.execute(
                "SELECT * FROM monitoring_watches WHERE watch_id = ?",
                (watch_id,),
            ).fetchall()
            if not watch_rows:
                return {
                    "endpoint": "run_monitoring",
                    "error": f"Watch '{watch_id}' not found.",
                }
        else:
            watch_rows = read_conn.execute(
                "SELECT * FROM monitoring_watches ORDER BY created_at"
            ).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return {
                "endpoint": "run_monitoring",
                "error": "No watches configured yet. Use create_watch first.",
                "watches_checked": 0,
                "alerts_created": 0,
            }
        raise

    if not watch_rows:
        return {
            "endpoint": "run_monitoring",
            "watches_checked": 0,
            "alerts_created": 0,
            "message": "No watches to check.",
        }

    wconn = _write_conn()
    _ensure_tables(wconn)

    watches_checked = 0
    alerts_created = 0
    details: list[dict[str, Any]] = []

    for w in watch_rows:
        wtype = w["watch_type"]
        target = w["target"]
        wid = w["watch_id"]
        threshold = w["alert_threshold"] or 5
        last_checked = w["last_checked"]

        params_raw = w["parameters"]
        try:
            params = json.loads(params_raw) if params_raw else None
        except (json.JSONDecodeError, TypeError):
            params = None

        finder = _FINDERS.get(wtype)
        if not finder:
            details.append({
                "watch_id": wid,
                "status": "skipped",
                "reason": f"Unknown watch_type '{wtype}'",
            })
            continue

        try:
            new_pubs = finder(read_conn, target, last_checked, params)
        except sqlite3.OperationalError as exc:
            _log.warning("Monitoring query failed for watch %s: %s", wid, exc)
            details.append({
                "watch_id": wid,
                "status": "error",
                "reason": str(exc),
            })
            continue

        watches_checked += 1
        now = _now_iso()
        count = len(new_pubs)

        # Build a human-readable summary
        summary_parts = [
            f"{count} new patent(s) found for {wtype}='{target}'",
        ]
        if last_checked:
            summary_parts.append(f"since {last_checked}")
        summary = "; ".join(summary_parts)

        detail: dict[str, Any] = {
            "watch_id": wid,
            "watch_type": wtype,
            "target": target,
            "new_patents_found": count,
            "threshold": threshold,
            "alert_created": False,
        }

        # Create alert if above threshold
        if count >= threshold:
            alert_id = _make_alert_id(wid, now)

            # Truncate stored list to avoid bloating the DB
            stored_ids = new_pubs[:_PATENT_RESULT_LIMIT]

            try:
                wconn.execute(
                    """INSERT OR IGNORE INTO monitoring_alerts
                       (alert_id, watch_id, detected_at, new_patents_count,
                        patent_ids, summary, acknowledged)
                       VALUES (?, ?, ?, ?, ?, ?, 0)""",
                    (
                        alert_id,
                        wid,
                        now,
                        count,
                        json.dumps(stored_ids, ensure_ascii=False),
                        summary,
                    ),
                )
                alerts_created += 1
                detail["alert_created"] = True
                detail["alert_id"] = alert_id
            except sqlite3.OperationalError as exc:
                _log.warning("Failed to insert alert for watch %s: %s", wid, exc)
                detail["alert_error"] = str(exc)

        # Update last_checked regardless of whether alert was created
        try:
            wconn.execute(
                "UPDATE monitoring_watches SET last_checked = ? WHERE watch_id = ?",
                (now, wid),
            )
        except sqlite3.OperationalError as exc:
            _log.warning("Failed to update last_checked for watch %s: %s", wid, exc)

        # Include a sample of patent IDs in the detail for immediate visibility
        detail["sample_patents"] = new_pubs[:10]
        detail["summary"] = summary
        details.append(detail)

    wconn.close()

    return {
        "endpoint": "run_monitoring",
        "watches_checked": watches_checked,
        "alerts_created": alerts_created,
        "details": details,
    }
