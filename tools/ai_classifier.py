"""Custom AI-powered patent classification tool.

Provides functions to create custom technology categories, classify patents
into them, analyze category landscapes, and benchmark firm positions within
categories.

Tables used (created externally):
    custom_categories     — category definitions with CPC/keyword patterns
    patent_category_mapping — patent-to-category assignments with confidence
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from typing import Any

from db.sqlite_store import PatentStore, _sanitize_fts5
import time as _time
from entity.resolver import EntityResolver
from tools.pagination import paginate

_DB_PATH = "/app/data/patents.db"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_conn() -> sqlite3.Connection:
    """Open a separate connection for writes (WAL allows concurrent reads).

    Uses isolation_level=None (autocommit) to avoid Python's implicit
    transaction management interfering with writes.
    """
    conn = sqlite3.connect(_DB_PATH, timeout=120, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def _slugify(name: str) -> str:
    """Convert a category name to a slug with an 8-char hash suffix."""
    slug = name.strip().lower()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s-]+", "_", slug).strip("_")
    # Truncate slug body so total length stays reasonable
    slug = slug[:60]
    hash_suffix = hashlib.sha256(name.encode("utf-8")).hexdigest()[:8]
    return f"{slug}_{hash_suffix}"


def _ensure_tables(conn: sqlite3.Connection) -> None:
    """Create custom_categories and patent_category_mapping if missing."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS custom_categories (
            category_id TEXT PRIMARY KEY,
            category_name TEXT NOT NULL,
            description TEXT,
            cpc_patterns TEXT,
            keyword_patterns TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            patent_count INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS patent_category_mapping (
            publication_number TEXT NOT NULL,
            category_id TEXT NOT NULL,
            confidence REAL DEFAULT 1.0,
            method TEXT DEFAULT 'rule',
            PRIMARY KEY (publication_number, category_id)
        )
    """)
    # Index may already exist; ignore error.
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pcm_category "
            "ON patent_category_mapping(category_id)"
        )
    except sqlite3.OperationalError:
        pass


def _fetch_title(conn: sqlite3.Connection, pub: str) -> str:
    """Return the best available title for a publication number."""
    try:
        row = conn.execute(
            "SELECT title_ja, title_en FROM patents "
            "WHERE publication_number = ?",
            (pub,),
        ).fetchone()
        if row:
            return row["title_en"] or row["title_ja"] or ""
    except sqlite3.OperationalError:
        pass
    return ""


def _batch_fetch_titles(
    conn: sqlite3.Connection, pubs: list[str]
) -> dict[str, str]:
    """Batch-fetch titles for a list of publication numbers."""
    title_map: dict[str, str] = {}
    for i in range(0, len(pubs), 500):
        batch = pubs[i : i + 500]
        ph = ",".join("?" * len(batch))
        try:
            rows = conn.execute(
                f"SELECT publication_number, title_ja, title_en "
                f"FROM patents WHERE publication_number IN ({ph})",
                batch,
            ).fetchall()
            for r in rows:
                title_map[r["publication_number"]] = (
                    r["title_en"] or r["title_ja"] or ""
                )
        except sqlite3.OperationalError:
            pass
    return title_map


def _load_category(
    conn: sqlite3.Connection, category_id: str
) -> dict[str, Any] | None:
    """Load a category row as a dict, or return None."""
    try:
        row = conn.execute(
            "SELECT * FROM custom_categories WHERE category_id = ?",
            (category_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    return dict(row)


def _parse_json_list(raw: str | None) -> list[str]:
    """Safely parse a JSON array stored as TEXT."""
    if not raw:
        return []
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


# ---------------------------------------------------------------------------
# Tool 1: create_category
# ---------------------------------------------------------------------------

def create_category(
    store: PatentStore,
    category_name: str | None = None,
    description: str | None = None,
    cpc_patterns: list[str] | None = None,
    keywords: list[str] | None = None,
) -> dict[str, Any]:
    """Create a custom technology category and auto-classify an initial batch.

    Args:
        store: PatentStore instance.
        category_name: Human-readable category name (required).
        description: Free-text description of the category.
        cpc_patterns: List of CPC prefixes (e.g. ["G06N", "G06F17"]).
        keywords: List of keywords in Japanese or English for FTS5 search.

    Returns:
        Dict with category metadata and sample classified patents.
    """
    if not category_name:
        return {
            "endpoint": "create_category",
            "error": "category_name is required.",
        }

    cpc_patterns = cpc_patterns or []
    keywords = keywords or []

    if not cpc_patterns and not keywords:
        return {
            "endpoint": "create_category",
            "error": "At least one of cpc_patterns or keywords must be provided.",
        }

    category_id = _slugify(category_name)
    conn = store._conn()
    _ensure_tables(conn)

    # Check for duplicate category_id
    try:
        existing = conn.execute(
            "SELECT category_id FROM custom_categories WHERE category_id = ?",
            (category_id,),
        ).fetchone()
        if existing:
            return {
                "endpoint": "create_category",
                "error": f"Category '{category_id}' already exists. Choose a different name.",
                "category_id": category_id,
            }
    except sqlite3.OperationalError:
        pass

    # Validate CPC patterns: check each has matches in patent_cpc
    valid_cpc: list[str] = []
    for pat in cpc_patterns:
        pat = pat.strip().upper()
        if not pat:
            continue
        try:
            check = conn.execute(
                "SELECT 1 FROM patent_cpc WHERE cpc_code LIKE ? || '%' LIMIT 1",
                (pat,),
            ).fetchone()
            if check:
                valid_cpc.append(pat)
        except sqlite3.OperationalError:
            # If query times out, still accept the pattern
            valid_cpc.append(pat)

    if not valid_cpc and not keywords:
        return {
            "endpoint": "create_category",
            "error": "None of the provided CPC patterns matched any patents. "
                     "Provide valid CPC prefixes or add keywords.",
            "cpc_patterns_tried": cpc_patterns,
        }

    # Insert category record (use separate write connection for WAL safety)
    cpc_json = json.dumps(valid_cpc)
    kw_json = json.dumps(keywords)

    try:
        wconn = _write_conn()
        _ensure_tables(wconn)
        wconn.execute(
            "INSERT INTO custom_categories "
            "(category_id, category_name, description, cpc_patterns, keyword_patterns) "
            "VALUES (?, ?, ?, ?, ?)",
            (category_id, category_name, description or "", cpc_json, kw_json),
        )
        # autocommit mode — no explicit commit needed
        wconn.close()
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as exc:
        return {
            "endpoint": "create_category",
            "error": f"Failed to insert category: {exc}",
        }

    # ------------------------------------------------------------------
    # Auto-classify initial batch
    # ------------------------------------------------------------------
    # Collect (publication_number, confidence, method) tuples.
    cpc_matches: dict[str, float] = {}   # pub -> best CPC confidence
    kw_matches: set[str] = set()

    # CPC-based matching
    for pat in valid_cpc:
        pat_len = len(pat)
        conf = 0.9 if pat_len >= 4 else 0.7
        try:
            rows = conn.execute(
                "SELECT DISTINCT publication_number FROM patent_cpc "
                "WHERE cpc_code LIKE ? || '%' LIMIT 5000",
                (pat,),
            ).fetchall()
            for r in rows:
                pub = r["publication_number"]
                if pub not in cpc_matches or cpc_matches[pub] < conf:
                    cpc_matches[pub] = conf
        except sqlite3.OperationalError:
            pass

    # Keyword-based matching via FTS5 (fallback to LIKE if FTS is corrupt)
    for kw in keywords:
        safe_kw = _sanitize_fts5(kw)
        if not safe_kw:
            continue
        try:
            rows = conn.execute(
                "SELECT publication_number FROM patents_fts "
                "WHERE patents_fts MATCH ? LIMIT 2000",
                (safe_kw,),
            ).fetchall()
            for r in rows:
                kw_matches.add(r["publication_number"])
        except (sqlite3.OperationalError, sqlite3.DatabaseError):
            # FTS5 may be malformed; fall back to LIKE on patents table
            try:
                rows = conn.execute(
                    "SELECT publication_number FROM patents "
                    "WHERE title_ja LIKE ? OR title_en LIKE ? LIMIT 500",
                    (f"%{kw}%", f"%{kw}%"),
                ).fetchall()
                for r in rows:
                    kw_matches.add(r["publication_number"])
            except (sqlite3.OperationalError, sqlite3.DatabaseError):
                pass

    # Merge and compute final confidence
    all_pubs: dict[str, tuple[float, str]] = {}  # pub -> (confidence, method)
    for pub, cpc_conf in cpc_matches.items():
        if pub in kw_matches:
            all_pubs[pub] = (0.95, "rule_cpc+keyword")
        else:
            all_pubs[pub] = (cpc_conf, "rule_cpc")

    for pub in kw_matches:
        if pub not in all_pubs:
            all_pubs[pub] = (0.6, "rule_keyword")

    # Batch insert into patent_category_mapping (separate write connection)
    batch_size = 500
    pub_list = list(all_pubs.items())
    try:
        wconn = _write_conn()
        _ensure_tables(wconn)
        wconn.execute("BEGIN")
        for i in range(0, len(pub_list), batch_size):
            batch = pub_list[i : i + batch_size]
            wconn.executemany(
                "INSERT OR REPLACE INTO patent_category_mapping "
                "(publication_number, category_id, confidence, method) "
                "VALUES (?, ?, ?, ?)",
                [
                    (pub, category_id, round(conf, 4), method)
                    for pub, (conf, method) in batch
                ],
            )
        # Update patent_count
        wconn.execute(
            "UPDATE custom_categories SET patent_count = ? WHERE category_id = ?",
            (len(all_pubs), category_id),
        )
        wconn.execute("COMMIT")
        wconn.close()
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as exc:
        try:
            wconn.execute("ROLLBACK")
            wconn.close()
        except Exception:
            pass
        return {
            "endpoint": "create_category",
            "error": f"Failed to insert patent mappings: {exc}",
            "category_id": category_id,
        }

    # Build sample patents (top 10 by confidence)
    sorted_pubs = sorted(all_pubs.items(), key=lambda x: -x[1][0])[:10]
    sample_pub_nums = [p[0] for p in sorted_pubs]
    title_map = _batch_fetch_titles(conn, sample_pub_nums)

    sample_patents = [
        {
            "publication_number": pub,
            "title": title_map.get(pub, ""),
            "confidence": round(conf, 4),
            "method": method,
        }
        for pub, (conf, method) in sorted_pubs
    ]

    return {
        "endpoint": "create_category",
        "category_id": category_id,
        "category_name": category_name,
        "description": description or "",
        "cpc_patterns": valid_cpc,
        "keywords": keywords,
        "initial_patent_count": len(all_pubs),
        "sample_patents": sample_patents,
        "note": "Category created. Use classify_patents to expand classification.",
    }


# ---------------------------------------------------------------------------
# Tool 2: classify_patents
# ---------------------------------------------------------------------------

def classify_patents(
    store: PatentStore,
    category_id: str | None = None,
    query: str | None = None,
    max_results: int = 100,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """Classify more patents into a category, or list existing classifications.

    If category_id given without query: return existing classifications
    for that category (paginated).

    If category_id AND query given: classify new patents matching query
    into the category and return results.

    Args:
        store: PatentStore instance.
        category_id: Existing category ID (required).
        query: Free-text query to find patents to classify.
        max_results: Maximum patents to classify in one call.
        page: Page number (1-based).
        page_size: Results per page.

    Returns:
        Dict with classification results or existing mappings.
    """
    if not category_id:
        return {
            "endpoint": "classify_patents",
            "error": "category_id is required.",
        }

    conn = store._conn()
    _ensure_tables(conn)

    cat = _load_category(conn, category_id)
    if cat is None:
        return {
            "endpoint": "classify_patents",
            "error": f"Category '{category_id}' not found.",
        }

    category_name = cat["category_name"]
    cat_cpc = _parse_json_list(cat.get("cpc_patterns"))
    cat_keywords = _parse_json_list(cat.get("keyword_patterns"))

    page = max(int(page), 1)
    page_size = min(max(int(page_size), 1), 100)
    max_results = max(1, min(int(max_results), 5000))

    # ------------------------------------------------------------------
    # Mode: LIST existing classifications
    # ------------------------------------------------------------------
    if not query:
        try:
            total_row = conn.execute(
                "SELECT COUNT(*) as cnt FROM patent_category_mapping "
                "WHERE category_id = ?",
                (category_id,),
            ).fetchone()
            total = total_row["cnt"] if total_row else 0
        except sqlite3.OperationalError:
            total = 0

        offset = (page - 1) * page_size
        try:
            rows = conn.execute(
                "SELECT publication_number, confidence, method "
                "FROM patent_category_mapping "
                "WHERE category_id = ? "
                "ORDER BY confidence DESC "
                "LIMIT ? OFFSET ?",
                (category_id, page_size, offset),
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []

        pub_nums = [r["publication_number"] for r in rows]
        title_map = _batch_fetch_titles(conn, pub_nums)

        results = [
            {
                "publication_number": r["publication_number"],
                "title": title_map.get(r["publication_number"], ""),
                "confidence": round(r["confidence"], 4),
                "method": r["method"],
            }
            for r in rows
        ]

        # Confidence distribution
        high = mid = low = 0
        try:
            for bucket in conn.execute(
                "SELECT "
                "  SUM(CASE WHEN confidence > 0.8 THEN 1 ELSE 0 END) AS high, "
                "  SUM(CASE WHEN confidence BETWEEN 0.5 AND 0.8 THEN 1 ELSE 0 END) AS mid, "
                "  SUM(CASE WHEN confidence < 0.5 THEN 1 ELSE 0 END) AS low "
                "FROM patent_category_mapping WHERE category_id = ?",
                (category_id,),
            ):
                high = bucket["high"] or 0
                mid = bucket["mid"] or 0
                low = bucket["low"] or 0
        except sqlite3.OperationalError:
            pass

        import math
        pages = math.ceil(total / page_size) if total > 0 else 1

        return {
            "endpoint": "classify_patents",
            "category_id": category_id,
            "category_name": category_name,
            "mode": "list",
            "newly_classified": 0,
            "confidence_distribution": {
                "high": int(high),
                "medium": int(mid),
                "low": int(low),
            },
            "total": total,
            "page": page,
            "pages": pages,
            "results": results,
        }

    # ------------------------------------------------------------------
    # Mode: QUERY — classify new patents matching query
    # ------------------------------------------------------------------
    candidate_pubs: set[str] = set()

    # Search via FTS5 (fallback to LIKE if FTS is corrupt)
    safe_q = _sanitize_fts5(query)
    if safe_q:
        try:
            rows = conn.execute(
                "SELECT publication_number FROM patents_fts "
                "WHERE patents_fts MATCH ? LIMIT ?",
                (safe_q, max_results),
            ).fetchall()
            for r in rows:
                candidate_pubs.add(r["publication_number"])
        except (sqlite3.OperationalError, sqlite3.DatabaseError):
            # FTS5 may be malformed; fall back to LIKE
            try:
                rows = conn.execute(
                    "SELECT publication_number FROM patents "
                    "WHERE title_ja LIKE ? OR title_en LIKE ? LIMIT ?",
                    (f"%{query}%", f"%{query}%", max_results),
                ).fetchall()
                for r in rows:
                    candidate_pubs.add(r["publication_number"])
            except (sqlite3.OperationalError, sqlite3.DatabaseError):
                pass

    # Also try CPC-based search if query looks like a CPC code
    query_upper = query.strip().upper()
    if re.match(r"^[A-H]\d{2}[A-Z]?\d*", query_upper):
        try:
            rows = conn.execute(
                "SELECT DISTINCT publication_number FROM patent_cpc "
                "WHERE cpc_code LIKE ? || '%' LIMIT ?",
                (query_upper, max_results),
            ).fetchall()
            for r in rows:
                candidate_pubs.add(r["publication_number"])
        except sqlite3.OperationalError:
            pass

    if not candidate_pubs:
        return {
            "endpoint": "classify_patents",
            "category_id": category_id,
            "category_name": category_name,
            "mode": "query",
            "newly_classified": 0,
            "confidence_distribution": {"high": 0, "medium": 0, "low": 0},
            "total": 0,
            "page": 1,
            "pages": 1,
            "results": [],
            "note": "No patents found matching the query.",
        }

    # Compute confidence for each candidate
    # Pre-fetch CPC codes for candidates in batches
    candidate_list = list(candidate_pubs)[:max_results]
    cpc_by_pub: dict[str, set[str]] = {}
    for i in range(0, len(candidate_list), 500):
        batch = candidate_list[i : i + 500]
        ph = ",".join("?" * len(batch))
        try:
            rows = conn.execute(
                f"SELECT publication_number, cpc_code FROM patent_cpc "
                f"WHERE publication_number IN ({ph})",
                batch,
            ).fetchall()
            for r in rows:
                cpc_by_pub.setdefault(r["publication_number"], set()).add(
                    r["cpc_code"]
                )
        except sqlite3.OperationalError:
            pass

    # Pre-fetch titles for keyword matching
    title_map = _batch_fetch_titles(conn, candidate_list)

    # Build keyword pattern set (lowered for matching)
    kw_lower = {kw.lower() for kw in cat_keywords if kw}

    classified: list[tuple[str, float, str]] = []  # (pub, confidence, method)

    for pub in candidate_list:
        pub_cpc_codes = cpc_by_pub.get(pub, set())
        # CPC overlap score
        cpc_score = 0.0
        if cat_cpc:
            matching = sum(
                1
                for code in pub_cpc_codes
                for pat in cat_cpc
                if code.upper().startswith(pat)
            )
            cpc_score = min(matching / len(cat_cpc), 1.0)

        # Keyword score
        kw_score = 0.0
        if kw_lower:
            title = title_map.get(pub, "").lower()
            matched_kw = sum(1 for kw in kw_lower if kw in title)
            kw_score = min(matched_kw / len(kw_lower), 1.0)

        # Combined confidence
        if cat_cpc and kw_lower:
            confidence = round(0.6 * cpc_score + 0.4 * kw_score, 4)
        elif cat_cpc:
            confidence = round(cpc_score, 4)
        else:
            confidence = round(kw_score, 4)

        # Determine method
        if cpc_score > 0 and kw_score > 0:
            method = "rule_cpc+keyword"
        elif cpc_score > 0:
            method = "rule_cpc"
        elif kw_score > 0:
            method = "rule_keyword"
        else:
            method = "rule"

        # Only classify if confidence > 0
        if confidence > 0:
            classified.append((pub, confidence, method))

    # Batch insert (separate write connection, autocommit mode)
    newly_classified = 0
    if classified:
        try:
            wconn = _write_conn()
            _ensure_tables(wconn)
            wconn.execute("BEGIN")
            wconn.executemany(
                "INSERT OR REPLACE INTO patent_category_mapping "
                "(publication_number, category_id, confidence, method) "
                "VALUES (?, ?, ?, ?)",
                [
                    (pub, category_id, conf, method)
                    for pub, conf, method in classified
                ],
            )
            newly_classified = len(classified)
            # Update patent_count
            count_row = wconn.execute(
                "SELECT COUNT(*) as cnt FROM patent_category_mapping "
                "WHERE category_id = ?",
                (category_id,),
            ).fetchone()
            new_count = count_row["cnt"] if count_row else 0
            wconn.execute(
                "UPDATE custom_categories SET patent_count = ? "
                "WHERE category_id = ?",
                (new_count, category_id),
            )
            wconn.execute("COMMIT")
            wconn.close()
        except (sqlite3.OperationalError, sqlite3.DatabaseError) as exc:
            try:
                wconn.execute("ROLLBACK")
                wconn.close()
            except Exception:
                pass
            return {
                "endpoint": "classify_patents",
                "error": f"Failed to insert classifications: {exc}",
                "category_id": category_id,
            }

    # Build paginated results from classified
    classified.sort(key=lambda x: -x[1])
    paged = paginate(
        [
            {
                "publication_number": pub,
                "title": title_map.get(pub, ""),
                "confidence": round(conf, 4),
                "method": method,
            }
            for pub, conf, method in classified
        ],
        page=page,
        page_size=page_size,
    )

    # Confidence distribution
    high = sum(1 for _, c, _ in classified if c > 0.8)
    mid = sum(1 for _, c, _ in classified if 0.5 <= c <= 0.8)
    low = sum(1 for _, c, _ in classified if c < 0.5)

    return {
        "endpoint": "classify_patents",
        "category_id": category_id,
        "category_name": category_name,
        "mode": "query",
        "newly_classified": newly_classified,
        "confidence_distribution": {
            "high": high,
            "medium": mid,
            "low": low,
        },
        "total": paged["total"],
        "page": paged["page"],
        "pages": paged["pages"],
        "results": paged["results"],
    }


# ---------------------------------------------------------------------------
# Tool 3: category_landscape
# ---------------------------------------------------------------------------

def category_landscape(
    store: PatentStore,
    category_id: str | None = None,
) -> dict[str, Any]:
    """Landscape analysis for a custom category's patents.

    Aggregates filing timeline, top applicants, sub-CPC areas, and top
    cited patents for all patents classified under the given category.

    Args:
        store: PatentStore instance.
        category_id: Category ID to analyze (required).

    Returns:
        Dict with timeline, top applicants, sub-areas, and top cited patents.
    """
    if not category_id:
        return {
            "endpoint": "category_landscape",
            "error": "category_id is required.",
        }

    conn = store._conn()
    _ensure_tables(conn)

    cat = _load_category(conn, category_id)
    if cat is None:
        return {
            "endpoint": "category_landscape",
            "error": f"Category '{category_id}' not found.",
        }

    category_name = cat["category_name"]

    # Get total patent count
    try:
        total_row = conn.execute(
            "SELECT COUNT(*) as cnt FROM patent_category_mapping "
            "WHERE category_id = ?",
            (category_id,),
        ).fetchone()
        total_patents = total_row["cnt"] if total_row else 0
    except sqlite3.OperationalError:
        total_patents = 0

    if total_patents == 0:
        return {
            "endpoint": "category_landscape",
            "category_id": category_id,
            "category_name": category_name,
            "total_patents": 0,
            "timeline": [],
            "top_applicants": [],
            "sub_areas": [],
            "top_cited_patents": [],
            "growth_assessment": "insufficient_data",
        }

    # ------------------------------------------------------------------
    # Timeline: patents by filing year
    # ------------------------------------------------------------------
    timeline: list[dict[str, Any]] = []
    try:
        rows = conn.execute(
            """
            SELECT CAST(p.filing_date / 10000 AS INTEGER) AS year,
                   COUNT(*) AS patent_count
            FROM patent_category_mapping m
            JOIN patents p ON m.publication_number = p.publication_number
            WHERE m.category_id = ?
              AND p.filing_date > 0
            GROUP BY year
            ORDER BY year
            """,
            (category_id,),
        ).fetchall()
        for r in rows:
            yr = r["year"]
            if yr and 1970 <= yr <= 2030:
                timeline.append({
                    "year": yr,
                    "patent_count": r["patent_count"],
                    "growth_rate": 0.0,
                })
    except sqlite3.OperationalError:
        pass

    # Compute year-over-year growth rates
    for i in range(1, len(timeline)):
        prev = timeline[i - 1]["patent_count"]
        curr = timeline[i]["patent_count"]
        if prev > 0:
            timeline[i]["growth_rate"] = round((curr - prev) / prev, 4)
        else:
            timeline[i]["growth_rate"] = 0.0

    # ------------------------------------------------------------------
    # Top applicants
    # ------------------------------------------------------------------
    top_applicants: list[dict[str, Any]] = []
    try:
        rows = conn.execute(
            """
            SELECT COALESCE(a.firm_id, a.harmonized_name) AS group_key,
                   COALESCE(a.harmonized_name, a.raw_name) AS name,
                   a.firm_id,
                   COUNT(DISTINCT m.publication_number) AS patent_count
            FROM patent_category_mapping m
            JOIN patent_assignees a ON m.publication_number = a.publication_number
            WHERE m.category_id = ?
            GROUP BY group_key
            ORDER BY patent_count DESC
            LIMIT 20
            """,
            (category_id,),
        ).fetchall()
        for r in rows:
            share = round(r["patent_count"] / total_patents, 4) if total_patents > 0 else 0.0
            top_applicants.append({
                "name": r["name"],
                "firm_id": r["firm_id"] or "",
                "patent_count": r["patent_count"],
                "share": share,
            })
    except sqlite3.OperationalError:
        pass

    # ------------------------------------------------------------------
    # Sub-CPC areas
    # ------------------------------------------------------------------
    sub_areas: list[dict[str, Any]] = []
    try:
        rows = conn.execute(
            """
            SELECT SUBSTR(c.cpc_code, 1, 4) AS cpc_class,
                   COUNT(DISTINCT m.publication_number) AS patent_count
            FROM patent_category_mapping m
            JOIN patent_cpc c ON m.publication_number = c.publication_number
            WHERE m.category_id = ?
            GROUP BY cpc_class
            ORDER BY patent_count DESC
            LIMIT 20
            """,
            (category_id,),
        ).fetchall()
        for r in rows:
            share = round(r["patent_count"] / total_patents, 4) if total_patents > 0 else 0.0
            sub_areas.append({
                "cpc_class": r["cpc_class"],
                "patent_count": r["patent_count"],
                "share": share,
            })
    except sqlite3.OperationalError:
        pass

    # ------------------------------------------------------------------
    # Top cited patents
    # ------------------------------------------------------------------
    top_cited: list[dict[str, Any]] = []
    try:
        rows = conn.execute(
            """
            SELECT m.publication_number,
                   COALESCE(cc.forward_citations, 0) AS forward_citations
            FROM patent_category_mapping m
            LEFT JOIN citation_counts cc
              ON m.publication_number = cc.publication_number
            WHERE m.category_id = ?
            ORDER BY forward_citations DESC
            LIMIT 10
            """,
            (category_id,),
        ).fetchall()
        pub_nums = [r["publication_number"] for r in rows]
        title_map = _batch_fetch_titles(conn, pub_nums)
        for r in rows:
            top_cited.append({
                "publication_number": r["publication_number"],
                "title": title_map.get(r["publication_number"], ""),
                "forward_citations": r["forward_citations"],
            })
    except sqlite3.OperationalError:
        pass

    # ------------------------------------------------------------------
    # Growth assessment
    # ------------------------------------------------------------------
    growth_assessment = "insufficient_data"
    if len(timeline) >= 3:
        recent = timeline[-1]["patent_count"]
        mid_idx = len(timeline) // 2
        mid = timeline[mid_idx]["patent_count"]
        if mid > 0:
            overall_growth = (recent - mid) / mid
            if overall_growth > 0.2:
                growth_assessment = "growing"
            elif overall_growth < -0.2:
                growth_assessment = "declining"
            else:
                growth_assessment = "stable"

    return {
        "endpoint": "category_landscape",
        "category_id": category_id,
        "category_name": category_name,
        "total_patents": total_patents,
        "timeline": timeline,
        "top_applicants": top_applicants,
        "sub_areas": sub_areas,
        "top_cited_patents": top_cited,
        "growth_assessment": growth_assessment,
    }


# ---------------------------------------------------------------------------
# Tool 4: portfolio_benchmark
# ---------------------------------------------------------------------------

def portfolio_benchmark(
    store: PatentStore,
    firm_query: str | None = None,
    category_id: str | None = None,
    resolver: EntityResolver | None = None,
) -> dict[str, Any]:
    """Benchmark a firm's position within a custom category vs peers.

    Args:
        store: PatentStore instance.
        firm_query: Company name, ticker, or Japanese name.
        category_id: Category ID to benchmark against (required).
        resolver: EntityResolver for firm name resolution.

    Returns:
        Dict with firm metrics, peer ranking, gap analysis, and recommendations.
    """
    if not firm_query:
        return {
            "endpoint": "portfolio_benchmark",
            "error": "firm_query is required.",
        }
    if not category_id:
        return {
            "endpoint": "portfolio_benchmark",
            "error": "category_id is required.",
        }
    if resolver is None:
        return {
            "endpoint": "portfolio_benchmark",
            "error": "resolver is required for firm name resolution.",
        }

    conn = store._conn()
    _ensure_tables(conn)

    cat = _load_category(conn, category_id)
    if cat is None:
        return {
            "endpoint": "portfolio_benchmark",
            "error": f"Category '{category_id}' not found.",
        }

    category_name = cat["category_name"]

    # Resolve firm
    resolved = resolver.resolve(firm_query, country_hint="JP")
    if resolved is None:
        return {
            "endpoint": "portfolio_benchmark",
            "error": f"Could not resolve firm: '{firm_query}'",
            "suggestion": "Try the exact company name, Japanese name, or stock ticker",
        }

    firm_id = resolved.entity.canonical_id
    firm_name = resolved.entity.canonical_name

    # ------------------------------------------------------------------
    # Count firm's patents in category
    # ------------------------------------------------------------------
    firm_count = 0
    avg_confidence = 0.0
    try:
        row = conn.execute(
            """
            SELECT COUNT(DISTINCT m.publication_number) AS cnt,
                   AVG(m.confidence) AS avg_conf
            FROM patent_category_mapping m
            JOIN patent_assignees a ON m.publication_number = a.publication_number
            WHERE m.category_id = ?
              AND a.firm_id = ?
            """,
            (category_id, firm_id),
        ).fetchone()
        if row:
            firm_count = row["cnt"] or 0
            avg_confidence = round(row["avg_conf"] or 0.0, 4)
    except sqlite3.OperationalError:
        pass

    # ------------------------------------------------------------------
    # All firms' patent counts in this category (peer ranking)
    # ------------------------------------------------------------------
    all_firms: list[dict[str, Any]] = []
    total_in_category = 0
    try:
        rows = conn.execute(
            """
            SELECT a.firm_id,
                   COALESCE(a.harmonized_name, a.raw_name) AS name,
                   COUNT(DISTINCT m.publication_number) AS patent_count
            FROM patent_category_mapping m
            JOIN patent_assignees a ON m.publication_number = a.publication_number
            WHERE m.category_id = ?
              AND a.firm_id IS NOT NULL
              AND a.firm_id != ''
            GROUP BY a.firm_id
            ORDER BY patent_count DESC
            LIMIT 50
            """,
            (category_id,),
        ).fetchall()
        for r in rows:
            all_firms.append({
                "firm_id": r["firm_id"],
                "firm_name": r["name"],
                "patent_count": r["patent_count"],
            })
            total_in_category += r["patent_count"]
    except sqlite3.OperationalError:
        pass

    # Compute shares
    for f in all_firms:
        f["share"] = round(f["patent_count"] / total_in_category, 4) if total_in_category > 0 else 0.0

    total_firms = len(all_firms)
    firm_market_share = round(firm_count / total_in_category, 4) if total_in_category > 0 else 0.0

    # Find firm's rank
    firm_rank = 0
    for idx, f in enumerate(all_firms, start=1):
        if f["firm_id"] == firm_id:
            firm_rank = idx
            break

    # If firm not found in top 50, check further
    if firm_rank == 0 and firm_count > 0:
        try:
            rank_row = conn.execute(
                """
                SELECT COUNT(*) + 1 AS rnk
                FROM (
                    SELECT a.firm_id,
                           COUNT(DISTINCT m.publication_number) AS cnt
                    FROM patent_category_mapping m
                    JOIN patent_assignees a ON m.publication_number = a.publication_number
                    WHERE m.category_id = ?
                      AND a.firm_id IS NOT NULL
                      AND a.firm_id != ''
                    GROUP BY a.firm_id
                    HAVING cnt > ?
                )
                """,
                (category_id, firm_count),
            ).fetchone()
            firm_rank = rank_row["rnk"] if rank_row else 0
        except sqlite3.OperationalError:
            firm_rank = 0

    # ------------------------------------------------------------------
    # Gap analysis
    # ------------------------------------------------------------------
    leader_name = ""
    leader_count = 0
    if all_firms:
        leader_name = all_firms[0]["firm_name"]
        leader_count = all_firms[0]["patent_count"]

    gap_to_leader = leader_count - firm_count
    percentile = 0.0
    if total_firms > 0 and firm_rank > 0:
        percentile = round((1 - (firm_rank - 1) / total_firms) * 100, 4)

    # ------------------------------------------------------------------
    # Recommendations
    # ------------------------------------------------------------------
    recommendations: list[str] = []
    if firm_count == 0:
        recommendations.append(
            f"No patents found for {firm_name} in this category. "
            "Consider filing in related CPC areas or acquiring IP."
        )
    elif firm_rank == 1:
        recommendations.append(
            f"{firm_name} leads this category. Maintain position with "
            "continued filing and monitor emerging entrants."
        )
    else:
        if gap_to_leader > 0:
            recommendations.append(
                f"Gap to leader ({leader_name}): {gap_to_leader} patents. "
                "Consider targeted R&D or licensing to close the gap."
            )
        if percentile < 50:
            recommendations.append(
                "Below median position. Evaluate whether strategic "
                "investment or partnership could improve standing."
            )
        if avg_confidence < 0.7:
            recommendations.append(
                f"Average classification confidence is {avg_confidence:.2f}. "
                "Some patents may be loosely related. Review for precision."
            )

    # Peer ranking (top 20 for response)
    peer_ranking = all_firms[:20]

    return {
        "endpoint": "portfolio_benchmark",
        "firm_name": firm_name,
        "firm_id": firm_id,
        "category_id": category_id,
        "category_name": category_name,
        "firm_metrics": {
            "patent_count": firm_count,
            "market_share": firm_market_share,
            "rank": firm_rank,
            "total_firms": total_firms,
            "avg_confidence": avg_confidence,
        },
        "peer_ranking": peer_ranking,
        "gap_analysis": {
            "leader": leader_name,
            "leader_count": leader_count,
            "gap_to_leader": gap_to_leader,
            "percentile": percentile,
        },
        "recommendations": recommendations,
    }
