"""SQLite patent metadata store.

v3: Added query timeout via set_progress_handler to prevent
    long-running queries from blocking the MCP server.
"""
from __future__ import annotations

import logging
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from .migrations import SCHEMA_SQL

_log = logging.getLogger(__name__)

# Maximum seconds a single query can run before being interrupted.
# Kept low (10s) to prevent blocking the single-threaded ASGI server.
# Pre-warm and admin queries use _disable_timeout() to bypass this.
_QUERY_TIMEOUT = 120  # Match _safe_call hard deadline; NVMe handles large scans well


def _sanitize_fts5(query: str) -> str | None:
    """Escape FTS5 special characters for safe MATCH queries.

    Returns None if the query is empty or all words are shorter than 3 chars
    (trigram tokenizer cannot match them).
    """
    words = re.split(r"\s+", query.strip())
    safe_words = []
    for w in words:
        if w.upper() in ("AND", "OR", "NOT", "NEAR"):
            continue
        # Keep word chars + CJK ranges
        w = re.sub(r"[^\w\u3000-\u9fff\uff00-\uffef-]", "", w)
        if w and len(w) >= 3:  # Trigram minimum
            safe_words.append(f'"{w}"')
    return " ".join(safe_words) if safe_words else None


def _has_short_words(query: str) -> bool:
    """Check if query has words shorter than 3 chars (not searchable via trigram FTS)."""
    words = re.split(r"\s+", query.strip())
    for w in words:
        w = re.sub(r"[^\w\u3000-\u9fff\uff00-\uffef-]", "", w)
        if w and len(w) < 3:
            return True
    return False


class QueryTimeoutError(sqlite3.OperationalError):
    """Raised when a query exceeds the timeout threshold."""
    pass


class PatentStore:
    def __init__(self, db_path: str | Path = "data/patents.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_db()

    def _init_db(self) -> None:
        self._disable_timeout()  # Schema creation can be slow
        try:
            with self._conn() as conn:
                conn.executescript(SCHEMA_SQL)
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                _log.warning(
                    "Database locked during init (concurrent writer). "
                    "Schema creation skipped — tables should already exist."
                )
            else:
                raise
        finally:
            self._reset_timeout()
            # Close and discard the init connection so queries get a fresh one.
            # executescript(SCHEMA_SQL) can leave stale FTS5 virtual table
            # metadata that causes "database disk image is malformed" errors.
            old_conn = getattr(self._local, "conn", None)
            if old_conn is not None:
                try:
                    old_conn.close()
                except Exception:
                    pass
                self._local.conn = None

    def _conn(self) -> sqlite3.Connection:
        """Return a persistent per-thread connection (PRAGMAs run once).

        Resets the query deadline each time it's called, giving each
        query batch up to _QUERY_TIMEOUT seconds. Skips reset if timeout
        is explicitly disabled via _disable_timeout().
        """
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            # Only reset deadline if timeout is not disabled and not relaxed
            if not getattr(self._local, "timeout_disabled", False):
                if not getattr(self._local, "relax_timeout", False):
                    self._local.query_start = time.monotonic()
            return conn

        conn = sqlite3.connect(
            self.db_path, timeout=60, check_same_thread=False,
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA cache_size=-2000000")   # 2 GB page cache
        conn.execute("PRAGMA mmap_size=32212254720")  # 30 GB memory-mapped I/O (NVMe)
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA read_uncommitted=ON")    # allow dirty reads for speed

        # Install query timeout handler
        local_ref = self._local
        # Preserve timeout_disabled / relax_timeout if already set
        if not getattr(local_ref, "timeout_disabled", False):
            if not getattr(local_ref, "relax_timeout", False):
                local_ref.query_start = time.monotonic()
            else:
                local_ref.query_start = None
            local_ref.timeout_disabled = False
        else:
            local_ref.query_start = None

        def _check_timeout():
            if getattr(local_ref, "timeout_disabled", False):
                return 0
            now = time.monotonic()
            # Check per-query timeout (resets with each _conn() call)
            start = getattr(local_ref, "query_start", None)
            if start is not None and (now - start) > _QUERY_TIMEOUT:
                _log.warning(
                    f"Query timeout after {_QUERY_TIMEOUT}s — interrupting"
                )
                return 1  # Non-zero = interrupt the query
            # Check hard deadline (set by _safe_call, never resets)
            hard_dl = getattr(local_ref, "hard_deadline", None)
            if hard_dl is not None and now > hard_dl:
                _log.warning("Hard deadline exceeded — interrupting")
                return 1
            return 0

        # Check every 50K virtual machine instructions (~10ms granularity)
        conn.set_progress_handler(_check_timeout, 50000)

        conn.row_factory = sqlite3.Row
        self._local.conn = conn
        return conn

    def _disable_timeout(self) -> None:
        """Disable query timeout for this thread (e.g., for pre-warm)."""
        self._local.timeout_disabled = True
        self._local.query_start = None

    def _reset_timeout(self) -> None:
        """Re-enable and reset query timeout to now + _QUERY_TIMEOUT."""
        self._local.timeout_disabled = False
        self._local.relax_timeout = False
        self._local.query_start = time.monotonic()

    def _relax_timeout(self) -> None:
        """Disable per-query timeout but keep hard_deadline active.

        When relaxed:
        - query_start is set to None (per-query timeout won't fire)
        - _conn() won't reset query_start
        - hard_deadline from _safe_call is still checked by progress handler
        """
        self._local.relax_timeout = True
        self._local.query_start = None

    def _unrelax_timeout(self) -> None:
        """Re-enable per-query timeout."""
        self._local.relax_timeout = False
        self._local.query_start = time.monotonic()

    def upsert_patent(self, patent: dict[str, Any]) -> None:
        """Insert or update a single patent record."""
        with self._conn() as conn:
            self._upsert_one(conn, patent)

    def _upsert_one(self, conn: sqlite3.Connection, p: dict[str, Any]) -> None:
        pub = p["publication_number"]

        conn.execute(
            """INSERT OR REPLACE INTO patents (
                publication_number, application_number, family_id,
                country_code, kind_code, title_ja, title_en,
                abstract_ja, abstract_en, filing_date, publication_date,
                grant_date, entity_status, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                pub,
                p.get("application_number"),
                p.get("family_id"),
                p.get("country_code", ""),
                p.get("kind_code"),
                p.get("title_ja"),
                p.get("title_en"),
                p.get("abstract_ja"),
                p.get("abstract_en"),
                p.get("filing_date"),
                p.get("publication_date"),
                p.get("grant_date"),
                p.get("entity_status"),
                p.get("source", "bigquery"),
            ),
        )

        # CPC codes
        conn.execute(
            "DELETE FROM patent_cpc WHERE publication_number = ?", (pub,)
        )
        for c in p.get("cpc_codes", []):
            if isinstance(c, dict):
                code = c.get("code", "")
                inventive = 1 if c.get("inventive") or c.get("is_inventive") else 0
                first = 1 if c.get("first") or c.get("is_first") else 0
            else:
                code = str(c)
                inventive = 0
                first = 0
            if code:
                conn.execute(
                    """INSERT OR IGNORE INTO patent_cpc
                       (publication_number, cpc_code, is_inventive, is_first)
                       VALUES (?, ?, ?, ?)""",
                    (pub, code, inventive, first),
                )

        # Assignees
        conn.execute(
            "DELETE FROM patent_assignees WHERE publication_number = ?", (pub,)
        )
        for a in p.get("applicants", []):
            if isinstance(a, dict):
                conn.execute(
                    """INSERT INTO patent_assignees
                       (publication_number, raw_name, harmonized_name,
                        country_code, firm_id)
                       VALUES (?, ?, ?, ?, ?)""",
                    (
                        pub,
                        a.get("raw_name", ""),
                        a.get("harmonized_name"),
                        a.get("country_code"),
                        a.get("firm_id"),
                    ),
                )

        # Raw assignees (JP/EN names from BigQuery 'assignee' field)
        for name in p.get("raw_assignees", []):
            if name:
                conn.execute(
                    """INSERT OR IGNORE INTO patent_assignees
                       (publication_number, raw_name, harmonized_name)
                       VALUES (?, ?, ?)""",
                    (pub, name, name),
                )

        # Inventors
        conn.execute(
            "DELETE FROM patent_inventors WHERE publication_number = ?", (pub,)
        )
        for inv in p.get("inventors", []):
            if isinstance(inv, dict):
                name = inv.get("name", "")
                cc = inv.get("country_code")
            else:
                name = str(inv)
                cc = None
            if name:
                conn.execute(
                    """INSERT INTO patent_inventors
                       (publication_number, name, country_code)
                       VALUES (?, ?, ?)""",
                    (pub, name, cc),
                )

        # Citations
        conn.execute(
            "DELETE FROM patent_citations WHERE citing_publication = ?", (pub,)
        )
        for cited in p.get("citations_backward", []):
            if cited:
                conn.execute(
                    """INSERT OR IGNORE INTO patent_citations
                       (citing_publication, cited_publication, citation_type)
                       VALUES (?, ?, ?)""",
                    (pub, cited, "patent"),
                )

    def upsert_batch(self, patents: list[dict[str, Any]]) -> int:
        """Batch insert/update patents. Returns count inserted."""
        count = 0
        with self._conn() as conn:
            for p in patents:
                try:
                    self._upsert_one(conn, p)
                    count += 1
                except Exception:
                    continue
        return count

    def search(
        self,
        query: str | None = None,
        cpc_prefix: str | None = None,
        assignee: str | None = None,
        firm_id: str | None = None,
        date_from: int | None = None,
        date_to: int | None = None,
        limit: int = 20,
        country_code: str | None = None,
    ) -> list[dict[str, Any]]:
        """Search patents using FTS5 and/or filters."""
        conditions = []
        params: list[Any] = []

        if query:
            safe_query = _sanitize_fts5(query)
            if safe_query:
                # FTS5 with DatabaseError fallback to LIKE
                try:
                    _fts_pubs = [r[0] for r in self._conn().execute(
                        "SELECT publication_number FROM patents_fts "
                        "WHERE patents_fts MATCH ? LIMIT 5000", (safe_query,)
                    ).fetchall()]
                    if _fts_pubs:
                        _ph = ",".join("?" * len(_fts_pubs))
                        conditions.append(f"p.publication_number IN ({_ph})")
                        params.extend(_fts_pubs)
                    else:
                        conditions.append("0")
                except (sqlite3.DatabaseError, sqlite3.OperationalError):
                    # FTS5 corrupt — fall back to LIKE on title
                    _words = [w.strip('"') for w in safe_query.split() if len(w.strip('"')) >= 3]
                    if _words:
                        _like_parts = []
                        for _w in _words[:4]:
                            _like_parts.append(
                                "(p.title_ja LIKE '%' || ? || '%' OR p.title_en LIKE '%' || ? || '%')"
                            )
                            params.extend([_w, _w])
                        conditions.append("(" + " AND ".join(_like_parts) + ")")
            elif _has_short_words(query):
                # Trigram can't match words < 3 chars — fall back to LIKE
                q_clean = re.sub(r"[^\w\u3000-\u9fff\uff00-\uffef]", "", query.strip())
                if q_clean:
                    conditions.append(
                        "(p.title_ja LIKE '%' || ? || '%' OR p.title_en LIKE '%' || ? || '%')"
                    )
                    params.extend([q_clean, q_clean])

        if cpc_prefix:
            conditions.append(
                "p.publication_number IN "
                "(SELECT publication_number FROM patent_cpc "
                "WHERE cpc_code LIKE ? || '%')"
            )
            params.append(cpc_prefix)

        if assignee:
            conditions.append(
                "p.publication_number IN "
                "(SELECT publication_number FROM patent_assignees "
                "WHERE harmonized_name LIKE '%' || ? || '%')"
            )
            params.append(assignee)

        if firm_id:
            conditions.append(
                "p.publication_number IN "
                "(SELECT publication_number FROM patent_assignees "
                "WHERE firm_id = ?)"
            )
            params.append(firm_id)

        if date_from:
            conditions.append("p.publication_date >= ?")
            params.append(date_from)

        if date_to:
            conditions.append("p.publication_date <= ?")
            params.append(date_to)

        if country_code:
            conditions.append("p.country_code = ?")
            params.append(country_code)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        sql = f"""
        SELECT p.publication_number, p.family_id, p.country_code,
               p.kind_code, p.title_ja, p.title_en,
               p.abstract_ja, p.abstract_en,
               p.filing_date, p.publication_date, p.grant_date,
               p.entity_status
        FROM patents p
        {where}
        ORDER BY p.publication_date DESC
        LIMIT ?
        """
        params.append(limit)

        conn = self._conn()
        try:
            rows = conn.execute(sql, params).fetchall()
        except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
            if "interrupted" in str(e):
                _log.warning(f"Search query timed out after {_QUERY_TIMEOUT}s")
                raise  # Let _safe_call handle it
            if "malformed" in str(e):
                _log.warning(f"DB corruption detected in search: {e}")
                return []
            raise

        if not rows:
            return []

        results = [dict(row) for row in rows]
        pub_numbers = [d["publication_number"] for d in results]

        # Batch fetch CPC codes
        self._reset_timeout()
        cpc_map: dict[str, list[str]] = {}
        for i in range(0, len(pub_numbers), 500):
            batch = pub_numbers[i : i + 500]
            ph = ",".join("?" * len(batch))
            try:
                for c in conn.execute(
                    f"SELECT publication_number, cpc_code FROM patent_cpc WHERE publication_number IN ({ph})",
                    batch,
                ):
                    cpc_map.setdefault(c["publication_number"], []).append(c["cpc_code"])
            except sqlite3.OperationalError:
                break

        # Batch fetch assignees
        self._reset_timeout()
        assignee_map: dict[str, list[dict]] = {}
        for i in range(0, len(pub_numbers), 500):
            batch = pub_numbers[i : i + 500]
            ph = ",".join("?" * len(batch))
            try:
                for a in conn.execute(
                    f"SELECT publication_number, harmonized_name, firm_id FROM patent_assignees WHERE publication_number IN ({ph})",
                    batch,
                ):
                    assignee_map.setdefault(a["publication_number"], []).append(
                        {"name": a["harmonized_name"], "firm_id": a["firm_id"]}
                    )
            except sqlite3.OperationalError:
                break

        for d in results:
            pub = d["publication_number"]
            d["cpc_codes"] = cpc_map.get(pub, [])
            d["assignees"] = assignee_map.get(pub, [])

        return results

    def count(
        self,
        query: str | None = None,
        cpc_prefix: str | None = None,
        assignee: str | None = None,
        date_from: int | None = None,
        date_to: int | None = None,
        country_code: str | None = None,
    ) -> int:
        """Count matching patents."""
        conditions = []
        params: list[Any] = []

        if query:
            safe_query = _sanitize_fts5(query)
            if safe_query:
                # FTS5 with DatabaseError fallback to LIKE
                try:
                    _fts_pubs = [r[0] for r in self._conn().execute(
                        "SELECT publication_number FROM patents_fts "
                        "WHERE patents_fts MATCH ? LIMIT 5000", (safe_query,)
                    ).fetchall()]
                    if _fts_pubs:
                        _ph = ",".join("?" * len(_fts_pubs))
                        conditions.append(f"p.publication_number IN ({_ph})")
                        params.extend(_fts_pubs)
                    else:
                        conditions.append("0")
                except (sqlite3.DatabaseError, sqlite3.OperationalError):
                    # FTS5 corrupt — fall back to LIKE on title
                    _words = [w.strip('"') for w in safe_query.split() if len(w.strip('"')) >= 3]
                    if _words:
                        _like_parts = []
                        for _w in _words[:4]:
                            _like_parts.append(
                                "(p.title_ja LIKE '%' || ? || '%' OR p.title_en LIKE '%' || ? || '%')"
                            )
                            params.extend([_w, _w])
                        conditions.append("(" + " AND ".join(_like_parts) + ")")
            elif _has_short_words(query):
                # LIKE-based count on 13M+ rows is too slow — return -1
                # to signal "unknown total" to the caller
                return -1

        if cpc_prefix:
            conditions.append(
                "p.publication_number IN "
                "(SELECT publication_number FROM patent_cpc "
                "WHERE cpc_code LIKE ? || '%')"
            )
            params.append(cpc_prefix)

        if assignee:
            conditions.append(
                "p.publication_number IN "
                "(SELECT publication_number FROM patent_assignees "
                "WHERE harmonized_name LIKE '%' || ? || '%')"
            )
            params.append(assignee)

        if date_from:
            conditions.append("p.publication_date >= ?")
            params.append(date_from)

        if date_to:
            conditions.append("p.publication_date <= ?")
            params.append(date_to)

        if country_code:
            conditions.append("p.country_code = ?")
            params.append(country_code)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        sql = f"SELECT COUNT(*) as cnt FROM patents p {where}"

        with self._conn() as conn:
            try:
                row = conn.execute(sql, params).fetchone()
                return row["cnt"] if row else 0
            except sqlite3.OperationalError as e:
                if "interrupted" in str(e):
                    _log.warning(f"Count query timed out after {_QUERY_TIMEOUT}s")
                    raise  # Let _safe_call handle it
                raise

    def get_patent(self, publication_number: str) -> dict[str, Any] | None:
        """Get a single patent by publication number."""
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM patents WHERE publication_number = ?",
            (publication_number,),
        ).fetchone()
        if not row:
            return None
        d = dict(row)

        cpcs = conn.execute(
            "SELECT cpc_code, is_inventive, is_first FROM patent_cpc WHERE publication_number = ?",
            (publication_number,),
        ).fetchall()
        d["cpc_codes"] = [dict(c) for c in cpcs]

        assignees = conn.execute(
            "SELECT * FROM patent_assignees WHERE publication_number = ?",
            (publication_number,),
        ).fetchall()
        d["assignees"] = [dict(a) for a in assignees]

        inventors = conn.execute(
            "SELECT * FROM patent_inventors WHERE publication_number = ?",
            (publication_number,),
        ).fetchall()
        d["inventors"] = [dict(i) for i in inventors]

        citations = conn.execute(
            "SELECT cited_publication FROM patent_citations WHERE citing_publication = ?",
            (publication_number,),
        ).fetchall()
        d["citations_backward"] = [c["cited_publication"] for c in citations]

        return d

    def get_patent_embedding(self, publication_number: str) -> bytes | None:
        """Get raw embedding blob for a patent from patent_research_data."""
        row = self._conn().execute(
            "SELECT embedding_v1 FROM patent_research_data WHERE publication_number = ?",
            (publication_number,),
        ).fetchone()
        return row["embedding_v1"] if row else None

    def get_patent_cluster(self, publication_number: str) -> dict[str, Any] | None:
        """Get cluster mapping for a patent via patent_cluster_mapping JOIN tech_clusters."""
        row = self._conn().execute(
            """SELECT pcm.publication_number, pcm.cluster_id, pcm.distance,
                      tc.label, tc.cpc_class, tc.patent_count, tc.growth_rate
               FROM patent_cluster_mapping pcm
               JOIN tech_clusters tc ON pcm.cluster_id = tc.cluster_id
               WHERE pcm.publication_number = ?""",
            (publication_number,),
        ).fetchone()
        return dict(row) if row else None

    def get_firm_portfolio(
        self,
        firm_id: str,
        date_from: int | None = None,
        date_to: int | None = None,
        cpc_prefix: str | None = None,
    ) -> dict[str, Any]:
        """Get patent portfolio statistics for a firm."""
        date_cond = ""
        params: list[Any] = [firm_id]
        cpc_cond = ""

        if date_from:
            date_cond += " AND p.publication_date >= ?"
            params.append(date_from)
        if date_to:
            date_cond += " AND p.publication_date <= ?"
            params.append(date_to)
        if cpc_prefix:
            cpc_cond += (
                " AND p.publication_number IN "
                "(SELECT publication_number FROM patent_cpc "
                "WHERE cpc_code LIKE ? || '%')"
            )
            params.append(cpc_prefix)

        with self._conn() as conn:
            try:
                # Count
                count_row = conn.execute(
                    f"""SELECT COUNT(DISTINCT p.publication_number) as cnt
                    FROM patents p
                    JOIN patent_assignees a ON p.publication_number = a.publication_number
                    WHERE a.firm_id = ? {date_cond} {cpc_cond}""",
                    params,
                ).fetchone()
                total = count_row["cnt"] if count_row else 0
            except sqlite3.OperationalError as e:
                if "interrupted" in str(e):
                    _log.warning(f"Portfolio count query timed out after {_QUERY_TIMEOUT}s")
                    raise  # Let _safe_call handle it
                raise

            self._reset_timeout()

            # CPC distribution
            try:
                cpc_rows = conn.execute(
                    f"""SELECT substr(c.cpc_code, 1, 4) as cpc_class,
                           COUNT(DISTINCT p.publication_number) as cnt
                    FROM patents p
                    JOIN patent_assignees a ON p.publication_number = a.publication_number
                    JOIN patent_cpc c ON p.publication_number = c.publication_number
                    WHERE a.firm_id = ? {date_cond} {cpc_cond}
                    GROUP BY cpc_class
                    ORDER BY cnt DESC
                    LIMIT 20""",
                    params,
                ).fetchall()
                cpc_dist = [
                    {"code": r["cpc_class"], "count": r["cnt"]} for r in cpc_rows
                ]
            except sqlite3.OperationalError:
                cpc_dist = []

            self._reset_timeout()

            # Filing trend (by year)
            try:
                trend_rows = conn.execute(
                    f"""SELECT CAST(p.filing_date / 10000 AS INTEGER) as year,
                           COUNT(DISTINCT p.publication_number) as cnt
                    FROM patents p
                    JOIN patent_assignees a ON p.publication_number = a.publication_number
                    WHERE a.firm_id = ? {date_cond} {cpc_cond}
                      AND p.filing_date IS NOT NULL
                    GROUP BY year
                    ORDER BY year""",
                    params,
                ).fetchall()
                filing_trend = [
                    {"year": r["year"], "count": r["cnt"]} for r in trend_rows
                ]
            except sqlite3.OperationalError:
                filing_trend = []

            self._reset_timeout()

            # Co-applicants
            try:
                co_rows = conn.execute(
                    f"""SELECT a2.harmonized_name as co_applicant,
                           COUNT(DISTINCT a2.publication_number) as cnt
                    FROM patent_assignees a1
                    JOIN patent_assignees a2
                        ON a1.publication_number = a2.publication_number
                        AND a1.firm_id != COALESCE(a2.firm_id, '')
                    JOIN patents p ON a1.publication_number = p.publication_number
                    WHERE a1.firm_id = ? {date_cond} {cpc_cond}
                      AND a2.harmonized_name IS NOT NULL
                    GROUP BY co_applicant
                    ORDER BY cnt DESC
                    LIMIT 10""",
                    params,
                ).fetchall()
                co_applicants = [
                    {"name": r["co_applicant"], "count": r["cnt"]}
                    for r in co_rows
                ]
            except sqlite3.OperationalError:
                co_applicants = []

            # Top technologies (CPC with labels)
            top_tech = cpc_dist[:10]

        return {
            "count": total,
            "cpc_distribution": cpc_dist,
            "top_technologies": top_tech,
            "filing_trend": filing_trend,
            "co_applicants": co_applicants,
        }

    def get_cpc_trend(
        self,
        cpc_prefix: str | None = None,
        date_from: int | None = None,
        date_to: int | None = None,
        granularity: str = "year",
        query: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get CPC filing trend by year or quarter."""
        conditions = ["p.filing_date IS NOT NULL"]
        params: list[Any] = []

        if cpc_prefix:
            conditions.append("c.cpc_code LIKE ? || '%'")
            params.append(cpc_prefix)
        if date_from:
            conditions.append("p.publication_date >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("p.publication_date <= ?")
            params.append(date_to)
        if query:
            safe_query = _sanitize_fts5(query)
            if safe_query:
                # FTS5 with DatabaseError fallback to LIKE
                try:
                    _fts_pubs = [r[0] for r in self._conn().execute(
                        "SELECT publication_number FROM patents_fts "
                        "WHERE patents_fts MATCH ? LIMIT 5000", (safe_query,)
                    ).fetchall()]
                    if _fts_pubs:
                        _ph = ",".join("?" * len(_fts_pubs))
                        conditions.append(f"p.publication_number IN ({_ph})")
                        params.extend(_fts_pubs)
                    else:
                        conditions.append("0")
                except (sqlite3.DatabaseError, sqlite3.OperationalError):
                    # FTS5 corrupt — fall back to LIKE on title
                    _words = [w.strip('"') for w in safe_query.split() if len(w.strip('"')) >= 3]
                    if _words:
                        _like_parts = []
                        for _w in _words[:4]:
                            _like_parts.append(
                                "(p.title_ja LIKE '%' || ? || '%' OR p.title_en LIKE '%' || ? || '%')"
                            )
                            params.extend([_w, _w])
                        conditions.append("(" + " AND ".join(_like_parts) + ")")
            elif _has_short_words(query):
                q_clean = re.sub(r"[^\w\u3000-\u9fff\uff00-\uffef]", "", query.strip())
                if q_clean:
                    conditions.append(
                        "(p.title_ja LIKE '%' || ? || '%' OR p.title_en LIKE '%' || ? || '%')"
                    )
                    params.extend([q_clean, q_clean])

        where = "WHERE " + " AND ".join(conditions)

        if granularity == "quarter":
            period_expr = (
                "printf('%04dQ%d', "
                "CAST(p.filing_date / 10000 AS INTEGER), "
                "CAST(((CAST((p.filing_date % 10000) / 100 AS INTEGER) - 1) / 3) "
                "AS INTEGER) + 1)"
            )
        else:
            period_expr = "CAST(p.filing_date / 10000 AS INTEGER)"

        sql = f"""
        SELECT substr(c.cpc_code, 1, 4) as cpc_class,
               {period_expr} as period,
               COUNT(DISTINCT p.publication_number) as cnt
        FROM patents p
        JOIN patent_cpc c ON p.publication_number = c.publication_number
        {where}
        GROUP BY cpc_class, period
        ORDER BY period, cnt DESC
        """

        with self._conn() as conn:
            try:
                rows = conn.execute(sql, params).fetchall()
            except sqlite3.OperationalError as e:
                if "interrupted" in str(e):
                    _log.warning(f"CPC trend query timed out after {_QUERY_TIMEOUT}s")
                    raise  # Let _safe_call handle it
                raise

        return [
            {
                "cpc_class": r["cpc_class"],
                "period": r["period"],
                "count": r["cnt"],
            }
            for r in rows
        ]

    def get_top_applicants_for_cpc(
        self,
        cpc_prefix: str | None = None,
        date_from: int | None = None,
        date_to: int | None = None,
        limit: int = 20,
        query: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get top applicants for a CPC area."""
        conditions = []
        params: list[Any] = []

        if cpc_prefix:
            conditions.append("c.cpc_code LIKE ? || '%'")
            params.append(cpc_prefix)
        if date_from:
            conditions.append("p.publication_date >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("p.publication_date <= ?")
            params.append(date_to)
        if query:
            safe_query = _sanitize_fts5(query)
            if safe_query:
                # FTS5 with DatabaseError fallback to LIKE
                try:
                    _fts_pubs = [r[0] for r in self._conn().execute(
                        "SELECT publication_number FROM patents_fts "
                        "WHERE patents_fts MATCH ? LIMIT 5000", (safe_query,)
                    ).fetchall()]
                    if _fts_pubs:
                        _ph = ",".join("?" * len(_fts_pubs))
                        conditions.append(f"p.publication_number IN ({_ph})")
                        params.extend(_fts_pubs)
                    else:
                        conditions.append("0")
                except (sqlite3.DatabaseError, sqlite3.OperationalError):
                    # FTS5 corrupt — fall back to LIKE on title
                    _words = [w.strip('"') for w in safe_query.split() if len(w.strip('"')) >= 3]
                    if _words:
                        _like_parts = []
                        for _w in _words[:4]:
                            _like_parts.append(
                                "(p.title_ja LIKE '%' || ? || '%' OR p.title_en LIKE '%' || ? || '%')"
                            )
                            params.extend([_w, _w])
                        conditions.append("(" + " AND ".join(_like_parts) + ")")
            elif _has_short_words(query):
                q_clean = re.sub(r"[^\w\u3000-\u9fff\uff00-\uffef]", "", query.strip())
                if q_clean:
                    conditions.append(
                        "(p.title_ja LIKE '%' || ? || '%' OR p.title_en LIKE '%' || ? || '%')"
                    )
                    params.extend([q_clean, q_clean])

        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        params.append(limit)

        sql = f"""
        SELECT COALESCE(a.harmonized_name, a.raw_name) as name,
               a.firm_id as firm_id,
               COUNT(DISTINCT p.publication_number) as cnt
        FROM patents p
        JOIN patent_assignees a ON p.publication_number = a.publication_number
        LEFT JOIN patent_cpc c ON p.publication_number = c.publication_number
        {where}
        GROUP BY name, firm_id
        ORDER BY cnt DESC
        LIMIT ?
        """

        with self._conn() as conn:
            try:
                rows = conn.execute(sql, params).fetchall()
            except sqlite3.OperationalError as e:
                if "interrupted" in str(e):
                    _log.warning(f"Top applicants query timed out after {_QUERY_TIMEOUT}s")
                    raise  # Let _safe_call handle it
                raise

        return [
            {
                "name": r["name"],
                "firm_id": r["firm_id"],
                "count": r["cnt"],
            }
            for r in rows
        ]

    def get_co_applicant_network(
        self,
        firm_id: str,
        min_count: int = 5,
    ) -> list[dict[str, Any]]:
        """Find co-applicant entities and shared technology classes.

        Optimized: 2-step query (no CPC join in self-join) + firm_tech_vectors
        for shared CPC lookup.
        """
        with self._conn() as conn:
            try:
                # Step 1: co-assignees WITHOUT CPC join (much faster)
                rows = conn.execute(
                    """SELECT
                           COALESCE(a2.firm_id, '__name__:' || COALESCE(a2.harmonized_name, a2.raw_name)) as co_id,
                           COALESCE(a2.harmonized_name, a2.raw_name) as co_name,
                           a2.firm_id as co_firm_id,
                           COUNT(DISTINCT a1.publication_number) as co_patent_count
                       FROM patent_assignees a1
                       JOIN patent_assignees a2
                         ON a1.publication_number = a2.publication_number
                        AND a1.id != a2.id
                       WHERE a1.firm_id = ?
                         AND COALESCE(a2.harmonized_name, a2.raw_name) IS NOT NULL
                         AND (a2.firm_id IS NULL OR a2.firm_id != ?)
                       GROUP BY co_id, co_name, co_firm_id
                       HAVING co_patent_count >= ?
                       ORDER BY co_patent_count DESC
                       LIMIT 100""",
                    (firm_id, firm_id, min_count),
                ).fetchall()
            except sqlite3.OperationalError as e:
                if "interrupted" in str(e):
                    _log.warning(f"Co-applicant network query timed out after {_QUERY_TIMEOUT}s")
                    raise
                raise

            # Step 2: get shared CPC from firm_tech_vectors (instant)
            src_cpc = None
            src_row = conn.execute(
                "SELECT dominant_cpc FROM firm_tech_vectors "
                "WHERE firm_id = ? ORDER BY year DESC LIMIT 1",
                (firm_id,),
            ).fetchone()
            if src_row:
                src_cpc = src_row["dominant_cpc"]

        network = []
        for r in rows:
            shared_cpc = []
            co_fid = r["co_firm_id"]
            if co_fid:
                with self._conn() as conn2:
                    ftv = conn2.execute(
                        "SELECT dominant_cpc FROM firm_tech_vectors "
                        "WHERE firm_id = ? ORDER BY year DESC LIMIT 1",
                        (co_fid,),
                    ).fetchone()
                    if ftv and ftv["dominant_cpc"]:
                        shared_cpc.append(ftv["dominant_cpc"])
                    if src_cpc and src_cpc not in shared_cpc:
                        shared_cpc.append(src_cpc)
            network.append(
                {
                    "co_id": r["co_id"],
                    "co_name": r["co_name"],
                    "co_firm_id": r["co_firm_id"],
                    "co_patent_count": r["co_patent_count"],
                    "shared_cpc_classes": sorted(shared_cpc),
                }
            )
        return network

    def get_firm_patent_count_fast(self, firm_id: str) -> int | None:
        """Get patent count from pre-computed firm_tech_vectors (instant)."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT patent_count FROM firm_tech_vectors "
                "WHERE firm_id = ? ORDER BY year DESC LIMIT 1",
                (firm_id,),
            ).fetchone()
            return row["patent_count"] if row else None

    def log_ingestion_start(
        self, batch_id: str, source: str, country_code: str
    ) -> None:
        self._disable_timeout()
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO ingestion_log (batch_id, source, country_code, status)
                   VALUES (?, ?, ?, 'running')""",
                (batch_id, source, country_code),
            )

    def log_ingestion_complete(
        self, batch_id: str, records_inserted: int
    ) -> None:
        self._disable_timeout()
        with self._conn() as conn:
            conn.execute(
                """UPDATE ingestion_log
                   SET completed_at = datetime('now'),
                       records_inserted = ?,
                       status = 'completed'
                   WHERE batch_id = ?""",
                (records_inserted, batch_id),
            )

    def log_ingestion_progress(
        self, batch_id: str, records_fetched: int, last_pub_date: int | None
    ) -> None:
        self._disable_timeout()
        with self._conn() as conn:
            conn.execute(
                """UPDATE ingestion_log
                   SET records_fetched = ?,
                       last_publication_date = ?
                   WHERE batch_id = ?""",
                (records_fetched, last_pub_date, batch_id),
            )

    # --- Phase 2/3 methods ---

    def get_firm_cluster_set(
        self, firm_id: str, year: int = 2023
    ) -> dict[str, dict[str, Any]]:
        """Get clusters where a firm has patents, with startability scores."""
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT ss.cluster_id, ss.score, ss.gate_open,
                       ss.phi_tech_cos, ss.phi_tech_dist, ss.phi_tech_cpc,
                       tc.cpc_class, tc.label, tc.patent_count AS cluster_patent_count
                FROM startability_surface ss
                JOIN tech_clusters tc ON ss.cluster_id = tc.cluster_id
                WHERE ss.firm_id = ? AND ss.year = ?
                """,
                (firm_id, year),
            ).fetchall()
        return {
            r["cluster_id"]: dict(r)
            for r in rows
        }

    def get_firm_startability_surface(
        self, firm_id: str, year: int = 2023
    ) -> list[dict[str, Any]]:
        """Get all startability scores for a firm across all clusters."""
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT ss.cluster_id, ss.score, ss.gate_open,
                       ss.phi_tech_cos, ss.phi_tech_dist, ss.phi_tech_cpc,
                       ss.phi_tech_cite,
                       tc.cpc_class, tc.label, tc.patent_count
                FROM startability_surface ss
                JOIN tech_clusters tc ON ss.cluster_id = tc.cluster_id
                WHERE ss.firm_id = ? AND ss.year = ?
                ORDER BY ss.score DESC
                """,
                (firm_id, year),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_cluster_alive_patents(
        self, cluster_id: str, top_n: int = 20
    ) -> list[dict[str, Any]]:
        """Get alive patents in a cluster, ordered by value_score."""
        with self._conn() as conn:
            try:
                rows = conn.execute(
                    """
                    SELECT p.publication_number, p.title_ja, p.title_en,
                           p.filing_date, p.publication_date,
                           pls.status, pls.expiry_date,
                           pvi.value_score,
                           GROUP_CONCAT(DISTINCT a.harmonized_name) AS assignees
                    FROM patent_cluster_mapping pcm
                    JOIN patents p ON pcm.publication_number = p.publication_number
                    LEFT JOIN patent_legal_status pls ON p.publication_number = pls.publication_number
                    LEFT JOIN patent_value_index pvi ON p.publication_number = pvi.publication_number
                    LEFT JOIN patent_assignees a ON p.publication_number = a.publication_number
                    WHERE pcm.cluster_id = ?
                      AND pls.status = 'alive'
                    GROUP BY p.publication_number
                    ORDER BY COALESCE(pvi.value_score, 0) DESC
                    LIMIT ?
                    """,
                    (cluster_id, top_n),
                ).fetchall()
            except sqlite3.OperationalError as e:
                if "interrupted" in str(e):
                    _log.warning(f"Cluster alive patents query timed out after {_QUERY_TIMEOUT}s")
                    raise  # Let _safe_call handle it
                raise
        return [dict(r) for r in rows]

    def get_cluster_momentum_history(
        self, cluster_id: str, years: int = 5
    ) -> list[dict[str, Any]]:
        """Get recent momentum data for a cluster."""
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT year, patent_count, growth_rate, acceleration
                FROM tech_cluster_momentum
                WHERE cluster_id = ?
                ORDER BY year DESC
                LIMIT ?
                """,
                (cluster_id, years),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_firm_avg_value_index(self, firm_id: str) -> float | None:
        """Average patent_value_index.value_score for a firm's portfolio."""
        with self._conn() as conn:
            try:
                row = conn.execute(
                    """
                    SELECT AVG(pvi.value_score) AS avg_value
                    FROM patent_assignees pa
                    JOIN patent_value_index pvi ON pa.publication_number = pvi.publication_number
                    WHERE pa.firm_id = ?
                    """,
                    (firm_id,),
                ).fetchone()
            except sqlite3.OperationalError as e:
                if "interrupted" in str(e):
                    _log.warning(f"Firm avg value query timed out after {_QUERY_TIMEOUT}s")
                    raise  # Let _safe_call handle it
                raise
        return row["avg_value"] if row and row["avg_value"] is not None else None

    def get_gdelt_features_latest(self, firm_id: str) -> dict[str, Any] | None:
        """Get most recent GDELT five-axis features for a firm."""
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT firm_id, year, quarter,
                       direction_score, openness_score, investment_score,
                       governance_friction_score, leadership_score,
                       total_mentions, total_sources
                FROM gdelt_company_features
                WHERE firm_id = ?
                ORDER BY year DESC, quarter DESC
                LIMIT 1
                """,
                (firm_id,),
            ).fetchone()
        return dict(row) if row else None

    def get_whitespace_clusters(
        self,
        exclude_clusters: set[str] | None = None,
        min_avg_startability: float = 0.5,
        max_recent_filings: int = 100,
        top_n: int = 10,
    ) -> list[dict[str, Any]]:
        """Find whitespace clusters: high startability potential, low filing density."""
        with self._conn() as conn:
            try:
                rows = conn.execute(
                    """
                    SELECT tc.cluster_id, tc.label, tc.cpc_class,
                           tc.patent_count, tc.growth_rate,
                           COALESCE(ss_avg.avg_score, 0) AS avg_startability,
                           COALESCE(recent.cnt, 0) AS recent_filings_3yr
                    FROM tech_clusters tc
                    LEFT JOIN (
                        SELECT cluster_id, AVG(score) AS avg_score
                        FROM startability_surface
                        WHERE gate_open = 1
                        GROUP BY cluster_id
                    ) ss_avg ON tc.cluster_id = ss_avg.cluster_id
                    LEFT JOIN (
                        SELECT pcm.cluster_id, COUNT(*) AS cnt
                        FROM patent_cluster_mapping pcm
                        JOIN patents p ON pcm.publication_number = p.publication_number
                        WHERE p.filing_date >= ?
                        GROUP BY pcm.cluster_id
                    ) recent ON tc.cluster_id = recent.cluster_id
                    WHERE COALESCE(ss_avg.avg_score, 0) >= ?
                      AND COALESCE(recent.cnt, 0) <= ?
                    ORDER BY COALESCE(ss_avg.avg_score, 0) DESC
                    LIMIT ?
                    """,
                    (20210101, min_avg_startability, max_recent_filings, top_n * 3),
                ).fetchall()
            except sqlite3.OperationalError as e:
                if "interrupted" in str(e):
                    _log.warning(f"Whitespace discovery query timed out after {_QUERY_TIMEOUT}s")
                    raise  # Let _safe_call handle it
                raise

        results = []
        for r in rows:
            cid = r["cluster_id"]
            if exclude_clusters and cid in exclude_clusters:
                continue
            results.append(dict(r))
            if len(results) >= top_n:
                break
        return results

    def get_cluster_top_firms(
        self, cluster_id: str, year: int = 2023, top_n: int = 10
    ) -> list[dict[str, Any]]:
        """Get top firms in a cluster by startability score."""
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT ss.firm_id, ss.score, ss.gate_open,
                       ss.phi_tech_cos, ftv.patent_count
                FROM startability_surface ss
                LEFT JOIN firm_tech_vectors ftv
                    ON ss.firm_id = ftv.firm_id AND ftv.year = ss.year
                WHERE ss.cluster_id = ? AND ss.year = ?
                ORDER BY ss.score DESC
                LIMIT ?
                """,
                (cluster_id, year, top_n),
            ).fetchall()
        return [dict(r) for r in rows]
