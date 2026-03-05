#!/usr/bin/env python3
"""Generate JSON datasets for a research paper on patent technology spaces.

Run with:
  ~/pyenv/bin/python3 ~/patent-space-mcp/scripts/generate_paper_data.py --db ~/patent-space-mcp/data/patents.db --out-dir ~/paper_data/pre_ingestion

Outputs (written into --out-dir):
  a) table_1_db_summary.json
  b) table_2_jp_yearly_coverage.json
  c) figure_1_startability_heatmap.json
  d) figure_2_tech_landscape.json
  e) figure_3_delta_s_timeseries.json
  f) figure_4_firm_diversification.json
  g) table_3_entity_resolution.json

Robustness: if a required table is missing or empty, the script writes a
"skipped" JSON for that output and prints a warning to stderr.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import struct
import sys
from pathlib import Path
from typing import Any

try:
    import numpy as np
except Exception:
    np = None  # type: ignore[assignment]

EMBED_DIM = 64
INCLUDE_SLOW_COUNTS = False


def _iso_now_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _expand(path: str) -> str:
    return os.path.expanduser(path)


def _ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def _write_json(path: str, payload: Any) -> None:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp.replace(out_path)


def _warn(msg: str) -> None:
    print(f"[generate_paper_data] WARNING: {msg}", file=sys.stderr)


def _connect(db_path: str) -> sqlite3.Connection:
    # Use a read-only connection so we don't contend with ingestion writes.
    # Avoid PRAGMA journal_mode=WAL here: switching modes can require write locks.
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-200000")  # ~200MB
    return conn


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def _table_has_rows(conn: sqlite3.Connection, name: str) -> bool:
    try:
        row = conn.execute(f"SELECT 1 FROM {_quote_ident(name)} LIMIT 1").fetchone()
    except sqlite3.Error:
        return False
    return row is not None


def _table_row_count(conn: sqlite3.Connection, name: str) -> int:
    try:
        row = conn.execute(f"SELECT COUNT(*) AS n FROM {_quote_ident(name)}").fetchone()
    except sqlite3.Error:
        return 0
    return int(row["n"]) if row else 0


def _skipped_payload(db_path: str, out_dir: str, reason: str) -> dict[str, Any]:
    return {
        "generated_at": _iso_now_utc(),
        "db_path": db_path,
        "out_dir": out_dir,
        "skipped": True,
        "reason": reason,
    }


def _year_bounds(year: int) -> tuple[int, int]:
    return year * 10000, year * 10000 + 9999


def _unpack_vec64(blob: bytes | None) -> "np.ndarray | None":
    if blob is None or np is None:
        return None
    try:
        values = struct.unpack("64d", blob)
    except (struct.error, TypeError):
        return None
    if len(values) != EMBED_DIM:
        return None
    return np.array(values, dtype=np.float64)


def table_1_db_summary(
    conn: sqlite3.Connection, db_path: str, out_dir: str
) -> dict[str, Any]:
    if not _table_exists(conn, "patents") or not _table_has_rows(conn, "patents"):
        return _skipped_payload(db_path, out_dir, "missing/empty table: patents")

    patents_by_country = [
        {"country_code": r["country_code"], "patents": int(r["n"])}
        for r in conn.execute(
            "SELECT country_code, COUNT(*) AS n FROM patents GROUP BY country_code ORDER BY n DESC"
        ).fetchall()
    ]

    total_embeddings = None
    embeddings_note = None
    if _table_exists(conn, "patent_research_data"):
        if INCLUDE_SLOW_COUNTS:
            total_embeddings = int(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM patent_research_data WHERE embedding_v1 IS NOT NULL"
                ).fetchone()["n"]
            )
        else:
            embeddings_note = "skipped (slow): COUNT(*) over patent_research_data.embedding_v1 requires a full table scan"

    total_firms = None
    if _table_exists(conn, "firm_tech_vectors") and _table_has_rows(conn, "firm_tech_vectors"):
        total_firms = int(
            conn.execute(
                """
                SELECT COUNT(DISTINCT firm_id) AS n
                FROM firm_tech_vectors
                WHERE firm_id IS NOT NULL AND trim(firm_id) <> ''
                """
            ).fetchone()["n"]
        )
    elif _table_exists(conn, "patent_assignees") and _table_has_rows(conn, "patent_assignees"):
        total_firms = int(
            conn.execute(
                """
                SELECT COUNT(DISTINCT firm_id) AS n
                FROM patent_assignees
                WHERE firm_id IS NOT NULL AND trim(firm_id) <> ''
                """
            ).fetchone()["n"]
        )

    total_clusters = None
    if _table_exists(conn, "tech_clusters"):
        total_clusters = int(
            conn.execute("SELECT COUNT(*) AS n FROM tech_clusters").fetchone()["n"]
        )

    return {
        "generated_at": _iso_now_utc(),
        "db_path": db_path,
        "patents_by_country": patents_by_country,
        "totals": {
            "patents": int(conn.execute("SELECT COUNT(*) AS n FROM patents").fetchone()["n"]),
            "embeddings": total_embeddings,
            "firms": total_firms,
            "clusters": total_clusters,
        },
        "notes": {"embeddings": embeddings_note} if embeddings_note else None,
    }


def table_2_jp_yearly_coverage(
    conn: sqlite3.Connection, db_path: str, out_dir: str
) -> dict[str, Any]:
    if not _table_exists(conn, "patents") or not _table_has_rows(conn, "patents"):
        return _skipped_payload(db_path, out_dir, "missing/empty table: patents")

    has_prd = _table_exists(conn, "patent_research_data")

    year_from, year_to = 2000, 2024
    start, end = _year_bounds(year_from)[0], _year_bounds(year_to)[1]

    if has_prd:
        yearly = conn.execute(
            """
            SELECT (p.filing_date / 10000) AS y,
                   COUNT(*) AS patents,
                   COUNT(prd.embedding_v1) AS embeddings
            FROM patents p
            LEFT JOIN patent_research_data prd
              ON prd.publication_number = p.publication_number
            WHERE p.country_code = 'JP'
              AND p.filing_date BETWEEN ? AND ?
            GROUP BY y
            ORDER BY y
            """,
            (start, end),
        ).fetchall()
        patent_counts = {int(r["y"]): int(r["patents"]) for r in yearly if r["y"] is not None}
        embedding_counts = {int(r["y"]): int(r["embeddings"]) for r in yearly if r["y"] is not None}
    else:
        yearly = conn.execute(
            """
            SELECT (filing_date / 10000) AS y, COUNT(*) AS patents
            FROM patents
            WHERE country_code = 'JP'
              AND filing_date BETWEEN ? AND ?
            GROUP BY y
            ORDER BY y
            """,
            (start, end),
        ).fetchall()
        patent_counts = {int(r["y"]): int(r["patents"]) for r in yearly if r["y"] is not None}
        embedding_counts = {}

    rows: list[dict[str, Any]] = []
    for year in range(year_from, year_to + 1):
        patents = int(patent_counts.get(year, 0))
        embeddings = int(embedding_counts.get(year, 0)) if has_prd else None
        coverage_pct = None if embeddings is None else (0.0 if patents == 0 else (embeddings / patents) * 100.0)
        rows.append({"year": year, "patents": patents, "embeddings": embeddings, "coverage_pct": coverage_pct})

    return {
        "generated_at": _iso_now_utc(),
        "db_path": db_path,
        "country": "JP",
        "year_from": year_from,
        "year_to": year_to,
        "rows": rows,
    }


def figure_1_startability_heatmap(
    conn: sqlite3.Connection, db_path: str, out_dir: str
) -> dict[str, Any]:
    required = ["patents", "patent_assignees", "patent_cluster_mapping", "startability_surface"]
    for t in required:
        if not _table_exists(conn, t) or not _table_has_rows(conn, t):
            return _skipped_payload(db_path, out_dir, f"missing/empty table: {t}")

    year = 2020
    start, end = _year_bounds(year)

    if _table_exists(conn, "firm_tech_vectors") and _table_has_rows(conn, "firm_tech_vectors"):
        firms = conn.execute(
            """
            SELECT firm_id, patent_count AS n
            FROM firm_tech_vectors
            WHERE year = ?
              AND firm_id IS NOT NULL AND trim(firm_id) <> ''
            ORDER BY patent_count DESC
            LIMIT 20
            """,
            (year,),
        ).fetchall()
    else:
        firms = conn.execute(
            """
            SELECT pa.firm_id, COUNT(DISTINCT p.publication_number) AS n
            FROM patents p
            JOIN patent_assignees pa ON pa.publication_number = p.publication_number
            WHERE pa.firm_id IS NOT NULL AND trim(pa.firm_id) <> ''
              AND p.filing_date BETWEEN ? AND ?
            GROUP BY pa.firm_id
            ORDER BY n DESC
            LIMIT 20
            """,
            (start, end),
        ).fetchall()

    clusters_have_labels = False
    if _table_exists(conn, "tech_clusters") and _table_has_rows(conn, "tech_clusters"):
        clusters = conn.execute(
            """
            SELECT cluster_id, label, patent_count AS n
            FROM tech_clusters
            ORDER BY patent_count DESC
            LIMIT 20
            """
        ).fetchall()
        clusters_have_labels = True
    else:
        clusters = conn.execute(
            """
            SELECT pcm.cluster_id, COUNT(*) AS n
            FROM patents p
            JOIN patent_cluster_mapping pcm ON pcm.publication_number = p.publication_number
            WHERE p.filing_date BETWEEN ? AND ?
            GROUP BY pcm.cluster_id
            ORDER BY n DESC
            LIMIT 20
            """,
            (start, end),
        ).fetchall()

    firm_ids = [str(r["firm_id"]) for r in firms]
    cluster_ids = [str(r["cluster_id"]) for r in clusters]
    if not firm_ids or not cluster_ids:
        return _skipped_payload(db_path, out_dir, "no JP firm/cluster data for 2020")

    labels: dict[str, str | None] = {}
    if clusters_have_labels:
        for r in clusters:
            labels[str(r["cluster_id"])] = r["label"]
    elif _table_exists(conn, "tech_clusters"):
        qmarks = ",".join(["?"] * len(cluster_ids))
        for r in conn.execute(
            f"SELECT cluster_id, label FROM tech_clusters WHERE cluster_id IN ({qmarks})",
            tuple(cluster_ids),
        ).fetchall():
            labels[str(r["cluster_id"])] = r["label"]

    q_f = ",".join(["?"] * len(firm_ids))
    q_c = ",".join(["?"] * len(cluster_ids))
    score_rows = conn.execute(
        f"""
        SELECT cluster_id, firm_id, score
        FROM startability_surface
        WHERE year = ?
          AND firm_id IN ({q_f})
          AND cluster_id IN ({q_c})
        """,
        (year, *firm_ids, *cluster_ids),
    ).fetchall()

    score_map: dict[tuple[str, str], float | None] = {}
    for r in score_rows:
        score_map[(str(r["firm_id"]), str(r["cluster_id"]))] = (
            float(r["score"]) if r["score"] is not None else None
        )

    matrix: list[list[float | None]] = []
    for fid in firm_ids:
        matrix.append([score_map.get((fid, cid)) for cid in cluster_ids])

    return {
        "generated_at": _iso_now_utc(),
        "db_path": db_path,
        "year": year,
        "firms": [{"firm_id": r["firm_id"], "patents": int(r["n"])} for r in firms],
        "clusters": [
            {
                "cluster_id": r["cluster_id"],
                "label": labels.get(str(r["cluster_id"])),
                "patents": int(r["n"]),
            }
            for r in clusters
        ],
        "scores": {
            "row_firm_ids": firm_ids,
            "col_cluster_ids": cluster_ids,
            "values": matrix,
        },
    }


def figure_2_tech_landscape(
    conn: sqlite3.Connection, db_path: str, out_dir: str
) -> dict[str, Any]:
    if not _table_exists(conn, "tech_clusters") or not _table_has_rows(conn, "tech_clusters"):
        return _skipped_payload(db_path, out_dir, "missing/empty table: tech_clusters")

    rows = conn.execute(
        "SELECT cluster_id, label, center_vector, patent_count, growth_rate FROM tech_clusters ORDER BY cluster_id"
    ).fetchall()

    clusters: list[dict[str, Any]] = []

    if np is None:
        _warn("numpy not available; skipping PCA projection")
        for r in rows:
            clusters.append(
                {
                    "cluster_id": r["cluster_id"],
                    "label": r["label"],
                    "patent_count": int(r["patent_count"]) if r["patent_count"] is not None else None,
                    "growth_rate": float(r["growth_rate"]) if r["growth_rate"] is not None else None,
                    "pca2d": None,
                }
            )
        return {
            "generated_at": _iso_now_utc(),
            "db_path": db_path,
            "clusters": clusters,
            "pca": {"skipped": True, "reason": "numpy not installed"},
        }

    vecs: list[tuple[str, np.ndarray]] = []
    meta: dict[str, dict[str, Any]] = {}
    for r in rows:
        cid = str(r["cluster_id"])
        vec = _unpack_vec64(r["center_vector"])
        if vec is None:
            continue
        vecs.append((cid, vec))
        meta[cid] = {
            "cluster_id": cid,
            "label": r["label"],
            "patent_count": int(r["patent_count"]) if r["patent_count"] is not None else None,
            "growth_rate": float(r["growth_rate"]) if r["growth_rate"] is not None else None,
        }

    if not vecs:
        return _skipped_payload(db_path, out_dir, "no cluster center vectors available")

    X = np.stack([v for _, v in vecs], axis=0)
    Xc = X - X.mean(axis=0, keepdims=True)

    _, s, vh = np.linalg.svd(Xc, full_matrices=False)
    coords = Xc @ vh[:2, :].T

    total_var = float((s**2).sum()) if s is not None else 0.0
    explained = [
        float((s[i] ** 2) / total_var) if total_var > 0 else 0.0
        for i in range(min(2, len(s)))
    ]

    for i, (cid, _) in enumerate(vecs):
        item = meta[cid]
        item["pca2d"] = [float(coords[i, 0]), float(coords[i, 1])]
        clusters.append(item)

    return {
        "generated_at": _iso_now_utc(),
        "db_path": db_path,
        "clusters": clusters,
        "pca": {"method": "svd", "dims": 2, "explained_variance_ratio": explained},
    }


def _find_best_firm_id(conn: sqlite3.Connection, keyword: str) -> dict[str, Any] | None:
    if _table_exists(conn, "firm_tech_vectors") and _table_has_rows(conn, "firm_tech_vectors"):
        like = f"%{keyword.casefold()}%"
        rows = conn.execute(
            """
            SELECT firm_id, MAX(patent_count) AS n
            FROM firm_tech_vectors
            WHERE firm_id IS NOT NULL AND trim(firm_id) <> ''
              AND lower(firm_id) LIKE ?
            GROUP BY firm_id
            ORDER BY n DESC
            LIMIT 5
            """,
            (like,),
        ).fetchall()
        if rows:
            best = rows[0]
            return {
                "firm_id": best["firm_id"],
                "matched_on": keyword,
                "candidates": [
                    {"firm_id": r["firm_id"], "patents": int(r["n"] or 0)} for r in rows
                ],
            }

    if not _table_exists(conn, "patent_assignees") or not _table_has_rows(conn, "patent_assignees"):
        return None

    like = f"%{keyword.casefold()}%"
    rows = conn.execute(
        """
        SELECT firm_id, COUNT(DISTINCT publication_number) AS n
        FROM patent_assignees
        WHERE firm_id IS NOT NULL AND trim(firm_id) <> ''
          AND lower(firm_id) LIKE ?
        GROUP BY firm_id
        ORDER BY n DESC
        LIMIT 5
        """,
        (like,),
    ).fetchall()
    if not rows:
        return None
    best = rows[0]
    return {
        "firm_id": best["firm_id"],
        "matched_on": keyword,
        "candidates": [{"firm_id": r["firm_id"], "patents": int(r["n"])} for r in rows],
    }


def figure_3_delta_s_timeseries(
    conn: sqlite3.Connection, db_path: str, out_dir: str
) -> dict[str, Any]:
    if not _table_exists(conn, "startability_surface") or not _table_has_rows(conn, "startability_surface"):
        return _skipped_payload(db_path, out_dir, "missing/empty table: startability_surface")

    targets = ["Toyota", "Sony", "Panasonic"]
    firm_infos: list[dict[str, Any]] = []
    missing: list[str] = []
    for name in targets:
        info = _find_best_firm_id(conn, name)
        if not info:
            missing.append(name)
            continue
        firm_infos.append(info)

    if not firm_infos:
        return _skipped_payload(db_path, out_dir, f"no firm_ids found for: {', '.join(targets)}")

    out: dict[str, Any] = {
        "generated_at": _iso_now_utc(),
        "db_path": db_path,
        "definition": {
            "delta": "avg_abs_delta_score across clusters",
            "detail": "For each firm and transition year y->y+1, compute average(|S_{y+1}(c)-S_y(c)|) over clusters with both years present.",
        },
        "firms": firm_infos,
        "series": {},
    }

    for finfo in firm_infos:
        fid = str(finfo["firm_id"])
        rows = conn.execute(
            """
            SELECT s1.year AS y,
                   s1.cluster_id AS cluster_id,
                   s1.score AS score_from,
                   s2.score AS score_to
            FROM startability_surface s1
            JOIN startability_surface s2
              ON s2.firm_id = s1.firm_id
             AND s2.cluster_id = s1.cluster_id
             AND s2.year = s1.year + 1
            WHERE s1.firm_id = ?
            ORDER BY s1.year
            """,
            (fid,),
        ).fetchall()

        by_year: dict[int, list[float]] = {}
        for r in rows:
            y = int(r["y"])
            a, b = r["score_from"], r["score_to"]
            if a is None or b is None:
                continue
            by_year.setdefault(y, []).append(abs(float(b) - float(a)))

        series = []
        for y in sorted(by_year):
            vals = by_year[y]
            if not vals:
                continue
            series.append(
                {
                    "from_year": y,
                    "to_year": y + 1,
                    "clusters": len(vals),
                    "avg_abs_delta": float(sum(vals) / len(vals)),
                    "sum_abs_delta": float(sum(vals)),
                }
            )
        out["series"][fid] = series

    if missing:
        out["missing_firms"] = missing
    return out


def figure_4_firm_diversification(
    conn: sqlite3.Connection, db_path: str, out_dir: str
) -> dict[str, Any]:
    if not _table_exists(conn, "firm_tech_vectors") or not _table_has_rows(conn, "firm_tech_vectors"):
        return _skipped_payload(db_path, out_dir, "missing/empty table: firm_tech_vectors")

    max_year = conn.execute("SELECT MAX(year) AS y FROM firm_tech_vectors").fetchone()["y"]
    if max_year is None:
        return _skipped_payload(db_path, out_dir, "firm_tech_vectors has no years")

    top = conn.execute(
        """
        SELECT firm_id, patent_count
        FROM firm_tech_vectors
        WHERE year = ?
        ORDER BY patent_count DESC
        LIMIT 20
        """,
        (int(max_year),),
    ).fetchall()

    firm_ids = [str(r["firm_id"]) for r in top]
    if not firm_ids:
        return _skipped_payload(db_path, out_dir, "no firms found")

    qmarks = ",".join(["?"] * len(firm_ids))
    rows = conn.execute(
        f"""
        SELECT firm_id, year, patent_count, tech_diversity, tech_concentration
        FROM firm_tech_vectors
        WHERE firm_id IN ({qmarks})
        ORDER BY firm_id, year
        """,
        tuple(firm_ids),
    ).fetchall()

    series: dict[str, list[dict[str, Any]]] = {fid: [] for fid in firm_ids}
    for r in rows:
        fid = str(r["firm_id"])
        series[fid].append(
            {
                "year": int(r["year"]),
                "patent_count": int(r["patent_count"]) if r["patent_count"] is not None else None,
                "tech_diversity": float(r["tech_diversity"]) if r["tech_diversity"] is not None else None,
                "tech_concentration": float(r["tech_concentration"]) if r["tech_concentration"] is not None else None,
            }
        )

    return {
        "generated_at": _iso_now_utc(),
        "db_path": db_path,
        "top_firms_year": int(max_year),
        "top_firms": [{"firm_id": r["firm_id"], "patent_count": int(r["patent_count"])} for r in top],
        "series": series,
        "notes": {
            "tech_diversity": "Shannon entropy over cumulative CPC distribution (as stored in firm_tech_vectors)",
        },
    }


def table_3_entity_resolution(
    conn: sqlite3.Connection, db_path: str, out_dir: str
) -> dict[str, Any]:
    if not _table_exists(conn, "patent_assignees") or not _table_has_rows(conn, "patent_assignees"):
        return _skipped_payload(db_path, out_dir, "missing/empty table: patent_assignees")

    total = int(conn.execute("SELECT COUNT(*) AS n FROM patent_assignees").fetchone()["n"])
    unresolved = int(
        conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM patent_assignees
            WHERE harmonized_name IS NULL OR trim(harmonized_name) = ''
            """
        ).fetchone()["n"]
    )
    exact = int(
        conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM patent_assignees
            WHERE harmonized_name IS NOT NULL AND trim(harmonized_name) <> ''
              AND lower(trim(raw_name)) = lower(trim(harmonized_name))
            """
        ).fetchone()["n"]
    )
    fuzzy = int(
        conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM patent_assignees
            WHERE harmonized_name IS NOT NULL AND trim(harmonized_name) <> ''
              AND lower(trim(raw_name)) <> lower(trim(harmonized_name))
            """
        ).fetchone()["n"]
    )
    firm_id_present = int(
        conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM patent_assignees
            WHERE firm_id IS NOT NULL AND trim(firm_id) <> ''
            """
        ).fetchone()["n"]
    )

    def pct(n: int) -> float:
        return 0.0 if total == 0 else (n / total) * 100.0

    return {
        "generated_at": _iso_now_utc(),
        "db_path": db_path,
        "total_rows": total,
        "exact_match": {"count": exact, "pct": pct(exact)},
        "fuzzy_match": {"count": fuzzy, "pct": pct(fuzzy)},
        "unresolved": {"count": unresolved, "pct": pct(unresolved)},
        "firm_id_present": {"count": firm_id_present, "pct": pct(firm_id_present)},
        "definition": {
            "exact": "harmonized_name present and equals raw_name (case-insensitive, trimmed)",
            "fuzzy": "harmonized_name present and differs from raw_name (case-insensitive, trimmed)",
            "unresolved": "harmonized_name missing/empty",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate paper-ready JSON datasets from patents.db")
    parser.add_argument(
        "--db",
        default="~/patent-space-mcp/data/patents.db",
        help="Path to SQLite DB (default: %(default)s)",
    )
    parser.add_argument(
        "--out-dir",
        default="~/paper_data",
        help="Output directory (default: %(default)s)",
    )
    parser.add_argument(
        "--slow-counts",
        action="store_true",
        help="Include very expensive global COUNT(*) stats (may full-scan large tables).",
    )
    args = parser.parse_args()

    db_path = _expand(args.db)
    out_dir = _expand(args.out_dir)
    global INCLUDE_SLOW_COUNTS
    INCLUDE_SLOW_COUNTS = bool(args.slow_counts)

    if not os.path.exists(db_path):
        raise SystemExit(f"DB not found: {db_path}")

    _ensure_dir(out_dir)

    outputs = [
        ("table_1_db_summary.json", table_1_db_summary),
        ("figure_1_startability_heatmap.json", figure_1_startability_heatmap),
        ("figure_2_tech_landscape.json", figure_2_tech_landscape),
        ("figure_3_delta_s_timeseries.json", figure_3_delta_s_timeseries),
        ("figure_4_firm_diversification.json", figure_4_firm_diversification),
        ("table_3_entity_resolution.json", table_3_entity_resolution),
        # This one can be relatively slow (large joins); run it last so other
        # outputs become available early.
        ("table_2_jp_yearly_coverage.json", table_2_jp_yearly_coverage),
    ]

    with _connect(db_path) as conn:
        for filename, fn in outputs:
            out_path = str(Path(out_dir) / filename)
            try:
                print(f"[generate_paper_data] start {filename}", file=sys.stderr, flush=True)
                payload = fn(conn, db_path=db_path, out_dir=out_dir)
            except sqlite3.Error as exc:
                _warn(f"{filename}: sqlite error: {exc}")
                payload = _skipped_payload(db_path, out_dir, f"sqlite error: {exc}")
            except Exception as exc:
                _warn(f"{filename}: unexpected error: {exc}")
                payload = _skipped_payload(db_path, out_dir, f"unexpected error: {exc}")
            _write_json(out_path, payload)
            print(f"[generate_paper_data] wrote {filename}", file=sys.stderr, flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
