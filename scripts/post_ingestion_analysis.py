"""Post-ingestion analysis for patents.db.

Produces a human-readable summary and (optionally) a JSON snapshot you can
compare against a baseline taken pre-ingestion.

Designed to run with: ~/pyenv/bin/python3
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sqlite3
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class EmbeddingValidity:
    expected_dim: int
    expected_bytes: int
    checked: int
    bad_length: int
    all_zero: int
    non_finite: int

    @property
    def ok(self) -> int:
        return self.checked - self.bad_length - self.all_zero - self.non_finite


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _iso_now_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _expand(path: str) -> str:
    return os.path.expanduser(path)


def _pct(n: int, d: int) -> float:
    if d == 0:
        return 0.0
    return (n / d) * 100.0


def _top_embedding_blob_lengths(conn: sqlite3.Connection, limit: int = 5) -> list[dict[str, int]]:
    rows = conn.execute(
        """
        SELECT length(embedding_v1) AS nbytes, COUNT(*) AS n
        FROM patent_research_data
        WHERE embedding_v1 IS NOT NULL
        GROUP BY length(embedding_v1)
        ORDER BY n DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    out: list[dict[str, int]] = []
    for r in rows:
        out.append({"nbytes": int(r["nbytes"]), "count": int(r["n"])})
    return out


def _infer_expected_embedding_shape(top_lengths: list[dict[str, int]]) -> tuple[int, int]:
    if not top_lengths:
        return (0, 0)
    nbytes = int(top_lengths[0]["nbytes"])
    if nbytes % 8 != 0:
        return (0, nbytes)
    return (nbytes // 8, nbytes)


def _iter_embedding_blobs(conn: sqlite3.Connection, limit: int) -> Iterable[bytes]:
    rows = conn.execute(
        """
        SELECT embedding_v1
        FROM patent_research_data
        WHERE embedding_v1 IS NOT NULL
        LIMIT ?
        """,
        (limit,),
    )
    for r in rows:
        blob = r[0]
        if blob is not None:
            yield blob


def _check_embedding_validity(
    conn: sqlite3.Connection,
    expected_dim: int,
    expected_bytes: int,
    sample_n: int,
) -> EmbeddingValidity:
    checked = 0
    bad_length = 0
    all_zero = 0
    non_finite = 0

    if expected_dim <= 0 or expected_bytes <= 0:
        return EmbeddingValidity(
            expected_dim=expected_dim,
            expected_bytes=expected_bytes,
            checked=0,
            bad_length=0,
            all_zero=0,
            non_finite=0,
        )

    fmt = "<" + ("d" * expected_dim)

    for blob in _iter_embedding_blobs(conn, sample_n):
        checked += 1
        if len(blob) != expected_bytes:
            bad_length += 1
            continue

        try:
            values = struct.unpack(fmt, blob)
        except struct.error:
            bad_length += 1
            continue

        # Non-finite check
        if any(not math.isfinite(v) for v in values):
            non_finite += 1
            continue

        # Non-zero check (cheap norm-ish)
        if all(v == 0.0 for v in values):
            all_zero += 1
            continue

    return EmbeddingValidity(
        expected_dim=expected_dim,
        expected_bytes=expected_bytes,
        checked=checked,
        bad_length=bad_length,
        all_zero=all_zero,
        non_finite=non_finite,
    )


def _get_totals(conn: sqlite3.Connection) -> dict[str, int]:
    patents = int(conn.execute("SELECT COUNT(*) AS n FROM patents").fetchone()["n"])
    research_rows = int(
        conn.execute("SELECT COUNT(*) AS n FROM patent_research_data").fetchone()["n"]
    )
    embeddings = int(
        conn.execute(
            "SELECT COUNT(*) AS n FROM patent_research_data WHERE embedding_v1 IS NOT NULL"
        ).fetchone()["n"]
    )
    return {"patents": patents, "research_rows": research_rows, "embeddings": embeddings}


def _embeddings_by_country(conn: sqlite3.Connection, top_n: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT p.country_code AS country_code, COUNT(*) AS n
        FROM patent_research_data pr
        JOIN patents p
          ON p.publication_number = pr.publication_number
        WHERE pr.embedding_v1 IS NOT NULL
        GROUP BY p.country_code
        ORDER BY n DESC
        LIMIT ?
        """,
        (top_n,),
    ).fetchall()

    out: list[dict[str, Any]] = []
    for r in rows:
        out.append({"country_code": r["country_code"], "embeddings": int(r["n"])})
    return out


def _jp_coverage_2019_2024(conn: sqlite3.Connection) -> dict[str, Any]:
    start = 20190101
    end = 20241231

    patents = int(
        conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM patents
            WHERE country_code = 'JP'
              AND publication_date BETWEEN ? AND ?
            """,
            (start, end),
        ).fetchone()["n"]
    )

    embeddings = int(
        conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM patents p
            JOIN patent_research_data pr
              ON pr.publication_number = p.publication_number
            WHERE p.country_code = 'JP'
              AND p.publication_date BETWEEN ? AND ?
              AND pr.embedding_v1 IS NOT NULL
            """,
            (start, end),
        ).fetchone()["n"]
    )

    return {
        "country_code": "JP",
        "publication_date_range": [start, end],
        "patents": patents,
        "embeddings": embeddings,
        "coverage_pct": round(_pct(embeddings, patents), 2),
    }


def _load_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, obj: dict[str, Any]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
        f.write("\n")


def _index_by_country(rows: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for r in rows:
        cc = str(r.get("country_code"))
        out[cc] = int(r.get("embeddings", 0))
    return out


def _compare_snapshots(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    b_tot = before.get("totals", {})
    a_tot = after.get("totals", {})

    b_by = _index_by_country(before.get("embeddings_by_country_top", []))
    a_by = _index_by_country(after.get("embeddings_by_country_top", []))

    countries = sorted(set(b_by) | set(a_by))
    deltas = []
    for cc in countries:
        d = a_by.get(cc, 0) - b_by.get(cc, 0)
        if d != 0:
            deltas.append({"country_code": cc, "delta_embeddings": d})
    deltas.sort(key=lambda x: abs(int(x["delta_embeddings"])), reverse=True)

    b_jp = before.get("jp_2019_2024", {})
    a_jp = after.get("jp_2019_2024", {})

    return {
        "totals": {
            "patents_delta": int(a_tot.get("patents", 0)) - int(b_tot.get("patents", 0)),
            "research_rows_delta": int(a_tot.get("research_rows", 0))
            - int(b_tot.get("research_rows", 0)),
            "embeddings_delta": int(a_tot.get("embeddings", 0))
            - int(b_tot.get("embeddings", 0)),
        },
        "jp_2019_2024": {
            "patents_delta": int(a_jp.get("patents", 0)) - int(b_jp.get("patents", 0)),
            "embeddings_delta": int(a_jp.get("embeddings", 0)) - int(b_jp.get("embeddings", 0)),
            "coverage_pct_before": b_jp.get("coverage_pct"),
            "coverage_pct_after": a_jp.get("coverage_pct"),
        },
        "embeddings_by_country_top_deltas": deltas[:25],
    }


def _print_summary(metrics: dict[str, Any], compare: dict[str, Any] | None) -> None:
    totals = metrics["totals"]
    jp = metrics["jp_2019_2024"]
    validity = metrics["embedding_validity_sample"]

    print("== Post-ingestion analysis ==")
    print(f"generated_at: {metrics['generated_at']}")
    print(f"db: {metrics['db_path']}")
    print()

    print("-- Totals --")
    print(f"patents:        {totals['patents']:,}")
    print(f"research_rows:  {totals['research_rows']:,}")
    print(f"embeddings:     {totals['embeddings']:,}")
    print()

    print("-- Embeddings by country (top) --")
    for r in metrics["embeddings_by_country_top"][:20]:
        print(f"{r['country_code']:>3}: {r['embeddings']:,}")
    print()

    print("-- JP coverage (2019-2024, publication_date) --")
    print(f"patents:    {jp['patents']:,}")
    print(f"embeddings: {jp['embeddings']:,}")
    print(f"coverage:   {jp['coverage_pct']:.2f}%")
    print()

    print("-- Embedding validity (sample) --")
    print(
        f"expected_dim={validity['expected_dim']} expected_bytes={validity['expected_bytes']} checked={validity['checked']}"
    )
    print(
        f"ok={validity['ok']} bad_length={validity['bad_length']} all_zero={validity['all_zero']} non_finite={validity['non_finite']}"
    )
    print()

    if compare:
        print("-- Before/after comparison --")
        td = compare["totals"]
        jd = compare["jp_2019_2024"]
        print(
            "totals_delta: "
            f"patents={td['patents_delta']:+,} research_rows={td['research_rows_delta']:+,} embeddings={td['embeddings_delta']:+,}"
        )
        print(
            "JP_2019_2024: "
            f"patents={jd['patents_delta']:+,} embeddings={jd['embeddings_delta']:+,} "
            f"coverage={jd['coverage_pct_before']}% -> {jd['coverage_pct_after']}%"
        )
        if compare.get("embeddings_by_country_top_deltas"):
            print("top_country_embedding_deltas:")
            for r in compare["embeddings_by_country_top_deltas"][:10]:
                print(f"  {r['country_code']:>3}: {int(r['delta_embeddings']):+,}")
        print()


def main() -> int:
    parser = argparse.ArgumentParser(description="Post-ingestion analysis and sanity checks")
    parser.add_argument(
        "--db",
        default="~/patent-space-mcp/data/patents.db",
        help="Path to SQLite DB (default: %(default)s)",
    )
    parser.add_argument(
        "--snapshot-out",
        default="",
        help="Write JSON snapshot to this path",
    )
    parser.add_argument(
        "--compare",
        default="",
        help="Compare current metrics to a baseline JSON snapshot",
    )
    parser.add_argument(
        "--sample-embeddings",
        type=int,
        default=500,
        help="Number of embeddings to validate (default: %(default)s)",
    )
    parser.add_argument(
        "--top-countries",
        type=int,
        default=50,
        help="Countries to include in top list/snapshot (default: %(default)s)",
    )

    args = parser.parse_args()

    db_path = _expand(args.db)
    if not os.path.exists(db_path):
        raise SystemExit(f"DB not found: {db_path}")

    baseline: dict[str, Any] | None = None
    if args.compare:
        baseline = _load_json(_expand(args.compare))

    with _connect(db_path) as conn:
        top_lengths = _top_embedding_blob_lengths(conn)
        expected_dim, expected_bytes = _infer_expected_embedding_shape(top_lengths)
        validity = _check_embedding_validity(
            conn,
            expected_dim=expected_dim,
            expected_bytes=expected_bytes,
            sample_n=max(0, int(args.sample_embeddings)),
        )

        metrics: dict[str, Any] = {
            "generated_at": _iso_now_utc(),
            "db_path": db_path,
            "totals": _get_totals(conn),
            "embedding_blob": {
                "top_lengths": top_lengths,
                "expected_dim": expected_dim,
                "expected_bytes": expected_bytes,
            },
            "embedding_validity_sample": {
                "expected_dim": validity.expected_dim,
                "expected_bytes": validity.expected_bytes,
                "checked": validity.checked,
                "bad_length": validity.bad_length,
                "all_zero": validity.all_zero,
                "non_finite": validity.non_finite,
                "ok": validity.ok,
            },
            "embeddings_by_country_top": _embeddings_by_country(conn, top_n=int(args.top_countries)),
            "jp_2019_2024": _jp_coverage_2019_2024(conn),
        }

    compare_out: dict[str, Any] | None = None
    if baseline is not None:
        compare_out = _compare_snapshots(baseline, metrics)
        metrics["comparison"] = compare_out

    if args.snapshot_out:
        _write_json(_expand(args.snapshot_out), metrics)

    _print_summary(metrics, compare_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
