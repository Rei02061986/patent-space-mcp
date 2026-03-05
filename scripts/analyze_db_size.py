"""Analyze SQLite database size breakdown and storage hotspots."""
from __future__ import annotations

import argparse
import os
import sqlite3
from dataclasses import dataclass


@dataclass
class NullColumnStat:
    table: str
    column: str
    null_count: int
    total_rows: int

    @property
    def null_pct(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return (self.null_count / self.total_rows) * 100.0


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _fmt_gb(size_bytes: float) -> str:
    return f"{size_bytes / (1024 ** 3):.2f} GB"


def _fmt_pct(value: float) -> str:
    return f"{value:.1f}%"


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _has_dbstat(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute("SELECT name, pgsize FROM dbstat LIMIT 1").fetchone()
        return True
    except sqlite3.OperationalError:
        return False


def _get_user_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [r["name"] for r in rows]


def _get_indexes(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    rows = conn.execute(
        """
        SELECT name, tbl_name
        FROM sqlite_master
        WHERE type = 'index'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [(r["name"], r["tbl_name"]) for r in rows]


def _get_row_counts(conn: sqlite3.Connection, table_names: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in table_names:
        quoted = _quote_ident(table)
        row = conn.execute(f"SELECT COUNT(*) AS n FROM {quoted}").fetchone()
        counts[table] = int(row["n"])
    return counts


def _get_object_sizes_from_dbstat(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT name, SUM(pgsize) AS bytes
        FROM dbstat
        GROUP BY name
        """
    ).fetchall()
    return {r["name"]: int(r["bytes"]) for r in rows if r["name"] is not None}


def _estimate_sizes_without_dbstat(
    used_bytes: int,
    tables: list[str],
    row_counts: dict[str, int],
    indexes: list[tuple[str, str]],
    conn: sqlite3.Connection,
) -> dict[str, int]:
    weights: dict[str, int] = {}
    for table in tables:
        weights[table] = max(1, row_counts.get(table, 0))

    for index_name, table_name in indexes:
        idx_info = conn.execute(f"PRAGMA index_info({_quote_ident(index_name)})").fetchall()
        indexed_cols = max(1, len(idx_info))
        table_rows = max(1, row_counts.get(table_name, 0))
        weights[index_name] = table_rows * indexed_cols

    total_weight = sum(weights.values()) or 1
    return {
        name: int(round((weight / total_weight) * used_bytes))
        for name, weight in weights.items()
    }


def _null_stats_for_table(
    conn: sqlite3.Connection, table_name: str, total_rows: int
) -> list[NullColumnStat]:
    cols = conn.execute(f"PRAGMA table_info({_quote_ident(table_name)})").fetchall()
    if not cols:
        return []

    select_parts: list[str] = []
    for col in cols:
        col_name = str(col["name"])
        q_col = _quote_ident(col_name)
        alias = col_name.replace('"', '""')
        select_parts.append(f"SUM(CASE WHEN {q_col} IS NULL THEN 1 ELSE 0 END) AS {alias}")
    query = f"SELECT {', '.join(select_parts)} FROM {_quote_ident(table_name)}"
    row = conn.execute(query).fetchone()

    stats: list[NullColumnStat] = []
    for col in cols:
        col_name = str(col["name"])
        stats.append(
            NullColumnStat(
                table=table_name,
                column=col_name,
                null_count=int(row[col_name]),
                total_rows=total_rows,
            )
        )
    return stats


def _index_columns(conn: sqlite3.Connection, index_name: str) -> tuple[str, ...]:
    rows = conn.execute(f"PRAGMA index_info({_quote_ident(index_name)})").fetchall()
    cols = []
    for r in rows:
        col_name = r["name"]
        if col_name is None:
            cols.append("<expr>")
        else:
            cols.append(str(col_name))
    return tuple(cols)


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze SQLite DB size breakdown")
    parser.add_argument(
        "--db",
        default="data/patents.db",
        help="Path to SQLite DB (default: data/patents.db)",
    )
    args = parser.parse_args()

    db_path = args.db
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")

    total_file_size = os.path.getsize(db_path)
    conn = _connect(db_path)
    try:
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        free_pct = (freelist_count / page_count * 100.0) if page_count else 0.0
        used_bytes = (page_count - freelist_count) * page_size

        tables = _get_user_tables(conn)
        indexes = _get_indexes(conn)
        row_counts = _get_row_counts(conn, tables)
        dbstat_available = _has_dbstat(conn)

        if dbstat_available:
            object_sizes = _get_object_sizes_from_dbstat(conn)
        else:
            object_sizes = _estimate_sizes_without_dbstat(
                used_bytes=used_bytes,
                tables=tables,
                row_counts=row_counts,
                indexes=indexes,
                conn=conn,
            )

        table_sizes = {t: object_sizes.get(t, 0) for t in tables}
        index_sizes = {idx: object_sizes.get(idx, 0) for idx, _ in indexes}

        fts_shadow_tables = sorted([t for t in tables if t.startswith("patents_fts")])
        fts_sizes = {t: table_sizes.get(t, 0) for t in fts_shadow_tables}

        null_stats: list[NullColumnStat] = []
        for table_name in ("patents", "patent_assignees"):
            if table_name in row_counts:
                null_stats.extend(
                    _null_stats_for_table(
                        conn=conn,
                        table_name=table_name,
                        total_rows=row_counts[table_name],
                    )
                )

        # Redundancy signal: same table + same indexed columns in multiple indexes.
        index_groups: dict[tuple[str, tuple[str, ...]], list[str]] = {}
        for index_name, table_name in indexes:
            cols = _index_columns(conn, index_name)
            key = (table_name, cols)
            index_groups.setdefault(key, []).append(index_name)

        potentially_redundant_groups = [
            (table_name, cols, sorted(names))
            for (table_name, cols), names in index_groups.items()
            if len(names) > 1
        ]
    finally:
        conn.close()

    print("=== Database Size Analysis ===")
    print(f"Total file size: {_fmt_gb(total_file_size)}")
    print(f"Page size: {page_size:,} bytes")
    print(f"Total pages: {page_count:,}")
    print(f"Free pages: {freelist_count:,} ({_fmt_pct(free_pct)})")
    if not dbstat_available:
        print("Note: dbstat unavailable; size values are row-weighted estimates.")
    print("")

    print("=== Table Sizes ===")
    print(f"{'Table Name':<24}{'Rows':>14}   {'Est. Size':>12}")
    for table_name, size_bytes in sorted(
        table_sizes.items(), key=lambda x: x[1], reverse=True
    ):
        rows = row_counts.get(table_name, 0)
        print(f"{table_name:<24}{rows:>14,}   {_fmt_gb(size_bytes):>12}")
    print("")

    print("=== Index Sizes ===")
    print(f"{'Index Name':<32}{'Table':<24}{'Est. Size':>12}")
    for index_name, table_name in sorted(
        indexes, key=lambda x: index_sizes.get(x[0], 0), reverse=True
    ):
        size_bytes = index_sizes.get(index_name, 0)
        print(f"{index_name:<32}{table_name:<24}{_fmt_gb(size_bytes):>12}")
    print("")

    print("=== FTS5 Shadow Tables ===")
    print(f"{'Table Name':<24}{'Rows':>14}   {'Est. Size':>12}")
    if fts_shadow_tables:
        for table_name in fts_shadow_tables:
            print(
                f"{table_name:<24}{row_counts.get(table_name, 0):>14,}   "
                f"{_fmt_gb(fts_sizes.get(table_name, 0)):>12}"
            )
    else:
        print("No patents_fts* tables found.")
    print("")

    print("=== NULL Column Analysis ===")
    print(f"{'Table':<20}{'Column':<28}{'NULL %':>8}   {'NULL/Total':>20}")
    for stat in sorted(null_stats, key=lambda s: (s.table, -s.null_pct, s.column)):
        print(
            f"{stat.table:<20}{stat.column:<28}{_fmt_pct(stat.null_pct):>8}   "
            f"{stat.null_count:,}/{stat.total_rows:,}"
        )
    print("")

    print("=== Row Counts (All Tables) ===")
    print(f"{'Table Name':<24}{'Rows':>14}")
    for table_name in sorted(tables):
        print(f"{table_name:<24}{row_counts.get(table_name, 0):>14,}")
    print("")

    print("=== Recommendations ===")
    if freelist_count > 0:
        reclaimable = freelist_count * page_size
        print(
            f"- VACUUM recommended: {freelist_count:,} free pages "
            f"({_fmt_gb(reclaimable)}) can potentially be reclaimed."
        )
    else:
        print("- VACUUM not urgent: no free pages reported.")

    large_index_threshold = max(int(total_file_size * 0.05), 512 * 1024 * 1024)
    large_indexes = sorted(
        [(name, size) for name, size in index_sizes.items() if size >= large_index_threshold],
        key=lambda x: x[1],
        reverse=True,
    )
    if large_indexes:
        print("- Large indexes to review for redundancy/workload fit:")
        for name, size in large_indexes[:10]:
            print(f"  * {name}: {_fmt_gb(size)}")
    else:
        print("- No unusually large indexes crossed review threshold.")

    if potentially_redundant_groups:
        print("- Potentially redundant index groups (same table + same columns):")
        for table_name, cols, names in potentially_redundant_groups:
            col_text = ", ".join(cols) if cols else "<none>"
            name_text = ", ".join(names)
            print(f"  * {table_name} ({col_text}): {name_text}")

    high_null_columns = [s for s in null_stats if s.null_pct > 90.0]
    if high_null_columns:
        print("- Columns with >90% NULL (candidates for removal or normalization):")
        for stat in sorted(high_null_columns, key=lambda s: s.null_pct, reverse=True):
            print(f"  * {stat.table}.{stat.column}: {_fmt_pct(stat.null_pct)} NULL")
    else:
        print("- No columns above 90% NULL in patents/patent_assignees.")


if __name__ == "__main__":
    main()
