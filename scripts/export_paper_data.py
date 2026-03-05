"""Export paper-ready datasets from the patent database for academic publication.

Generates CSV files and a Markdown statistical summary suitable for
inclusion in research papers. Uses only stdlib modules (no pandas).

Usage:
    python scripts/export_paper_data.py --db data/patents.db --output-dir paper_data/
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import sqlite3
import statistics
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TABLE_DESCRIPTIONS: dict[str, str] = {
    "patents": "Main patent metadata (publication_number, dates, titles, abstracts)",
    "patent_cpc": "CPC classification codes per patent",
    "patent_assignees": "Assignee names with firm_id linkage",
    "patent_inventors": "Inventor names per patent",
    "patent_citations": "Forward and backward citation links",
    "patent_research_data": "Embeddings and top terms from Google Patents Research",
    "citation_counts": "Pre-aggregated forward citation counts",
    "gdelt_company_features": "GDELT-derived five-axis company features per quarter",
    "firm_tech_vectors": "Per-firm-per-year 64-dim technology vectors and CPC metrics",
    "tech_clusters": "Technology clusters with center vectors, labels, growth rates",
    "patent_cluster_mapping": "Patent-to-cluster assignment with distance",
    "startability_surface": "Startability S(v,f,t) scores for firm-cluster-year triples",
    "tech_cluster_momentum": "Cluster momentum: patent_count, growth_rate, acceleration by year",
    "patent_legal_status": "Derived legal status (alive/expired/abandoned/pending)",
    "patent_value_index": "Composite patent value scores (citations, family, recency, momentum)",
    "patent_family": "Patent family membership and family sizes",
    "patent_litigation": "Litigation data (cases, plaintiffs, defendants, outcomes)",
    "patents_fts": "Full-text search index on patent titles (FTS5 virtual table)",
    "ingestion_log": "Batch ingestion progress tracking",
}


def _connect(db_path: str) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _write_csv(
    filepath: str,
    header: list[str],
    rows: list[list],
) -> int:
    """Write rows to a CSV file and return the number of data rows written."""
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)
    return len(rows)


def _safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0


def _skewness(values: list[float]) -> float:
    """Compute Fisher-Pearson skewness coefficient."""
    n = len(values)
    if n < 3:
        return 0.0
    mean = statistics.mean(values)
    sd = statistics.stdev(values)
    if sd == 0:
        return 0.0
    m3 = sum((x - mean) ** 3 for x in values) / n
    return m3 / (sd ** 3)


def _get_user_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [r["name"] for r in rows]


def _row_count(conn: sqlite3.Connection, table: str) -> int:
    try:
        row = conn.execute(f'SELECT COUNT(*) AS n FROM "{table}"').fetchone()
        return int(row["n"])
    except sqlite3.OperationalError:
        return 0


# ---------------------------------------------------------------------------
# Export 1: Database Summary
# ---------------------------------------------------------------------------

def export_table1_database_summary(conn: sqlite3.Connection, out_dir: str) -> str:
    log.info("Exporting table1_database_summary.csv ...")
    tables = _get_user_tables(conn)
    rows: list[list] = []
    for table in sorted(tables):
        count = _row_count(conn, table)
        desc = TABLE_DESCRIPTIONS.get(table, "")
        rows.append([table, count, desc])

    filepath = os.path.join(out_dir, "table1_database_summary.csv")
    _write_csv(filepath, ["table_name", "row_count", "description"], rows)
    log.info("  -> %d tables written to %s", len(rows), filepath)
    return filepath


# ---------------------------------------------------------------------------
# Export 2: Startability Distribution
# ---------------------------------------------------------------------------

def export_table2_startability_distribution(conn: sqlite3.Connection, out_dir: str) -> str:
    log.info("Exporting table2_startability_distribution.csv ...")

    year_rows = conn.execute(
        """
        SELECT year, score, gate_open
        FROM startability_surface
        ORDER BY year
        """
    ).fetchall()

    # Group by year
    year_data: dict[int, list[float]] = defaultdict(list)
    year_gate: dict[int, list[int]] = defaultdict(list)
    for r in year_rows:
        yr = r["year"]
        year_data[yr].append(r["score"] if r["score"] is not None else 0.0)
        year_gate[yr].append(r["gate_open"] if r["gate_open"] is not None else 0)

    rows: list[list] = []
    for yr in sorted(year_data.keys()):
        scores = year_data[yr]
        gates = year_gate[yr]
        n = len(scores)
        if n == 0:
            continue
        sorted_scores = sorted(scores)
        q1_idx = max(0, int(n * 0.25) - 1)
        q3_idx = min(n - 1, int(n * 0.75))
        mean_val = statistics.mean(scores)
        std_val = statistics.stdev(scores) if n > 1 else 0.0
        median_val = statistics.median(scores)
        min_val = min(scores)
        max_val = max(scores)
        q1 = sorted_scores[q1_idx]
        q3 = sorted_scores[q3_idx]
        skew = _skewness(scores)
        gate_open_ratio = _safe_div(sum(gates), len(gates))
        rows.append([
            yr, n, f"{mean_val:.6f}", f"{std_val:.6f}", f"{median_val:.6f}",
            f"{min_val:.6f}", f"{max_val:.6f}", f"{q1:.6f}", f"{q3:.6f}",
            f"{skew:.4f}", f"{gate_open_ratio:.4f}",
        ])

    filepath = os.path.join(out_dir, "table2_startability_distribution.csv")
    _write_csv(
        filepath,
        [
            "year", "n_pairs", "mean", "std", "median",
            "min", "max", "Q1", "Q3", "skewness", "gate_open_ratio",
        ],
        rows,
    )
    log.info("  -> %d year rows written to %s", len(rows), filepath)
    return filepath


# ---------------------------------------------------------------------------
# Export 3: Top Firms by Startability
# ---------------------------------------------------------------------------

def export_table3_top_firms(conn: sqlite3.Connection, out_dir: str) -> str:
    log.info("Exporting table3_top_firms_by_startability.csv ...")

    # Find the latest year in the surface
    latest_row = conn.execute(
        "SELECT MAX(year) AS y FROM startability_surface"
    ).fetchone()
    latest_year = latest_row["y"] if latest_row and latest_row["y"] else 2024

    firm_rows = conn.execute(
        """
        SELECT
            s.firm_id,
            AVG(s.score) AS avg_score,
            MAX(s.score) AS max_score,
            SUM(s.gate_open) AS cluster_count_open
        FROM startability_surface s
        WHERE s.year = ?
        GROUP BY s.firm_id
        ORDER BY avg_score DESC
        LIMIT 50
        """,
        (latest_year,),
    ).fetchall()

    # Fetch patent_count and tech_diversity from firm_tech_vectors
    firm_meta: dict[str, dict] = {}
    meta_rows = conn.execute(
        """
        SELECT firm_id, patent_count, tech_diversity
        FROM firm_tech_vectors
        WHERE year = ?
        """,
        (latest_year,),
    ).fetchall()
    for mr in meta_rows:
        firm_meta[mr["firm_id"]] = {
            "patent_count": mr["patent_count"] or 0,
            "tech_diversity": mr["tech_diversity"] or 0.0,
        }

    rows: list[list] = []
    for r in firm_rows:
        fid = r["firm_id"]
        meta = firm_meta.get(fid, {"patent_count": 0, "tech_diversity": 0.0})
        rows.append([
            fid,
            f"{r['avg_score']:.6f}",
            f"{r['max_score']:.6f}",
            r["cluster_count_open"],
            meta["patent_count"],
            f"{meta['tech_diversity']:.4f}",
        ])

    filepath = os.path.join(out_dir, "table3_top_firms_by_startability.csv")
    _write_csv(
        filepath,
        ["firm_id", "avg_score", "max_score", "cluster_count_gate_open",
         "patent_count", "tech_diversity"],
        rows,
    )
    log.info("  -> %d firms written to %s (year=%d)", len(rows), filepath, latest_year)
    return filepath


# ---------------------------------------------------------------------------
# Export 4: Cluster Momentum Top 20
# ---------------------------------------------------------------------------

def export_table4_cluster_momentum(conn: sqlite3.Connection, out_dir: str) -> str:
    log.info("Exporting table4_cluster_momentum_top20.csv ...")

    # Latest year in momentum
    latest_row = conn.execute(
        "SELECT MAX(year) AS y FROM tech_cluster_momentum"
    ).fetchone()
    latest_year = latest_row["y"] if latest_row and latest_row["y"] else 2024

    cluster_rows = conn.execute(
        """
        SELECT
            m.cluster_id,
            tc.cpc_class,
            tc.label,
            m.patent_count,
            m.growth_rate,
            m.acceleration,
            tc.top_applicants
        FROM tech_cluster_momentum m
        JOIN tech_clusters tc ON tc.cluster_id = m.cluster_id
        WHERE m.year = ?
        ORDER BY m.growth_rate DESC
        LIMIT 20
        """,
        (latest_year,),
    ).fetchall()

    rows: list[list] = []
    for r in cluster_rows:
        # Parse top_applicants JSON to get top 3 firm names
        top_firms = ["", "", ""]
        if r["top_applicants"]:
            try:
                applicants = json.loads(r["top_applicants"])
                if isinstance(applicants, list):
                    for i, entry in enumerate(applicants[:3]):
                        if isinstance(entry, dict):
                            top_firms[i] = entry.get("name", entry.get("firm_id", ""))
                        elif isinstance(entry, str):
                            top_firms[i] = entry
                elif isinstance(applicants, dict):
                    # Might be {name: count, ...}
                    sorted_app = sorted(applicants.items(), key=lambda x: x[1], reverse=True)
                    for i, (name, _) in enumerate(sorted_app[:3]):
                        top_firms[i] = name
            except (json.JSONDecodeError, TypeError):
                pass

        rows.append([
            r["cluster_id"],
            r["cpc_class"] or "",
            r["label"] or "",
            r["patent_count"] or 0,
            f"{r['growth_rate']:.4f}" if r["growth_rate"] is not None else "",
            f"{r['acceleration']:.4f}" if r["acceleration"] is not None else "",
            top_firms[0],
            top_firms[1],
            top_firms[2],
        ])

    filepath = os.path.join(out_dir, "table4_cluster_momentum_top20.csv")
    _write_csv(
        filepath,
        ["cluster_id", "cpc_class", "label", "patent_count",
         "growth_rate", "acceleration", "top_firm_1", "top_firm_2", "top_firm_3"],
        rows,
    )
    log.info("  -> %d clusters written to %s", len(rows), filepath)
    return filepath


# ---------------------------------------------------------------------------
# Export 5: phi_tech Component Analysis
# ---------------------------------------------------------------------------

def export_table5_phi_tech_components(conn: sqlite3.Connection, out_dir: str) -> str:
    log.info("Exporting table5_phi_tech_components.csv ...")

    component_rows = conn.execute(
        """
        SELECT phi_tech_cos, phi_tech_dist, phi_tech_cpc, phi_tech_cite
        FROM startability_surface
        WHERE gate_open = 1
        """
    ).fetchall()

    if not component_rows:
        log.warning("  No gate_open=1 rows found in startability_surface")
        filepath = os.path.join(out_dir, "table5_phi_tech_components.csv")
        _write_csv(filepath, ["metric", "phi_cos", "phi_dist", "phi_cpc", "phi_cite"], [])
        return filepath

    cos_vals = [r["phi_tech_cos"] or 0.0 for r in component_rows]
    dist_vals = [r["phi_tech_dist"] or 0.0 for r in component_rows]
    cpc_vals = [r["phi_tech_cpc"] or 0.0 for r in component_rows]
    cite_vals = [r["phi_tech_cite"] or 0.0 for r in component_rows]

    n = len(cos_vals)
    components = [cos_vals, dist_vals, cpc_vals, cite_vals]
    names = ["phi_cos", "phi_dist", "phi_cpc", "phi_cite"]
    # Calibrated beta weights from startability.py
    beta_weights = [6.0, 3.0, 2.0, 1.0]

    # Compute means and stds
    means = [statistics.mean(c) for c in components]
    stds = [statistics.stdev(c) if n > 1 else 0.0 for c in components]

    # Correlation matrix (Pearson)
    def _pearson(xs: list[float], ys: list[float]) -> float:
        if len(xs) < 2:
            return 0.0
        mx, my = statistics.mean(xs), statistics.mean(ys)
        sx = math.sqrt(sum((x - mx) ** 2 for x in xs) / (len(xs) - 1))
        sy = math.sqrt(sum((y - my) ** 2 for y in ys) / (len(ys) - 1))
        if sx == 0 or sy == 0:
            return 0.0
        cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (len(xs) - 1)
        return cov / (sx * sy)

    rows: list[list] = []

    # Mean row
    rows.append(["mean"] + [f"{m:.6f}" for m in means])

    # Std row
    rows.append(["std"] + [f"{s:.6f}" for s in stds])

    # Beta weight row
    rows.append(["beta_weight"] + [f"{b:.1f}" for b in beta_weights])

    # Contribution (beta * mean)
    contributions = [beta_weights[i] * means[i] for i in range(4)]
    rows.append(["contribution_beta_x_mean"] + [f"{c:.6f}" for c in contributions])

    # Correlation matrix rows
    for i, name_i in enumerate(names):
        corr_row = [f"corr_{name_i}"]
        for j in range(4):
            corr_row.append(f"{_pearson(components[i], components[j]):.4f}")
        rows.append(corr_row)

    # N observations
    rows.append(["n_gate_open", str(n), "", "", ""])

    filepath = os.path.join(out_dir, "table5_phi_tech_components.csv")
    _write_csv(filepath, ["metric", "phi_cos", "phi_dist", "phi_cpc", "phi_cite"], rows)
    log.info("  -> Written to %s (n=%d gate_open pairs)", filepath, n)
    return filepath


# ---------------------------------------------------------------------------
# Export 6: Startability Delta Summary
# ---------------------------------------------------------------------------

def export_table6_startability_delta(conn: sqlite3.Connection, out_dir: str) -> str:
    log.info("Exporting table6_startability_delta_summary.csv ...")

    # Get distinct years
    year_list = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT year FROM startability_surface ORDER BY year"
        ).fetchall()
    ]

    if len(year_list) < 2:
        log.warning("  Fewer than 2 years in startability_surface; skipping delta")
        filepath = os.path.join(out_dir, "table6_startability_delta_summary.csv")
        _write_csv(
            filepath,
            ["year_from", "year_to", "n_nonzero_deltas", "mean_abs_delta",
             "max_gainer_firm", "max_gainer_cluster", "max_gainer_delta",
             "max_loser_firm", "max_loser_cluster", "max_loser_delta"],
            [],
        )
        return filepath

    rows: list[list] = []
    for idx in range(len(year_list) - 1):
        y1, y2 = year_list[idx], year_list[idx + 1]
        log.info("  Computing delta %d -> %d ...", y1, y2)

        delta_rows = conn.execute(
            """
            SELECT
                a.firm_id,
                a.cluster_id,
                (b.score - a.score) AS delta
            FROM startability_surface a
            JOIN startability_surface b
              ON b.firm_id = a.firm_id
             AND b.cluster_id = a.cluster_id
             AND b.year = ?
            WHERE a.year = ?
            """,
            (y2, y1),
        ).fetchall()

        deltas = [(r["firm_id"], r["cluster_id"], r["delta"] or 0.0) for r in delta_rows]
        nonzero = [(fid, cid, d) for fid, cid, d in deltas if abs(d) > 1e-9]

        if not nonzero:
            rows.append([
                y1, y2, 0, "0.000000",
                "", "", "", "", "", "",
            ])
            continue

        abs_deltas = [abs(d) for _, _, d in nonzero]
        mean_abs = statistics.mean(abs_deltas)

        # Max gainer
        gainer = max(nonzero, key=lambda x: x[2])
        # Max loser
        loser = min(nonzero, key=lambda x: x[2])

        rows.append([
            y1, y2, len(nonzero), f"{mean_abs:.6f}",
            gainer[0], gainer[1], f"{gainer[2]:.6f}",
            loser[0], loser[1], f"{loser[2]:.6f}",
        ])

    filepath = os.path.join(out_dir, "table6_startability_delta_summary.csv")
    _write_csv(
        filepath,
        ["year_from", "year_to", "n_nonzero_deltas", "mean_abs_delta",
         "max_gainer_firm", "max_gainer_cluster", "max_gainer_delta",
         "max_loser_firm", "max_loser_cluster", "max_loser_delta"],
        rows,
    )
    log.info("  -> %d year-pair rows written to %s", len(rows), filepath)
    return filepath


# ---------------------------------------------------------------------------
# Export 7: Cross-Sector Analysis
# ---------------------------------------------------------------------------

def export_table7_cross_sector(conn: sqlite3.Connection, out_dir: str) -> str:
    log.info("Exporting table7_cross_sector_analysis.csv ...")

    # Get CPC sections per firm (A, B, C, ... H, Y)
    log.info("  Loading firm CPC sections ...")
    firm_sections: dict[str, set[str]] = defaultdict(set)
    cursor = conn.execute(
        """
        SELECT pa.firm_id, SUBSTR(pc.cpc_code, 1, 1) AS section
        FROM patent_assignees pa
        JOIN patent_cpc pc ON pc.publication_number = pa.publication_number
        WHERE pa.firm_id IS NOT NULL AND pa.firm_id <> ''
          AND pc.cpc_code IS NOT NULL AND LENGTH(pc.cpc_code) >= 1
        GROUP BY pa.firm_id, section
        """
    )
    for r in cursor:
        firm_sections[r["firm_id"]].add(r["section"])

    log.info("  %d firms with CPC section data", len(firm_sections))

    # Build all section pairs and count firms active in both
    all_sections = sorted(set(s for secs in firm_sections.values() for s in secs))
    pair_counts: dict[tuple[str, str], int] = {}
    for i, s1 in enumerate(all_sections):
        for s2 in all_sections[i + 1:]:
            count = sum(
                1 for secs in firm_sections.values()
                if s1 in secs and s2 in secs
            )
            pair_counts[(s1, s2)] = count

    rows: list[list] = []
    for (s1, s2), count in sorted(pair_counts.items(), key=lambda x: x[1], reverse=True):
        rows.append([f"{s1}x{s2}", s1, s2, count])

    filepath = os.path.join(out_dir, "table7_cross_sector_analysis.csv")
    _write_csv(filepath, ["section_pair", "section_1", "section_2", "firm_count_both"], rows)
    log.info("  -> %d section pairs written to %s", len(rows), filepath)
    return filepath


# ---------------------------------------------------------------------------
# Export 8: Filing Trend by Country
# ---------------------------------------------------------------------------

def export_figure_data_filing_trend(conn: sqlite3.Connection, out_dir: str) -> str:
    log.info("Exporting figure_data_filing_trend.csv ...")

    cursor = conn.execute(
        """
        SELECT filing_date / 10000 AS filing_year, country_code, COUNT(*) AS cnt
        FROM patents
        WHERE filing_date IS NOT NULL AND filing_date > 0
        GROUP BY filing_year, country_code
        ORDER BY filing_year, country_code
        """
    )

    year_country: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    all_countries: set[str] = set()
    for r in cursor:
        yr = r["filing_year"]
        cc = r["country_code"]
        if yr and yr > 1900 and cc:
            year_country[yr][cc] = r["cnt"]
            all_countries.add(cc)

    # Sort countries: JP, US, EP first, then alphabetical
    priority = {"JP": 0, "US": 1, "EP": 2, "WO": 3}
    sorted_countries = sorted(
        all_countries,
        key=lambda c: (priority.get(c, 100), c),
    )

    header = ["year"] + [f"{c}_count" for c in sorted_countries]
    rows: list[list] = []
    for yr in sorted(year_country.keys()):
        row = [yr]
        for cc in sorted_countries:
            row.append(year_country[yr].get(cc, 0))
        rows.append(row)

    filepath = os.path.join(out_dir, "figure_data_filing_trend.csv")
    _write_csv(filepath, header, rows)
    log.info("  -> %d years x %d countries written to %s", len(rows), len(sorted_countries), filepath)
    return filepath


# ---------------------------------------------------------------------------
# Export 9: Gate Function Threshold Analysis
# ---------------------------------------------------------------------------

def export_figure_data_gate_function(conn: sqlite3.Connection, out_dir: str) -> str:
    log.info("Exporting figure_data_gate_function.csv ...")

    cursor = conn.execute(
        """
        SELECT phi_tech_cos, gate_open
        FROM startability_surface
        WHERE phi_tech_cos IS NOT NULL
        """
    )

    # Bin cos_sim into 0.1 increments from 0.0 to 1.0
    bin_total: dict[str, int] = {}
    bin_open: dict[str, int] = {}
    bin_labels = []
    for i in range(10):
        lo = i * 0.1
        hi = (i + 1) * 0.1
        label = f"{lo:.1f}-{hi:.1f}"
        bin_labels.append(label)
        bin_total[label] = 0
        bin_open[label] = 0

    # Also handle negative cos_sim values
    neg_label = "<0.0"
    bin_labels.insert(0, neg_label)
    bin_total[neg_label] = 0
    bin_open[neg_label] = 0

    for r in cursor:
        cos_val = r["phi_tech_cos"]
        go = r["gate_open"] or 0
        if cos_val < 0:
            label = neg_label
        else:
            bin_idx = min(int(cos_val * 10), 9)
            lo = bin_idx * 0.1
            hi = (bin_idx + 1) * 0.1
            label = f"{lo:.1f}-{hi:.1f}"
        bin_total[label] = bin_total.get(label, 0) + 1
        if go:
            bin_open[label] = bin_open.get(label, 0) + 1

    rows: list[list] = []
    for label in bin_labels:
        total = bin_total.get(label, 0)
        opened = bin_open.get(label, 0)
        rate = _safe_div(opened, total)
        rows.append([label, total, opened, f"{rate:.4f}"])

    filepath = os.path.join(out_dir, "figure_data_gate_function.csv")
    _write_csv(filepath, ["cos_sim_bin", "total_pairs", "gate_open_count", "gate_open_rate"], rows)
    log.info("  -> %d bins written to %s", len(rows), filepath)
    return filepath


# ---------------------------------------------------------------------------
# Export 10: Stats Summary (Markdown)
# ---------------------------------------------------------------------------

def export_stats_summary(conn: sqlite3.Connection, out_dir: str) -> str:
    log.info("Generating stats_summary.md ...")

    lines: list[str] = []
    lines.append("# Patent Space MCP -- Statistical Summary for Paper")
    lines.append("")
    lines.append(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # --- 1. Database overview ---
    lines.append("## 1. Database Overview")
    lines.append("")
    tables = _get_user_tables(conn)
    total_rows = 0
    for t in tables:
        c = _row_count(conn, t)
        total_rows += c
    lines.append(f"- Total tables: {len(tables)}")
    lines.append(f"- Total rows (all tables): {total_rows:,}")
    lines.append("")

    # Key table counts
    key_tables = [
        "patents", "patent_cpc", "patent_assignees", "patent_citations",
        "patent_research_data", "firm_tech_vectors", "tech_clusters",
        "startability_surface", "tech_cluster_momentum",
        "patent_value_index", "gdelt_company_features",
    ]
    lines.append("| Table | Row Count |")
    lines.append("|-------|-----------|")
    for t in key_tables:
        c = _row_count(conn, t)
        lines.append(f"| {t} | {c:,} |")
    lines.append("")

    # --- 2. Patent coverage ---
    lines.append("## 2. Patent Coverage")
    lines.append("")

    # By country
    country_rows = conn.execute(
        """
        SELECT country_code, COUNT(*) AS cnt
        FROM patents
        GROUP BY country_code
        ORDER BY cnt DESC
        LIMIT 10
        """
    ).fetchall()
    lines.append("### Top countries by patent count")
    lines.append("")
    lines.append("| Country | Count |")
    lines.append("|---------|-------|")
    for r in country_rows:
        lines.append(f"| {r['country_code']} | {r['cnt']:,} |")
    lines.append("")

    # Filing year range
    year_range = conn.execute(
        """
        SELECT MIN(filing_date / 10000) AS min_year, MAX(filing_date / 10000) AS max_year
        FROM patents
        WHERE filing_date > 0
        """
    ).fetchone()
    if year_range:
        lines.append(f"- Filing year range: {year_range['min_year']} -- {year_range['max_year']}")
    lines.append("")

    # --- 3. Entity coverage ---
    lines.append("## 3. Entity (Firm) Coverage")
    lines.append("")

    firm_count = conn.execute(
        "SELECT COUNT(DISTINCT firm_id) AS n FROM patent_assignees WHERE firm_id IS NOT NULL AND firm_id <> ''"
    ).fetchone()
    lines.append(f"- Distinct firms with patent linkage: {firm_count['n']:,}")

    ftv_count = conn.execute(
        "SELECT COUNT(DISTINCT firm_id) AS n FROM firm_tech_vectors WHERE tech_vector IS NOT NULL"
    ).fetchone()
    lines.append(f"- Firms with tech vectors: {ftv_count['n']:,}")
    lines.append("")

    # --- 4. Embedding coverage ---
    lines.append("## 4. Embedding Coverage")
    lines.append("")

    total_patents = _row_count(conn, "patents")
    embed_count = conn.execute(
        "SELECT COUNT(*) AS n FROM patent_research_data WHERE embedding_v1 IS NOT NULL"
    ).fetchone()
    embed_n = embed_count["n"]
    embed_pct = _safe_div(embed_n, total_patents) * 100
    lines.append(f"- Patents with embeddings: {embed_n:,} / {total_patents:,} ({embed_pct:.1f}%)")
    lines.append("")

    # Embedding coverage by year
    embed_by_year = conn.execute(
        """
        SELECT p.filing_date / 10000 AS yr,
               COUNT(*) AS total,
               SUM(CASE WHEN prd.embedding_v1 IS NOT NULL THEN 1 ELSE 0 END) AS with_embed
        FROM patents p
        LEFT JOIN patent_research_data prd ON prd.publication_number = p.publication_number
        WHERE p.filing_date > 0
        GROUP BY yr
        HAVING yr >= 2000
        ORDER BY yr
        """
    ).fetchall()
    if embed_by_year:
        lines.append("| Year | Total | With Embedding | Coverage % |")
        lines.append("|------|-------|----------------|------------|")
        for r in embed_by_year:
            pct = _safe_div(r["with_embed"], r["total"]) * 100
            lines.append(f"| {r['yr']} | {r['total']:,} | {r['with_embed']:,} | {pct:.1f}% |")
        lines.append("")

    # --- 5. Startability score statistics ---
    lines.append("## 5. Startability Score Statistics")
    lines.append("")

    surface_count = _row_count(conn, "startability_surface")
    lines.append(f"- Total surface entries: {surface_count:,}")

    gate_stats = conn.execute(
        """
        SELECT
            SUM(gate_open) AS n_open,
            COUNT(*) AS n_total,
            AVG(score) AS avg_score,
            AVG(CASE WHEN gate_open = 1 THEN score END) AS avg_score_open,
            MIN(score) AS min_score,
            MAX(score) AS max_score
        FROM startability_surface
        """
    ).fetchone()
    if gate_stats and gate_stats["n_total"]:
        open_pct = _safe_div(gate_stats["n_open"] or 0, gate_stats["n_total"]) * 100
        lines.append(f"- Gate open: {gate_stats['n_open']:,} / {gate_stats['n_total']:,} ({open_pct:.1f}%)")
        lines.append(f"- Average score (all): {gate_stats['avg_score']:.6f}")
        if gate_stats["avg_score_open"] is not None:
            lines.append(f"- Average score (gate_open=1 only): {gate_stats['avg_score_open']:.6f}")
        lines.append(f"- Score range: [{gate_stats['min_score']:.6f}, {gate_stats['max_score']:.6f}]")
    lines.append("")

    # Distinct firms and clusters in surface
    surface_dims = conn.execute(
        """
        SELECT COUNT(DISTINCT firm_id) AS n_firms,
               COUNT(DISTINCT cluster_id) AS n_clusters,
               COUNT(DISTINCT year) AS n_years
        FROM startability_surface
        """
    ).fetchone()
    if surface_dims:
        lines.append(f"- Firms in surface: {surface_dims['n_firms']:,}")
        lines.append(f"- Clusters in surface: {surface_dims['n_clusters']:,}")
        lines.append(f"- Years in surface: {surface_dims['n_years']}")
    lines.append("")

    # --- 6. Gate function statistics ---
    lines.append("## 6. Gate Function Statistics")
    lines.append("")
    lines.append("Gate condition: `cpc_overlap > 0.01 OR cite_prox > 0 OR cos_sim > 0.3`")
    lines.append("")

    gate_breakdown = conn.execute(
        """
        SELECT
            SUM(CASE WHEN phi_tech_cos > 0.3 THEN 1 ELSE 0 END) AS cos_trigger,
            SUM(CASE WHEN phi_tech_cpc > 0.01 THEN 1 ELSE 0 END) AS cpc_trigger,
            SUM(CASE WHEN phi_tech_cite > 0 THEN 1 ELSE 0 END) AS cite_trigger,
            COUNT(*) AS total
        FROM startability_surface
        WHERE gate_open = 1
        """
    ).fetchone()
    if gate_breakdown and gate_breakdown["total"]:
        gt = gate_breakdown["total"]
        lines.append(f"- Gate opened by cos_sim > 0.3: {gate_breakdown['cos_trigger']:,} ({_safe_div(gate_breakdown['cos_trigger'], gt)*100:.1f}%)")
        lines.append(f"- Gate opened by cpc_overlap > 0.01: {gate_breakdown['cpc_trigger']:,} ({_safe_div(gate_breakdown['cpc_trigger'], gt)*100:.1f}%)")
        lines.append(f"- Gate opened by cite_prox > 0: {gate_breakdown['cite_trigger']:,} ({_safe_div(gate_breakdown['cite_trigger'], gt)*100:.1f}%)")
    lines.append("")

    # --- 7. Top 10 firms by various metrics ---
    lines.append("## 7. Top 10 Firms")
    lines.append("")

    # Latest year
    latest_year_row = conn.execute(
        "SELECT MAX(year) AS y FROM firm_tech_vectors"
    ).fetchone()
    fy = latest_year_row["y"] if latest_year_row and latest_year_row["y"] else 2024

    # By patent count
    lines.append(f"### By patent count (year={fy})")
    lines.append("")
    lines.append("| Rank | Firm ID | Patent Count | Tech Diversity |")
    lines.append("|------|---------|-------------|----------------|")
    top_patent = conn.execute(
        """
        SELECT firm_id, patent_count, tech_diversity
        FROM firm_tech_vectors
        WHERE year = ?
        ORDER BY patent_count DESC
        LIMIT 10
        """,
        (fy,),
    ).fetchall()
    for i, r in enumerate(top_patent, 1):
        lines.append(f"| {i} | {r['firm_id']} | {r['patent_count']:,} | {r['tech_diversity']:.4f} |")
    lines.append("")

    # By average startability
    latest_surface_year_row = conn.execute(
        "SELECT MAX(year) AS y FROM startability_surface"
    ).fetchone()
    sy = latest_surface_year_row["y"] if latest_surface_year_row and latest_surface_year_row["y"] else 2024

    lines.append(f"### By average startability score (year={sy})")
    lines.append("")
    lines.append("| Rank | Firm ID | Avg Score | Max Score | Gate Open Clusters |")
    lines.append("|------|---------|-----------|-----------|-------------------|")
    top_start = conn.execute(
        """
        SELECT firm_id, AVG(score) AS avg_s, MAX(score) AS max_s,
               SUM(gate_open) AS go
        FROM startability_surface
        WHERE year = ?
        GROUP BY firm_id
        ORDER BY avg_s DESC
        LIMIT 10
        """,
        (sy,),
    ).fetchall()
    for i, r in enumerate(top_start, 1):
        lines.append(
            f"| {i} | {r['firm_id']} | {r['avg_s']:.6f} | {r['max_s']:.6f} | {r['go']} |"
        )
    lines.append("")

    # By tech diversity
    lines.append(f"### By technology diversity (Shannon entropy, year={fy})")
    lines.append("")
    lines.append("| Rank | Firm ID | Tech Diversity | Patent Count |")
    lines.append("|------|---------|----------------|-------------|")
    top_div = conn.execute(
        """
        SELECT firm_id, tech_diversity, patent_count
        FROM firm_tech_vectors
        WHERE year = ? AND tech_diversity IS NOT NULL
        ORDER BY tech_diversity DESC
        LIMIT 10
        """,
        (fy,),
    ).fetchall()
    for i, r in enumerate(top_div, 1):
        lines.append(f"| {i} | {r['firm_id']} | {r['tech_diversity']:.4f} | {r['patent_count']:,} |")
    lines.append("")

    # --- 8. Tech clusters ---
    lines.append("## 8. Technology Clusters")
    lines.append("")
    cluster_count = _row_count(conn, "tech_clusters")
    lines.append(f"- Total clusters: {cluster_count}")
    cluster_stats = conn.execute(
        """
        SELECT AVG(patent_count) AS avg_pc, MIN(patent_count) AS min_pc,
               MAX(patent_count) AS max_pc, AVG(growth_rate) AS avg_gr
        FROM tech_clusters
        """
    ).fetchone()
    if cluster_stats and cluster_stats["avg_pc"] is not None:
        lines.append(f"- Avg patents per cluster: {cluster_stats['avg_pc']:.0f}")
        lines.append(f"- Cluster size range: [{cluster_stats['min_pc']}, {cluster_stats['max_pc']}]")
        if cluster_stats["avg_gr"] is not None:
            lines.append(f"- Avg growth rate: {cluster_stats['avg_gr']:.4f}")
    lines.append("")

    # --- 9. GDELT coverage ---
    lines.append("## 9. GDELT Company Features")
    lines.append("")
    gdelt_count = _row_count(conn, "gdelt_company_features")
    if gdelt_count > 0:
        gdelt_dims = conn.execute(
            """
            SELECT COUNT(DISTINCT firm_id) AS n_firms,
                   MIN(year) AS min_year, MAX(year) AS max_year
            FROM gdelt_company_features
            """
        ).fetchone()
        lines.append(f"- Total rows: {gdelt_count:,}")
        if gdelt_dims:
            lines.append(f"- Firms covered: {gdelt_dims['n_firms']:,}")
            lines.append(f"- Year range: {gdelt_dims['min_year']} -- {gdelt_dims['max_year']}")
    else:
        lines.append("- No GDELT data ingested yet.")
    lines.append("")

    # --- 10. Patent value index ---
    lines.append("## 10. Patent Value Index")
    lines.append("")
    pvi_count = _row_count(conn, "patent_value_index")
    if pvi_count > 0:
        pvi_stats = conn.execute(
            """
            SELECT AVG(value_score) AS avg_vs, MIN(value_score) AS min_vs,
                   MAX(value_score) AS max_vs,
                   AVG(citation_component) AS avg_cite,
                   AVG(family_component) AS avg_fam,
                   AVG(recency_component) AS avg_rec,
                   AVG(cluster_momentum_component) AS avg_mom
            FROM patent_value_index
            """
        ).fetchone()
        lines.append(f"- Patents scored: {pvi_count:,}")
        if pvi_stats:
            lines.append(f"- Value score range: [{pvi_stats['min_vs']:.4f}, {pvi_stats['max_vs']:.4f}]")
            lines.append(f"- Mean value score: {pvi_stats['avg_vs']:.4f}")
            lines.append(f"- Mean components: citation={pvi_stats['avg_cite']:.4f}, "
                         f"family={pvi_stats['avg_fam']:.4f}, "
                         f"recency={pvi_stats['avg_rec']:.4f}, "
                         f"momentum={pvi_stats['avg_mom']:.4f}")
    else:
        lines.append("- No patent value index computed yet.")
    lines.append("")

    lines.append("---")
    lines.append("*End of statistical summary.*")
    lines.append("")

    filepath = os.path.join(out_dir, "stats_summary.md")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    log.info("  -> Written to %s", filepath)
    return filepath


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export paper-ready datasets from the patent database."
    )
    parser.add_argument(
        "--db", default="data/patents.db",
        help="Path to SQLite database (default: data/patents.db)",
    )
    parser.add_argument(
        "--output-dir", default="paper_data/",
        help="Output directory for CSV files and summary (default: paper_data/)",
    )
    args = parser.parse_args()

    db_path = args.db
    out_dir = args.output_dir

    log.info("=== Patent Space MCP: Paper Data Export ===")
    log.info("Database: %s", db_path)
    log.info("Output directory: %s", out_dir)

    os.makedirs(out_dir, exist_ok=True)
    conn = _connect(db_path)
    t0 = time.time()

    try:
        outputs: list[str] = []

        outputs.append(export_table1_database_summary(conn, out_dir))
        outputs.append(export_table2_startability_distribution(conn, out_dir))
        outputs.append(export_table3_top_firms(conn, out_dir))
        outputs.append(export_table4_cluster_momentum(conn, out_dir))
        outputs.append(export_table5_phi_tech_components(conn, out_dir))
        outputs.append(export_table6_startability_delta(conn, out_dir))
        outputs.append(export_table7_cross_sector(conn, out_dir))
        outputs.append(export_figure_data_filing_trend(conn, out_dir))
        outputs.append(export_figure_data_gate_function(conn, out_dir))
        outputs.append(export_stats_summary(conn, out_dir))

        elapsed = time.time() - t0
        log.info("")
        log.info("=== Export Complete ===")
        log.info("Elapsed: %.1f seconds", elapsed)
        log.info("Files generated:")
        for path in outputs:
            log.info("  %s", path)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
