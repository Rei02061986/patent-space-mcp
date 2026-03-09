"""visualization tools — structured data + Mermaid diagrams for Patent Space MCP.

No image rendering. Returns nodes/edges, matrix data, and optional Mermaid
diagram strings that downstream clients can render.

Uses pre-computed tables (firm_tech_vectors, startability_surface,
tech_clusters) for fast-path queries on spinning-disk SQLite.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver
from tools.cpc_labels_ja import CPC_CLASS_JA


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cpc4(code: str) -> str:
    """Normalise a CPC code to its 4-char subclass (e.g. 'H01M10/05' -> 'H01M')."""
    return (code or "").strip().upper()[:4]


def _sanitize_mermaid_id(raw: str) -> str:
    """Make a string safe for use as a Mermaid node ID."""
    return raw.replace(" ", "_").replace("-", "_").replace("/", "_").replace(".", "_")


def _sanitize_mermaid_label(raw: str, max_len: int = 30) -> str:
    """Truncate and escape label text for Mermaid."""
    safe = raw.replace('"', "'").replace("[", "(").replace("]", ")")
    if len(safe) > max_len:
        safe = safe[: max_len - 1] + "\u2026"
    return safe


def _cpc_label(code: str) -> str:
    """Return a human-readable label for a CPC 4-char subclass."""
    c4 = _cpc4(code)
    return CPC_CLASS_JA.get(c4, c4)


def _resolve_firm(resolver: EntityResolver | None, name: str) -> tuple[str | None, str]:
    """Resolve a firm name. Returns (firm_id, display_name)."""
    if resolver is None:
        return None, name
    resolved = resolver.resolve(name, country_hint="JP")
    if resolved is None:
        return None, name
    return resolved.entity.canonical_id, resolved.entity.canonical_name


# ---------------------------------------------------------------------------
# 1. tech_map
# ---------------------------------------------------------------------------

def tech_map(
    store: PatentStore,
    cpc_prefix: str | None = None,
    query: str | None = None,
    firm_query: str | None = None,
    resolver: EntityResolver | None = None,
    max_nodes: int = 30,
) -> dict[str, Any]:
    """Technology map as a bipartite graph of firms <-> CPC areas.

    Three modes (at least one parameter required):
    - cpc_prefix: Show top sub-CPCs and their top applicants.
    - firm_query: Show a single firm's CPC distribution.
    - query: Text search in patent titles (uses FTS if available).

    Returns nodes, edges, and a compact Mermaid ``graph LR`` string.
    """
    if not cpc_prefix and not query and not firm_query:
        return {
            "endpoint": "tech_map",
            "error": "At least one of cpc_prefix, query, or firm_query is required.",
        }

    max_nodes = max(10, min(max_nodes, 60))
    conn = store._conn()

    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    seen_node_ids: set[str] = set()

    # ── Firm-centric mode ─────────────────────────────────────────
    if firm_query:
        firm_id, firm_display = _resolve_firm(resolver, firm_query)

        # Add the firm node
        firm_node_id = firm_id or _sanitize_mermaid_id(firm_display)

        if firm_id:
            # Use pre-computed firm_tech_vectors (fast path)
            row = conn.execute(
                "SELECT dominant_cpc, patent_count, tech_diversity "
                "FROM firm_tech_vectors WHERE firm_id = ? "
                "ORDER BY year DESC LIMIT 1",
                (firm_id,),
            ).fetchone()

            firm_patent_count = row["patent_count"] if row else 0

            # Get CPC distribution from patent_assignees + patent_cpc
            # Limit to indexed column (firm_id) for speed on HDD
            cpc_rows = conn.execute(
                """
                SELECT c.cpc_code, COUNT(*) AS cnt
                FROM patent_assignees a
                JOIN patent_cpc c ON a.publication_number = c.publication_number
                WHERE a.firm_id = ?
                  AND c.is_first = 1
                GROUP BY c.cpc_code
                ORDER BY cnt DESC
                LIMIT ?
                """,
                (firm_id, max_nodes - 1),
            ).fetchall()

            if not cpc_rows:
                # Fallback: try harmonized_name LIKE
                cpc_rows = conn.execute(
                    """
                    SELECT c.cpc_code, COUNT(*) AS cnt
                    FROM patent_assignees a
                    JOIN patent_cpc c ON a.publication_number = c.publication_number
                    WHERE a.harmonized_name LIKE ?
                      AND c.is_first = 1
                    GROUP BY c.cpc_code
                    ORDER BY cnt DESC
                    LIMIT ?
                    """,
                    (f"%{firm_query}%", max_nodes - 1),
                ).fetchall()

            # Aggregate to 4-char CPC subclass
            cpc_agg: dict[str, int] = defaultdict(int)
            for r in cpc_rows:
                c4 = _cpc4(r["cpc_code"])
                if c4:
                    cpc_agg[c4] += r["cnt"]

            # Sort and limit
            sorted_cpcs = sorted(cpc_agg.items(), key=lambda x: x[1], reverse=True)
            sorted_cpcs = sorted_cpcs[: max_nodes - 1]

            nodes.append({
                "id": firm_node_id,
                "label": firm_display,
                "size": firm_patent_count,
                "type": "firm",
            })
            seen_node_ids.add(firm_node_id)

            for cpc_code, cnt in sorted_cpcs:
                label = _cpc_label(cpc_code)
                if cpc_code not in seen_node_ids:
                    nodes.append({
                        "id": cpc_code,
                        "label": label,
                        "size": cnt,
                        "type": "technology",
                    })
                    seen_node_ids.add(cpc_code)
                edges.append({
                    "source": firm_node_id,
                    "target": cpc_code,
                    "weight": cnt,
                })
        else:
            nodes.append({
                "id": firm_node_id,
                "label": firm_display,
                "size": 0,
                "type": "firm",
            })
            return {
                "endpoint": "tech_map",
                "error": f"Could not resolve firm: '{firm_query}'",
                "suggestion": "Try the exact company name, Japanese name, or stock ticker.",
                "nodes": nodes,
                "edges": [],
                "mermaid": "",
            }

    # ── CPC-prefix mode ───────────────────────────────────────────
    elif cpc_prefix:
        prefix = cpc_prefix.strip().upper()
        half_nodes = max_nodes // 2

        # Top sub-CPC areas under this prefix
        sub_rows = conn.execute(
            """
            SELECT cpc_code, COUNT(*) AS cnt
            FROM patent_cpc
            WHERE cpc_code LIKE ?
              AND is_first = 1
            GROUP BY SUBSTR(cpc_code, 1, 4)
            ORDER BY cnt DESC
            LIMIT ?
            """,
            (f"{prefix}%", half_nodes),
        ).fetchall()

        cpc_ids: list[str] = []
        for r in sub_rows:
            c4 = _cpc4(r["cpc_code"])
            if c4 and c4 not in seen_node_ids:
                label = _cpc_label(c4)
                nodes.append({
                    "id": c4,
                    "label": label,
                    "size": r["cnt"],
                    "type": "technology",
                })
                seen_node_ids.add(c4)
                cpc_ids.append(c4)

        # Top applicants in this CPC prefix
        applicant_rows = conn.execute(
            """
            SELECT a.harmonized_name, a.firm_id, COUNT(*) AS cnt
            FROM patent_cpc c
            JOIN patent_assignees a ON c.publication_number = a.publication_number
            WHERE c.cpc_code LIKE ?
              AND c.is_first = 1
            GROUP BY COALESCE(a.firm_id, a.harmonized_name)
            ORDER BY cnt DESC
            LIMIT ?
            """,
            (f"{prefix}%", half_nodes),
        ).fetchall()

        # Build edges: firm -> CPC (simplified: link each firm to the prefix)
        for ar in applicant_rows:
            firm_nid = ar["firm_id"] or _sanitize_mermaid_id(ar["harmonized_name"])
            firm_label = ar["harmonized_name"]
            if firm_nid not in seen_node_ids:
                nodes.append({
                    "id": firm_nid,
                    "label": firm_label,
                    "size": ar["cnt"],
                    "type": "firm",
                })
                seen_node_ids.add(firm_nid)

            # Link to the primary CPC prefix node
            for c4 in cpc_ids[:5]:  # link to top 5 CPCs at most
                edges.append({
                    "source": firm_nid,
                    "target": c4,
                    "weight": ar["cnt"],
                })

    # ── Query mode (text search) ──────────────────────────────────
    elif query:
        # Use store's search capability (FTS5 if available)
        results = store.search(query=query, limit=200)
        patents = results if isinstance(results, list) else results.get("patents", [])

        # Aggregate CPC codes and applicants
        cpc_count: dict[str, int] = defaultdict(int)
        firm_count: dict[str, int] = defaultdict(int)
        firm_cpc_link: dict[tuple[str, str], int] = defaultdict(int)

        pubs = [p.get("publication_number", "") for p in patents if p.get("publication_number")]
        if pubs:
            chunk_size = 200
            for i in range(0, len(pubs), chunk_size):
                chunk = pubs[i:i + chunk_size]
                ph = ",".join("?" for _ in chunk)

                cpc_rows = conn.execute(
                    f"SELECT publication_number, cpc_code FROM patent_cpc "
                    f"WHERE publication_number IN ({ph}) AND is_first = 1",
                    chunk,
                ).fetchall()
                pub_cpc: dict[str, str] = {}
                for r in cpc_rows:
                    c4 = _cpc4(r["cpc_code"])
                    if c4:
                        cpc_count[c4] += 1
                        pub_cpc[r["publication_number"]] = c4

                asgn_rows = conn.execute(
                    f"SELECT publication_number, harmonized_name, firm_id "
                    f"FROM patent_assignees WHERE publication_number IN ({ph})",
                    chunk,
                ).fetchall()
                for r in asgn_rows:
                    fname = r["firm_id"] or r["harmonized_name"]
                    if fname:
                        firm_count[fname] += 1
                        pub = r["publication_number"]
                        if pub in pub_cpc:
                            firm_cpc_link[(fname, pub_cpc[pub])] += 1

        half = max_nodes // 2
        top_cpcs = sorted(cpc_count.items(), key=lambda x: x[1], reverse=True)[:half]
        top_firms = sorted(firm_count.items(), key=lambda x: x[1], reverse=True)[:half]

        for c4, cnt in top_cpcs:
            if c4 not in seen_node_ids:
                nodes.append({
                    "id": c4,
                    "label": _cpc_label(c4),
                    "size": cnt,
                    "type": "technology",
                })
                seen_node_ids.add(c4)

        for fname, cnt in top_firms:
            nid = _sanitize_mermaid_id(fname)
            if nid not in seen_node_ids:
                nodes.append({
                    "id": nid,
                    "label": fname,
                    "size": cnt,
                    "type": "firm",
                })
                seen_node_ids.add(nid)

        for (fname, c4), cnt in firm_cpc_link.items():
            nid = _sanitize_mermaid_id(fname)
            if nid in seen_node_ids and c4 in seen_node_ids:
                edges.append({
                    "source": nid,
                    "target": c4,
                    "weight": cnt,
                })

    # ── Build Mermaid ─────────────────────────────────────────────
    mermaid = _build_mermaid_graph(nodes, edges, max_nodes=max_nodes)

    return {
        "endpoint": "tech_map",
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
        "mermaid": mermaid,
    }


def _build_mermaid_graph(
    nodes: list[dict], edges: list[dict], max_nodes: int = 30,
) -> str:
    """Build a compact Mermaid ``graph LR`` from nodes and edges."""
    if not nodes:
        return ""

    lines = ["graph LR"]
    shown_ids: set[str] = set()

    # Limit to top nodes by size
    sorted_nodes = sorted(nodes, key=lambda n: n.get("size", 0), reverse=True)
    top_nodes = sorted_nodes[:max_nodes]
    top_ids = {n["id"] for n in top_nodes}

    for n in top_nodes:
        nid = _sanitize_mermaid_id(n["id"])
        label = _sanitize_mermaid_label(n.get("label", n["id"]))
        size = n.get("size", 0)
        ntype = n.get("type", "")
        if ntype == "firm":
            lines.append(f"  {nid}({label}: {size})")
        else:
            lines.append(f"  {nid}[{label}: {size}]")
        shown_ids.add(n["id"])

    for e in edges:
        src, tgt = e["source"], e["target"]
        if src in shown_ids and tgt in shown_ids:
            sid = _sanitize_mermaid_id(src)
            tid = _sanitize_mermaid_id(tgt)
            w = e.get("weight", 1)
            lines.append(f"  {sid} --{w}--> {tid}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 2. citation_graph_viz
# ---------------------------------------------------------------------------

def citation_graph_viz(
    store: PatentStore,
    publication_number: str,
    depth: int = 1,
    max_nodes: int = 30,
) -> dict[str, Any]:
    """Citation network visualization data with Mermaid flowchart.

    BFS through patent_citations (forward + backward). Returns a compact
    node/edge graph and a Mermaid ``graph TD`` string.

    Args:
        store: PatentStore instance.
        publication_number: Seed patent (e.g. 'JP-7637366-B1').
        depth: BFS depth (1 or 2). Default 1.
        max_nodes: Maximum nodes. Default 30.

    Returns:
        Dict with nodes, edges, mermaid, and summary stats.
    """
    if not publication_number:
        return {
            "endpoint": "citation_graph_viz",
            "error": "publication_number is required.",
        }

    depth = max(1, min(depth, 2))
    max_nodes = max(5, min(max_nodes, 50))
    conn = store._conn()
    seed = publication_number.strip()

    # Verify seed exists
    seed_row = conn.execute(
        "SELECT publication_number, title FROM patents WHERE publication_number = ?",
        (seed,),
    ).fetchone()
    if not seed_row:
        return {
            "endpoint": "citation_graph_viz",
            "error": f"Patent not found: '{seed}'",
        }

    # BFS traversal (forward + backward)
    visited: set[str] = {seed}
    edge_set: set[tuple[str, str]] = set()
    frontier: set[str] = {seed}

    for _d in range(depth):
        if len(visited) >= max_nodes:
            break
        next_frontier: set[str] = set()
        frontier_list = list(frontier)
        chunk_size = 100

        for i in range(0, len(frontier_list), chunk_size):
            chunk = frontier_list[i:i + chunk_size]
            ph = ",".join("?" for _ in chunk)

            # Forward: who cites these? (cited_publication = our patent)
            fwd = conn.execute(
                f"SELECT citing_publication, cited_publication "
                f"FROM patent_citations WHERE cited_publication IN ({ph})",
                chunk,
            ).fetchall()
            for r in fwd:
                citing = r["citing_publication"]
                cited = r["cited_publication"]
                edge_set.add((citing, cited))
                if citing not in visited and len(visited) + len(next_frontier) < max_nodes:
                    next_frontier.add(citing)

            # Backward: what do these cite?
            bwd = conn.execute(
                f"SELECT citing_publication, cited_publication "
                f"FROM patent_citations WHERE citing_publication IN ({ph})",
                chunk,
            ).fetchall()
            for r in bwd:
                citing = r["citing_publication"]
                cited = r["cited_publication"]
                edge_set.add((citing, cited))
                if cited not in visited and len(visited) + len(next_frontier) < max_nodes:
                    next_frontier.add(cited)

        visited.update(next_frontier)
        frontier = next_frontier

    # Fetch metadata for all nodes
    all_ids = list(visited)[:max_nodes]
    node_meta = _batch_patent_meta(conn, all_ids)

    # Build nodes
    nodes: list[dict[str, Any]] = []
    in_degree: dict[str, int] = defaultdict(int)
    out_degree: dict[str, int] = defaultdict(int)

    for src, tgt in edge_set:
        if src in visited and tgt in visited:
            out_degree[src] += 1
            in_degree[tgt] += 1

    for pid in all_ids:
        meta = node_meta.get(pid, {})
        nodes.append({
            "id": pid,
            "label": meta.get("title", pid)[:60],
            "filing_date": meta.get("filing_date"),
            "is_seed": pid == seed,
            "in_degree": in_degree.get(pid, 0),
            "out_degree": out_degree.get(pid, 0),
        })

    # Build edges list
    edges: list[dict[str, Any]] = []
    for src, tgt in edge_set:
        if src in visited and tgt in visited:
            edges.append({"source": src, "target": tgt, "type": "cites"})

    # Mermaid
    mermaid = _build_citation_mermaid(seed, nodes, edges, max_nodes=max_nodes)

    return {
        "endpoint": "citation_graph_viz",
        "seed": seed,
        "seed_title": (seed_row["title"] or "")[:80],
        "node_count": len(nodes),
        "edge_count": len(edges),
        "depth": depth,
        "nodes": nodes,
        "edges": edges,
        "hub_patents": sorted(
            nodes, key=lambda n: n["in_degree"], reverse=True,
        )[:5],
        "mermaid": mermaid,
    }


def _batch_patent_meta(
    conn: Any, pub_ids: list[str],
) -> dict[str, dict[str, Any]]:
    """Batch-fetch title and filing_date for a list of publication numbers."""
    result: dict[str, dict[str, Any]] = {}
    chunk_size = 200
    for i in range(0, len(pub_ids), chunk_size):
        chunk = pub_ids[i:i + chunk_size]
        ph = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT publication_number, title, filing_date "
            f"FROM patents WHERE publication_number IN ({ph})",
            chunk,
        ).fetchall()
        for r in rows:
            result[r["publication_number"]] = {
                "title": r["title"],
                "filing_date": str(r["filing_date"]) if r["filing_date"] else None,
            }
    return result


def _build_citation_mermaid(
    seed: str,
    nodes: list[dict],
    edges: list[dict],
    max_nodes: int = 30,
) -> str:
    """Build a Mermaid flowchart for citation network."""
    if not nodes:
        return ""

    lines = ["graph TD"]

    # Show at most max_nodes nodes (prioritise seed, then high in_degree)
    sorted_nodes = sorted(
        nodes,
        key=lambda n: (n.get("is_seed", False), n.get("in_degree", 0)),
        reverse=True,
    )
    top = sorted_nodes[:max_nodes]
    shown_ids = {n["id"] for n in top}

    for n in top:
        nid = _sanitize_mermaid_id(n["id"])
        short_label = _sanitize_mermaid_label(n["id"].split("-")[1] if "-" in n["id"] else n["id"], 20)
        if n.get("is_seed"):
            lines.append(f"  {nid}(({short_label}))")  # double-circle for seed
        else:
            lines.append(f"  {nid}[{short_label}]")

    for e in edges:
        if e["source"] in shown_ids and e["target"] in shown_ids:
            sid = _sanitize_mermaid_id(e["source"])
            tid = _sanitize_mermaid_id(e["target"])
            lines.append(f"  {sid} --> {tid}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 3. firm_landscape
# ---------------------------------------------------------------------------

def firm_landscape(
    store: PatentStore,
    firms: list[str],
    cpc_prefix: str | None = None,
    resolver: EntityResolver | None = None,
    year: int | None = None,
) -> dict[str, Any]:
    """Multi-firm comparison chart data (grouped bar format).

    Compares patent portfolio metrics across multiple firms.

    Args:
        store: PatentStore instance.
        firms: List of firm names / tickers to compare.
        cpc_prefix: Optional CPC filter (e.g. 'H01M' for batteries).
        resolver: EntityResolver for name resolution.
        year: Year for firm_tech_vectors lookup. Default: latest available.

    Returns:
        Dict with series data suitable for grouped bar chart rendering.
    """
    if not firms:
        return {
            "endpoint": "firm_landscape",
            "error": "firms list is required (provide at least 2 firm names).",
        }

    if len(firms) > 20:
        firms = firms[:20]

    conn = store._conn()

    # Resolve all firms
    resolved_firms: list[dict[str, Any]] = []
    for name in firms:
        firm_id, display_name = _resolve_firm(resolver, name)
        resolved_firms.append({
            "input": name,
            "firm_id": firm_id,
            "display_name": display_name,
        })

    display_names: list[str] = [f["display_name"] for f in resolved_firms]

    # Collect per-firm data from firm_tech_vectors
    total_patents: list[int] = []
    tech_diversities: list[float] = []
    dominant_cpcs: list[str] = []
    cpc_matrix: dict[str, dict[str, int]] = {}

    for rf in resolved_firms:
        fid = rf["firm_id"]
        fname = rf["display_name"]
        cpc_matrix[fname] = {}

        if fid is None:
            total_patents.append(0)
            tech_diversities.append(0.0)
            dominant_cpcs.append("")
            continue

        # Get firm_tech_vectors data
        if year:
            ftv = conn.execute(
                "SELECT patent_count, tech_diversity, dominant_cpc "
                "FROM firm_tech_vectors WHERE firm_id = ? AND year = ?",
                (fid, year),
            ).fetchone()
        else:
            ftv = conn.execute(
                "SELECT patent_count, tech_diversity, dominant_cpc, year "
                "FROM firm_tech_vectors WHERE firm_id = ? "
                "ORDER BY year DESC LIMIT 1",
                (fid,),
            ).fetchone()

        if ftv:
            total_patents.append(ftv["patent_count"] or 0)
            tech_diversities.append(round((ftv["tech_diversity"] or 0.0), 3))
            dominant_cpcs.append(ftv["dominant_cpc"] or "")
        else:
            total_patents.append(0)
            tech_diversities.append(0.0)
            dominant_cpcs.append("")

        # Get CPC breakdown via startability_surface (fast path)
        ss_rows = conn.execute(
            """
            SELECT ss.cluster_id, tc.cpc_class, ss.score
            FROM startability_surface ss
            JOIN tech_clusters tc ON ss.cluster_id = tc.cluster_id
            WHERE ss.firm_id = ?
            ORDER BY ss.score DESC
            LIMIT 20
            """,
            (fid,),
        ).fetchall()

        for sr in ss_rows:
            c4 = _cpc4(sr["cpc_class"])
            if c4:
                cpc_matrix[fname][c4] = cpc_matrix[fname].get(c4, 0) + 1

    # If cpc_prefix is given, compute CPC-specific patent counts
    cpc_filtered_counts: list[int] = []
    if cpc_prefix:
        prefix = cpc_prefix.strip().upper()
        for rf in resolved_firms:
            fid = rf["firm_id"]
            if fid is None:
                cpc_filtered_counts.append(0)
                continue
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT a.publication_number) AS cnt
                FROM patent_assignees a
                JOIN patent_cpc c ON a.publication_number = c.publication_number
                WHERE a.firm_id = ?
                  AND c.cpc_code LIKE ?
                  AND c.is_first = 1
                """,
                (fid, f"{prefix}%"),
            ).fetchone()
            cpc_filtered_counts.append(row["cnt"] if row else 0)

    # Build series
    series: list[dict[str, Any]] = [
        {"name": "Total patents", "data": total_patents},
        {"name": "Tech diversity (entropy)", "data": tech_diversities},
    ]
    if cpc_prefix:
        series.append({
            "name": f"{cpc_prefix} patents",
            "data": cpc_filtered_counts,
        })

    return {
        "endpoint": "firm_landscape",
        "firms": display_names,
        "firm_ids": [f["firm_id"] for f in resolved_firms],
        "series": series,
        "dominant_cpcs": dominant_cpcs,
        "cpc_matrix": cpc_matrix,
        "chart_type": "grouped_bar",
        "year": year,
    }


# ---------------------------------------------------------------------------
# 4. startability_heatmap
# ---------------------------------------------------------------------------

def startability_heatmap(
    store: PatentStore,
    firms: list[str] | None = None,
    clusters: list[str] | None = None,
    year: int = 2024,
    resolver: EntityResolver | None = None,
    top_n: int = 20,
) -> dict[str, Any]:
    """Startability scores as heatmap data: rows (firms) x cols (clusters).

    If firms or clusters are not specified, auto-selects the top entries
    by patent count.

    Args:
        store: PatentStore instance.
        firms: List of firm names/tickers. Auto-selects top_n if None.
        clusters: List of cluster_ids (e.g. 'H01M_0'). Auto-selects if None.
        year: Year for startability scores. Default: 2024.
        resolver: EntityResolver for name resolution.
        top_n: How many firms/clusters to auto-select. Default: 20.

    Returns:
        Dict with rows, columns, values matrix, and label maps.
    """
    conn = store._conn()
    top_n = max(5, min(top_n, 50))

    # ── Resolve or auto-select firms ──────────────────────────────
    firm_entries: list[dict[str, Any]] = []  # [{firm_id, display_name}]

    if firms:
        for name in firms[:top_n]:
            fid, dname = _resolve_firm(resolver, name)
            if fid:
                firm_entries.append({"firm_id": fid, "display_name": dname})
    else:
        # Auto-select: top firms by patent_count in firm_tech_vectors
        auto_rows = conn.execute(
            """
            SELECT firm_id, patent_count
            FROM firm_tech_vectors
            WHERE year = ?
            ORDER BY patent_count DESC
            LIMIT ?
            """,
            (year, top_n),
        ).fetchall()

        # Fallback to latest year if requested year has no data
        if not auto_rows:
            fallback = conn.execute(
                "SELECT DISTINCT year FROM firm_tech_vectors ORDER BY year DESC LIMIT 1",
            ).fetchone()
            if fallback:
                actual_year = fallback["year"]
                auto_rows = conn.execute(
                    "SELECT firm_id, patent_count FROM firm_tech_vectors "
                    "WHERE year = ? ORDER BY patent_count DESC LIMIT ?",
                    (actual_year, top_n),
                ).fetchall()
                year = actual_year

        for r in auto_rows:
            firm_entries.append({
                "firm_id": r["firm_id"],
                "display_name": r["firm_id"],  # Will be overridden below if possible
            })

    if not firm_entries:
        return {
            "endpoint": "startability_heatmap",
            "error": "No firms found for the specified criteria.",
            "year": year,
        }

    # ── Resolve or auto-select clusters ───────────────────────────
    if clusters:
        cluster_ids = [c.strip() for c in clusters[:top_n]]
    else:
        # Auto-select: top clusters by patent_count
        cl_rows = conn.execute(
            """
            SELECT cluster_id, cpc_class, label, patent_count
            FROM tech_clusters
            ORDER BY patent_count DESC
            LIMIT ?
            """,
            (top_n,),
        ).fetchall()
        cluster_ids = [r["cluster_id"] for r in cl_rows]

    if not cluster_ids:
        # Fallback: infer clusters from startability_surface
        cl_rows = conn.execute(
            """
            SELECT DISTINCT cluster_id
            FROM startability_surface
            WHERE year = ?
            LIMIT ?
            """,
            (year, top_n),
        ).fetchall()
        cluster_ids = [r["cluster_id"] for r in cl_rows]

    if not cluster_ids:
        return {
            "endpoint": "startability_heatmap",
            "error": "No clusters found.",
            "year": year,
        }

    # ── Fetch cluster metadata ────────────────────────────────────
    cl_ph = ",".join("?" for _ in cluster_ids)
    cl_meta_rows = conn.execute(
        f"SELECT cluster_id, cpc_class, label FROM tech_clusters "
        f"WHERE cluster_id IN ({cl_ph})",
        cluster_ids,
    ).fetchall()
    cluster_meta: dict[str, dict[str, str]] = {
        r["cluster_id"]: {"cpc_class": r["cpc_class"], "label": r["label"] or r["cpc_class"]}
        for r in cl_meta_rows
    }

    # ── Fetch startability scores (batch) ─────────────────────────
    firm_ids = [f["firm_id"] for f in firm_entries]
    firm_ph = ",".join("?" for _ in firm_ids)
    cl_ph2 = ",".join("?" for _ in cluster_ids)

    score_rows = conn.execute(
        f"""
        SELECT firm_id, cluster_id, score
        FROM startability_surface
        WHERE firm_id IN ({firm_ph})
          AND cluster_id IN ({cl_ph2})
          AND year = ?
        """,
        [*firm_ids, *cluster_ids, year],
    ).fetchall()

    # Build score lookup
    score_map: dict[tuple[str, str], float] = {}
    for r in score_rows:
        score_map[(r["firm_id"], r["cluster_id"])] = round(r["score"], 4)

    # ── Build matrix ──────────────────────────────────────────────
    row_labels: dict[str, str] = {}
    for fe in firm_entries:
        row_labels[fe["firm_id"]] = fe["display_name"]

    column_labels: dict[str, str] = {}
    for cid in cluster_ids:
        meta = cluster_meta.get(cid, {})
        column_labels[cid] = meta.get("label", cid)

    values: list[list[float | None]] = []
    for fid in firm_ids:
        row: list[float | None] = []
        for cid in cluster_ids:
            score = score_map.get((fid, cid))
            row.append(score)
        values.append(row)

    # Compute summary stats
    all_scores = [s for s in score_map.values() if s is not None]
    avg_score = round(sum(all_scores) / len(all_scores), 4) if all_scores else 0.0
    coverage = round(len(all_scores) / (len(firm_ids) * len(cluster_ids)) * 100, 1) if firm_ids and cluster_ids else 0.0

    return {
        "endpoint": "startability_heatmap",
        "rows": [fe["display_name"] for fe in firm_entries],
        "row_ids": firm_ids,
        "columns": cluster_ids,
        "values": values,
        "row_labels": row_labels,
        "column_labels": column_labels,
        "year": year,
        "stats": {
            "avg_score": avg_score,
            "data_coverage_pct": coverage,
            "total_cells": len(firm_ids) * len(cluster_ids),
            "filled_cells": len(all_scores),
        },
    }
