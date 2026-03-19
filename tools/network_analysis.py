"""Complex network analysis tools — topology, knowledge flow, resilience,
tech fusion detection, and entropy analysis.

All graph algorithms are self-contained (no networkx dependency).
Uses patent_citations + patent_cpc + citation_counts for network data.
Heavy sampling to avoid full table scans on HDD.
"""
from __future__ import annotations

import sqlite3

import math
import random
from collections import Counter, defaultdict, deque
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver
from tools.cpc_labels_ja import CPC_CLASS_JA
try:
    from tools.jp_tech_cpc_map import JP_TECH_CPC_MAP as _EXT_CPC
except ImportError:
    _EXT_CPC = {}


# ─── helpers ─────────────────────────────────────────────────────────

# Mapping: entity canonical_id → patent_assignees firm_id
_NETWORK_FIRM_REMAP = {
    "samsung": "samsung_electronics",
    "bosch": "bosch_robert",
    "huawei": "huawei_tech",
    "lg": "lg_electronics",
    "siemens": "siemens_ag",
    "byd": "byd_co",
}


def _resolve_firm(resolver: EntityResolver, name: str) -> str | None:
    res = resolver.resolve(name, country_hint="JP")
    return res.entity.canonical_id if res else None


def _cpc4(code: str) -> str:
    return code[:4] if code else ""


def _find_best_year(conn, firm_id: str) -> int:
    """Find the latest year with substantial data for a firm.

    Picks the latest year whose row count is >= 90% of the max count.
    This avoids picking ancient years when counts are equal/similar.
    """
    rows = conn.execute(
        "SELECT year, COUNT(*) as cnt FROM startability_surface "
        "WHERE firm_id = ? GROUP BY year ORDER BY cnt DESC",
        (firm_id,),
    ).fetchall()
    if not rows:
        return 2023
    max_cnt = rows[0]["cnt"]
    threshold = max(1, int(max_cnt * 0.9))
    # Among years with >= 90% of max count, pick the latest
    best = max(
        (r["year"] for r in rows if r["cnt"] >= threshold),
        default=rows[0]["year"],
    )
    return best


# ─── graph primitives (no networkx) ─────────────────────────────────

def _build_subgraph(conn, seed_patents: list[str], max_nodes: int,
                     direction: str = "both") -> tuple[dict, dict]:
    """BFS from seed patents to build citation subgraph.

    Returns:
        adjacency: {node: set(neighbors)} (undirected view)
        edges: {(src, tgt): "cites"} directed edges
    """
    adjacency: dict[str, set] = defaultdict(set)
    edges: dict[tuple, str] = {}
    visited = set()
    queue = deque(seed_patents)

    for s in seed_patents:
        visited.add(s)

    while queue and len(visited) < max_nodes:
        node = queue.popleft()

        if direction in ("both", "forward"):
            # Forward: this patent cites others
            rows = conn.execute(
                "SELECT cited_publication FROM patent_citations "
                "WHERE citing_publication = ? LIMIT 50",
                (node,),
            ).fetchall()
            for r in rows:
                target = r["cited_publication"]
                adjacency[node].add(target)
                adjacency[target].add(node)
                edges[(node, target)] = "cites"
                if target not in visited and len(visited) < max_nodes:
                    visited.add(target)
                    queue.append(target)

        if direction in ("both", "backward"):
            # Backward: patents that cite this one
            rows = conn.execute(
                "SELECT citing_publication FROM patent_citations "
                "WHERE cited_publication = ? LIMIT 50",
                (node,),
            ).fetchall()
            for r in rows:
                source = r["citing_publication"]
                adjacency[source].add(node)
                adjacency[node].add(source)
                edges[(source, node)] = "cites"
                if source not in visited and len(visited) < max_nodes:
                    visited.add(source)
                    queue.append(source)

    return dict(adjacency), edges


def _in_degree(edges: dict) -> dict[str, int]:
    """Count in-degree (cited-by count) from directed edges."""
    deg: dict[str, int] = defaultdict(int)
    for (src, tgt), _ in edges.items():
        deg[tgt] += 1
    return dict(deg)


def _out_degree(edges: dict) -> dict[str, int]:
    deg: dict[str, int] = defaultdict(int)
    for (src, tgt), _ in edges.items():
        deg[src] += 1
    return dict(deg)


def _total_degree(adjacency: dict) -> dict[str, int]:
    return {n: len(nbrs) for n, nbrs in adjacency.items()}


def _largest_component_size(adjacency: dict) -> int:
    """BFS to find largest connected component."""
    if not adjacency:
        return 0
    visited = set()
    max_size = 0
    for start in adjacency:
        if start in visited:
            continue
        q = deque([start])
        visited.add(start)
        size = 0
        while q:
            node = q.popleft()
            size += 1
            for nb in adjacency.get(node, set()):
                if nb not in visited:
                    visited.add(nb)
                    q.append(nb)
        max_size = max(max_size, size)
    return max_size


def _local_clustering(adjacency: dict, node: str) -> float:
    """Clustering coefficient for a single node."""
    nbrs = adjacency.get(node, set())
    k = len(nbrs)
    if k < 2:
        return 0.0
    nbr_list = list(nbrs)
    triangles = 0
    for i in range(len(nbr_list)):
        for j in range(i + 1, len(nbr_list)):
            if nbr_list[j] in adjacency.get(nbr_list[i], set()):
                triangles += 1
    return 2.0 * triangles / (k * (k - 1))


def _avg_clustering(adjacency: dict, sample_size: int = 200) -> float:
    """Average clustering coefficient (sampled)."""
    nodes = list(adjacency.keys())
    if not nodes:
        return 0.0
    sample = random.sample(nodes, min(sample_size, len(nodes)))
    coeffs = [_local_clustering(adjacency, n) for n in sample]
    return sum(coeffs) / len(coeffs) if coeffs else 0.0


def _bfs_distance(adjacency: dict, start: str, max_dist: int = 20) -> dict[str, int]:
    """BFS distances from start node."""
    dist = {start: 0}
    q = deque([start])
    while q:
        node = q.popleft()
        d = dist[node]
        if d >= max_dist:
            continue
        for nb in adjacency.get(node, set()):
            if nb not in dist:
                dist[nb] = d + 1
                q.append(nb)
    return dist


def _avg_path_length_sampled(adjacency: dict, sample_size: int = 50) -> float:
    """Average shortest path length (sampled BFS)."""
    nodes = list(adjacency.keys())
    if len(nodes) < 2:
        return 0.0
    sample = random.sample(nodes, min(sample_size, len(nodes)))
    total_dist = 0
    total_pairs = 0
    for s in sample:
        dists = _bfs_distance(adjacency, s)
        for d in dists.values():
            if d > 0:
                total_dist += d
                total_pairs += 1
    return total_dist / total_pairs if total_pairs > 0 else 0.0


def _fit_power_law(degree_dist: dict[int, int]) -> float:
    """Estimate power law exponent gamma using MLE (Clauset et al.)."""
    # Simple linear regression on log-log
    import numpy as np
    degs = []
    for k, count in degree_dist.items():
        if k >= 1:
            degs.extend([k] * count)
    if len(degs) < 10:
        return 0.0
    k_min = 1
    n = len([d for d in degs if d >= k_min])
    if n == 0:
        return 0.0
    # MLE: gamma = 1 + n / sum(ln(d / k_min))
    s = sum(math.log(d / (k_min - 0.5)) for d in degs if d >= k_min)
    if s <= 0:
        return 0.0
    gamma = 1 + n / s
    return gamma


def _greedy_communities(adjacency: dict, max_communities: int = 10) -> list[set]:
    """Simple greedy community detection (label propagation)."""
    if not adjacency:
        return []
    labels = {n: i for i, n in enumerate(adjacency)}
    for _ in range(10):  # iterations
        changed = False
        for node in adjacency:
            if not adjacency[node]:
                continue
            nbr_labels = Counter(labels[nb] for nb in adjacency[node] if nb in labels)
            if nbr_labels:
                best_label = nbr_labels.most_common(1)[0][0]
                if labels[node] != best_label:
                    labels[node] = best_label
                    changed = True
        if not changed:
            break

    communities: dict[int, set] = defaultdict(set)
    for node, label in labels.items():
        communities[label].add(node)

    # Sort by size, return top N
    sorted_comms = sorted(communities.values(), key=len, reverse=True)
    return sorted_comms[:max_communities]


# ─── Tool 5: network_topology ───────────────────────────────────────

def network_topology(
    store: PatentStore,
    resolver: EntityResolver | None = None,
    cpc_prefix: str | None = None,
    firm: str | None = None,
    max_patents: int = 1000,
    year: int = 2024,
) -> dict[str, Any]:
    """Citation network topology analysis."""
    store._relax_timeout()
    conn = store._conn()

    max_patents = min(max_patents, 1000)

    # Get seed patents (top-cited in CPC or firm)
    if firm and resolver:
        firm_id = _resolve_firm(resolver, firm)
        if not firm_id:
            return {"error": f"Could not resolve firm: '{firm}'"}
        seed_rows = conn.execute(
            "SELECT pa.publication_number FROM patent_assignees pa "
            "JOIN citation_counts cc ON pa.publication_number = cc.publication_number "
            "WHERE pa.firm_id = ? ORDER BY cc.forward_citations DESC LIMIT ?",
            (firm_id, min(50, max_patents // 5)),
        ).fetchall()
    elif cpc_prefix:
        seed_rows = conn.execute(
            "SELECT pc.publication_number FROM patent_cpc pc "
            "JOIN citation_counts cc ON pc.publication_number = cc.publication_number "
            "WHERE pc.cpc_code LIKE ? ORDER BY cc.forward_citations DESC LIMIT ?",
            (f"{cpc_prefix}%", min(50, max_patents // 5)),
        ).fetchall()
    else:
        return {"error": "Either cpc_prefix or firm is required."}

    seeds = [r["publication_number"] for r in seed_rows]
    if not seeds:
        return {"error": "No patents found for the given query."}

    # Build subgraph via BFS
    adjacency, edges = _build_subgraph(conn, seeds, max_patents)
    num_nodes = len(adjacency)
    num_edges = len(edges)

    if num_nodes < 5:
        return {"error": "Too few patents in network (< 5 nodes)."}

    # Degree distribution
    in_deg = _in_degree(edges)
    deg_dist = Counter(in_deg.values())

    # Power law fit
    gamma = _fit_power_law(deg_dist)
    is_scale_free = 2.0 < gamma < 3.5

    # Clustering coefficient
    cc = _avg_clustering(adjacency)

    # Average path length
    apl = _avg_path_length_sampled(adjacency, sample_size=min(50, num_nodes))

    # Small world index
    density = 2 * num_edges / (num_nodes * (num_nodes - 1)) if num_nodes > 1 else 0
    c_random = density  # Expected CC for random graph
    l_random = math.log(num_nodes) / math.log(max(2, num_nodes * density)) if density > 0 and num_nodes > 1 else apl
    small_world = (cc / max(c_random, 1e-6)) / (apl / max(l_random, 1e-6)) if apl > 0 and l_random > 0 else 0

    # Hub patents
    hubs = sorted(in_deg.items(), key=lambda x: x[1], reverse=True)[:10]
    hub_details = []
    for pub, cited_count in hubs:
        pat = conn.execute(
            "SELECT title_ja, title_en FROM patents WHERE publication_number = ?", (pub,)
        ).fetchone()
        assignee_row = conn.execute(
            "SELECT firm_id FROM patent_assignees WHERE publication_number = ? LIMIT 1", (pub,)
        ).fetchone()
        hub_details.append({
            "patent": pub,
            "title": (pat["title_ja"] or pat["title_en"] or "") if pat else "",
            "cited_by": cited_count,
            "assignee": (assignee_row["firm_id"] or "") if assignee_row else "",
            "betweenness_centrality": round(cited_count / max(num_nodes, 1), 4),
        })

    # Community detection
    communities = _greedy_communities(adjacency, max_communities=10)
    comm_details = []
    for i, comm in enumerate(communities[:5]):
        # Get dominant CPC for this community
        cpc_counter: Counter = Counter()
        for node in list(comm)[:100]:
            cpc_rows = conn.execute(
                "SELECT cpc_code FROM patent_cpc WHERE publication_number = ? LIMIT 3", (node,)
            ).fetchall()
            for cr in cpc_rows:
                cpc_counter[_cpc4(cr["cpc_code"])] += 1

        dom_cpc = cpc_counter.most_common(1)[0][0] if cpc_counter else ""
        # Key patent (most cited in community)
        comm_in_deg = {n: in_deg.get(n, 0) for n in comm}
        key_pat = max(comm_in_deg.items(), key=lambda x: x[1])[0] if comm_in_deg else ""

        comm_details.append({
            "id": i,
            "size": len(comm),
            "dominant_cpc": dom_cpc,
            "label": CPC_CLASS_JA.get(dom_cpc, ""),
            "key_patent": key_pat,
        })

    return {
        "endpoint": "network_topology",
        "query": {"cpc_prefix": cpc_prefix, "firm": firm},
        "network_stats": {
            "nodes": num_nodes,
            "edges": num_edges,
            "density": round(density, 6),
            "power_law_gamma": round(gamma, 3),
            "is_scale_free": is_scale_free,
            "clustering_coefficient": round(cc, 4),
            "avg_path_length": round(apl, 2),
            "small_world_index": round(small_world, 2),
            "is_small_world": small_world > 1.0,
        },
        "hub_patents": hub_details,
        "communities": comm_details,
        "degree_distribution_sample": {
            str(k): v for k, v in sorted(deg_dist.items())[:20]
        },
        "interpretation": (
            f"このネットワーク({num_nodes}ノード, {num_edges}エッジ)は"
            f"{'スケールフリー' if is_scale_free else '非スケールフリー'}(γ={gamma:.2f})"
            f"{'かつスモールワールド' if small_world > 1 else ''}。"
            f"{len(hub_details)}件のハブ特許がネットワーク構造を支配。"
            f"{len(comm_details)}個の技術コミュニティを検出。"
        ),
    }


# ─── Tool 6: knowledge_flow ─────────────────────────────────────────

def knowledge_flow(
    store: PatentStore,
    resolver: EntityResolver | None = None,
    source_cpc: str | None = None,
    target_cpc: str | None = None,
    firm: str | None = None,
    date_from: str = "2018-01-01",
    date_to: str = "2024-12-31",
    top_n: int = 20,
) -> dict[str, Any]:
    """Cross-CPC knowledge flow analysis."""
    import time as _time
    _kf_deadline = _time.monotonic() + 50  # 50s time budget
    store._relax_timeout()
    conn = store._conn()

    year_from = int(date_from[:4])
    year_to = int(date_to[:4])

    # Build firm filter
    firm_filter = ""
    firm_params: list = []
    if firm and resolver:
        firm_id = _resolve_firm(resolver, firm)
        if firm_id:
            firm_filter = "AND pc.citing_publication IN (SELECT publication_number FROM patent_assignees WHERE firm_id = ?) "
            firm_params = [firm_id]

    # Build CPC filters
    cpc_filter_citing = ""
    cpc_filter_cited = ""
    cpc_params_citing: list = []
    cpc_params_cited: list = []
    if source_cpc:
        cpc_filter_cited = "AND c2.cpc_code LIKE ? "
        cpc_params_cited = [f"{source_cpc.upper()}%"]
    if target_cpc:
        cpc_filter_citing = "AND c1.cpc_code LIKE ? "
        cpc_params_citing = [f"{target_cpc.upper()}%"]

    # Query cross-CPC citations (sampled)
    query = f"""
    SELECT
        SUBSTR(c2.cpc_code, 1, 4) as source_cpc4,
        SUBSTR(c1.cpc_code, 1, 4) as target_cpc4,
        COUNT(*) as flow_count
    FROM patent_citations pc
    JOIN patent_cpc c1 ON pc.citing_publication = c1.publication_number AND c1.is_first = 1
    JOIN patent_cpc c2 ON pc.cited_publication = c2.publication_number AND c2.is_first = 1
    JOIN patents p ON pc.citing_publication = p.publication_number
    WHERE SUBSTR(c1.cpc_code, 1, 4) != SUBSTR(c2.cpc_code, 1, 4)
    AND p.filing_date >= ? AND p.filing_date <= ?
    {cpc_filter_citing} {cpc_filter_cited} {firm_filter}
    GROUP BY source_cpc4, target_cpc4
    ORDER BY flow_count DESC
    LIMIT 5000
    """
    filing_from = year_from * 10000 + 101
    filing_to = year_to * 10000 + 1231
    params = [filing_from, filing_to] + cpc_params_citing + cpc_params_cited + firm_params

    try:
        rows = conn.execute(query, params).fetchall()
    except sqlite3.OperationalError as e:
        if "interrupt" in str(e).lower():
            return {
                "endpoint": "knowledge_flow",
                "filters": {
                    "source_cpc": source_cpc,
                    "target_cpc": target_cpc,
                    "firm": firm,
                    "date_range": {"from": year_from, "to": year_to},
                },
                "flow_summary": {
                    "total_cross_cpc_citations": 0,
                    "spillover_rate": None,
                    "unique_cpc_pairs": 0,
                    "top_knowledge_exporters": [],
                    "top_knowledge_importers": [],
                },
                "flow_pairs": [],
                "note": "Query timed out during cross-CPC citation analysis. Try narrower CPC or date filters.",
                "interpretation": "タイムアウトのため分析できませんでした。CPCコードや日付範囲を絞ってください。",
            }
        raise

    if not rows:
        return {
            "error": "No cross-CPC citation data found for the given filters.",
            "suggestion": "Try broader date range or remove CPC filters.",
        }

    # Build flow matrix
    exports: dict[str, int] = defaultdict(int)   # source → others
    imports: dict[str, int] = defaultdict(int)    # others → target
    flow_pairs = []

    total_flow = 0
    for r in rows:
        src = r["source_cpc4"]
        tgt = r["target_cpc4"]
        cnt = r["flow_count"]
        exports[src] += cnt
        imports[tgt] += cnt
        total_flow += cnt
        flow_pairs.append({
            "from": src,
            "to": tgt,
            "count": cnt,
            "label": f"{CPC_CLASS_JA.get(src, src)} → {CPC_CLASS_JA.get(tgt, tgt)}",
        })

    # Net flow
    all_cpcs = set(exports.keys()) | set(imports.keys())
    net_flows = []
    for cpc in all_cpcs:
        net = exports.get(cpc, 0) - imports.get(cpc, 0)
        net_flows.append({
            "cpc": cpc,
            "label": CPC_CLASS_JA.get(cpc, ""),
            "exports": exports.get(cpc, 0),
            "imports": imports.get(cpc, 0),
            "net_flow": net,
            "role": "基盤技術（知識供給者）" if net > 0 else "応用技術（知識消費者）",
        })

    net_flows.sort(key=lambda x: x["net_flow"], reverse=True)

    # Spillover rate (skip if running out of time — this COUNT is very expensive)
    if _time.monotonic() < _kf_deadline - 5:
        try:
            total_citations_row = conn.execute(
                "SELECT COUNT(*) as cnt FROM patent_citations pc "
                "JOIN patents p ON pc.citing_publication = p.publication_number "
                "WHERE p.filing_date >= ? AND p.filing_date <= ? LIMIT 1",
                (filing_from, filing_to),
            ).fetchone()
            total_citations = total_citations_row["cnt"] if total_citations_row else total_flow
        except sqlite3.OperationalError:
            total_citations = total_flow  # fallback: use flow count as denominator
    else:
        total_citations = total_flow  # skip expensive COUNT — use flow count
    spillover_rate = round(total_flow / max(total_citations, 1), 4)

    top_exporters = [f for f in net_flows if f["net_flow"] > 0][:top_n]
    top_importers = [f for f in net_flows if f["net_flow"] < 0]
    top_importers.sort(key=lambda x: x["net_flow"])
    top_importers = top_importers[:top_n]

    return {
        "endpoint": "knowledge_flow",
        "filters": {
            "source_cpc": source_cpc,
            "target_cpc": target_cpc,
            "firm": firm,
            "date_range": {"from": year_from, "to": year_to},
        },
        "flow_summary": {
            "total_cross_cpc_citations": total_flow,
            "spillover_rate": spillover_rate,
            "unique_cpc_pairs": len(flow_pairs),
            "top_knowledge_exporters": top_exporters[:10],
            "top_knowledge_importers": top_importers[:10],
        },
        "flow_pairs": flow_pairs[:top_n],
        "interpretation": (
            f"異分野間引用{total_flow}件、スピルオーバー率{spillover_rate:.1%}。"
            f"最大の知識供給源は{top_exporters[0]['cpc']}({top_exporters[0]['label']})。"
            if top_exporters else "知識フローデータが不十分。"
        ),
        "visualization_hint": {
            "recommended_chart": "sankey",
            "title": "技術間知識フロー",
            "data": "flow_pairs",
        },
    }


# ─── Tool 7: network_resilience ─────────────────────────────────────

def network_resilience(
    store: PatentStore,
    resolver: EntityResolver | None = None,
    firm: str | None = None,
    cpc_prefix: str | None = None,
    attack_mode: str = "targeted",
    removal_steps: int = 10,
    max_patents: int = 500,
) -> dict[str, Any]:
    """Network resilience analysis (percolation theory)."""
    store._relax_timeout()
    conn = store._conn()

    max_patents = min(max_patents, 500)

    # Get seed patents
    if firm and resolver:
        firm_id = _resolve_firm(resolver, firm)
        if not firm_id:
            return {"error": f"Could not resolve firm: '{firm}'"}
        cpc_filter = f"AND pc2.cpc_code LIKE '{cpc_prefix}%'" if cpc_prefix else ""
        seed_rows = conn.execute(
            f"SELECT pa.publication_number FROM patent_assignees pa "
            f"JOIN citation_counts cc ON pa.publication_number = cc.publication_number "
            f"{'JOIN patent_cpc pc2 ON pa.publication_number = pc2.publication_number ' + cpc_filter if cpc_prefix else ''}"
            f"WHERE pa.firm_id = ? ORDER BY cc.forward_citations DESC LIMIT ?",
            (firm_id, min(30, max_patents // 5)),
        ).fetchall()
    elif cpc_prefix:
        seed_rows = conn.execute(
            "SELECT pc.publication_number FROM patent_cpc pc "
            "JOIN citation_counts cc ON pc.publication_number = cc.publication_number "
            "WHERE pc.cpc_code LIKE ? ORDER BY cc.forward_citations DESC LIMIT ?",
            (f"{cpc_prefix}%", min(30, max_patents // 5)),
        ).fetchall()
    else:
        return {"error": "Either firm or cpc_prefix is required."}

    seeds = [r["publication_number"] for r in seed_rows]
    if not seeds:
        return {"error": "No patents found."}

    # Build graph
    adjacency, edges = _build_subgraph(conn, seeds, max_patents)
    original_size = len(adjacency)
    if original_size < 10:
        return {"error": f"Network too small ({original_size} nodes, need >=10)."}

    original_lcc = _largest_component_size(adjacency)

    # Simulate both targeted and random removal
    results = {}
    for mode in ["targeted", "random"]:
        adj_copy = {n: set(nbrs) for n, nbrs in adjacency.items()}
        nodes_list = list(adj_copy.keys())
        in_deg = _in_degree(edges)

        curve = [{"removal_pct": 0, "largest_component_pct": 100}]
        collapse_threshold = 1.0
        removed_nodes = []
        nodes_per_step = max(1, original_size // removal_steps)

        for step in range(1, removal_steps + 1):
            for _ in range(nodes_per_step):
                if not adj_copy:
                    break
                if mode == "targeted":
                    # Remove highest degree node
                    max_node = max(adj_copy.keys(), key=lambda n: len(adj_copy.get(n, set())))
                    remove_node = max_node
                else:
                    remove_node = random.choice(list(adj_copy.keys()))

                removed_nodes.append(remove_node)
                # Remove from adjacency
                for nb in adj_copy.get(remove_node, set()):
                    if nb in adj_copy:
                        adj_copy[nb].discard(remove_node)
                del adj_copy[remove_node]

            lcc = _largest_component_size(adj_copy)
            lcc_pct = round(lcc / original_lcc * 100, 1)
            removal_pct = round(len(removed_nodes) / original_size * 100, 1)
            curve.append({"removal_pct": removal_pct, "largest_component_pct": lcc_pct})

            if lcc_pct < 50 and collapse_threshold == 1.0:
                collapse_threshold = round(len(removed_nodes) / original_size, 3)

        results[mode] = {
            "curve": curve,
            "collapse_threshold": collapse_threshold,
        }

    # Vulnerability index
    t_thresh = results["targeted"]["collapse_threshold"]
    r_thresh = results["random"]["collapse_threshold"]
    vulnerability_index = round(t_thresh / max(r_thresh, 0.01), 3)

    # Critical patents (from targeted removal)
    # Re-run targeted to identify which patents matter most
    adj_copy2 = {n: set(nbrs) for n, nbrs in adjacency.items()}
    critical_patents = []
    for _ in range(min(10, original_size)):
        if not adj_copy2:
            break
        max_node = max(adj_copy2.keys(), key=lambda n: len(adj_copy2.get(n, set())))
        before = _largest_component_size(adj_copy2)
        # Remove
        for nb in adj_copy2.get(max_node, set()):
            if nb in adj_copy2:
                adj_copy2[nb].discard(max_node)
        del adj_copy2[max_node]
        after = _largest_component_size(adj_copy2)
        impact = round((before - after) / max(original_lcc, 1), 3)

        # Get patent info
        pat = conn.execute(
            "SELECT title_ja, title_en, filing_date FROM patents WHERE publication_number = ?",
            (max_node,),
        ).fetchone()
        fd = pat["filing_date"] if pat else 0
        fy = int(str(fd)[:4]) if fd and fd > 0 else 2010
        remaining = max(0, 20 - (2024 - fy))

        critical_patents.append({
            "patent": max_node,
            "title": (pat["title_ja"] or pat["title_en"] or "") if pat else "",
            "removal_impact": impact,
            "remaining_years": remaining,
        })

    return {
        "endpoint": "network_resilience",
        "query": {"firm": firm, "cpc_prefix": cpc_prefix, "attack_mode": attack_mode},
        "network_size": {"nodes": original_size, "edges": len(edges)},
        "resilience_curves": {
            "targeted": {
                "removal_pct": [p["removal_pct"] for p in results["targeted"]["curve"]],
                "largest_component_pct": [p["largest_component_pct"] for p in results["targeted"]["curve"]],
                "collapse_threshold": results["targeted"]["collapse_threshold"],
            },
            "random": {
                "removal_pct": [p["removal_pct"] for p in results["random"]["curve"]],
                "largest_component_pct": [p["largest_component_pct"] for p in results["random"]["curve"]],
                "collapse_threshold": results["random"]["collapse_threshold"],
            },
        },
        "vulnerability_index": vulnerability_index,
        "critical_patents": critical_patents[:10],
        "interpretation": (
            f"ネットワーク({original_size}ノード)はハブ攻撃に"
            f"{'脆弱' if t_thresh < 0.3 else '中程度の耐性' if t_thresh < 0.5 else '強い耐性'}"
            f"(閾値{t_thresh*100:.0f}%)。"
            f"ランダム故障には{'強い' if r_thresh > 0.4 else '弱い'}耐性(閾値{r_thresh*100:.0f}%)。"
            f"脆弱性指数={vulnerability_index}。"
            + (f"{critical_patents[0]['patent']}の除去でネットワークの"
               f"{critical_patents[0]['removal_impact']*100:.0f}%が分断。"
               if critical_patents and critical_patents[0]['removal_impact'] > 0.05 else "")
        ),
        "visualization_hint": {
            "recommended_chart": "line",
            "title": "ネットワークレジリエンス曲線",
            "axes": {"x": "removal_pct", "y": "largest_component_pct"},
            "series": ["targeted", "random"],
        },
    }


# ─── Tool 8: tech_fusion_detector ───────────────────────────────────

def tech_fusion_detector(
    store: PatentStore,
    resolver: EntityResolver | None = None,
    cpc_a: str | None = None,
    cpc_b: str | None = None,
    firm: str | None = None,
    date_from: str = "2015-01-01",
    date_to: str = "2024-12-31",
    min_co_citation: int = 5,
) -> dict[str, Any]:
    """Technology fusion detection via co-citation analysis."""
    store._relax_timeout()
    conn = store._conn()

    year_from = int(date_from[:4])
    year_to = int(date_to[:4])

    if cpc_a and cpc_b:
        # Pair analysis mode
        return _fusion_pair_analysis(conn, cpc_a.upper(), cpc_b.upper(),
                                      firm, resolver, year_from, year_to, min_co_citation)
    else:
        # Auto-detect mode
        return _fusion_auto_detect(conn, firm, resolver, year_from, year_to, min_co_citation)


def _fusion_pair_analysis(conn, cpc_a, cpc_b, firm, resolver, year_from, year_to, min_co) -> dict:
    """Analyze fusion between two specific CPC areas.

    Optimized: 2-step Python approach instead of year-loop correlated subqueries.
    Step 1: Get citing patents for CPC-A with year info (single query)
    Step 2: Check which of those also cite CPC-B (batched)
    """
    cpc_a4 = _cpc4(cpc_a)
    cpc_b4 = _cpc4(cpc_b)

    firm_filter = ""
    firm_params: list = []
    if firm and resolver:
        firm_id = _resolve_firm(resolver, firm)
        if firm_id:
            firm_filter = "AND pa_f.firm_id = ? "
            firm_params = [firm_id]

    filing_from = year_from * 10000 + 101
    filing_to = year_to * 10000 + 1231

    # Fast approach: use pre-computed tech_cluster_momentum + startability_surface
    # to estimate fusion strength, then sample raw citations for bridge patents only.

    # Find cluster IDs for both CPC areas
    cluster_a_rows = conn.execute(
        "SELECT cluster_id FROM tech_clusters WHERE cpc_class LIKE ?",
        (f"{cpc_a4}%",),
    ).fetchall()
    cluster_b_rows = conn.execute(
        "SELECT cluster_id FROM tech_clusters WHERE cpc_class LIKE ?",
        (f"{cpc_b4}%",),
    ).fetchall()
    cluster_a_ids = [r["cluster_id"] for r in cluster_a_rows]
    cluster_b_ids = [r["cluster_id"] for r in cluster_b_rows]

    # Get firms active in BOTH areas via startability_surface
    timeline_data: dict[int, int] = defaultdict(int)
    bridge_pubs: list[str] = []

    if cluster_a_ids and cluster_b_ids:
        ph_a = ",".join("?" for _ in cluster_a_ids)
        ph_b = ",".join("?" for _ in cluster_b_ids)
        # Count firms with presence in both areas, by year
        overlap_rows = conn.execute(
            f"""SELECT sa.year, COUNT(DISTINCT sa.firm_id) as cnt
            FROM startability_surface sa
            JOIN startability_surface sb ON sa.firm_id = sb.firm_id AND sa.year = sb.year
            WHERE sa.cluster_id IN ({ph_a})
              AND sb.cluster_id IN ({ph_b})
              AND sa.score > 0.1 AND sb.score > 0.1
            GROUP BY sa.year
            ORDER BY sa.year""",
            cluster_a_ids + cluster_b_ids,
        ).fetchall()
        for r in overlap_rows:
            timeline_data[r["year"]] = r["cnt"]

    # Sample bridge patents via citation_index (limited query)
    try:
        bridge_rows = conn.execute(
            """SELECT DISTINCT ci.citing_publication
            FROM citation_index ci
            JOIN patent_cpc ca ON ci.cited_publication = ca.publication_number
            JOIN patent_cpc cb ON ci.cited_publication = cb.publication_number
            WHERE substr(ca.cpc_code, 1, 4) = ?
              AND substr(cb.cpc_code, 1, 4) = ?
              AND ca.is_first = 1
            LIMIT 10""",
            (cpc_a4, cpc_b4),
        ).fetchall()
        bridge_pubs = [r["citing_publication"] for r in bridge_rows]
    except Exception:
        # Fallback: use patent_citations with tight LIMIT
        try:
            bridge_rows = conn.execute(
                """SELECT DISTINCT pc.citing_publication
                FROM patent_citations pc
                JOIN patent_cpc ca ON pc.cited_publication = ca.publication_number
                WHERE substr(ca.cpc_code, 1, 4) = ?
                  AND ca.is_first = 1
                  AND pc.citing_publication IN (
                      SELECT pc2.citing_publication FROM patent_citations pc2
                      JOIN patent_cpc cb ON pc2.cited_publication = cb.publication_number
                      WHERE substr(cb.cpc_code, 1, 4) = ? AND cb.is_first = 1
                      LIMIT 10000
                  )
                LIMIT 10""",
                (cpc_a4, cpc_b4),
            ).fetchall()
            bridge_pubs = [r["citing_publication"] for r in bridge_rows]
        except Exception:
            pass

    years = list(range(year_from, year_to + 1))
    counts = [timeline_data.get(y, 0) for y in years]
    total_co = sum(counts)

    if total_co < min_co:
        return {
            "endpoint": "tech_fusion_detector",
            "mode": "pair_analysis",
            "cpc_a": {"code": cpc_a, "label": CPC_CLASS_JA.get(cpc_a4, "")},
            "cpc_b": {"code": cpc_b, "label": CPC_CLASS_JA.get(cpc_b4, "")},
            "total_co_citations": total_co,
            "fusion_stage": "no_fusion",
            "interpretation": f"{cpc_a}と{cpc_b}の間に有意な融合シグナルは検出されませんでした。",
        }

    # Fusion index (normalized)
    max_count = max(counts) if counts else 1
    fusion_indices = [round(c / max(max_count, 1), 4) for c in counts]

    # Acceleration
    if len(counts) >= 3 and counts[0] > 0:
        growth_rates = []
        for i in range(1, len(counts)):
            if counts[i - 1] > 0:
                growth_rates.append(math.log(max(counts[i], 1) / counts[i - 1]))
        acceleration = sum(growth_rates) / len(growth_rates) if growth_rates else 0
    else:
        acceleration = 0

    # Stage
    latest_idx = fusion_indices[-1] if fusion_indices else 0
    if acceleration > 0.3:
        stage = "rapid_convergence"
    elif latest_idx > 0.7:
        stage = "mature_fusion"
    elif total_co > min_co:
        stage = "emerging"
    else:
        stage = "no_fusion"

    # Bridge patents (already collected during step 2)
    bridge_patents = []
    for pub in bridge_pubs[:10]:
        pat = conn.execute(
            "SELECT title_ja, title_en, citation_count_forward FROM patents WHERE publication_number = ?",
            (pub,),
        ).fetchone()
        bridge_patents.append({
            "patent": pub,
            "title": (pat["title_ja"] or pat["title_en"] or "") if pat else "",
            "cited_by": (pat["citation_count_forward"] or 0) if pat else 0,
        })

    # Key players — use startability_surface for both CPC areas (fast)
    key_players_rows = conn.execute(
        """SELECT ss.firm_id, SUM(ss.score) as score
        FROM startability_surface ss
        JOIN tech_clusters tc ON ss.cluster_id = tc.cluster_id
        WHERE (tc.cpc_class LIKE ? OR tc.cpc_class LIKE ?)
          AND ss.year = (SELECT MAX(year) FROM startability_surface
                         WHERE year IN (SELECT year FROM startability_surface
                                        GROUP BY year HAVING COUNT(*) > 1000))
        GROUP BY ss.firm_id ORDER BY score DESC LIMIT 10""",
        (f"{cpc_a4}%", f"{cpc_b4}%"),
    ).fetchall()
    key_players = key_players_rows if key_players_rows else []

    return {
        "endpoint": "tech_fusion_detector",
        "mode": "pair_analysis",
        "cpc_a": {"code": cpc_a, "label": CPC_CLASS_JA.get(cpc_a4, "")},
        "cpc_b": {"code": cpc_b, "label": CPC_CLASS_JA.get(cpc_b4, "")},
        "fusion_timeline": {
            "years": years,
            "co_citation_count": counts,
            "fusion_index": fusion_indices,
        },
        "fusion_acceleration": round(acceleration, 4),
        "fusion_stage": stage,
        "bridge_patents": bridge_patents,
        "key_players": [
            {"firm_id": r["firm_id"], "co_domain_patents": int(r["score"] if "score" in r.keys() else r["cnt"] if "cnt" in r.keys() else 0)}
            for r in key_players
        ],
        "interpretation": (
            f"{CPC_CLASS_JA.get(cpc_a4, cpc_a)}と{CPC_CLASS_JA.get(cpc_b4, cpc_b)}の融合: "
            f"共引用{total_co}件、加速度{acceleration:.2f}。"
            f"段階: {stage}。"
            + (f"主要ブリッジ企業: {key_players[0]['firm_id']}。" if key_players else "")
        ),
        "visualization_hint": {
            "recommended_chart": "line",
            "title": f"技術融合: {cpc_a} × {cpc_b}",
            "axes": {"x": "years", "y": "co_citation_count"},
        },
    }


def _fusion_auto_detect(conn, firm, resolver, year_from, year_to, min_co) -> dict:
    """Auto-detect emerging technology fusions."""
    filing_from = year_from * 10000 + 101
    filing_to = year_to * 10000 + 1231

    firm_filter = ""
    firm_params: list = []
    if firm and resolver:
        firm_id = _resolve_firm(resolver, firm)
        if firm_id:
            firm_filter = "AND pc.citing_publication IN (SELECT publication_number FROM patent_assignees WHERE firm_id = ?) "
            firm_params = [firm_id]

    # Find CPC pairs with growing cross-citations
    rows = conn.execute(
        f"""SELECT
            SUBSTR(c1.cpc_code, 1, 4) as cpc_citing,
            SUBSTR(c2.cpc_code, 1, 4) as cpc_cited,
            COUNT(*) as cnt
        FROM patent_citations pc
        JOIN patent_cpc c1 ON pc.citing_publication = c1.publication_number AND c1.is_first = 1
        JOIN patent_cpc c2 ON pc.cited_publication = c2.publication_number AND c2.is_first = 1
        JOIN patents p ON pc.citing_publication = p.publication_number
        WHERE SUBSTR(c1.cpc_code, 1, 1) != SUBSTR(c2.cpc_code, 1, 1)
        AND p.filing_date >= ? AND p.filing_date <= ?
        {firm_filter}
        GROUP BY cpc_citing, cpc_cited
        HAVING cnt >= ?
        ORDER BY cnt DESC
        LIMIT 50""",
        (filing_from, filing_to, *firm_params, min_co),
    ).fetchall()

    if not rows:
        return {
            "endpoint": "tech_fusion_detector",
            "mode": "auto_detect",
            "emerging_fusions": [],
            "interpretation": "有意な技術融合パターンは検出されませんでした。",
        }

    # Check growth: batch all top pairs in a single query
    mid_year = (year_from + year_to) // 2
    top_pairs = rows[:20]

    # Build batch query for recent counts of all pairs at once
    recent_map: dict[tuple[str, str], int] = {}
    if top_pairs:
        pair_conditions = []
        pair_params_batch: list = [mid_year * 10000 + 101]
        for r in top_pairs:
            pair_conditions.append(
                "(SUBSTR(c1.cpc_code,1,4)=? AND SUBSTR(c2.cpc_code,1,4)=?)"
            )
            pair_params_batch.extend([r["cpc_citing"], r["cpc_cited"]])
        pair_params_batch.extend(firm_params)

        batch_q = f"""SELECT SUBSTR(c1.cpc_code,1,4) as cpc_citing,
                             SUBSTR(c2.cpc_code,1,4) as cpc_cited,
                             COUNT(*) as cnt
                      FROM patent_citations pc
                      JOIN patent_cpc c1 ON pc.citing_publication = c1.publication_number AND c1.is_first = 1
                      JOIN patent_cpc c2 ON pc.cited_publication = c2.publication_number AND c2.is_first = 1
                      JOIN patents p ON pc.citing_publication = p.publication_number
                      WHERE p.filing_date >= ?
                        AND ({' OR '.join(pair_conditions)})
                        {firm_filter}
                      GROUP BY cpc_citing, cpc_cited"""
        try:
            recent_rows = conn.execute(batch_q, pair_params_batch).fetchall()
            for rr in recent_rows:
                recent_map[(rr["cpc_citing"], rr["cpc_cited"])] = rr["cnt"]
        except Exception:
            pass  # Fallback: all accel = 0

    fusions = []
    for r in top_pairs:
        cpc_citing = r["cpc_citing"]
        cpc_cited = r["cpc_cited"]
        total = r["cnt"]

        recent = recent_map.get((cpc_citing, cpc_cited), 0)
        older = max(total - recent, 1)
        accel = round(recent / older - 1, 3) if older > 0 else 0

        stage = "rapid_convergence" if accel > 0.5 else ("emerging" if accel > 0 else "mature_fusion")

        fusions.append({
            "cpc_a": cpc_cited,
            "cpc_a_label": CPC_CLASS_JA.get(cpc_cited, ""),
            "cpc_b": cpc_citing,
            "cpc_b_label": CPC_CLASS_JA.get(cpc_citing, ""),
            "total_cross_citations": total,
            "acceleration": accel,
            "stage": stage,
        })

    fusions.sort(key=lambda x: x["acceleration"], reverse=True)

    return {
        "endpoint": "tech_fusion_detector",
        "mode": "auto_detect",
        "date_range": {"from": year_from, "to": year_to},
        "emerging_fusions": fusions[:10],
        "total_pairs_analyzed": len(rows),
        "interpretation": (
            f"{len(fusions)}件の技術融合パターンを検出。"
            f"最も活発: {fusions[0]['cpc_a_label']}×{fusions[0]['cpc_b_label']} "
            f"(加速度{fusions[0]['acceleration']:.2f})。"
            if fusions else "技術融合パターンなし。"
        ),
    }


# ─── Tool 9: tech_entropy ───────────────────────────────────────────

def tech_entropy(
    store: PatentStore,
    resolver: EntityResolver | None = None,
    cpc_prefix: str | None = None,
    query: str | None = None,
    date_from: str = "2015-01-01",
    date_to: str = "2024-12-31",
    granularity: str = "year",
) -> dict[str, Any]:
    """Technology maturity analysis via Shannon entropy of applicant distribution.

    Uses cpc4_firm_year_counts (pre-computed actual filing data) when available.
    Falls back to startability_surface for share calculation otherwise.
    Always uses tech_cluster_momentum for total filing counts.
    """
    store._relax_timeout()
    conn = store._conn()

    year_from = int(date_from[:4])
    year_to = int(date_to[:4])

    # Resolve CPC prefix
    effective_cpc = (cpc_prefix or query or "").strip().upper()
    if not effective_cpc:
        return {"error": "cpc_prefix or query is required."}

    # Map keywords to CPC
    _KW_CPC = {
        "電池": "H01M", "バッテリー": "H01M", "battery": "H01M",
        "半導体": "H01L", "semiconductor": "H01L",
        "AI": "G06N", "人工知能": "G06N", "機械学習": "G06N",
        "自動運転": "B60W", "EV": "B60L", "医薬": "A61K",
    }
    for kw, cpc in _KW_CPC.items():
        if kw.lower() in (query or "").lower():
            effective_cpc = cpc
            break
    else:
        # Extended lookup from 100-entry JP tech CPC map
        for kw, cpc in _EXT_CPC.items():
            if kw.lower() in (query or "").lower():
                effective_cpc = cpc
                break

    cpc4 = _cpc4(effective_cpc)

    # Find matching tech_clusters for this CPC
    cluster_rows = conn.execute(
        "SELECT cluster_id FROM tech_clusters WHERE cpc_class LIKE ?",
        (f"{effective_cpc}%",),
    ).fetchall()
    cluster_ids = [r["cluster_id"] for r in cluster_rows]

    if not cluster_ids:
        return {"error": f"No tech_clusters found for CPC prefix \'{effective_cpc}\'."}

    # Get ACTUAL filing counts per year from tech_cluster_momentum
    ph = ",".join("?" for _ in cluster_ids)
    tcm_rows = conn.execute(
        f"""SELECT year, SUM(patent_count) as total_filings
        FROM tech_cluster_momentum
        WHERE cluster_id IN ({ph}) AND year BETWEEN ? AND ?
        GROUP BY year ORDER BY year""",
        cluster_ids + [year_from, year_to],
    ).fetchall()
    yearly_filings = {r["year"]: r["total_filings"] for r in tcm_rows}

    # Check if cpc4_firm_year_counts table exists and has data for this CPC
    has_precomputed = False
    try:
        check = conn.execute(
            "SELECT COUNT(*) FROM cpc4_firm_year_counts WHERE cpc4 = ?",
            (cpc4,),
        ).fetchone()
        has_precomputed = check and check[0] > 0
    except Exception:
        pass

    entropy_timeline = []
    data_source = "pre_computed" if has_precomputed else "startability_surface"

    for yr in range(year_from, year_to + 1):
        if has_precomputed:
            # Use actual filing data from cpc4_firm_year_counts
            rows = conn.execute(
                """SELECT firm_id, patent_count as cnt
                FROM cpc4_firm_year_counts
                WHERE cpc4 = ? AND year = ? AND patent_count > 0
                ORDER BY patent_count DESC""",
                (cpc4, yr),
            ).fetchall()
        else:
            # Fallback to startability_surface scores
            rows = conn.execute(
                f"""SELECT firm_id, SUM(score) as cnt
                FROM startability_surface
                WHERE cluster_id IN ({ph})
                AND year = ? AND gate_open = 1
                GROUP BY firm_id
                ORDER BY cnt DESC""",
                cluster_ids + [yr],
            ).fetchall()

        if not rows:
            continue

        total = sum(r["cnt"] for r in rows)
        if total == 0:
            continue

        shares = [r["cnt"] / total for r in rows]

        # Shannon entropy
        H = -sum(p * math.log2(p) for p in shares if p > 0)

        # HHI
        hhi = sum(p**2 for p in shares)

        # Use ACTUAL filing count from tech_cluster_momentum
        total_filings = yearly_filings.get(yr, 0)

        entropy_timeline.append({
            "year": yr,
            "entropy": round(H, 4),
            "hhi": round(hhi, 4),
            "num_applicants": len(rows),
            "total_filings": total_filings,
        })

    if len(entropy_timeline) < 2:
        return {
            "error": f"Insufficient yearly data for \'{effective_cpc}\' (need >=2 years).",
            "years_found": len(entropy_timeline),
        }

    # Trends
    import numpy as np
    years_arr = np.array([e["year"] for e in entropy_timeline])
    entropy_arr = np.array([e["entropy"] for e in entropy_timeline])
    filing_arr = np.array([e["total_filings"] for e in entropy_timeline])

    if len(years_arr) >= 3:
        entropy_slope = float(np.polyfit(years_arr, entropy_arr, 1)[0])
        filing_slope = float(np.polyfit(years_arr, filing_arr, 1)[0])
    else:
        entropy_slope = 0
        filing_slope = 0

    # Lifecycle stage
    if entropy_slope > 0.05 and filing_slope > 0:
        lifecycle = "growth"
        lifecycle_label = "成長期（新規参入増加、出願増加）"
    elif abs(entropy_slope) <= 0.05 and abs(filing_slope / max(float(np.mean(filing_arr)), 1)) <= 0.1:
        lifecycle = "mature"
        lifecycle_label = "成熟期（多数プレイヤー安定）"
    elif entropy_slope < -0.05:
        lifecycle = "declining"
        lifecycle_label = "衰退期（撤退増加、集中化）"
    else:
        lifecycle = "introduction"
        lifecycle_label = "導入期（少数の先駆者）"

    # Latest state — skip years with very low filing counts (incomplete)
    good_entries = [e for e in entropy_timeline if e["total_filings"] > 50]
    if not good_entries:
        good_entries = entropy_timeline
    latest = good_entries[-1]
    latest_yr = latest["year"]

    # Dominant players from the latest year
    if has_precomputed:
        top_rows = conn.execute(
            """SELECT firm_id, patent_count as cnt
            FROM cpc4_firm_year_counts
            WHERE cpc4 = ? AND year = ? AND patent_count > 0
            ORDER BY patent_count DESC LIMIT 20""",
            (cpc4, latest_yr),
        ).fetchall()
    else:
        top_rows = conn.execute(
            f"""SELECT firm_id, SUM(score) as cnt
            FROM startability_surface
            WHERE cluster_id IN ({ph})
            AND year = ? AND gate_open = 1
            GROUP BY firm_id
            ORDER BY cnt DESC LIMIT 20""",
            cluster_ids + [latest_yr],
        ).fetchall()

    total_top = sum(r["cnt"] for r in top_rows) if top_rows else 1
    total_filings_latest = yearly_filings.get(latest_yr, 0) or 1
    dominant_players = []
    top5_share = 0

    for r in top_rows[:5]:
        share = round(r["cnt"] / total_top, 4)
        top5_share += share

        if has_precomputed:
            patent_count = r["cnt"]
        else:
            patent_count = max(1, round(share * total_filings_latest))

        # Trend: compare earliest vs latest year
        firm_trend = "stable"
        if len(entropy_timeline) >= 3:
            early_yr = entropy_timeline[0]["year"]
            if has_precomputed:
                early_row = conn.execute(
                    "SELECT patent_count FROM cpc4_firm_year_counts WHERE cpc4 = ? AND firm_id = ? AND year = ?",
                    (cpc4, r["firm_id"], early_yr),
                ).fetchone()
                early_cnt = early_row["patent_count"] if early_row else 0
            else:
                early_row = conn.execute(
                    f"""SELECT SUM(score) as cnt FROM startability_surface
                    WHERE cluster_id IN ({ph}) AND firm_id = ? AND year = ?""",
                    cluster_ids + [r["firm_id"], early_yr],
                ).fetchone()
                early_cnt = early_row["cnt"] if early_row and early_row["cnt"] else 0
            if r["cnt"] > early_cnt * 1.3:
                firm_trend = "increasing"
            elif early_cnt > 0 and r["cnt"] < early_cnt * 0.7:
                firm_trend = "decreasing"

        dominant_players.append({
            "firm_id": r["firm_id"],
            "share": share,
            "patent_count": patent_count,
            "trend": firm_trend,
        })

    # New entrants (firms present in latest year but not 3+ years ago)
    new_entrants = []
    three_years_ago = latest_yr - 3
    if has_precomputed:
        new_rows = conn.execute(
            """SELECT c.firm_id, c.patent_count as cnt
            FROM cpc4_firm_year_counts c
            WHERE c.cpc4 = ? AND c.year = ? AND c.patent_count > 2
            AND c.firm_id NOT IN (
                SELECT DISTINCT firm_id FROM cpc4_firm_year_counts
                WHERE cpc4 = ? AND year <= ?
            )
            ORDER BY c.patent_count DESC LIMIT 10""",
            (cpc4, latest_yr, cpc4, three_years_ago),
        ).fetchall()
        for nr in new_rows:
            new_entrants.append({
                "firm_id": nr["firm_id"],
                "first_filing_year": latest_yr,
                "patents": nr["cnt"],
            })
    else:
        new_rows = conn.execute(
            f"""SELECT s_new.firm_id, SUM(s_new.score) as tech_score
            FROM startability_surface s_new
            WHERE s_new.cluster_id IN ({ph})
            AND s_new.year = ? AND s_new.score > 0.05
            AND s_new.firm_id NOT IN (
                SELECT DISTINCT firm_id FROM startability_surface
                WHERE cluster_id IN ({ph}) AND year <= ?
            )
            GROUP BY s_new.firm_id
            ORDER BY tech_score DESC LIMIT 10""",
            cluster_ids + [latest_yr] + cluster_ids + [three_years_ago],
        ).fetchall()
        for nr in new_rows:
            est_patents = max(1, round(nr["tech_score"] / total_top * total_filings_latest))
            new_entrants.append({
                "firm_id": nr["firm_id"],
                "first_filing_year": latest_yr,
                "patents": est_patents,
            })

    return {
        "endpoint": "tech_entropy",
        "cpc_prefix": effective_cpc,
        "cpc_label": CPC_CLASS_JA.get(cpc4, ""),
        "date_range": {"from": year_from, "to": year_to},
        "data_source": data_source,
        "entropy_timeline": entropy_timeline,
        "current_state": {
            "entropy": latest["entropy"],
            "hhi": latest["hhi"],
            "top_5_concentration": round(top5_share, 4),
            "num_applicants": latest["num_applicants"],
            "lifecycle_stage": lifecycle,
            "lifecycle_label": lifecycle_label,
            "entropy_trend": round(entropy_slope, 4),
            "filing_trend": round(filing_slope, 2),
        },
        "dominant_players": dominant_players,
        "new_entrants_last_3yr": new_entrants,
        "interpretation": (
            f"{CPC_CLASS_JA.get(cpc4, effective_cpc)}は{lifecycle_label}。"
            f"エントロピーH={latest['entropy']:.2f}, HHI={latest['hhi']:.3f}。"
            f"上位5社集中度{top5_share*100:.1f}%。"
            + (f"新規参入{len(new_entrants)}社。" if new_entrants else "")
        ),
        "visualization_hint": {
            "recommended_chart": "dual_axis",
            "title": f"技術成熟度: {effective_cpc}",
            "axes": {
                "x": "entropy_timeline[].year",
                "y_left": "entropy_timeline[].entropy",
                "y_right": "entropy_timeline[].total_filings",
            },
        },
    }


