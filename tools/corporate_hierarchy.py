"""corporate_hierarchy tools — corporate group analysis.

Provides three tools:
- corporate_hierarchy: Build and traverse a corporate group tree.
- group_portfolio: Aggregate patent portfolio across all group members.
- group_startability: Aggregate startability scores for a corporate group.

Uses the corporate_hierarchy table for parent/child relationships,
firm_tech_vectors for patent counts and tech profiles, and
startability_surface for technology readiness scores.
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any

from db.sqlite_store import PatentStore
from entity.resolver import EntityResolver


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_firm(
    resolver: EntityResolver | None,
    firm_query: str | None,
) -> tuple[str | None, str | None, str | None]:
    """Resolve a firm query to (firm_id, firm_name, company_id).

    Returns (None, None, None) if resolution fails.
    company_id is the 'company_XXXX' format used in DB tables like
    corporate_hierarchy, firm_tech_vectors, and startability_surface.
    """
    if not firm_query or resolver is None:
        return None, None, None
    resolved = resolver.resolve(firm_query, country_hint="JP")
    if resolved is None:
        return None, None, None
    entity = resolved.entity
    # Build company_XXXX id from ticker if available
    company_id = None
    if entity.ticker:
        company_id = f"company_{entity.ticker}"
    elif entity.canonical_id.startswith("company_"):
        company_id = entity.canonical_id
    return entity.canonical_id, entity.canonical_name, company_id


def _firm_display_name(
    resolver: EntityResolver | None,
    firm_id: str,
) -> str:
    """Get human-readable name for a firm_id, falling back to the id."""
    if resolver is None:
        return firm_id
    entity = resolver.registry.get(firm_id)
    if entity is not None:
        return entity.canonical_name
    return firm_id


def _find_root(conn: sqlite3.Connection, firm_id: str) -> str:
    """Walk up the corporate_hierarchy to find the topmost parent.

    If no parent record exists, the firm itself is the root.
    Limits traversal to 10 levels to prevent infinite loops.
    """
    current = firm_id
    visited: set[str] = set()
    for _ in range(10):
        if current in visited:
            break
        visited.add(current)
        row = conn.execute(
            "SELECT parent_firm_id FROM corporate_hierarchy WHERE firm_id = ?",
            (current,),
        ).fetchone()
        if row is None:
            break
        current = row["parent_firm_id"]
    return current


def _get_group_members(
    conn: sqlite3.Connection,
    firm_id: str,
    depth: int = 2,
) -> list[str]:
    """BFS from *root* of the group containing firm_id.

    Returns all firm_ids in the group (including the root).
    The root is determined by walking up from the given firm_id.
    """
    root = _find_root(conn, firm_id)
    members: list[str] = [root]
    visited: set[str] = {root}
    frontier: list[str] = [root]

    for _ in range(depth):
        if not frontier:
            break
        next_frontier: list[str] = []
        placeholders = ",".join("?" for _ in frontier)
        rows = conn.execute(
            f"SELECT firm_id, parent_firm_id FROM corporate_hierarchy "
            f"WHERE parent_firm_id IN ({placeholders})",
            frontier,
        ).fetchall()
        for row in rows:
            child = row["firm_id"]
            if child not in visited:
                visited.add(child)
                members.append(child)
                next_frontier.append(child)
        frontier = next_frontier

    return members


def _resolve_cluster(
    conn: sqlite3.Connection,
    tech_query_or_cluster_id: str,
) -> dict[str, Any] | None:
    """Resolve a cluster by ID or label LIKE match.

    Returns dict with cluster_id and label, or None.
    """
    q = (tech_query_or_cluster_id or "").strip()
    if not q:
        return None

    looks_like_id = "_" in q and len(q) <= 40

    if looks_like_id:
        row = conn.execute(
            "SELECT cluster_id, label FROM tech_clusters WHERE cluster_id = ?",
            (q,),
        ).fetchone()
        if row is not None:
            return {"cluster_id": row["cluster_id"], "label": row["label"]}

    # Fallback: LIKE search on label or cpc_class
    like = f"%{q}%"
    row = conn.execute(
        "SELECT cluster_id, label FROM tech_clusters "
        "WHERE label LIKE ? OR cpc_class LIKE ? || '%' "
        "ORDER BY patent_count DESC LIMIT 1",
        (like, q),
    ).fetchone()
    if row is not None:
        return {"cluster_id": row["cluster_id"], "label": row["label"]}
    return None


def _build_tree(
    conn: sqlite3.Connection,
    resolver: EntityResolver | None,
    root_id: str,
    depth: int,
    include_patents: bool,
    relationship_counts: dict[str, int],
) -> dict[str, Any]:
    """Recursively build a tree node for the given firm_id.

    Args:
        conn: SQLite connection.
        resolver: Entity resolver for display names.
        root_id: Current node firm_id.
        depth: Remaining depth to traverse.
        include_patents: Whether to attach patent_count.
        relationship_counts: Mutable dict to accumulate relationship type counts.

    Returns:
        Tree node dict with firm_id, firm_name, children, etc.
    """
    node: dict[str, Any] = {
        "firm_id": root_id,
        "firm_name": _firm_display_name(resolver, root_id),
    }

    if include_patents:
        pat_row = conn.execute(
            "SELECT patent_count FROM firm_tech_vectors "
            "WHERE firm_id = ? ORDER BY year DESC LIMIT 1",
            (root_id,),
        ).fetchone()
        node["patent_count"] = pat_row["patent_count"] if pat_row else 0

    children: list[dict[str, Any]] = []
    if depth > 0:
        rows = conn.execute(
            "SELECT firm_id, relationship, ownership_pct "
            "FROM corporate_hierarchy WHERE parent_firm_id = ?",
            (root_id,),
        ).fetchall()
        for row in rows:
            rel = row["relationship"] or "subsidiary"
            relationship_counts[rel] = relationship_counts.get(rel, 0) + 1

            child_node = _build_tree(
                conn,
                resolver,
                row["firm_id"],
                depth - 1,
                include_patents,
                relationship_counts,
            )
            child_node["relationship"] = rel
            child_node["ownership_pct"] = (
                round(row["ownership_pct"], 2)
                if row["ownership_pct"] is not None
                else None
            )
            children.append(child_node)

    node["children"] = children
    return node


def _count_tree_members(node: dict[str, Any]) -> int:
    """Count total members in a tree (including root)."""
    count = 1
    for child in node.get("children", []):
        count += _count_tree_members(child)
    return count


# ---------------------------------------------------------------------------
# Tool 1: corporate_hierarchy
# ---------------------------------------------------------------------------

def corporate_hierarchy(
    store: PatentStore,
    firm_query: str | None = None,
    resolver: EntityResolver | None = None,
    depth: int = 2,
    include_patents: bool = False,
) -> dict[str, Any]:
    """Build and return the corporate group tree for a given firm.

    Resolves firm_query to a canonical firm_id, finds the topmost parent,
    then does a BFS traversal down to the specified depth building a tree
    of subsidiaries and affiliates.

    Args:
        store: PatentStore for DB access.
        firm_query: Company name, ticker, or identifier.
        resolver: EntityResolver for name resolution.
        depth: Maximum traversal depth from root (default: 2).
        include_patents: Attach patent_count to each member (default: False).

    Returns:
        Dict with tree structure, total member count, and relationship summary.
    """
    if not firm_query:
        return {
            "endpoint": "corporate_hierarchy",
            "error": "firm_query is required.",
        }

    firm_id, firm_name, company_id = _resolve_firm(resolver, firm_query)
    if firm_id is None:
        return {
            "endpoint": "corporate_hierarchy",
            "error": f"Could not resolve firm: '{firm_query}'",
            "suggestion": "Try the exact company name, Japanese name, or stock ticker.",
        }

    # Use company_XXXX format for DB lookups (hierarchy table uses this format)
    lookup_id = company_id or firm_id
    depth = max(1, min(int(depth), 5))

    try:
        conn = store._conn()

        # Find root of the group
        root_id = _find_root(conn, lookup_id)

        # Check if there is any hierarchy data at all
        has_data = conn.execute(
            "SELECT 1 FROM corporate_hierarchy "
            "WHERE firm_id = ? OR parent_firm_id = ? LIMIT 1",
            (root_id, root_id),
        ).fetchone()

        if has_data is None and root_id == lookup_id:
            # No hierarchy data — return the firm itself with a note
            node: dict[str, Any] = {
                "firm_id": firm_id,
                "firm_name": firm_name or _firm_display_name(resolver, firm_id),
                "children": [],
            }
            if include_patents:
                pat_row = conn.execute(
                    "SELECT patent_count FROM firm_tech_vectors "
                    "WHERE firm_id = ? ORDER BY year DESC LIMIT 1",
                    (firm_id,),
                ).fetchone()
                node["patent_count"] = pat_row["patent_count"] if pat_row else 0

            return {
                "endpoint": "corporate_hierarchy",
                "root": node,
                "total_members": 1,
                "relationship_summary": {},
                "note": (
                    "No corporate hierarchy data found for this firm. "
                    "The firm is shown as a standalone entity."
                ),
            }

        # Build the tree from root
        relationship_counts: dict[str, int] = {}
        tree = _build_tree(
            conn, resolver, root_id, depth, include_patents, relationship_counts,
        )
        total_members = _count_tree_members(tree)

        return {
            "endpoint": "corporate_hierarchy",
            "root": tree,
            "total_members": total_members,
            "relationship_summary": relationship_counts,
        }

    except sqlite3.OperationalError as exc:
        return {
            "endpoint": "corporate_hierarchy",
            "error": f"Database query timed out or failed: {exc}",
            "suggestion": "Try reducing depth or retry later.",
        }


# ---------------------------------------------------------------------------
# Tool 2: group_portfolio
# ---------------------------------------------------------------------------

def group_portfolio(
    store: PatentStore,
    firm_query: str | None = None,
    resolver: EntityResolver | None = None,
    year: int = 2024,
) -> dict[str, Any]:
    """Aggregate patent portfolio across all members of a corporate group.

    Finds all subsidiaries/affiliates via corporate_hierarchy, queries
    firm_tech_vectors for each, and combines patent counts and CPC
    distributions.

    Args:
        store: PatentStore for DB access.
        firm_query: Company name, ticker, or identifier.
        resolver: EntityResolver for name resolution.
        year: Target analysis year (default: 2024). Falls back to latest.

    Returns:
        Dict with aggregated group portfolio metrics.
    """
    if not firm_query:
        return {
            "endpoint": "group_portfolio",
            "error": "firm_query is required.",
        }

    firm_id, firm_name, company_id = _resolve_firm(resolver, firm_query)
    if firm_id is None:
        return {
            "endpoint": "group_portfolio",
            "error": f"Could not resolve firm: '{firm_query}'",
            "suggestion": "Try the exact company name, Japanese name, or stock ticker.",
        }

    lookup_id = company_id or firm_id
    try:
        conn = store._conn()

        # Get all group members
        members = _get_group_members(conn, lookup_id, depth=2)
        root_id = members[0] if members else lookup_id
        group_name = _firm_display_name(resolver, root_id)

        total_patent_count = 0
        cpc_counts: dict[str, int] = defaultdict(int)
        member_details: list[dict[str, Any]] = []
        actual_years: list[int] = []

        for mid in members:
            # Try requested year, fall back to latest available
            row = conn.execute(
                "SELECT patent_count, dominant_cpc, tech_diversity, tech_concentration "
                "FROM firm_tech_vectors WHERE firm_id = ? AND year = ?",
                (mid, year),
            ).fetchone()
            used_year = year

            if row is None:
                row = conn.execute(
                    "SELECT patent_count, dominant_cpc, tech_diversity, "
                    "tech_concentration, year "
                    "FROM firm_tech_vectors WHERE firm_id = ? "
                    "ORDER BY year DESC LIMIT 1",
                    (mid,),
                ).fetchone()
                if row is not None:
                    used_year = row["year"]

            if row is None:
                member_details.append({
                    "firm_id": mid,
                    "firm_name": _firm_display_name(resolver, mid),
                    "patent_count": 0,
                    "dominant_cpc": None,
                    "note": "No tech vector data available.",
                })
                continue

            pc = row["patent_count"] or 0
            dom_cpc = row["dominant_cpc"] or ""
            diversity = row["tech_diversity"]
            total_patent_count += pc
            actual_years.append(used_year)

            if dom_cpc:
                cpc_counts[dom_cpc] += pc

            member_details.append({
                "firm_id": mid,
                "firm_name": _firm_display_name(resolver, mid),
                "patent_count": pc,
                "dominant_cpc": dom_cpc if dom_cpc else None,
                "tech_diversity": round(diversity / 5.0, 4) if diversity else None,
                "year": used_year,
            })

        # Sort members by patent_count descending
        member_details.sort(key=lambda m: m.get("patent_count", 0), reverse=True)

        # Combined dominant CPCs (sorted by aggregate count)
        combined_cpcs = sorted(
            [{"cpc": k, "aggregate_count": v} for k, v in cpc_counts.items()],
            key=lambda x: x["aggregate_count"],
            reverse=True,
        )

        # Group-level tech diversity: Shannon entropy approximation
        # over the CPC distribution
        group_diversity = 0.0
        if combined_cpcs and total_patent_count > 0:
            import math
            for entry in combined_cpcs:
                p = entry["aggregate_count"] / total_patent_count
                if p > 0:
                    group_diversity -= p * math.log2(p)
            group_diversity = round(group_diversity, 4)

        effective_year = year
        if actual_years:
            # Use the most common year among members
            year_freq: dict[int, int] = defaultdict(int)
            for y in actual_years:
                year_freq[y] += 1
            effective_year = max(year_freq, key=lambda y: year_freq[y])

        result: dict[str, Any] = {
            "endpoint": "group_portfolio",
            "group_name": group_name,
            "year": effective_year,
            "total_patent_count": total_patent_count,
            "member_count": len(members),
            "members": member_details,
            "combined_dominant_cpcs": combined_cpcs[:20],
            "tech_diversity": group_diversity,
        }

        if effective_year != year:
            result["note"] = (
                f"Requested year {year} had limited data. "
                f"Most members used year {effective_year}."
            )

        return result

    except sqlite3.OperationalError as exc:
        return {
            "endpoint": "group_portfolio",
            "error": f"Database query timed out or failed: {exc}",
            "suggestion": "Retry later; the server may be under heavy I/O load.",
        }


# ---------------------------------------------------------------------------
# Tool 3: group_startability
# ---------------------------------------------------------------------------

def group_startability(
    store: PatentStore,
    firm_query: str | None = None,
    tech_query_or_cluster_id: str | None = None,
    resolver: EntityResolver | None = None,
    year: int = 2024,
) -> dict[str, Any]:
    """Aggregate startability scores for a corporate group in a technology area.

    Finds all group members, queries startability_surface for each, and
    returns max/mean scores along with the strongest member.

    Args:
        store: PatentStore for DB access.
        firm_query: Company name, ticker, or identifier.
        tech_query_or_cluster_id: Cluster ID (e.g. "H01M_0") or text query.
        resolver: EntityResolver for name resolution.
        year: Target analysis year (default: 2024). Falls back to best available.

    Returns:
        Dict with group-level and per-member startability scores.
    """
    if not firm_query:
        return {
            "endpoint": "group_startability",
            "error": "firm_query is required.",
        }
    if not tech_query_or_cluster_id:
        return {
            "endpoint": "group_startability",
            "error": "tech_query_or_cluster_id is required.",
        }

    firm_id, firm_name, company_id = _resolve_firm(resolver, firm_query)
    if firm_id is None:
        return {
            "endpoint": "group_startability",
            "error": f"Could not resolve firm: '{firm_query}'",
            "suggestion": "Try the exact company name, Japanese name, or stock ticker.",
        }

    lookup_id = company_id or firm_id
    try:
        conn = store._conn()

        # Resolve cluster
        cluster_info = _resolve_cluster(conn, tech_query_or_cluster_id)
        if cluster_info is None:
            return {
                "endpoint": "group_startability",
                "error": f"No tech cluster found for query: '{tech_query_or_cluster_id}'",
                "suggestion": "Use a cluster_id like 'H01M_0' or a technology keyword.",
            }

        cluster_id = cluster_info["cluster_id"]
        cluster_label = cluster_info["label"] or cluster_id

        # Get all group members
        members = _get_group_members(conn, lookup_id, depth=2)
        root_id = members[0] if members else lookup_id
        group_name = _firm_display_name(resolver, root_id)

        # Query startability for each member
        member_scores: list[dict[str, Any]] = []
        actual_year = year

        for mid in members:
            row = conn.execute(
                "SELECT score, gate_open, phi_tech_cos "
                "FROM startability_surface "
                "WHERE firm_id = ? AND cluster_id = ? AND year = ?",
                (mid, cluster_id, year),
            ).fetchone()

            used_year = year

            if row is None:
                # Year fallback: try best available year
                fallback_row = conn.execute(
                    "SELECT score, gate_open, phi_tech_cos, year "
                    "FROM startability_surface "
                    "WHERE firm_id = ? AND cluster_id = ? "
                    "ORDER BY year DESC LIMIT 1",
                    (mid, cluster_id),
                ).fetchone()
                if fallback_row is not None:
                    row = fallback_row
                    used_year = fallback_row["year"]

            if row is None:
                member_scores.append({
                    "firm_id": mid,
                    "firm_name": _firm_display_name(resolver, mid),
                    "score": 0.0,
                    "gate_open": False,
                    "note": "No startability data for this member-cluster pair.",
                })
                continue

            score_val = row["score"] if row["score"] is not None else 0.0
            member_scores.append({
                "firm_id": mid,
                "firm_name": _firm_display_name(resolver, mid),
                "score": round(float(score_val), 4),
                "gate_open": bool(row["gate_open"]),
                "year": used_year,
            })
            if used_year != year:
                actual_year = used_year

        # Compute aggregates
        scores_with_data = [
            m["score"] for m in member_scores if m["score"] > 0
        ]

        if scores_with_data:
            group_score = round(max(scores_with_data), 4)
            group_mean = round(
                sum(scores_with_data) / len(scores_with_data), 4
            )
        else:
            group_score = 0.0
            group_mean = 0.0

        # Sort members by score descending
        member_scores.sort(key=lambda m: m["score"], reverse=True)

        # Identify strongest member
        recommended = member_scores[0] if member_scores else None
        recommended_entity = (
            recommended["firm_name"]
            if recommended and recommended["score"] > 0
            else None
        )

        # Synergy analysis text
        active_count = sum(1 for m in member_scores if m.get("gate_open"))
        total_count = len(member_scores)

        if active_count == 0:
            synergy_text = (
                f"グループ内に{cluster_label}領域でのgate_open企業はありません。"
                "この技術領域への参入にはグループ外からの技術獲得が必要です。"
            )
        elif active_count == 1:
            best = member_scores[0]
            synergy_text = (
                f"{best['firm_name']}のみがgate_openです "
                f"(スコア: {best['score']})。"
                f"グループ内で{cluster_label}領域を牽引する単一拠点です。"
            )
        else:
            synergy_text = (
                f"{total_count}社中{active_count}社がgate_openです。"
                f"グループ内に{cluster_label}領域の複数拠点があり、"
                "連携によるシナジーが期待できます。"
                f"最強メンバーは{member_scores[0]['firm_name']} "
                f"(スコア: {member_scores[0]['score']})です。"
            )

        result: dict[str, Any] = {
            "endpoint": "group_startability",
            "group_name": group_name,
            "cluster_id": cluster_id,
            "cluster_label": cluster_label,
            "year": actual_year,
            "group_score": group_score,
            "group_mean": group_mean,
            "member_scores": member_scores,
            "recommended_entity": recommended_entity,
            "synergy_analysis": synergy_text,
        }

        if not scores_with_data:
            result["note"] = (
                "No startability data found for any group member in this cluster. "
                "The cluster may not be pre-computed for these firms."
            )

        return result

    except sqlite3.OperationalError as exc:
        return {
            "endpoint": "group_startability",
            "error": f"Database query timed out or failed: {exc}",
            "suggestion": "Retry later; the server may be under heavy I/O load.",
        }
