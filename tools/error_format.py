"""Standardized error response format for all MCP tools.

Wraps tool errors into a consistent structure:
  {
    "status": "error" | "warning" | "partial",
    "tool": "<tool_name>",
    "message": "<human-readable message>",
    "suggestion": "<actionable next step>",
    "data": null | {...partial data...}
  }
"""
from __future__ import annotations
from typing import Any


# Common error patterns → user-friendly messages + suggestions
_ERROR_PATTERNS = [
    ("timed out", "database under heavy",
     "Query timed out due to heavy I/O load.",
     "Try pre-computed tools: startability, tech_fit, adversarial_strategy, similar_firms."),
    ("no patent data", "firm_tech_vectors",
     "No patent portfolio data found for this firm.",
     "Check the firm name spelling, or try entity_resolve to find the correct ID."),
    ("could not resolve firm", "",
     "Could not identify the firm in the database.",
     "Try alternate names (Japanese/English), stock ticker, or entity_resolve."),
    ("insufficient data", "beta",
     "Insufficient filing history for statistical analysis.",
     "This firm or technology needs 3+ years of data. Try a broader CPC code."),
    ("no tech_clusters found", "",
     "No technology clusters match the given input.",
     "Use a CPC code (e.g., H01M, G06N) instead of a keyword, or check tech_clusters_list."),
    ("fts5", "malformed",
     "Full-text search index needs rebuilding.",
     "Use CPC-based search or tech_landscape instead of free-text search."),
    ("no startability", "surface",
     "No pre-computed startability data for this firm-technology pair.",
     "The firm may not be in the pre-computed set. Try tech_fit for on-demand calculation."),
]


def standardize_error(result: dict, tool_name: str = "") -> dict:
    """Standardize error responses into consistent format."""
    if not isinstance(result, dict):
        return result
    if "error" not in result:
        return result

    err_msg = str(result.get("error", "")).lower()
    suggestion = result.get("suggestion", "")
    message = str(result.get("error", ""))

    # Match against known patterns
    for pattern1, pattern2, std_msg, std_suggestion in _ERROR_PATTERNS:
        if pattern1 in err_msg and (not pattern2 or pattern2 in err_msg):
            message = std_msg
            if not suggestion:
                suggestion = std_suggestion
            break

    # Build standardized response
    standardized = {
        "status": "error",
        "tool": tool_name or result.get("endpoint", result.get("tool", "")),
        "message": message,
        "suggestion": suggestion or "Check input parameters and try again.",
    }

    # Preserve any partial data
    for key in result:
        if key not in ("error", "suggestion", "status", "tool", "message"):
            if result[key] is not None and result[key] != [] and result[key] != {}:
                if "data" not in standardized:
                    standardized["data"] = {}
                standardized["data"][key] = result[key]

    return standardized
