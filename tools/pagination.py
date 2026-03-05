"""Pagination helpers for tool responses."""
from __future__ import annotations

import math


def paginate(items: list, page: int = 1, page_size: int = 20) -> dict:
    """Return paginated metadata + slice for a list of items."""
    page_size = min(max(page_size, 1), 100)
    total = len(items)
    pages = math.ceil(total / page_size) if total > 0 else 1
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": pages,
        "results": items[start:end],
    }
