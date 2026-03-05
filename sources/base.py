"""Base class for patent data sources."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseSource(ABC):
    """Abstract base for patent data sources."""

    @abstractmethod
    def search_patents(
        self,
        query: str | None = None,
        cpc_codes: list[str] | None = None,
        applicant: str | None = None,
        jurisdiction: str | None = None,
        date_from: int | None = None,
        date_to: int | None = None,
        max_results: int = 20,
    ) -> list[dict[str, Any]]:
        ...

    @abstractmethod
    def get_applicant_patents(
        self,
        applicant_names: list[str],
        jurisdiction: str | None = None,
        date_from: int | None = None,
        date_to: int | None = None,
    ) -> list[dict[str, Any]]:
        ...
