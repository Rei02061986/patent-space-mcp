"""EPO Open Patent Services (OPS) REST API connector.

Provides access to European patent data including bibliographic data,
legal status (INPADOC), patent family information, and full-text search.

Authentication: OAuth2 client credentials (consumer_key + consumer_secret).
Free tier limits: ~2.5 GB/week traffic, ~4 requests/second sustained.

Environment variables:
    EPO_OPS_KEY     -- OAuth2 consumer key (registered at https://developers.epo.org)
    EPO_OPS_SECRET  -- OAuth2 consumer secret
"""
from __future__ import annotations

import logging
import os
import time
import xml.etree.ElementTree as ET
from typing import Any

import requests
from dotenv import load_dotenv

from .base import BaseSource

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = "https://ops.epo.org/3.2/"

# EPO OPS XML namespaces used across responses.
_NS = {
    "ops": "http://ops.epo.org",
    "epo": "http://www.epo.org/exchange",
    "ft": "http://www.epo.org/fulltext",
}

# Rate-limiting defaults for the free tier.
_DEFAULT_MIN_INTERVAL = 0.25  # seconds between requests (~4 req/s)
_DEFAULT_WEEKLY_BYTE_LIMIT = 2_500_000_000  # 2.5 GB


class EPOOPSError(Exception):
    """Base exception for EPO OPS API errors."""


class EPOAuthError(EPOOPSError):
    """Raised when OAuth2 authentication fails."""


class EPORateLimitError(EPOOPSError):
    """Raised when the API rate limit is exceeded."""


class EPOPatentNotFoundError(EPOOPSError):
    """Raised when the requested patent is not found."""


class EPOOPSSource(BaseSource):
    """EPO Open Patent Services data source.

    Uses OAuth2 bearer tokens for authentication and implements
    rate limiting to stay within the free tier constraints.

    Usage::

        source = EPOOPSSource()
        info = source.test_connection()
        patent = source.get_patent("EP-1000000-A1")
    """

    def __init__(
        self,
        consumer_key: str | None = None,
        consumer_secret: str | None = None,
        min_request_interval: float = _DEFAULT_MIN_INTERVAL,
    ):
        self.consumer_key = consumer_key or os.getenv("EPO_OPS_KEY", "")
        self.consumer_secret = consumer_secret or os.getenv("EPO_OPS_SECRET", "")

        if not self.consumer_key or not self.consumer_secret:
            raise EPOAuthError(
                "EPO OPS credentials not configured. "
                "Set EPO_OPS_KEY and EPO_OPS_SECRET environment variables."
            )

        self._session = requests.Session()
        self._access_token: str | None = None
        self._token_expires_at: float = 0.0

        # Rate-limiting state.
        self._min_interval = min_request_interval
        self._last_request_time: float = 0.0

    # ------------------------------------------------------------------
    # OAuth2 authentication
    # ------------------------------------------------------------------

    def _authenticate(self) -> None:
        """Obtain or refresh the OAuth2 bearer token.

        EPO OPS uses the client_credentials grant type.
        Tokens are valid for 20 minutes.
        """
        url = "https://ops.epo.org/3.2/auth/accesstoken"
        resp = requests.post(
            url,
            data={"grant_type": "client_credentials"},
            auth=(self.consumer_key, self.consumer_secret),
            timeout=30,
        )
        if resp.status_code != 200:
            raise EPOAuthError(
                f"OAuth2 authentication failed (HTTP {resp.status_code}): {resp.text}"
            )

        token_data = resp.json()
        self._access_token = token_data["access_token"]
        # Expire a minute early to avoid edge-case failures.
        expires_in = int(token_data.get("expires_in", 1200))
        self._token_expires_at = time.time() + expires_in - 60

    def _ensure_token(self) -> str:
        """Return a valid bearer token, refreshing if necessary."""
        if self._access_token is None or time.time() >= self._token_expires_at:
            self._authenticate()
        assert self._access_token is not None
        return self._access_token

    # ------------------------------------------------------------------
    # Rate-limited HTTP request
    # ------------------------------------------------------------------

    def _throttle(self) -> None:
        """Sleep if necessary to respect the per-second rate limit."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.time()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        accept: str = "application/xml",
        retries: int = 2,
    ) -> requests.Response:
        """Execute an authenticated, rate-limited request to EPO OPS.

        Handles automatic token refresh and retries on transient errors
        (429 rate limit, 5xx server errors).

        Args:
            method: HTTP method (GET, POST).
            path: Relative path appended to BASE_URL.
            params: Query parameters.
            accept: Accept header value.
            retries: Number of retry attempts for transient failures.

        Returns:
            The HTTP response object.

        Raises:
            EPORateLimitError: If rate limit is still exceeded after retries.
            EPOPatentNotFoundError: If the patent is not found (404).
            EPOOPSError: For other API errors.
        """
        url = BASE_URL + path.lstrip("/")

        for attempt in range(retries + 1):
            self._throttle()
            token = self._ensure_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": accept,
            }

            try:
                resp = self._session.request(
                    method, url, headers=headers, params=params, timeout=60
                )
            except requests.RequestException as exc:
                if attempt < retries:
                    logger.warning(
                        "EPO OPS request failed (attempt %d/%d): %s",
                        attempt + 1,
                        retries + 1,
                        exc,
                    )
                    time.sleep(2 ** attempt)
                    continue
                raise EPOOPSError(f"Request to {url} failed: {exc}") from exc

            if resp.status_code == 200:
                return resp

            if resp.status_code == 404:
                raise EPOPatentNotFoundError(
                    f"Patent not found at {url} (HTTP 404)"
                )

            if resp.status_code == 403:
                # EPO returns 403 with X-Rejection-Reason for rate limits.
                rejection = resp.headers.get("X-Rejection-Reason", "")
                if "AnonymousQuotaPerMinute" in rejection or "RegisteredQuota" in rejection:
                    retry_after = int(resp.headers.get("Retry-After", "60"))
                    if attempt < retries:
                        logger.warning(
                            "EPO rate limit hit (%s). Waiting %d seconds.",
                            rejection,
                            retry_after,
                        )
                        time.sleep(retry_after)
                        continue
                    raise EPORateLimitError(
                        f"Rate limit exceeded: {rejection}. Retry after {retry_after}s."
                    )

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "60"))
                if attempt < retries:
                    logger.warning(
                        "EPO 429 Too Many Requests. Waiting %d seconds.", retry_after
                    )
                    time.sleep(retry_after)
                    continue
                raise EPORateLimitError(
                    f"Rate limit exceeded (HTTP 429). Retry after {retry_after}s."
                )

            if resp.status_code >= 500 and attempt < retries:
                logger.warning(
                    "EPO server error %d (attempt %d/%d). Retrying.",
                    resp.status_code,
                    attempt + 1,
                    retries + 1,
                )
                time.sleep(2 ** attempt)
                continue

            raise EPOOPSError(
                f"EPO OPS error (HTTP {resp.status_code}) at {url}: "
                f"{resp.text[:500]}"
            )

        # Should not be reached, but satisfy the type checker.
        raise EPOOPSError(f"Request to {url} failed after {retries + 1} attempts")

    # ------------------------------------------------------------------
    # XML parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _xml_root(text: str) -> ET.Element:
        """Parse XML response text and return the root element."""
        return ET.fromstring(text)

    @staticmethod
    def _find_text(element: ET.Element, xpath: str) -> str | None:
        """Find text content at *xpath* using EPO namespaces."""
        node = element.find(xpath, _NS)
        if node is not None and node.text:
            return node.text.strip()
        return None

    @staticmethod
    def _find_all_text(element: ET.Element, xpath: str) -> list[str]:
        """Find all text values at *xpath* using EPO namespaces."""
        results: list[str] = []
        for node in element.findall(xpath, _NS):
            if node.text and node.text.strip():
                results.append(node.text.strip())
        return results

    # ------------------------------------------------------------------
    # Legal event parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_legal_events(xml_text: str) -> list[dict[str, Any]]:
        """Extract legal events from an INPADOC legal status XML response.

        Each event contains:
            - event_code: The legal event code (e.g. "PG25").
            - event_date: Date of the event (YYYYMMDD string).
            - description: Human-readable description of the event.
            - patent_office: Two-letter code of the patent office.

        Args:
            xml_text: Raw XML response from the legal service endpoint.

        Returns:
            List of dicts, one per legal event, sorted by date descending.
        """
        root = ET.fromstring(xml_text)
        events: list[dict[str, Any]] = []

        for legal_elem in root.findall(
            ".//ops:legal", _NS
        ):
            # The ops:legal element carries attributes directly.
            event_code = legal_elem.get("code", "")
            event_date = legal_elem.get("date", "")
            desc = legal_elem.get("desc", "")
            office = legal_elem.get("office", "")

            # Some responses nest the data in child elements instead.
            if not event_code:
                event_code_node = legal_elem.find("ops:code", _NS)
                event_code = (
                    event_code_node.text.strip() if event_code_node is not None and event_code_node.text else ""
                )
            if not event_date:
                date_node = legal_elem.find("ops:date", _NS)
                event_date = (
                    date_node.text.strip() if date_node is not None and date_node.text else ""
                )
            if not desc:
                desc_node = legal_elem.find("ops:desc", _NS)
                desc = (
                    desc_node.text.strip() if desc_node is not None and desc_node.text else ""
                )
            if not office:
                office_node = legal_elem.find("ops:office", _NS)
                office = (
                    office_node.text.strip() if office_node is not None and office_node.text else ""
                )

            events.append(
                {
                    "event_code": event_code,
                    "event_date": event_date,
                    "description": desc,
                    "patent_office": office,
                }
            )

        # Sort newest first.
        events.sort(key=lambda e: e.get("event_date", ""), reverse=True)
        return events

    # ------------------------------------------------------------------
    # Bibliographic data parsing
    # ------------------------------------------------------------------

    def _parse_biblio(self, xml_text: str) -> list[dict[str, Any]]:
        """Parse a published-data / biblio XML response into dicts.

        Returns a list because the response may contain multiple
        exchange-documents (e.g. A1 + B1 publications for the same family).
        """
        root = self._xml_root(xml_text)
        results: list[dict[str, Any]] = []

        for doc in root.findall(".//epo:exchange-document", _NS):
            pub_ref = doc.find(".//epo:publication-reference/epo:document-id[@document-id-type='docdb']", _NS)
            pub_number = self._build_pub_number(pub_ref) if pub_ref is not None else ""

            app_ref = doc.find(".//epo:application-reference/epo:document-id[@document-id-type='docdb']", _NS)
            app_number = self._build_pub_number(app_ref) if app_ref is not None else None

            country_code = doc.get("country", "")
            kind_code = doc.get("kind", "")
            family_id = doc.get("family-id", "")

            # Titles
            title_en = None
            title_other = None
            for title_elem in doc.findall(".//epo:invention-title", _NS):
                lang = title_elem.get("lang", "")
                text = title_elem.text.strip() if title_elem.text else ""
                if lang == "en":
                    title_en = text
                elif not title_other:
                    title_other = text

            # Abstract
            abstract_en = None
            for abst in doc.findall(".//epo:abstract", _NS):
                lang = abst.get("lang", "")
                parts = []
                for p in abst.findall("epo:p", _NS):
                    if p.text:
                        parts.append(p.text.strip())
                text = " ".join(parts)
                if lang == "en" and text:
                    abstract_en = text
                elif not abstract_en and text:
                    abstract_en = text

            # CPC codes
            cpc_codes: list[str] = []
            cpc_primary: str | None = None
            for classif in doc.findall(
                ".//epo:patent-classifications/epo:patent-classification", _NS
            ):
                scheme = self._find_text(classif, "epo:classification-scheme")
                if scheme and scheme.upper() not in ("CPC", "CPCI", "CPCA"):
                    continue
                section = self._find_text(classif, "epo:section") or ""
                cls = self._find_text(classif, "epo:class") or ""
                subclass = self._find_text(classif, "epo:subclass") or ""
                main_group = self._find_text(classif, "epo:main-group") or ""
                subgroup = self._find_text(classif, "epo:subgroup") or ""
                code = f"{section}{cls}{subclass}{main_group}/{subgroup}".strip("/")
                if code and code not in cpc_codes:
                    cpc_codes.append(code)
                    if cpc_primary is None:
                        cpc_primary = code

            # IPC codes
            ipc_codes: list[str] = []
            for classif in doc.findall(
                ".//epo:classifications-ipcr/epo:classification-ipcr", _NS
            ):
                ipc_text = self._find_text(classif, "epo:text")
                if ipc_text:
                    cleaned = ipc_text.replace(" ", "").strip()
                    if cleaned and cleaned not in ipc_codes:
                        ipc_codes.append(cleaned)

            # Applicants / Assignees
            applicants: list[dict[str, Any]] = []
            for party in doc.findall(
                ".//epo:parties/epo:applicants/epo:applicant[@data-format='docdb']", _NS
            ):
                name_elem = party.find(".//epo:name", _NS)
                name = name_elem.text.strip() if name_elem is not None and name_elem.text else ""
                app_country = self._find_text(party, ".//epo:country") or ""
                if name:
                    applicants.append(
                        {
                            "raw_name": name,
                            "harmonized_name": name,
                            "firm_id": None,
                            "country_code": app_country,
                        }
                    )

            # Inventors
            inventors: list[str] = []
            for inv in doc.findall(
                ".//epo:parties/epo:inventors/epo:inventor[@data-format='docdb']", _NS
            ):
                name_elem = inv.find(".//epo:name", _NS)
                if name_elem is not None and name_elem.text:
                    inventors.append(name_elem.text.strip())

            # Dates
            filing_date = self._extract_date(doc, ".//epo:application-reference/epo:document-id/epo:date")
            publication_date = self._extract_date(doc, ".//epo:publication-reference/epo:document-id/epo:date")

            # Citations
            citations_backward: list[str] = []
            for cite in doc.findall(
                ".//epo:references-cited/epo:citation/epo:patcit/epo:document-id", _NS
            ):
                cite_num = self._build_pub_number(cite)
                if cite_num:
                    citations_backward.append(cite_num)

            results.append(
                {
                    "publication_number": pub_number,
                    "application_number": app_number,
                    "family_id": family_id,
                    "country_code": country_code,
                    "kind_code": kind_code,
                    "title_en": title_en,
                    "title_ja": title_other if country_code == "JP" else None,
                    "abstract_ja": None,
                    "abstract_en": abstract_en,
                    "cpc_codes": cpc_codes,
                    "cpc_primary": cpc_primary,
                    "ipc_codes": ipc_codes,
                    "applicants": applicants,
                    "raw_assignees": [a["raw_name"] for a in applicants],
                    "inventors": inventors,
                    "filing_date": filing_date,
                    "publication_date": publication_date,
                    "grant_date": None,
                    "citations_backward": citations_backward,
                    "entity_status": None,
                    "source": "epo_ops",
                }
            )

        return results

    def _build_pub_number(self, doc_id_elem: ET.Element) -> str:
        """Build a normalized publication number from a document-id element.

        Produces the format "CC-NNNNNNN-KK" (e.g. "EP-1000000-A1").
        """
        country = self._find_text(doc_id_elem, "epo:country") or ""
        number = self._find_text(doc_id_elem, "epo:doc-number") or ""
        kind = self._find_text(doc_id_elem, "epo:kind") or ""
        parts = [p for p in (country, number, kind) if p]
        return "-".join(parts)

    def _extract_date(self, element: ET.Element, xpath: str) -> int | None:
        """Extract a date as an integer (YYYYMMDD) from the given xpath."""
        text = self._find_text(element, xpath)
        if text:
            cleaned = text.replace("-", "").strip()
            if cleaned.isdigit() and len(cleaned) == 8:
                return int(cleaned)
        return None

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    def test_connection(self) -> dict[str, Any]:
        """Verify API access by requesting a well-known patent.

        Returns a dict with status information.

        Raises:
            EPOAuthError: If credentials are invalid.
            EPOOPSError: If the API is unreachable.
        """
        try:
            resp = self._request(
                "GET",
                "rest-services/published-data/publication/docdb/EP.1000000.A1/biblio",
            )
            root = self._xml_root(resp.text)
            doc = root.find(".//epo:exchange-document", _NS)
            title = ""
            if doc is not None:
                title_elem = doc.find(".//epo:invention-title[@lang='en']", _NS)
                title = (
                    title_elem.text.strip()
                    if title_elem is not None and title_elem.text
                    else "(title not found)"
                )
            return {
                "status": "ok",
                "service": "EPO OPS 3.2",
                "test_patent": "EP-1000000-A1",
                "test_title": title,
            }
        except EPOOPSError:
            raise
        except Exception as exc:
            raise EPOOPSError(f"Connection test failed: {exc}") from exc

    def get_patent(self, publication_number: str) -> dict[str, Any]:
        """Retrieve full bibliographic data for a single patent.

        The *publication_number* should use dotted docdb format
        (e.g. "EP.1000000.A1") or hyphenated format ("EP-1000000-A1").
        Both are accepted and normalized internally.

        Args:
            publication_number: Patent publication number.

        Returns:
            A dict matching the UnifiedPatent schema.

        Raises:
            EPOPatentNotFoundError: If the patent does not exist.
        """
        docdb_ref = self._normalize_docdb_ref(publication_number)

        # Fetch bibliographic data.
        resp = self._request(
            "GET",
            f"rest-services/published-data/publication/docdb/{docdb_ref}/biblio",
        )
        patents = self._parse_biblio(resp.text)
        if not patents:
            raise EPOPatentNotFoundError(
                f"No bibliographic data found for {publication_number}"
            )

        patent = patents[0]

        # Attempt to attach legal status.
        try:
            events = self.get_legal_status(publication_number)
            patent["legal_events"] = events
            # Derive a simple entity_status from events.
            patent["entity_status"] = self._derive_status(events)
        except EPOPatentNotFoundError:
            patent["legal_events"] = []
        except EPOOPSError as exc:
            logger.warning("Could not fetch legal status for %s: %s", publication_number, exc)
            patent["legal_events"] = []

        return patent

    def get_legal_status(self, publication_number: str) -> list[dict[str, Any]]:
        """Retrieve INPADOC legal status events for a patent.

        Args:
            publication_number: Patent publication number.

        Returns:
            List of legal event dicts sorted by date (newest first).
            Each dict has keys: event_code, event_date, description, patent_office.

        Raises:
            EPOPatentNotFoundError: If the patent does not exist.
        """
        docdb_ref = self._normalize_docdb_ref(publication_number)
        resp = self._request(
            "GET",
            f"rest-services/published-data/publication/docdb/{docdb_ref}/legal",
        )
        return self._parse_legal_events(resp.text)

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
        """Search published patents on EPO OPS.

        Builds a CQL query string from the parameters and uses the
        ``published-data/search`` endpoint. Results are enriched with
        bibliographic data.

        Args:
            query: Free-text search term (searches title and abstract).
            cpc_codes: Filter by one or more CPC classification codes.
            applicant: Filter by applicant/assignee name (partial match).
            jurisdiction: Country code filter (e.g. "EP", "JP").
            date_from: Start publication date as YYYYMMDD integer.
            date_to: End publication date as YYYYMMDD integer.
            max_results: Maximum number of results to return (default 20, max 100).

        Returns:
            List of dicts in UnifiedPatent-compatible format.
        """
        cql_parts: list[str] = []

        if query:
            cql_parts.append(f'txt = "{query}"')
        if cpc_codes:
            cpc_clauses = " OR ".join(f'cpc = "{c}"' for c in cpc_codes)
            cql_parts.append(f"({cpc_clauses})")
        if applicant:
            cql_parts.append(f'pa = "{applicant}"')
        if jurisdiction:
            cql_parts.append(f'pn = {jurisdiction}')
        if date_from or date_to:
            start = str(date_from) if date_from else "19000101"
            end = str(date_to) if date_to else "99991231"
            cql_parts.append(f"pd within \"{start} {end}\"")

        if not cql_parts:
            raise EPOOPSError("At least one search parameter is required.")

        cql = " AND ".join(cql_parts)
        safe_max = min(max_results, 100)

        # OPS search returns up to 100 results per request (range header).
        resp = self._request(
            "GET",
            "rest-services/published-data/search",
            params={"q": cql, "Range": f"1-{safe_max}"},
        )

        # Extract publication references from search results.
        root = self._xml_root(resp.text)
        pub_refs: list[str] = []
        for doc_id in root.findall(
            ".//ops:search-result/ops:publication-reference/epo:document-id[@document-id-type='docdb']",
            _NS,
        ):
            ref = self._build_pub_number(doc_id)
            if ref:
                pub_refs.append(ref)

        if not pub_refs:
            return []

        # Fetch bibliographic data for each result.
        results: list[dict[str, Any]] = []
        for ref in pub_refs[:safe_max]:
            try:
                patent = self.get_patent(ref)
                results.append(patent)
            except EPOPatentNotFoundError:
                logger.debug("Skipping %s: not found during enrichment.", ref)
            except EPOOPSError as exc:
                logger.warning("Error fetching %s: %s", ref, exc)

        return results

    def get_family(self, publication_number: str) -> dict[str, Any]:
        """Get INPADOC patent family members for a publication.

        Args:
            publication_number: Patent publication number.

        Returns:
            Dict with ``publication_number``, ``family_id``, and
            ``members`` (list of dicts with publication_number, country_code,
            kind_code, and title).

        Raises:
            EPOPatentNotFoundError: If the patent does not exist.
        """
        docdb_ref = self._normalize_docdb_ref(publication_number)
        resp = self._request(
            "GET",
            f"rest-services/family/publication/docdb/{docdb_ref}",
        )

        root = self._xml_root(resp.text)
        members: list[dict[str, Any]] = []

        for member in root.findall(".//ops:patent-family/ops:family-member", _NS):
            pub_ref = member.find(
                ".//epo:publication-reference/epo:document-id[@document-id-type='docdb']", _NS
            )
            if pub_ref is None:
                continue

            member_number = self._build_pub_number(pub_ref)
            country = self._find_text(pub_ref, "epo:country") or ""
            kind = self._find_text(pub_ref, "epo:kind") or ""

            # Try to get the title.
            title = None
            for title_elem in member.findall(".//epo:invention-title", _NS):
                lang = title_elem.get("lang", "")
                if title_elem.text:
                    if lang == "en":
                        title = title_elem.text.strip()
                        break
                    elif title is None:
                        title = title_elem.text.strip()

            members.append(
                {
                    "publication_number": member_number,
                    "country_code": country,
                    "kind_code": kind,
                    "title": title,
                }
            )

        # Extract family-id from the first member's exchange-document.
        family_id = ""
        first_doc = root.find(".//epo:exchange-document", _NS)
        if first_doc is not None:
            family_id = first_doc.get("family-id", "")

        return {
            "publication_number": publication_number,
            "family_id": family_id,
            "member_count": len(members),
            "members": members,
        }

    def bulk_legal_status(
        self,
        publication_numbers: list[str],
        *,
        continue_on_error: bool = True,
    ) -> list[dict[str, Any]]:
        """Batch legal status lookup with rate limiting.

        Iterates over a list of publication numbers and fetches
        legal events for each one, respecting rate limits.

        Args:
            publication_numbers: List of patent publication numbers.
            continue_on_error: If True, skip patents that fail and log
                warnings. If False, raise on first error.

        Returns:
            List of dicts, each with ``publication_number`` and ``events``.
            Failed lookups are included with an empty ``events`` list
            and an ``error`` field when *continue_on_error* is True.
        """
        results: list[dict[str, Any]] = []

        for idx, pub_num in enumerate(publication_numbers):
            logger.info(
                "Fetching legal status %d/%d: %s",
                idx + 1,
                len(publication_numbers),
                pub_num,
            )
            try:
                events = self.get_legal_status(pub_num)
                results.append(
                    {
                        "publication_number": pub_num,
                        "events": events,
                    }
                )
            except EPOOPSError as exc:
                if continue_on_error:
                    logger.warning(
                        "Failed to fetch legal status for %s: %s", pub_num, exc
                    )
                    results.append(
                        {
                            "publication_number": pub_num,
                            "events": [],
                            "error": str(exc),
                        }
                    )
                else:
                    raise

        return results

    def get_applicant_patents(
        self,
        applicant_names: list[str],
        jurisdiction: str | None = None,
        date_from: int | None = None,
        date_to: int | None = None,
    ) -> list[dict[str, Any]]:
        """Get all patents for given applicant names.

        Required by BaseSource. Delegates to ``search_patents``.
        """
        if not applicant_names:
            return []
        return self.search_patents(
            applicant=applicant_names[0],
            jurisdiction=jurisdiction,
            date_from=date_from,
            date_to=date_to,
            max_results=100,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_docdb_ref(publication_number: str) -> str:
        """Convert various publication number formats to the EPO docdb
        dot-separated format.

        Accepts:
            "EP-1000000-A1"  -> "EP.1000000.A1"
            "EP.1000000.A1"  -> "EP.1000000.A1" (unchanged)
            "EP1000000A1"    -> best-effort split (unreliable for ambiguous formats)

        Returns:
            Dot-separated docdb reference string.
        """
        # Already in dot format.
        if "." in publication_number:
            return publication_number

        # Hyphen format is the project's canonical format.
        if "-" in publication_number:
            return publication_number.replace("-", ".")

        # Best-effort: assume 2-letter country prefix and optional kind suffix.
        # This is a heuristic and may fail for edge cases.
        s = publication_number.strip()
        if len(s) >= 3 and s[:2].isalpha():
            country = s[:2]
            rest = s[2:]
            # Find where digits end and kind code begins.
            num_end = 0
            for i, ch in enumerate(rest):
                if ch.isdigit():
                    num_end = i + 1
                else:
                    if num_end > 0:
                        break
            if num_end > 0:
                number = rest[:num_end]
                kind = rest[num_end:]
                parts = [country, number]
                if kind:
                    parts.append(kind)
                return ".".join(parts)

        # Fallback: return as-is and let the API return an error.
        return publication_number

    @staticmethod
    def _derive_status(events: list[dict[str, Any]]) -> str | None:
        """Derive a simple entity status string from legal events.

        Looks for common EPO event codes that indicate grant, lapse,
        withdrawal, etc.

        Returns:
            One of "active", "expired", "withdrawn", "pending", or None.
        """
        if not events:
            return None

        codes = {e.get("event_code", "") for e in events}
        descriptions_lower = " ".join(
            e.get("description", "") for e in events
        ).lower()

        # Check for lapse/expiry indicators.
        lapse_codes = {"PGFP", "PG25", "REG"}
        if "lapsed" in descriptions_lower or "laps" in descriptions_lower:
            return "expired"

        # Check for withdrawal.
        if "withdrawn" in descriptions_lower or "withdrawal" in descriptions_lower:
            return "withdrawn"

        # Check for grant.
        if "grant" in descriptions_lower or "B1" in codes or "B2" in codes:
            return "active"

        # If events exist but nothing conclusive, assume pending.
        return "pending"
