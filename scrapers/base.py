"""Base scraper class for event sources."""

import logging
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

GERMAN_MONTHS = {
    "januar": 1, "jan": 1,
    "februar": 2, "feb": 2,
    "märz": 3, "maerz": 3, "mär": 3,
    "april": 4, "apr": 4,
    "mai": 5,
    "juni": 6, "jun": 6,
    "juli": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "oktober": 10, "okt": 10,
    "november": 11, "nov": 11,
    "dezember": 12, "dez": 12,
}

# Pattern: "15. März 2026" or "15. März 2026, 19:30 Uhr"
RE_GERMAN_DATE = re.compile(
    r"(\d{1,2})\.\s*"
    r"(\w+)\s+"
    r"(\d{4})"
    r"(?:[,\s]+(\d{1,2})[:\.](\d{2}))?"
    r"(?:\s*Uhr)?",
    re.IGNORECASE,
)

# Pattern: "25.04.2026" or "25.04.2026 18:00"
RE_NUMERIC_DATE = re.compile(
    r"(\d{1,2})\.(\d{1,2})\.(\d{4})"
    r"(?:\s+(\d{1,2})[:\.](\d{2}))?"
)

# Pattern: "2026-03-15" or "2026-03-15T19:30:00"
RE_ISO_DATE = re.compile(
    r"(\d{4})-(\d{2})-(\d{2})"
    r"(?:[T\s](\d{2}):(\d{2})(?::(\d{2}))?)?"
)


@dataclass
class Event:
    """Represents a single event."""

    title: str
    date_start: datetime
    source: str
    url: str
    description: str = ""
    date_end: datetime | None = None
    location: str = ""
    city: str = "Bielefeld"
    category: str = ""
    image_url: str = ""
    price: str = ""
    tags: list[str] = field(default_factory=list)


def parse_german_date(text: str) -> datetime | None:
    """Parse a variety of German and ISO date formats from text."""
    if not text:
        return None

    text = text.strip()

    # Try ISO format first (from datetime attributes)
    m = RE_ISO_DATE.search(text)
    if m:
        year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
        hour = int(m.group(4)) if m.group(4) else 0
        minute = int(m.group(5)) if m.group(5) else 0
        try:
            return datetime(year, month, day, hour, minute)
        except ValueError:
            pass

    # Try numeric German date: 25.04.2026 18:00
    m = RE_NUMERIC_DATE.search(text)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        hour = int(m.group(4)) if m.group(4) else 0
        minute = int(m.group(5)) if m.group(5) else 0
        try:
            return datetime(year, month, day, hour, minute)
        except ValueError:
            pass

    # Try German month name: 15. März 2026, 19:30 Uhr
    m = RE_GERMAN_DATE.search(text)
    if m:
        day = int(m.group(1))
        month_str = m.group(2).lower()
        year = int(m.group(3))
        hour = int(m.group(4)) if m.group(4) else 0
        minute = int(m.group(5)) if m.group(5) else 0
        month = GERMAN_MONTHS.get(month_str)
        if month:
            try:
                return datetime(year, month, day, hour, minute)
            except ValueError:
                pass

    return None


class BaseScraper(ABC):
    """Abstract base class for all event scrapers."""

    name: str = "base"
    base_url: str = ""

    def __init__(self):
        self.logger = logging.getLogger(f"scraper.{self.name}")
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic and connection pooling."""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.5",
        })
        return session

    @abstractmethod
    def scrape(self) -> list[Event]:
        """Scrape events from the source. Returns a list of Event objects."""
        ...

    def _get_page(self, url: str, **kwargs) -> str:
        """Fetch a page using the session with retries."""
        self.logger.debug("Fetching %s", url)
        response = self.session.get(url, timeout=30, **kwargs)
        response.raise_for_status()
        return response.text

    def _parse_date_element(self, el) -> datetime | None:
        """Extract a datetime from an HTML element (time tag or text)."""
        if el is None:
            return None

        # Try datetime attribute first (e.g. <time datetime="2026-03-15">)
        dt_attr = el.get("datetime", "") if hasattr(el, "get") else ""
        if dt_attr:
            result = parse_german_date(dt_attr)
            if result:
                return result

        # Fall back to text content
        text = el.get_text(strip=True) if hasattr(el, "get_text") else str(el)
        return parse_german_date(text)

    def _absolute_url(self, url: str) -> str:
        """Convert a relative URL to absolute."""
        if not url:
            return ""
        if url.startswith("http"):
            return url
        if url.startswith("//"):
            return "https:" + url
        if url.startswith("/"):
            return self.base_url + url
        return self.base_url + "/" + url

    def _extract_location_from_card(self, card) -> str:
        """Extract location from an HTML card using multiple strategies."""
        # Strategy 1: CSS selectors for common location elements
        loc_selectors = [
            ".location", ".venue", ".ort", ".veranstaltungsort",
            ".event-location", ".event-venue", ".event-ort",
            "[class*='location']", "[class*='venue']", "[class*='ort']",
            "[itemprop='location']",
        ]
        for selector in loc_selectors:
            el = card.select_one(selector)
            if el:
                text = el.get_text(strip=True)
                if text and len(text) >= 3:
                    return text

        # Strategy 2: <address> tag
        addr_el = card.select_one("address")
        if addr_el:
            text = addr_el.get_text(strip=True)
            if text and len(text) >= 3:
                return text

        # Strategy 3: Look for "Ort:" / "Veranstaltungsort:" / "Wo:" patterns in text
        card_text = card.get_text(separator="\n")
        location = _extract_location_from_text(card_text)
        if location:
            return location

        return ""

    @staticmethod
    def _parse_jsonld_location(location_data) -> str:
        """Extract a location name from JSON-LD location data (handles all formats)."""
        if not location_data:
            return ""

        # Simple string
        if isinstance(location_data, str):
            return location_data.strip()

        # Object with name (Place, VirtualLocation, etc.)
        if isinstance(location_data, dict):
            name = location_data.get("name", "")
            address = location_data.get("address", "")

            # Resolve address to a string
            if isinstance(address, dict):
                parts = []
                venue = address.get("name", "")
                if venue:
                    parts.append(venue)
                street = address.get("streetAddress", "")
                if street:
                    parts.append(street)
                locality = address.get("addressLocality", "")
                if locality:
                    parts.append(locality)
                address = ", ".join(parts)

            if name and address:
                return f"{name}, {address}"
            return name or address or ""

        # Array of locations - take the first
        if isinstance(location_data, list) and location_data:
            return BaseScraper._parse_jsonld_location(location_data[0])

        return ""


# Pattern for extracting location from text like "Ort: Stadthalle Bielefeld"
RE_LOCATION_LABEL = re.compile(
    r"(?:Ort|Veranstaltungsort|Location|Venue|Wo|Spielort|Spielstätte)"
    r"\s*[:]\s*(.+?)(?:\n|$)",
    re.IGNORECASE,
)


def _extract_location_from_text(text: str) -> str:
    """Try to extract a location from text using label patterns."""
    m = RE_LOCATION_LABEL.search(text)
    if m:
        loc = m.group(1).strip().rstrip(",;.")
        if len(loc) >= 3:
            return loc
    return ""
