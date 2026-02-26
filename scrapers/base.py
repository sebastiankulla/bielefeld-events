"""Base scraper class for event sources."""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


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


class BaseScraper(ABC):
    """Abstract base class for all event scrapers."""

    name: str = "base"
    base_url: str = ""

    def __init__(self):
        self.logger = logging.getLogger(f"scraper.{self.name}")

    @abstractmethod
    def scrape(self) -> list[Event]:
        """Scrape events from the source. Returns a list of Event objects."""
        ...

    def _get_page(self, url: str, **kwargs) -> str:
        """Fetch a page with standard headers."""
        import requests

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (compatible; BielefeldEvents/1.0; "
                "+https://github.com/sebastiankulla/bielefeld-events)"
            ),
        }
        response = requests.get(url, headers=headers, timeout=30, **kwargs)
        response.raise_for_status()
        return response.text
