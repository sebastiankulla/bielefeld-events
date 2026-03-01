"""Scraper for Bühnen und Orchester der Stadt Bielefeld (BUO) event listings."""

import json
import re

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event, parse_german_date

# Match dates like "So., 01.03.2026 19:30 Uhr" or "01.03.2026 19:30"
RE_BUO_DATE = re.compile(
    r"(?:Mo|Di|Mi|Do|Fr|Sa|So)\.\,?\s*"
    r"(\d{1,2})\.(\d{1,2})\.(\d{4})\s+"
    r"(\d{1,2}):(\d{2})\s*(?:Uhr)?",
    re.IGNORECASE,
)


class BuoScraper(BaseScraper):
    """Scrapes events from BUO Bielefeld (theater & orchestra).

    The calendar pages use a Tailwind-based grid layout.  Each event sits
    inside a ``div`` whose class list contains the ``thea`` or ``phil``
    theme token together with ``grid`` and ``grid-cols-12``.  The title is
    in ``h2 > a[href*='veranstaltung']``, and the date is found in the
    left-hand column text.
    """

    name = "buo"
    base_url = "https://www.buo-bielefeld.de"

    PATHS = [
        "/theater/kalender",
        "/philharmoniker/kalender",
    ]

    def scrape(self) -> list[Event]:
        events = []
        seen = set()

        for path in self.PATHS:
            try:
                html = self._get_page(f"{self.base_url}{path}")
                soup = BeautifulSoup(html, "lxml")
                page_events = self._extract_events(soup)
                for ev in page_events:
                    key = (ev.title, ev.date_start.date())
                    if key not in seen:
                        seen.add(key)
                        events.append(ev)
            except Exception:
                self.logger.exception(
                    "Failed to scrape %s%s", self.base_url, path,
                )

        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    def _extract_events(self, soup: BeautifulSoup) -> list[Event]:
        events = []

        # Find all grid containers that hold an event link.
        for grid_div in soup.select("div.grid"):
            link = grid_div.select_one(
                'a[href*="/theater/veranstaltung/"], '
                'a[href*="/philharmoniker/veranstaltung/"]'
            )
            if not link:
                continue

            # Only process leaf-level event grids (not parent wrappers).
            inner_grids = grid_div.select("div.grid")
            event_links_inside = sum(
                1 for g in inner_grids
                if g.select_one('a[href*="/veranstaltung/"]')
            )
            if event_links_inside > 1:
                continue

            event = self._parse_grid_event(grid_div)
            if event:
                events.append(event)

        return events

    def _parse_grid_event(self, container) -> Event | None:
        # Title
        title_link = container.select_one(
            'h2 a[href*="/veranstaltung/"]'
        )
        if not title_link:
            return None
        title = title_link.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        url = self._absolute_url(title_link["href"])

        # Subtitle / author
        subtitle_el = container.select_one("h3")
        description = subtitle_el.get_text(strip=True) if subtitle_el else ""

        # Date – search for "So., 01.03.2026 19:30 Uhr" in the text
        text = container.get_text(" ", strip=True)
        date_start = None
        m = RE_BUO_DATE.search(text)
        if m:
            day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
            hour, minute = int(m.group(4)), int(m.group(5))
            try:
                from datetime import datetime
                date_start = datetime(year, month, day, hour, minute)
            except ValueError:
                pass

        if not date_start:
            date_start = parse_german_date(text)
        if not date_start:
            return None

        # Location from text
        location = self._extract_location_from_card(container)
        if not location:
            location = "Theater Bielefeld"

        # Tags / category
        category = ""
        tags = container.select("ul li")
        tag_texts = [
            t.get_text(strip=True) for t in tags
            if t.get_text(strip=True) and len(t.get_text(strip=True)) < 30
        ]
        if tag_texts:
            category = " / ".join(tag_texts)

        return Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=url,
            description=description,
            location=location,
            category=category or "Theater & Musik",
        )
