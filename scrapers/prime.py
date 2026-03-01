"""Scraper for Prime Club Bielefeld event listings."""

import re
from datetime import datetime

from bs4 import BeautifulSoup

from scrapers.base import GERMAN_MONTHS, BaseScraper, Event

# Short German month abbreviations used by Prime (3 letters without dot)
SHORT_MONTHS = {
    "jan": 1, "feb": 2, "mär": 3, "mar": 3,
    "apr": 4, "mai": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9,
    "okt": 10, "nov": 11, "dez": 12,
}

# Fallback: extract date from URL slug like "event-name-06-03-2026"
RE_SLUG_DATE = re.compile(r"-(\d{2})-(\d{2})-(\d{4})$")


class PrimeScraper(BaseScraper):
    """Scrapes events from Prime Club Bielefeld (nightclub).

    The site is powered by the Disco2App framework and renders events as
    ``div.event-snippet`` cards.  Each card contains:

    * ``span.event-date-cal-weekday`` / ``event-date-cal-day`` /
      ``event-date-cal-month`` for the date.
    * ``h4.title`` for the event name.
    * An ``<a>`` link with an ``<img>`` for the poster.
    """

    name = "prime"
    base_url = "https://www.prime-night.de"

    def scrape(self) -> list[Event]:
        events = []
        try:
            html = self._get_page(f"{self.base_url}/events")
            soup = BeautifulSoup(html, "lxml")
            events = self._extract_events(soup)
            self.logger.info(
                "Scraped %d events from %s", len(events), self.name,
            )
        except Exception:
            self.logger.exception("Failed to scrape %s", self.name)
        return events

    def _extract_events(self, soup: BeautifulSoup) -> list[Event]:
        events = []

        # Determine the current year; month headings (h3) give month context
        current_year = datetime.now().year

        snippets = soup.select("div.event-snippet")
        for snippet in snippets:
            event = self._parse_snippet(snippet, current_year)
            if event:
                events.append(event)
        return events

    def _parse_snippet(self, card, fallback_year: int) -> Event | None:
        # Title
        title_el = card.select_one("h4.title, h4, h3.title")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        # URL
        link_el = card.select_one('a[href*="/events/"]')
        if not link_el:
            link_el = card.select_one("a[href]")
        url = ""
        if link_el:
            href = link_el.get("href", "")
            if href.startswith("/"):
                url = self.base_url + href
            elif href.startswith("http"):
                url = href
            else:
                url = self.base_url + "/" + href

        # Date – from calendar spans
        day_el = card.select_one(".event-date-cal-day")
        month_el = card.select_one(".event-date-cal-month")

        date_start = None
        if day_el and month_el:
            try:
                day = int(day_el.get_text(strip=True))
                month_text = month_el.get_text(strip=True).lower().rstrip(".")
                month = SHORT_MONTHS.get(month_text) or GERMAN_MONTHS.get(month_text)
                if month:
                    # Try to determine year from URL slug
                    year = fallback_year
                    if link_el:
                        m = RE_SLUG_DATE.search(link_el.get("href", ""))
                        if m:
                            year = int(m.group(3))
                    date_start = datetime(year, month, day)
            except (ValueError, TypeError):
                pass

        if not date_start:
            # Fallback: try to parse date from URL slug
            if link_el:
                m = RE_SLUG_DATE.search(link_el.get("href", ""))
                if m:
                    try:
                        date_start = datetime(
                            int(m.group(3)), int(m.group(2)), int(m.group(1)),
                        )
                    except ValueError:
                        pass

        if not date_start:
            return None

        # Image
        image_url = ""
        img_el = card.select_one("img[src]")
        if img_el:
            image_url = img_el.get("data-src", "") or img_el.get("src", "")

        return Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=url,
            location="Prime Club Bielefeld",
            category="Party",
            image_url=image_url,
        )
