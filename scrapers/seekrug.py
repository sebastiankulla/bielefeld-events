"""Scraper for Seekrug am Obersee event listings."""

import re
from datetime import datetime, timedelta

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event, parse_german_date

# Number of weeks to generate for recurring weekday events like "jeden Montag"
_RECURRING_WEEKS = 8

# Weekday name → weekday index (Monday=0)
_WEEKDAY_MAP = {
    "montag": 0, "dienstag": 1, "mittwoch": 2, "donnerstag": 3,
    "freitag": 4, "samstag": 5, "sonntag": 6,
}
_RE_JEDEN = re.compile(
    r"jeden\s+(" + "|".join(_WEEKDAY_MAP) + r")", re.IGNORECASE
)


def _next_weekday(weekday: int) -> datetime:
    """Return today or the next upcoming date for the given weekday (0=Mon)."""
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    days_ahead = (weekday - today.weekday()) % 7
    return today + timedelta(days=days_ahead)

# Matches "DD.MM.YY" with a 2-digit year, e.g. "31.01.26"
_RE_SHORT_YEAR = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{2})(?!\d)")

# Matches "DD.MM." without a year, e.g. "02.04." or "29.03."
_RE_NO_YEAR = re.compile(r"(\d{1,2})\.(\d{1,2})\.")


def _parse_seekrug_date(text: str) -> datetime | None:
    """Extended date parser that handles Seekrug-specific quirks.

    On top of the standard ``parse_german_date`` it handles:

    * 2-digit years: "31.01.26" → 2026-01-31
    * Missing year:  "02.04. ab 19 Uhr" → uses current or next year
    """
    if not text:
        return None

    # 1. Normalise 2-digit year ("31.01.26" → "31.01.2026") before passing
    #    to the generic parser so all its patterns still apply.
    normalised = _RE_SHORT_YEAR.sub(
        lambda m: f"{m.group(1)}.{m.group(2)}.20{m.group(3)}", text
    )
    result = parse_german_date(normalised)
    if result:
        return result

    # 2. Try "DD.MM." without a year – pick the first match and assume the
    #    nearest future occurrence (current year, or next year if past).
    m = _RE_NO_YEAR.search(text)
    if m:
        day, month = int(m.group(1)), int(m.group(2))
        now = datetime.now()
        year = now.year
        try:
            candidate = datetime(year, month, day)
        except ValueError:
            return None
        # If the date has already passed this year, try next year
        if candidate.date() < now.date():
            try:
                candidate = datetime(year + 1, month, day)
            except ValueError:
                return None
        return candidate

    return None


class SeekrugScraper(BaseScraper):
    """Scrapes events from Seekrug am Obersee (https://seekrug.com/aktuelles/).

    The /aktuelles/ page renders events as ``div.tmb`` cards, each containing:

    * ``div.t-entry-cf-detail-195899``: Date text in various formats
    * ``h3.t-entry-title a``: Event title and link to the individual page
    * ``img.adaptive-async``: Thumbnail (``data-guid`` holds the full-size URL)

    Date formats encountered on the page:

    * "30.04.2026" – standard numeric
    * "Ostersonntag, 05.04.2026" – weekday prefix with full year
    * "Gründonnerstag, 02.04. ab 19 Uhr" – weekday prefix, no year
    * "So 29.03./ von 09.30-12 Uhr" – abbreviation prefix, no year
    * "ab dem 03.04. und über Ostern" – prose with partial date
    * "02./15. ab 17h, am 10.05. ab 12h" – multiple dates, take first
    * "Samstag, 31.01.26" – 2-digit year
    * "19.07.25" – 2-digit year
    * "jeden Montag im SEEKRUG" – no parseable date → skipped
    * "OSTERN im SEEKRUG" – no parseable date → skipped
    """

    name = "seekrug"
    base_url = "https://seekrug.com"
    EVENTS_URL = "https://seekrug.com/aktuelles/"
    LOCATION = "Seekrug am Obersee, Am Obersee 1, Bielefeld"

    def scrape(self) -> list[Event]:
        events: list[Event] = []
        try:
            html = self._get_page(self.EVENTS_URL)
            soup = BeautifulSoup(html, "lxml")
            events = self._parse_cards(soup)
        except Exception:
            self.logger.exception("Failed to scrape %s", self.EVENTS_URL)

        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    def _parse_cards(self, soup: BeautifulSoup) -> list[Event]:
        events: list[Event] = []
        seen: set[tuple] = set()

        for card in soup.select("div.tmb"):
            for event in self._parse_card(card):
                key = (event.title, event.date_start.date())
                if key not in seen:
                    seen.add(key)
                    events.append(event)

        return events

    def _parse_card(self, card) -> list[Event]:
        # Title + URL (needed for all branches)
        title_el = card.select_one("h3.t-entry-title a")
        if not title_el:
            return []
        title = title_el.get_text(strip=True)
        if not title:
            return []
        url = self._absolute_url(title_el.get("href", "")) or self.EVENTS_URL

        # Image – prefer data-guid (full-size) over src (thumbnail)
        img_el = card.select_one("img.adaptive-async")
        image_url = ""
        if img_el:
            image_url = img_el.get("data-guid", "") or img_el.get("src", "")

        date_el = card.select_one("div.t-entry-cf-detail-195899")
        if not date_el:
            return []
        date_text = date_el.get_text(strip=True)

        # Recurring weekday pattern: "jeden Montag …"
        m = _RE_JEDEN.search(date_text)
        if m:
            weekday = _WEEKDAY_MAP[m.group(1).lower()]
            first = _next_weekday(weekday)
            return [
                Event(
                    title=title,
                    date_start=first + timedelta(weeks=i),
                    source=self.name,
                    url=url,
                    location=self.LOCATION,
                    image_url=image_url,
                    category="Veranstaltung",
                )
                for i in range(_RECURRING_WEEKS)
            ]

        # Single date
        date_start = _parse_seekrug_date(date_text)
        if date_start is None:
            self.logger.debug("Could not parse date: %r", date_text)
            return []

        return [Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=url,
            location=self.LOCATION,
            image_url=image_url,
            category="Veranstaltung",
        )]
