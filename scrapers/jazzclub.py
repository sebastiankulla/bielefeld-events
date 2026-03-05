"""Scraper for Bielefelder Jazzclub event listings."""

import re
from datetime import datetime

from bs4 import BeautifulSoup, Tag

from scrapers.base import BaseScraper, Event


# Date format used by the Jazz Club: "Freitag, 06.03.26, 20:30 Uhr - ..."
# The year is 2-digit, e.g. "26" -> 2026
RE_JAZZCLUB_DATE = re.compile(
    r"(\d{1,2})\.(\d{1,2})\.(\d{2})"          # DD.MM.YY
    r"(?:[,\s]+(\d{1,2})[:\.](\d{2}))?"       # optional HH:MM
)


def _parse_jazzclub_date(text: str) -> datetime | None:
    """Parse the Jazz Club's two-digit-year date format."""
    m = RE_JAZZCLUB_DATE.search(text)
    if not m:
        return None
    day, month, year_short = int(m.group(1)), int(m.group(2)), int(m.group(3))
    year = 2000 + year_short
    hour = int(m.group(4)) if m.group(4) else 0
    minute = int(m.group(5)) if m.group(5) else 0
    try:
        return datetime(year, month, day, hour, minute)
    except ValueError:
        return None


class JazzclubScraper(BaseScraper):
    """Scrapes events from Bielefelder Jazzclub (https://www.bielefelder-jazzclub.de).

    The /programm page lists concerts in card-style containers. Each card has:
    * An ``<img>`` with the artist photo.
    * An ``<a>`` linking to the detail page.
    * An ``<h4>`` with date/time like "Freitag, 06.03.26, 20:30 Uhr - Offenes Ende".
    * A ``<p>`` with the short description.

    All events take place at Alte Kuxmann-Fabrik, Beckhausstr. 72, Bielefeld.
    """

    name = "jazzclub"
    base_url = "https://www.bielefelder-jazzclub.de"
    PROGRAM_URL = "https://www.bielefelder-jazzclub.de/programm"
    LOCATION = "Bielefelder Jazzclub, Beckhausstraße 72, Bielefeld"

    def scrape(self) -> list[Event]:
        events: list[Event] = []
        try:
            html = self._get_page(self.PROGRAM_URL)
            soup = BeautifulSoup(html, "lxml")
            events = self._parse_events(soup)
        except Exception:
            self.logger.exception("Failed to scrape %s", self.PROGRAM_URL)

        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    def _parse_events(self, soup: BeautifulSoup) -> list[Event]:
        events: list[Event] = []
        seen: set[tuple] = set()

        # Each event card links to the detail page via tx_calendarize_calendar
        # Find all anchor tags that point to detail pages
        for link_el in soup.select("a[href*='tx_calendarize_calendar']"):
            # Skip "Mehr" / "mehr" links that are secondary links inside a card
            link_text = link_el.get_text(strip=True).lower()
            if link_text in ("mehr", "more", "details", "→", "»"):
                continue

            event = self._parse_card(link_el)
            if event:
                key = (event.title, event.date_start.date())
                if key not in seen:
                    seen.add(key)
                    events.append(event)

        return events

    def _parse_card(self, link_el: Tag) -> Event | None:
        """Extract an event from a link element and its surrounding context."""
        title = link_el.get_text(strip=True)
        if not title or len(title) < 2:
            return None

        url = self._absolute_url(link_el.get("href", ""))

        # Walk up the DOM to find the card container that holds date/desc/image
        container = self._find_card_container(link_el)
        if container is None:
            container = link_el.parent

        # Date: look for <h4> with the date pattern
        date_start = None
        date_end = None
        for h4 in container.select("h4"):
            text = h4.get_text(strip=True)
            dt = _parse_jazzclub_date(text)
            if dt:
                date_start = dt
                # Try to extract end time from "20:30 Uhr - 22:30 Uhr"
                end_match = re.search(r"-\s*(\d{1,2})[:\.](\d{2})\s*Uhr", text)
                if end_match:
                    try:
                        date_end = datetime(
                            dt.year, dt.month, dt.day,
                            int(end_match.group(1)), int(end_match.group(2)),
                        )
                    except ValueError:
                        pass
                break

        if not date_start:
            return None

        # Description: first <p> in the container
        description = ""
        p_el = container.select_one("p")
        if p_el:
            description = p_el.get_text(strip=True)

        # Image: check for lazy-loaded images first (data-lazy-src / data-src),
        # then fall back to a plain src attribute.
        image_url = ""
        img_el = container.select_one(
            "img[data-lazy-src], img[data-src], img[src]"
        )
        if img_el:
            image_url = self._absolute_url(
                img_el.get("data-lazy-src", "")
                or img_el.get("data-src", "")
                or img_el.get("src", "")
            )

        return Event(
            title=title,
            date_start=date_start,
            date_end=date_end,
            source=self.name,
            url=url,
            description=description,
            location=self.LOCATION,
            image_url=image_url,
            category="Musik",
        )

    @staticmethod
    def _find_card_container(el: Tag) -> Tag | None:
        """Walk up the DOM tree to find a container that holds both a date and a title."""
        node = el.parent
        for _ in range(6):  # max 6 levels up
            if node is None:
                break
            # A useful container has at least an h4 with a date pattern
            if node.find("h4") and RE_JAZZCLUB_DATE.search(node.get_text()):
                return node
            node = node.parent
        return None
