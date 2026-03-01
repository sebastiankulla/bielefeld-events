"""Scraper for Nr.z.P. (Nummer zu Platz) Bielefeld event listings."""

import re
from datetime import datetime

from bs4 import BeautifulSoup, Tag

from scrapers.base import BaseScraper, Event

# Date format on the page: "Mi. 04 03" (weekday. day month)
RE_NRZP_DATE = re.compile(r"(\d{1,2})\s+(\d{1,2})")

# Time format on the page: "20 00 H" (hour minute)
RE_NRZP_TIME = re.compile(r"(\d{1,2})\s+(\d{2})\s*H", re.IGNORECASE)


class NrzpScraper(BaseScraper):
    """Scrapes events from Nr.z.P. Bielefeld (subculture venue)."""

    name = "nrzp"
    base_url = "https://nrzp.de"

    def scrape(self) -> list[Event]:
        events = []
        try:
            html = self._get_page(f"{self.base_url}/programm")
            soup = BeautifulSoup(html, "lxml")
            events = self._extract_events(soup)
            self.logger.info("Scraped %d events from %s", len(events), self.name)
        except Exception:
            self.logger.exception("Failed to scrape %s", self.name)
        return events

    def _extract_events(self, soup: BeautifulSoup) -> list[Event]:
        """Extract events from the NRZP event calendar structure.

        The HTML pattern is pairs of:
          <div class="eventcalender-row">  (date, category, time)
          <a class="menu_img_btn">          (title, link, image)
        """
        events = []
        now = datetime.now()

        for row in soup.select("div.eventcalender-row"):
            # Extract metadata from the calendar row
            date_text = self._text_from(row, ".eventcalender-date")
            category = self._text_from(row, ".eventcalender-art")
            time_text = self._text_from(row, ".eventcalender-time")

            date_start = self._parse_nrzp_datetime(date_text, time_text, now)
            if not date_start:
                continue

            # The event link is the next sibling <a> element
            link_el = row.find_next_sibling("a", class_="menu_img_btn")
            if not link_el:
                continue

            title_el = link_el.select_one("span.span_left")
            title = title_el.get_text(strip=True) if title_el else ""
            if not title:
                title = link_el.get_text(strip=True)
            if not title:
                continue

            url = link_el.get("href", "")

            img_el = link_el.select_one("img.menu_img")
            image_url = ""
            if img_el:
                image_url = img_el.get("data-src", "") or img_el.get("src", "")

            events.append(Event(
                title=title,
                date_start=date_start,
                source=self.name,
                url=url,
                description="",
                location="Nr.z.P. Bielefeld",
                category=category or "Subkultur",
                image_url=image_url,
            ))

        return events

    @staticmethod
    def _text_from(parent: Tag, selector: str) -> str:
        """Get stripped text from a child element selected by CSS."""
        el = parent.select_one(selector)
        return el.get_text(strip=True) if el else ""

    @staticmethod
    def _parse_nrzp_datetime(
        date_text: str, time_text: str, reference: datetime
    ) -> datetime | None:
        """Parse NRZP date ('Mi. 04 03') and time ('20 00 H') into a datetime.

        The year is not shown on the page, so we infer it from the current date:
        dates more than 2 months in the past are assumed to be next year.
        """
        date_match = RE_NRZP_DATE.search(date_text)
        if not date_match:
            return None

        day = int(date_match.group(1))
        month = int(date_match.group(2))

        hour, minute = 0, 0
        time_match = RE_NRZP_TIME.search(time_text)
        if time_match:
            hour = int(time_match.group(1))
            minute = int(time_match.group(2))

        # Infer year: assume current year, but if date is >2 months in the past,
        # use next year (venue likely lists upcoming events only)
        year = reference.year
        try:
            dt = datetime(year, month, day, hour, minute)
        except ValueError:
            return None

        if (reference - dt).days > 60:
            year += 1
            try:
                dt = datetime(year, month, day, hour, minute)
            except ValueError:
                return None

        return dt
