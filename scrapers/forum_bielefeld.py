"""Scraper for Forum Bielefeld event listings."""

import re
from datetime import datetime

from bs4 import BeautifulSoup, Tag

from scrapers.base import BaseScraper, Event, parse_german_date

# English and German 3-letter month abbreviations
MONTH_MAP = {
    "jan": 1,
    "feb": 2,
    "mar": 3, "mär": 3,
    "apr": 4,
    "mai": 5, "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "okt": 10, "oct": 10,
    "nov": 11,
    "dez": 12, "dec": 12,
}

RE_BG_IMAGE = re.compile(r"background-image:\s*url\(['\"]?(.+?)['\"]?\)")
RE_TIME = re.compile(r"(\d{1,2}):(\d{2})\s*Uhr", re.IGNORECASE)
RE_TIME_FIRST = re.compile(r"(\d{1,2}):(\d{2})")  # captures first HH:MM (start of range)
RE_PRICE = re.compile(r"Eintritt:\s*(.+)", re.IGNORECASE)


class ForumBielefeldScraper(BaseScraper):
    """Scrapes events from Forum Bielefeld (https://forum-bielefeld.com).

    The homepage lists upcoming events as ``div.article-wrap.event-entry``
    containers. Each card contains:

    * ``div.forumevent_date``: ``span.day`` (day number) + ``span.month``
      (3-letter month abbreviation, English or German)
    * ``div.category-name``: event category string
    * ``div.entry-title a``: event title and link URL
    * ``div.coverall-image``: CSS ``background-image`` containing the image URL

    Each detail page (e.g. ``/mothers-cake-3/``) has a
    ``div.semi-trans.block-content.details`` sidebar with the full numeric date
    (``DD.MM.YYYY``), start time (``HH:MM Uhr``), admission time, price
    (``Eintritt: …``), and a prose description in ``div.entry-content``.
    """

    name = "forum_bielefeld"
    base_url = "https://forum-bielefeld.com"
    EVENTS_URL = "https://forum-bielefeld.com/"
    LOCATION = "Forum Bielefeld, Ritterstraße 26, 33602 Bielefeld"

    def scrape(self) -> list[Event]:
        events: list[Event] = []
        try:
            html = self._get_page(self.EVENTS_URL)
            soup = BeautifulSoup(html, "lxml")
            events = self._parse_listing(soup)
        except Exception:
            self.logger.exception("Failed to scrape %s", self.EVENTS_URL)

        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    # ------------------------------------------------------------------
    # Listing page
    # ------------------------------------------------------------------

    def _parse_listing(self, soup: BeautifulSoup) -> list[Event]:
        events: list[Event] = []
        seen: set[str] = set()

        for card in soup.select("div.article-wrap.event-entry"):
            event = self._parse_card(card)
            if event and event.url not in seen:
                seen.add(event.url)
                events.append(event)

        return events

    def _parse_card(self, card: Tag) -> Event | None:
        # Title & URL
        link_el = card.select_one("div.entry-title a")
        if not link_el:
            return None
        url = (link_el.get("href") or "").strip()
        title = link_el.get_text(separator=" ", strip=True)
        if not title or not url:
            return None

        # Category
        cat_el = card.select_one("div.category-name")
        category = cat_el.get_text(strip=True) if cat_el else ""

        # Image: extracted from CSS background-image on the coverall div
        image_url = ""
        img_div = card.select_one("div.coverall-image")
        if img_div:
            style = img_div.get("style", "")
            m = RE_BG_IMAGE.search(style)
            if m:
                image_url = m.group(1).strip()

        # Approximate date from listing (day + month only, year inferred)
        date_start = self._parse_card_date(card)
        if date_start is None:
            return None

        # Enrich with full date, time, price, and description from detail page
        description, price, date_start = self._enrich_from_detail(url, date_start)

        return Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=url,
            description=description,
            location=self.LOCATION,
            image_url=image_url,
            category=category,
            price=price,
        )

    def _parse_card_date(self, card: Tag) -> datetime | None:
        """Parse the abbreviated date (day + month) from a listing card.

        The year is inferred: use the current year; if that date has already
        passed, assume next year (the site only shows upcoming events).
        """
        date_div = card.select_one("div.forumevent_date")
        if not date_div:
            return None

        day_el = date_div.select_one("span.day")
        month_el = date_div.select_one("span.month")
        if not day_el or not month_el:
            return None

        try:
            day = int(day_el.get_text(strip=True))
        except ValueError:
            return None

        month_str = month_el.get_text(strip=True).lower()[:3]
        month = MONTH_MAP.get(month_str)
        if not month:
            return None

        now = datetime.now()
        year = now.year
        try:
            dt = datetime(year, month, day)
        except ValueError:
            return None

        # If the computed date is in the past, the event is next year
        if dt.date() < now.date():
            try:
                dt = datetime(year + 1, month, day)
            except ValueError:
                return None

        return dt

    # ------------------------------------------------------------------
    # Detail page
    # ------------------------------------------------------------------

    def _enrich_from_detail(
        self, url: str, date_fallback: datetime
    ) -> tuple[str, str, datetime]:
        """Fetch the event detail page and extract full date, time, price, description."""
        description = ""
        price = ""
        date_start = date_fallback

        try:
            html = self._get_page(url)
            soup = BeautifulSoup(html, "lxml")

            details = soup.select_one("div.semi-trans.block-content.details")
            if details:
                # Each fact lives in its own <div> inside div.bottom
                bottom = details.select_one("div.bottom") or details
                divs = [d.get_text(strip=True) for d in bottom.find_all("div", recursive=False)]
                full_text = "\n".join(divs)

                # Full numeric date (DD.MM.YYYY)
                date_parsed = parse_german_date(full_text)
                if date_parsed:
                    date_start = date_parsed

                # Start time: find first div containing "Uhr" but not "Einlass".
                # Use RE_TIME_FIRST to capture the first HH:MM in the div so that
                # ranges like "22:00 – 05:00 Uhr" yield the start time (22:00),
                # not the end time (05:00) which RE_TIME would find instead.
                for div_text in divs:
                    if "einlass" in div_text.lower():
                        continue
                    if "uhr" not in div_text.lower():
                        continue
                    m_time = RE_TIME_FIRST.search(div_text)
                    if m_time:
                        date_start = date_start.replace(
                            hour=int(m_time.group(1)),
                            minute=int(m_time.group(2)),
                        )
                        break

                # Price ("Eintritt: …")
                m_price = RE_PRICE.search(full_text)
                if m_price:
                    price = m_price.group(1).strip().rstrip(",").strip()

            # Description: first substantial <p> in the entry content area
            content_block = soup.select_one("div.entry-content")
            if content_block:
                for p in content_block.select("p"):
                    text = p.get_text(strip=True)
                    if len(text) > 40:
                        description = text
                        break

        except Exception:
            self.logger.debug("Could not enrich event detail from %s", url)

        return description, price, date_start
