"""Scraper for Gütersloh city event listings."""

import re
from urllib.parse import urlencode, urlparse, parse_qs

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event, parse_german_date


class GueterslohScraper(BaseScraper):
    """Scrapes events from the Stadt Gütersloh website."""

    name = "guetersloh"
    base_url = "https://www.guetersloh.de"
    _list_url = "https://www.guetersloh.de/de/veranstaltungen/"

    def scrape(self) -> list[Event]:
        events = []
        try:
            html = self._get_page(self._list_url)
            soup = BeautifulSoup(html, "lxml")
            events = self._extract_events(soup)
            self.logger.info("Scraped %d events from %s", len(events), self.name)
        except Exception:
            self.logger.exception("Failed to scrape %s", self.name)
        return events

    def _extract_events(self, soup: BeautifulSoup) -> list[Event]:
        entries = soup.find_all(
            "li",
            class_=lambda c: c and "eventMulti" in c if c else False,
        )
        events = []
        for entry in entries:
            event = self._parse_entry(entry)
            if event:
                events.append(event)
        return events

    def _parse_entry(self, entry) -> Event | None:
        # --- Title ---
        title_el = entry.select_one("h3.listEntryTitle, h2.listEntryTitle, .listEntryTitle")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        # --- URL ---
        inner = entry.select_one(".listEntryInner[data-url]")
        raw_url = inner["data-url"] if inner else ""
        url = self._absolute_url(raw_url) if raw_url else ""

        # --- Date start ---
        date_start = self._extract_date_from_entry(entry, raw_url)
        if not date_start:
            return None

        # --- Date end ---
        date_end = self._extract_end_date_from_entry(entry, raw_url)

        # --- Image ---
        image_url = self._extract_image(entry)

        # --- Category (from listEntry classes like listEntryObject-eventMulti) ---
        category = self._extract_category(entry)

        return Event(
            title=title,
            date_start=date_start,
            date_end=date_end,
            source=self.name,
            url=url,
            location="",
            city="Gütersloh",
            category=category,
            image_url=image_url,
        )

    def _extract_date_from_entry(self, entry, raw_url: str):
        """Try to parse the start date from the entry HTML or the data-url."""
        # Prefer the structured HTML spans
        date_span = entry.select_one("span.dayDate.dayFrom")
        time_span = entry.select_one("span.timeFrom")
        if date_span:
            date_text = date_span.get_text(strip=True)
            if time_span:
                # e.g. "23.01.2026" + ", 19:00"
                date_text = date_text + " " + time_span.get_text(strip=True).strip(", ")
            dt = parse_german_date(date_text)
            if dt:
                return dt

        # Fallback: parse from data-url ?from=2026-03-07%2019:00:00
        if raw_url:
            decoded = raw_url.replace("%20", " ")
            m = re.search(r"from=(\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?)", decoded)
            if m:
                dt = parse_german_date(m.group(1))
                if dt:
                    return dt

        return None

    def _extract_end_date_from_entry(self, entry, raw_url: str):
        """Try to parse the end date from the entry HTML."""
        date_span = entry.select_one("span.dayTo.dayDate")
        time_span = entry.select_one("span.timeTo")
        if date_span:
            date_text = date_span.get_text(strip=True)
            if time_span:
                date_text = date_text + " " + time_span.get_text(strip=True).strip(", ")
            dt = parse_german_date(date_text)
            if dt:
                return dt

        # Fallback: ?to=... in data-url  (URL may contain %20 as space)
        if raw_url:
            decoded = raw_url.replace("%20", " ")
            m = re.search(r"to=(\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?)", decoded)
            if m:
                dt = parse_german_date(m.group(1))
                if dt:
                    return dt

        return None

    def _extract_image(self, entry) -> str:
        """Extract the best available image URL from a lazy-loaded picture element."""
        source = entry.select_one("picture source[data-src]")
        if source:
            srcset = source.get("data-src", "")
            # data-src may contain "url1 374w, url2 748w" — take first
            first = srcset.split(",")[0].split()[0].strip()
            if first:
                return self._absolute_url(first)

        img = entry.select_one("img[data-src], img[src]")
        if img:
            src = img.get("data-src") or img.get("src", "")
            if src and not src.startswith("data:"):
                return self._absolute_url(src)

        return ""

    def _extract_category(self, entry) -> str:
        # The data-categories attribute may carry category info
        cats = entry.get("data-categories", "").strip()
        if cats:
            return cats.split(",")[0].strip().title()
        return "Veranstaltung"
