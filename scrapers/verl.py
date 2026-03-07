"""Scraper for Verl city event calendar (verl.de)."""

import re
import time

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event, parse_german_date


class VerlScraper(BaseScraper):
    """Scrapes events from the Stadt Verl event calendar."""

    name = "verl"
    base_url = "https://www.verl.de"
    _list_url = "https://www.verl.de/freizeit-kultur/veranstaltungskalender.html"

    def scrape(self) -> list[Event]:
        events: list[Event] = []
        try:
            visited_urls: set[str] = set()
            next_url = self._list_url

            while next_url and next_url not in visited_urls:
                visited_urls.add(next_url)
                html = self._get_page(next_url)
                soup = BeautifulSoup(html, "lxml")

                page_events = self._extract_events(soup)
                events.extend(page_events)
                self.logger.debug(
                    "Page %s: %d events (total so far: %d)",
                    next_url, len(page_events), len(events),
                )

                next_url = self._next_page_url(soup, visited_urls)
                if next_url:
                    time.sleep(0.5)

            self.logger.info("Scraped %d events from %s", len(events), self.name)
        except Exception:
            self.logger.exception("Failed to scrape %s", self.name)
        return events

    def _extract_events(self, soup: BeautifulSoup) -> list[Event]:
        cards = soup.select("ul.tile-list > li")
        events = []
        for card in cards:
            event = self._parse_card(card)
            if event:
                events.append(event)
        return events

    def _parse_card(self, card) -> Event | None:
        link_el = card.select_one("a.item")
        if not link_el:
            return None

        # URL
        href = link_el.get("href", "")
        # Skip the "Veranstaltung anmelden" registration link
        if "veranstaltung-anmelden" in href:
            return None
        url = self._absolute_url(href)

        # Title
        title_el = link_el.select_one("h3.title")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        # Date / time  — format: "07.03.2026\n14.00\n- 17.00\nUhr"
        date_start, date_end = self._parse_date_time(link_el)
        if not date_start:
            return None

        # Location
        location = ""
        loc_el = link_el.select_one("div.location")
        if loc_el:
            # Remove SVG noise, get plain text
            for svg in loc_el.find_all("svg"):
                svg.decompose()
            location = loc_el.get_text(strip=True)

        # Organizer → put in description if present
        organizer = ""
        org_el = link_el.select_one("div.organizer")
        if org_el:
            for svg in org_el.find_all("svg"):
                svg.decompose()
            organizer = org_el.get_text(strip=True)

        # Image
        img_url = ""
        img_el = link_el.select_one("img")
        if img_el:
            src = img_el.get("src") or img_el.get("data-src", "")
            if src and not src.startswith("data:"):
                img_url = self._absolute_url(src)

        return Event(
            title=title,
            date_start=date_start,
            date_end=date_end,
            source=self.name,
            url=url,
            location=location,
            city="Verl",
            description=f"Veranstalter: {organizer}" if organizer else "",
            image_url=img_url,
        )

    # ------------------------------------------------------------------
    # Date parsing helpers
    # ------------------------------------------------------------------

    # Matches "07.03.2026" inside a date-time block
    _RE_DATE = re.compile(r"(\d{2}\.\d{2}\.\d{4})")
    # Matches a time like "14.00" or "14:00"
    _RE_TIME = re.compile(r"(\d{1,2})[\.:](\d{2})")

    def _parse_date_time(self, link_el):
        """Return (date_start, date_end) datetimes from the .date-time div."""
        dt_div = link_el.select_one("div.date-time")
        if not dt_div:
            return None, None

        # Remove SVG elements first
        for svg in dt_div.find_all("svg"):
            svg.decompose()

        text = dt_div.get_text(separator=" ", strip=True)
        # text example: "07.03.2026 14.00 - 17.00 Uhr"
        # or just:      "07.03.2026 14.00 Uhr"

        date_match = self._RE_DATE.search(text)
        if not date_match:
            return None, None
        date_str = date_match.group(1)

        # Find all times in the text after the date
        after_date = text[date_match.end():]
        times = self._RE_TIME.findall(after_date)

        date_start = None
        date_end = None

        if times:
            h, m = int(times[0][0]), int(times[0][1])
            date_start = parse_german_date(f"{date_str} {h:02d}:{m:02d}")
        else:
            date_start = parse_german_date(date_str)

        if len(times) >= 2:
            h, m = int(times[1][0]), int(times[1][1])
            date_end = parse_german_date(f"{date_str} {h:02d}:{m:02d}")

        return date_start, date_end

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------

    def _next_page_url(self, soup: BeautifulSoup, visited: set[str]) -> str | None:
        """Return the URL of the next unvisited pagination page, or None."""
        pag_links = soup.select(".cyt-eventcalendar-pagination a")
        for a in pag_links:
            href = a.get("href", "")
            if not href:
                continue
            full_url = self._absolute_url(href)
            if full_url not in visited:
                return full_url
        return None
