"""Scraper for Lokschuppen Bielefeld event listings."""

import json
import re

from bs4 import BeautifulSoup, Tag

from scrapers.base import BaseScraper, Event, parse_german_date


class LokschuppenScraper(BaseScraper):
    """Scrapes events from Lokschuppen Bielefeld (event venue).

    The ``/event/`` page lists events in a custom layout using
    ``div.event`` containers (not standard article elements).  Each
    container has:

    * ``div.img`` with an ``<a>`` linking to the detail page and an
      ``<img>`` for the poster.
    * ``span.details`` with a child ``<div>`` holding the title and a
      date string like ``01.03.2026``.
    """

    name = "lokschuppen"
    base_url = "https://www.lokschuppen-bielefeld.de"

    def scrape(self) -> list[Event]:
        events = []
        seen = set()

        try:
            html = self._get_page(f"{self.base_url}/event/")
            soup = BeautifulSoup(html, "lxml")
            page_events = self._extract_event_divs(soup)
            for ev in page_events:
                key = (ev.title, ev.date_start.date())
                if key not in seen:
                    seen.add(key)
                    events.append(ev)
        except Exception:
            self.logger.exception("Failed to scrape %s/event/", self.base_url)

        if not events:
            try:
                html = self._get_page(f"{self.base_url}/veranstaltungen/")
                soup = BeautifulSoup(html, "lxml")
                events = self._extract_from_jsonld(soup)
            except Exception:
                self.logger.exception(
                    "Failed to scrape %s/veranstaltungen/", self.base_url,
                )

        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    def _extract_event_divs(self, soup: BeautifulSoup) -> list[Event]:
        """Extract events from the custom div.event containers."""
        events = []
        # Select only direct event divs (have "event" as a class plus
        # modifier classes like cnt0, mod40, etc.)
        archive = soup.select_one(".events-archive, .events-grid")
        if not archive:
            return events

        for card in archive.select("div.event"):
            event = self._parse_event_div(card)
            if event:
                events.append(event)
        return events

    def _parse_event_div(self, card: Tag) -> Event | None:
        # Title – inside span.details > div (first div child)
        details = card.select_one("span.details")
        if not details:
            return None

        title_div = details.select_one("div")
        if not title_div:
            return None

        # The title div contains the title text followed by a date string.
        # Extract only the title (everything before the date).
        full_text = title_div.get_text(strip=True)
        if not full_text:
            return None

        # Split title from date: date looks like "DD.MM.YYYY" at the end
        title = full_text
        date_match = re.search(r"(\d{1,2}\.\d{1,2}\.\d{4})", full_text)
        if date_match:
            title = full_text[:date_match.start()].strip()

        if not title or len(title) < 2:
            return None

        # Date
        date_start = None
        if date_match:
            date_start = parse_german_date(date_match.group(1))
        if not date_start:
            date_start = parse_german_date(card.get_text())
        if not date_start:
            return None

        # URL – from the image link or any link
        link_el = card.select_one("a.img, a[href*='/event/']")
        if not link_el:
            link_el = card.select_one("a[href]")
        url = self._absolute_url(link_el["href"]) if link_el else ""

        # Image
        image_url = ""
        img_el = card.select_one("img[src]")
        if img_el:
            image_url = self._absolute_url(
                img_el.get("data-lazy-src", "")
                or img_el.get("data-src", "")
                or img_el.get("src", "")
            )

        # Description – check for extra text after the date
        description = ""
        if date_match:
            after_date = full_text[date_match.end():].strip()
            # Remove "Tickets kaufen" etc. from the end
            after_date = re.sub(
                r"(?:Tickets\s*kaufen|Ausverkauft|Abgesagt|"
                r"Nur Abendkasse|Verschoben.*?)$",
                "", after_date, flags=re.IGNORECASE,
            ).strip()
            if after_date and after_date != title:
                description = after_date

        return Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=url,
            description=description,
            location="Lokschuppen Bielefeld",
            image_url=image_url,
        )

    def _extract_from_jsonld(self, soup: BeautifulSoup) -> list[Event]:
        """Extract events from JSON-LD structured data."""
        events = []
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(script.string)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") in ("Event", "MusicEvent", "DanceEvent"):
                        date = parse_german_date(item.get("startDate", ""))
                        if date:
                            image = item.get("image", "")
                            if isinstance(image, list):
                                image = image[0] if image else ""
                            elif isinstance(image, dict):
                                image = image.get("url", "")
                            events.append(Event(
                                title=item.get("name", ""),
                                date_start=date,
                                source=self.name,
                                url=item.get("url", ""),
                                description=item.get("description", ""),
                                location=self._parse_jsonld_location(
                                    item.get("location")
                                ) or "Lokschuppen Bielefeld",
                                image_url=image,
                            ))
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue
        return events
