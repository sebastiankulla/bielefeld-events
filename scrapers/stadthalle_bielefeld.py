"""Scraper for Stadthalle Bielefeld event listings."""

import re

from bs4 import BeautifulSoup, Tag

from scrapers.base import BaseScraper, Event, parse_german_date


class StadthalleBielefeldScraper(BaseScraper):
    """Scrapes events from Stadthalle Bielefeld.

    The ``/fuer-besucher/veranstaltungen`` page lists events as Bootstrap
    ``div.card`` containers.  Each card contains:

    * A ``<picture>`` element with ``<source>`` tags providing the event
      image URL via ``srcset``.
    * A ``div.card-body`` with:
      * ``div.text-primary-light`` holding the date (and optional end date)
        plus venue, e.g. ``"Do, 05.03.2026 – Stadthalle Bielefeld"`` or a
        date range ``"Sa, 07.03.2026 - So, 08.03.2026\\nStadthalle Bielefeld"``.
      * ``p.fw-semibold`` holding the event title.
    """

    name = "stadthalle_bielefeld"
    base_url = "https://www.stadthalle-bielefeld.de"

    # "05.03.2026" — used to pull individual dates out of date-range text
    _RE_DATE = re.compile(r"\d{1,2}\.\d{2}\.\d{4}")

    def scrape(self) -> list[Event]:
        events: list[Event] = []
        seen: set[tuple] = set()
        try:
            html = self._get_page(f"{self.base_url}/fuer-besucher/veranstaltungen")
            soup = BeautifulSoup(html, "lxml")
            for card in soup.select("div.card"):
                event = self._parse_card(card)
                if event:
                    key = (event.title, event.date_start.date())
                    if key not in seen:
                        seen.add(key)
                        events.append(event)
        except Exception:
            self.logger.exception("Failed to scrape %s", self.base_url)

        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    def _parse_card(self, card: Tag) -> Event | None:
        # --- URL ---
        link_el = card.select_one("a[href]")
        if not link_el:
            return None
        url = self._absolute_url(link_el["href"])

        # --- Image: first srcset value from the first <source> tag ---
        image_url = ""
        source_el = card.select_one("source[srcset]")
        if source_el:
            first_src = source_el["srcset"].split(",")[0].split()[0].strip()
            if first_src:
                image_url = self._absolute_url(first_src)

        # --- Date & location ---
        date_div = card.select_one("div.text-primary-light")
        if not date_div:
            return None

        date_text = date_div.get_text(separator=" ", strip=True)

        # Extract all DD.MM.YYYY occurrences
        date_matches = self._RE_DATE.findall(date_text)
        if not date_matches:
            return None

        date_start = parse_german_date(date_matches[0])
        if not date_start:
            return None

        date_end = None
        if len(date_matches) >= 2:
            date_end = parse_german_date(date_matches[1])

        # Location: text after the last date match
        last_date_end = date_text.rfind(date_matches[-1]) + len(date_matches[-1])
        location_raw = date_text[last_date_end:].strip().lstrip("–-").strip()
        location = location_raw if len(location_raw) >= 3 else "Stadthalle Bielefeld"

        # --- Title ---
        title_el = card.select_one("p.fw-semibold, p.mb-4")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title:
            return None

        return Event(
            title=title,
            date_start=date_start,
            date_end=date_end,
            source=self.name,
            url=url,
            location=location,
            image_url=image_url,
        )
