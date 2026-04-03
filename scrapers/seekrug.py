"""Scraper for Seekrug am Obersee event listings."""

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event, parse_german_date


class SeekrugScraper(BaseScraper):
    """Scrapes events from Seekrug am Obersee (https://seekrug.com/aktuelles/).

    The /aktuelles/ page renders events as ``div.tmb`` cards, each containing:

    * ``div.t-entry-cf-detail-195899``: Date text (e.g. "30.04.2026" or
      "Ostersonntag, 05.04.2026")
    * ``h3.t-entry-title a``: Event title and link to the individual page
    * ``img.adaptive-async``: Thumbnail image (``data-guid`` holds the full URL)
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
            event = self._parse_card(card)
            if event is None:
                continue
            key = (event.title, event.date_start.date())
            if key not in seen:
                seen.add(key)
                events.append(event)

        return events

    def _parse_card(self, card) -> Event | None:
        # Date
        date_el = card.select_one("div.t-entry-cf-detail-195899")
        if not date_el:
            return None
        date_text = date_el.get_text(strip=True)
        date_start = parse_german_date(date_text)
        if date_start is None:
            return None

        # Title + URL
        title_el = card.select_one("h3.t-entry-title a")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title:
            return None
        url = self._absolute_url(title_el.get("href", "")) or self.EVENTS_URL

        # Image – prefer data-guid (full-size) over src (thumbnail)
        img_el = card.select_one("img.adaptive-async")
        image_url = ""
        if img_el:
            image_url = img_el.get("data-guid", "") or img_el.get("src", "")

        return Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=url,
            location=self.LOCATION,
            image_url=image_url,
            category="Veranstaltung",
        )
