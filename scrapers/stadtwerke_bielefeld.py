"""Scraper for events from bielefeld.de (Stadt Bielefeld) and Bielefeld Marketing."""

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event, parse_german_date


class StadtwerkeBielefeldScraper(BaseScraper):
    """Scrapes events from the official Bielefeld Marketing website."""

    name = "bielefeld_marketing"
    base_url = "https://www.bielefeld-marketing.de"

    PATHS = [
        "/events",
        "/termine/tickets",
    ]

    def scrape(self) -> list[Event]:
        events = []
        seen_titles = set()

        for path in self.PATHS:
            try:
                html = self._get_page(f"{self.base_url}{path}")
                soup = BeautifulSoup(html, "lxml")
                page_events = self._extract_events(soup)
                for ev in page_events:
                    key = (ev.title, ev.date_start.date())
                    if key not in seen_titles:
                        seen_titles.add(key)
                        events.append(ev)
            except Exception:
                self.logger.exception("Failed to scrape %s%s", self.base_url, path)

        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    def _extract_events(self, soup: BeautifulSoup) -> list[Event]:
        events = []

        # Try Drupal masonry layout first (used by bielefeld-marketing.de)
        containers = soup.select(".veranstaltung.masonry-view-item")

        # Fall back to generic selectors
        if not containers:
            containers = soup.select(
                "article, .event-item, .event-card, .event, "
                ".veranstaltung, .termin, .termin-item, "
                "[class*='event'], [class*='veranstaltung'], "
                ".card, .list-item, .teaser"
            )

        for card in containers:
            event = self._parse_card(card)
            if event:
                events.append(event)

        # Fallback: look for structured data (JSON-LD)
        if not events:
            events = self._extract_from_jsonld(soup)

        return events

    def _parse_card(self, card) -> Event | None:
        # Prefer headings – select_one returns first in document order,
        # so we search headings separately to avoid matching a wrapping
        # <a> or Drupal field div before the actual heading.
        title_el = card.select_one("h2, h3, h4")
        if not title_el:
            title_el = card.select_one(
                ".titel, .title, [class*='title'], [class*='titel']"
            )
        if not title_el:
            title_el = card.select_one("a[href]")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        link_el = card.select_one("a[href]")
        url = self._absolute_url(link_el["href"]) if link_el else ""

        date_el = card.select_one(
            "time, .datum, .date, [class*='date'], [class*='datum']"
        )
        date_start = self._parse_date_element(date_el)
        if not date_start:
            date_start = parse_german_date(card.get_text())
        if not date_start:
            return None

        desc_el = card.select_one(
            "p, .beschreibung, .text, .teaser-text, "
            "[class*='desc'], [class*='text']"
        )
        description = desc_el.get_text(strip=True) if desc_el else ""

        # Location - use the robust multi-strategy extraction
        location = self._extract_location_from_card(card)

        # Image – check card itself, then parent (masonry layouts put
        # images in a sibling wrapper, not inside the text container)
        img_el = card.select_one("img[src], img[data-src]")
        if not img_el and card.parent:
            img_el = card.parent.select_one("img[src], img[data-src]")
        image_url = ""
        if img_el:
            image_url = self._absolute_url(
                img_el.get("data-src", "") or img_el.get("src", "")
            )

        cat_el = card.select_one(
            ".kategorie, .category, [class*='category'], [class*='kategorie']"
        )
        category = cat_el.get_text(strip=True) if cat_el else ""

        return Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=url,
            description=description,
            location=location,
            category=category,
            image_url=image_url,
        )

    def _extract_from_jsonld(self, soup: BeautifulSoup) -> list[Event]:
        """Extract events from JSON-LD structured data if available."""
        import json

        events = []
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(script.string)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") in ("Event", "MusicEvent", "TheaterEvent"):
                        date = parse_german_date(item.get("startDate", ""))
                        if date:
                            events.append(Event(
                                title=item.get("name", ""),
                                date_start=date,
                                source=self.name,
                                url=item.get("url", ""),
                                description=item.get("description", ""),
                                location=self._parse_jsonld_location(
                                    item.get("location")
                                ),
                                image_url=item.get("image", ""),
                            ))
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue
        return events
