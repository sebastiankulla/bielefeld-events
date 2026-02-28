"""Scraper for Kulturamt Bielefeld event listings."""

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event, parse_german_date


class KulturamtScraper(BaseScraper):
    """Scrapes events from the Kulturamt Bielefeld website."""

    name = "kulturamt"
    base_url = "https://kulturamt-bielefeld.de"

    def scrape(self) -> list[Event]:
        events = []
        try:
            html = self._get_page(
                f"{self.base_url}/kultur-erleben/veranstaltungskalender/"
            )
            soup = BeautifulSoup(html, "lxml")
            events = self._extract_events(soup)
            self.logger.info("Scraped %d events from %s", len(events), self.name)
        except Exception:
            self.logger.exception("Failed to scrape %s", self.name)
        return events

    def _extract_events(self, soup: BeautifulSoup) -> list[Event]:
        events = []

        containers = soup.select(
            "article, .event-item, .event-card, .event, "
            ".veranstaltung, .termin, [class*='event'], "
            "[class*='veranstaltung'], [class*='termin'], "
            ".card, .entry, .list-item, .teaser"
        )

        for card in containers:
            event = self._parse_card(card)
            if event:
                events.append(event)

        # Fallback: JSON-LD
        if not events:
            events = self._extract_from_jsonld(soup)

        return events

    def _parse_card(self, card) -> Event | None:
        title_el = card.select_one(
            "h2, h3, h4, .title, .titel, "
            "[class*='title'], [class*='titel'], a[href]"
        )
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
            "p, .beschreibung, .text, .description, "
            "[class*='desc'], [class*='text']"
        )
        description = desc_el.get_text(strip=True) if desc_el else ""

        # Location - use the robust multi-strategy extraction
        location = self._extract_location_from_card(card)

        img_el = card.select_one("img[src]")
        image_url = ""
        if img_el:
            image_url = self._absolute_url(
                img_el.get("data-src", "") or img_el.get("src", "")
            )

        cat_el = card.select_one(
            ".kategorie, .category, [class*='category'], [class*='kategorie']"
        )
        category = cat_el.get_text(strip=True) if cat_el else "Kultur"

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
                                category="Kultur",
                                image_url=item.get("image", ""),
                            ))
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue
        return events
