"""Scraper for Bunker Ulmenwall Bielefeld event listings."""

import json

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event, parse_german_date


class BunkerUlmenwallScraper(BaseScraper):
    """Scrapes events from Bunker Ulmenwall (sociocultural venue)."""

    name = "bunker_ulmenwall"
    base_url = "https://bunker-ulmenwall.org"

    def scrape(self) -> list[Event]:
        events = []
        try:
            html = self._get_page(self.base_url)
            soup = BeautifulSoup(html, "lxml")
            events = self._extract_events(soup)
            if not events:
                events = self._extract_from_jsonld(soup)
            self.logger.info("Scraped %d events from %s", len(events), self.name)
        except Exception:
            self.logger.exception("Failed to scrape %s", self.name)
        return events

    def _extract_events(self, soup: BeautifulSoup) -> list[Event]:
        events = []

        # Bunker uses li items with kb-post-list-item class
        containers = soup.select(
            ".kb-post-list-item, article, .event-item, .event, "
            "[class*='event'], [class*='post-list-item'], "
            ".entry, .card, li.wp-block-post"
        )

        for card in containers:
            event = self._parse_card(card)
            if event:
                events.append(event)

        return events

    def _parse_card(self, card) -> Event | None:
        title_el = card.select_one(
            "h2, h3, h4, .title, .entry-title, "
            "[class*='title'], a[href]"
        )
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        link_el = card.select_one("a[href]")
        url = self._absolute_url(link_el["href"]) if link_el else ""

        # Date is typically in a <p> tag after the title
        date_start = None
        date_el = card.select_one(
            "time, .date, .datum, [class*='date'], [class*='datum']"
        )
        date_start = self._parse_date_element(date_el)
        if not date_start:
            date_start = parse_german_date(card.get_text())
        if not date_start:
            return None

        desc_el = card.select_one(
            "p, .description, .excerpt, [class*='desc'], [class*='excerpt']"
        )
        description = desc_el.get_text(strip=True) if desc_el else ""

        # Extract category from category paragraph (genres separated by |)
        category = ""
        for p in card.select("p"):
            text = p.get_text(strip=True)
            if "|" in text and len(text) < 100:
                category = text.split("|")[0].strip()
                break

        img_el = card.select_one("img[src]")
        image_url = ""
        if img_el:
            image_url = self._absolute_url(
                img_el.get("data-src", "") or img_el.get("src", "")
            )

        return Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=url,
            description=description,
            location="Bunker Ulmenwall",
            category=category or "Kultur",
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
                    if item.get("@type") in ("Event", "MusicEvent", "TheaterEvent"):
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
                                location="Bunker Ulmenwall",
                                category="Kultur",
                                image_url=image,
                            ))
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue
        return events
