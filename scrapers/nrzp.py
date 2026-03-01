"""Scraper for Nr.z.P. (Nummer zu Platz) Bielefeld event listings."""

import json

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event, parse_german_date


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
            if not events:
                events = self._extract_from_jsonld(soup)
            self.logger.info("Scraped %d events from %s", len(events), self.name)
        except Exception:
            self.logger.exception("Failed to scrape %s", self.name)
        return events

    def _extract_events(self, soup: BeautifulSoup) -> list[Event]:
        events = []

        # NRZP uses Elementor-based cards
        containers = soup.select(
            "article, .event-item, .event-card, .event, "
            "[class*='event'], .elementor-post, "
            ".card, .entry, .wp-block-post, "
            ".elementor-element a[href]"
        )

        for card in containers:
            event = self._parse_card(card)
            if event:
                events.append(event)

        # Fallback: try to extract events from link patterns
        if not events:
            events = self._extract_from_links(soup)

        return events

    def _parse_card(self, card) -> Event | None:
        title_el = card.select_one(
            "h2, h3, h4, .title, .event-title, "
            "[class*='title'], a[href]"
        )
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        link_el = card.select_one("a[href]")
        url = self._absolute_url(link_el["href"]) if link_el else ""

        date_el = card.select_one(
            "time, .date, .datum, [class*='date'], [class*='datum']"
        )
        date_start = self._parse_date_element(date_el)
        if not date_start:
            date_start = parse_german_date(card.get_text())
        if not date_start:
            return None

        desc_el = card.select_one(
            "p, .description, .text, [class*='desc']"
        )
        description = desc_el.get_text(strip=True) if desc_el else ""

        # Category from labels
        category = ""
        for el in card.select("span, .category, [class*='category'], [class*='tag']"):
            text = el.get_text(strip=True)
            if text and len(text) < 30:
                category = text
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
            location="Nr.z.P. Bielefeld",
            category=category or "Subkultur",
            image_url=image_url,
        )

    def _extract_from_links(self, soup: BeautifulSoup) -> list[Event]:
        """Fallback: extract events from links with date info."""
        events = []
        seen = set()
        for link in soup.select("a[href]"):
            href = link.get("href", "")
            if not href or href == "#":
                continue
            text = link.get_text(strip=True)
            if not text or len(text) < 3:
                continue

            # Look for date in surrounding context
            parent = link.parent
            if parent:
                parent_text = parent.get_text()
                date = parse_german_date(parent_text)
                if date and text not in seen:
                    seen.add(text)
                    url = self._absolute_url(href)
                    img_el = link.select_one("img[src]")
                    image_url = ""
                    if img_el:
                        image_url = self._absolute_url(
                            img_el.get("data-src", "") or img_el.get("src", "")
                        )
                    events.append(Event(
                        title=text,
                        date_start=date,
                        source=self.name,
                        url=url,
                        location="Nr.z.P. Bielefeld",
                        category="Subkultur",
                        image_url=image_url,
                    ))
        return events

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
                                location="Nr.z.P. Bielefeld",
                                category="Subkultur",
                                image_url=image,
                            ))
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue
        return events
