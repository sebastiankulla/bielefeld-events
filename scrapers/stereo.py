"""Scraper for Stereo Bielefeld event listings."""

import json
import re

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event, parse_german_date


class StereoScraper(BaseScraper):
    """Scrapes events from Stereo Bielefeld (club/party venue).

    The site uses the EventON WordPress plugin which renders events as
    ``div.eventon_list_event`` containers with microdata attributes and
    (often malformed) JSON-LD blocks.
    """

    name = "stereo"
    base_url = "https://stereo-bielefeld.de"

    def scrape(self) -> list[Event]:
        events = []
        try:
            html = self._get_page(f"{self.base_url}/programm/")
            soup = BeautifulSoup(html, "lxml")
            events = self._extract_eventon_events(soup)
            if not events:
                events = self._extract_from_jsonld(soup)
            if not events:
                events = self._extract_events(soup)
            self.logger.info("Scraped %d events from %s", len(events), self.name)
        except Exception:
            self.logger.exception("Failed to scrape %s", self.name)
        return events

    def _extract_eventon_events(self, soup: BeautifulSoup) -> list[Event]:
        """Extract events from EventON plugin containers."""
        events = []
        containers = soup.select("div.eventon_list_event")
        for card in containers:
            event = self._parse_eventon_card(card)
            if event:
                events.append(event)
        return events

    def _parse_eventon_card(self, card) -> Event | None:
        # Title
        title_el = card.select_one(
            ".evoet_title, .evcal_event_title, .evcal_desc2"
        )
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        # URL
        link_el = card.select_one('a[href*="/events/"]')
        url = link_el["href"] if link_el else ""

        # Date – prefer itemprop startDate from the schema div
        date_start = None
        schema_start = card.select_one('[itemprop="startDate"]')
        if schema_start:
            date_str = schema_start.get("content", "")
            date_start = parse_german_date(date_str)

        # Fallback: Unix timestamp in data-time attribute ("start-end")
        if not date_start:
            data_time = card.get("data-time", "")
            if data_time:
                try:
                    from datetime import datetime
                    ts = int(data_time.split("-")[0])
                    date_start = datetime.fromtimestamp(ts)
                except (ValueError, IndexError, OSError):
                    pass

        if not date_start:
            return None

        # Image – from itemprop or img tag
        image_url = ""
        schema_img = card.select_one('[itemprop="image"]')
        if schema_img:
            image_url = schema_img.get("content", "") or schema_img.get("src", "")
        if not image_url:
            img_el = card.select_one("img.evo_event_main_img, img")
            if img_el:
                image_url = img_el.get("data-src", "") or img_el.get("src", "")
        image_url = self._absolute_url(image_url)

        # Description
        desc_el = card.select_one(".eventon_desc_in, .event_excerpt")
        description = ""
        if desc_el:
            description = desc_el.get_text(strip=True)[:500]

        return Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=url,
            description=description,
            location="Stereo Bielefeld",
            category="Party",
            image_url=image_url,
        )

    def _extract_from_jsonld(self, soup: BeautifulSoup) -> list[Event]:
        """Extract events from JSON-LD structured data (with sanitization)."""
        events = []
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                raw = script.string or ""
                # Sanitize trailing commas before ] and } (common in EventON)
                raw = re.sub(r",\s*([}\]])", r"\1", raw)
                data = json.loads(raw)
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
                                description=item.get("description", "")[:500],
                                location=self._parse_jsonld_location(
                                    item.get("location")
                                ) or "Stereo Bielefeld",
                                category="Party",
                                image_url=image,
                            ))
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue
        return events

    def _extract_events(self, soup: BeautifulSoup) -> list[Event]:
        """Fallback: extract events from HTML structure."""
        events = []
        containers = soup.select(
            "article, .event-item, .event-card, .event, "
            "[class*='event'], .card, .entry"
        )
        for card in containers:
            event = self._parse_card(card)
            if event:
                events.append(event)
        return events

    def _parse_card(self, card) -> Event | None:
        title_el = card.select_one("h2, h3, h4, .title, a[href]")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        link_el = card.select_one("a[href]")
        url = self._absolute_url(link_el["href"]) if link_el else ""

        date_el = card.select_one("time, .date, .datum, [class*='date']")
        date_start = self._parse_date_element(date_el)
        if not date_start:
            date_start = parse_german_date(card.get_text())
        if not date_start:
            return None

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
            location="Stereo Bielefeld",
            category="Party",
            image_url=image_url,
        )
