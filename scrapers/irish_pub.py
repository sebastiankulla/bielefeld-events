"""Scraper for Irish Pub Bielefeld event calendar."""

import json
import re

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event, parse_german_date


class IrishPubScraper(BaseScraper):
    """Scrapes events from Irish Pub Bielefeld (https://www.irishpub-bielefeld.de).

    The /eventkalender/ page embeds all events as JSON-LD schema.org Event objects
    inside ``<script type="application/ld+json">`` tags. Each object contains:

    * ``name``: Event title
    * ``startDate`` / ``endDate``: ISO 8601 timestamps (e.g. "2026-03-04T20:00:00+01:00")
    * ``description``: HTML description string
    * ``image``: Event image URL
    * ``url``: Link to the individual event page
    * ``location``: Place object with address details
    * ``offers``: Price info (price + priceCurrency)
    """

    name = "irish_pub"
    base_url = "https://www.irishpub-bielefeld.de"
    EVENTS_URL = "https://www.irishpub-bielefeld.de/eventkalender/"
    LOCATION = "Irish Pub Bielefeld, Niedernstraße 24, Bielefeld"

    def scrape(self) -> list[Event]:
        events: list[Event] = []
        try:
            html = self._get_page(self.EVENTS_URL)
            soup = BeautifulSoup(html, "lxml")
            events = self._parse_jsonld_events(soup)
        except Exception:
            self.logger.exception("Failed to scrape %s", self.EVENTS_URL)

        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    def _parse_jsonld_events(self, soup: BeautifulSoup) -> list[Event]:
        events: list[Event] = []
        seen: set[tuple] = set()

        for script_tag in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script_tag.string or "")
            except (json.JSONDecodeError, TypeError):
                continue

            # Handle both a single object and an array
            items = data if isinstance(data, list) else [data]

            for item in items:
                if item.get("@type") != "Event":
                    continue

                event = self._parse_jsonld_item(item)
                if event is None:
                    continue

                key = (event.title, event.date_start.date())
                if key not in seen:
                    seen.add(key)
                    events.append(event)

        return events

    def _parse_jsonld_item(self, item: dict) -> Event | None:
        title = (item.get("name") or "").strip()
        if not title:
            return None

        date_start = parse_german_date(item.get("startDate", ""))
        if date_start is None:
            return None

        date_end = parse_german_date(item.get("endDate", "")) or None

        # Strip HTML tags from description
        raw_desc = item.get("description", "") or ""
        description = re.sub(r"<[^>]+>", "", raw_desc).strip()

        url = (item.get("url") or "").strip() or self.EVENTS_URL
        image_url = (item.get("image") or "").strip()

        location = self._parse_jsonld_location(item.get("location")) or self.LOCATION

        price = ""
        offers = item.get("offers")
        if isinstance(offers, dict):
            amount = offers.get("price", "")
            currency = offers.get("priceCurrency", "EUR")
            if amount not in ("", None, "0", 0):
                price = f"{amount} {currency}"
            else:
                price = "Eintritt frei"
        elif isinstance(offers, list) and offers:
            first = offers[0]
            amount = first.get("price", "")
            currency = first.get("priceCurrency", "EUR")
            if amount not in ("", None, "0", 0):
                price = f"{amount} {currency}"
            else:
                price = "Eintritt frei"

        return Event(
            title=title,
            date_start=date_start,
            date_end=date_end,
            source=self.name,
            url=url,
            description=description,
            location=location,
            image_url=image_url,
            price=price,
            category="Party / Live",
        )
