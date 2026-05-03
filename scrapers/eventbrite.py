"""Scraper for Eventbrite events in Bielefeld via the official API v3.

Requires the environment variable EVENTBRITE_TOKEN to be set to a valid
Eventbrite private OAuth token.  Without it the scraper silently returns an
empty list so the rest of the pipeline keeps running.

Get a token at: https://www.eventbrite.com/platform/api-keys
"""

import os
from datetime import datetime, timedelta

from scrapers.base import BaseScraper, Event

API_BASE = "https://www.eventbriteapi.com/v3"
PAGE_SIZE = 50

# Eventbrite place ID for Bielefeld, Germany (used as fallback query)
_LOCATION_ADDRESS = "Bielefeld, Germany"
_LOCATION_WITHIN = "10km"


class EventbriteScraper(BaseScraper):
    """Scrapes events from Eventbrite for Bielefeld using the official API."""

    name = "eventbrite"
    base_url = "https://www.eventbrite.de"

    def scrape(self) -> list[Event]:
        token = os.environ.get("EVENTBRITE_TOKEN", "").strip()
        if not token:
            self.logger.info(
                "EVENTBRITE_TOKEN not set – skipping Eventbrite scraper. "
                "Get a token at https://www.eventbrite.com/platform/api-keys"
            )
            return []

        self.session.headers["Authorization"] = f"Bearer {token}"

        today = datetime.now()
        end_date = today + timedelta(days=30)

        events: list[Event] = []
        page = 1
        while True:
            params = {
                "location.address": _LOCATION_ADDRESS,
                "location.within": _LOCATION_WITHIN,
                "start_date.range_start": today.strftime("%Y-%m-%dT00:00:00Z"),
                "start_date.range_end": end_date.strftime("%Y-%m-%dT23:59:59Z"),
                "expand": "venue,category,logo,ticket_availability",
                "page": page,
                "page_size": PAGE_SIZE,
            }
            try:
                resp = self.session.get(
                    f"{API_BASE}/destination/search/",
                    params=params,
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception:
                self.logger.exception("Failed to fetch Eventbrite page %d", page)
                break

            raw_events = data.get("events") or []
            for item in raw_events:
                event = self._parse_event(item)
                if event:
                    events.append(event)

            pagination = data.get("pagination") or {}
            if not pagination.get("has_more_items"):
                break
            page += 1

        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    def _parse_event(self, item: dict) -> Event | None:
        # Title
        title = (item.get("name") or {}).get("text", "").strip()
        if not title:
            return None

        # Start date (use local time so we don't need tz conversion)
        start = item.get("start") or {}
        date_start = self._parse_iso(start.get("local", ""))
        if not date_start:
            return None

        # End date
        end = item.get("end") or {}
        date_end = self._parse_iso(end.get("local", ""))

        url = item.get("url", "")

        # Description (plain text)
        description = (item.get("description") or {}).get("text", "")

        # Image – prefer the full-size original logo
        logo = item.get("logo") or {}
        image_url = (
            (logo.get("original") or {}).get("url", "")
            or logo.get("url", "")
        )

        # Venue / location
        venue = item.get("venue") or {}
        venue_name = venue.get("name", "")
        address = venue.get("address") or {}
        city = address.get("city", "Bielefeld")
        street = address.get("address_1", "")
        if venue_name and street:
            location = f"{venue_name}, {street}"
        else:
            location = venue_name or street

        # Category
        category = (item.get("category") or {}).get("name", "")

        # Price
        if item.get("is_free"):
            price = "Kostenlos"
        else:
            ticket_avail = item.get("ticket_availability") or {}
            min_ticket = ticket_avail.get("minimum_ticket_price") or {}
            price = min_ticket.get("display", "")

        return Event(
            title=title,
            date_start=date_start,
            date_end=date_end,
            source=self.name,
            url=url,
            description=description,
            location=location,
            city=city,
            category=category,
            image_url=image_url,
            price=price,
        )

    @staticmethod
    def _parse_iso(date_str: str) -> datetime | None:
        if not date_str:
            return None
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str[:19], fmt)
            except ValueError:
                continue
        return None
