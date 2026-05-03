"""Scraper for Eventbrite events in Bielefeld via the official API v3.

Requires the environment variable EVENTBRITE_TOKEN to be set to a valid
Eventbrite private OAuth token.  Without it the scraper silently returns an
empty list so the rest of the pipeline keeps running.

Get a token at: https://www.eventbrite.com/platform/api-keys
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from scrapers.base import BaseScraper, Event

API_BASE = "https://www.eventbriteapi.com/v3"

# Bounding box around Bielefeld (approx. 15 km radius)
_BBOX = {
    "top": 52.12,
    "bottom": 51.90,
    "left": 8.30,
    "right": 8.75,
}


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
        # Override Accept so the API returns JSON instead of HTML (the
        # browser-like User-Agent from BaseScraper would otherwise trigger
        # the WAF to serve an HTML CAPTCHA page).
        self.session.headers["Accept"] = "application/json"

        today = datetime.now()
        end_date = today + timedelta(days=30)

        raw = self._search_all(today.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        self.logger.info("Found %d raw events from destination/search", len(raw))

        events = self._enrich_all(raw)
        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    # ------------------------------------------------------------------ search

    def _search_all(self, start_date: str, end_date: str) -> list[dict]:
        """Return all search result dicts for the date range using continuation pagination."""
        results: list[dict] = []
        continuation: str | None = None

        while True:
            event_search: dict = {
                "q": "",
                "dedup": True,
                "page_size": 50,
                "bbox": _BBOX,
                "date_range": {"from": start_date, "to": end_date},
            }
            if continuation:
                event_search["continuation"] = continuation

            try:
                resp = self.session.post(
                    f"{API_BASE}/destination/search/",
                    json={"locale": "de_DE", "event_search": event_search},
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception:
                self.logger.exception("destination/search request failed")
                break

            events_block = data.get("events") or {}
            results.extend(events_block.get("results") or [])

            pagination = events_block.get("pagination") or {}
            continuation = pagination.get("continuation") or None
            if not continuation:
                break

        return results

    # ----------------------------------------------------------------- enrich

    def _enrich_all(self, raw: list[dict]) -> list[Event]:
        """Fetch full event details in parallel and build Event objects."""
        events: list[Event] = []

        def fetch_and_parse(item: dict) -> Event | None:
            event_id = item.get("eventbrite_event_id") or item.get("eid")
            if not event_id:
                return None
            try:
                resp = self.session.get(
                    f"{API_BASE}/events/{event_id}/",
                    params={"expand": "logo,venue,ticket_availability"},
                    timeout=30,
                )
                resp.raise_for_status()
                full = resp.json()
            except Exception:
                self.logger.warning("Could not fetch event %s – using search data", event_id)
                full = {}
            return self._parse(item, full)

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(fetch_and_parse, item): item for item in raw}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    events.append(result)

        return events

    # ------------------------------------------------------------------ parse

    def _parse(self, search_item: dict, full: dict) -> Event | None:
        # Title
        title = (
            (full.get("name") or {}).get("text", "").strip()
            or search_item.get("name", "").strip()
        )
        if not title:
            return None

        # Dates – prefer full event's start/end (already combined datetime)
        date_start = self._iso(
            (full.get("start") or {}).get("local")
            or f"{search_item.get('start_date', '')}T{search_item.get('start_time', '00:00')}"
        )
        if not date_start:
            return None

        date_end = self._iso(
            (full.get("end") or {}).get("local")
            or (
                f"{search_item['end_date']}T{search_item.get('end_time', '00:00')}"
                if search_item.get("end_date")
                else None
            )
        )

        url = full.get("url") or search_item.get("url", "")

        # Description: full text first, fall back to summary
        description = (
            (full.get("description") or {}).get("text", "")
            or search_item.get("summary", "")
            or (full.get("summary") or "")
        )

        # Image
        logo = full.get("logo") or {}
        image_url = (
            (logo.get("original") or {}).get("url", "")
            or logo.get("url", "")
        )

        # Venue
        venue = full.get("venue") or {}
        venue_name = venue.get("name", "")
        address = venue.get("address") or {}
        city = address.get("city", "")
        street = address.get("address_1", "")
        if venue_name and street:
            location = f"{venue_name}, {street}"
        else:
            location = venue_name or street

        # City fallback from search locations list
        if not city:
            for loc in (search_item.get("locations") or []):
                if loc.get("type") == "locality":
                    city = loc.get("name", "")
                    break
        if not city:
            city = "Bielefeld"

        # Category from EventbriteCategory tag (prefer German display_name)
        category = ""
        for tag in (search_item.get("tags") or []):
            if tag.get("prefix") == "EventbriteCategory":
                category = (
                    (tag.get("localized") or {}).get("display_name")
                    or tag.get("display_name", "")
                )
                break

        # Price
        if full.get("is_free"):
            price = "Kostenlos"
        else:
            ticket_avail = full.get("ticket_availability") or {}
            min_ticket = (ticket_avail.get("minimum_ticket_price") or {})
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

    # ---------------------------------------------------------------- helpers

    @staticmethod
    def _iso(date_str: str | None) -> datetime | None:
        if not date_str:
            return None
        # Slice to the exact character length each format expects.
        for fmt, length in (
            ("%Y-%m-%dT%H:%M:%S", 19),
            ("%Y-%m-%dT%H:%M", 16),
            ("%Y-%m-%d", 10),
        ):
            try:
                return datetime.strptime(date_str[:length], fmt)
            except ValueError:
                continue
        return None
