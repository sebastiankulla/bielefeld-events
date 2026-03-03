"""Scraper for NW.de event portal (nw-event-prod.vercel.app API)."""

from datetime import datetime, timezone

from scrapers.base import BaseScraper, Event

API_BASE = "https://nw-event-prod.vercel.app"
PORTAL_BASE = "https://www.nw.de/events"

# Cities to scrape with their API search name and canonical city name
CITIES = [
    ("bielefeld",     "Bielefeld"),
    ("Gütersloh",     "Gütersloh"),
    ("Herford",       "Herford"),
    ("Detmold",       "Detmold"),
    ("Paderborn",     "Paderborn"),
    ("Bad Salzuflen", "Bad Salzuflen"),
    ("Bad Oeynhausen", "Bad Oeynhausen"),
]

PAGE_SIZE = 100


class NwEventsScraper(BaseScraper):
    """Scrapes events from the NW.de event portal API for OWL cities."""

    name = "nw_events"
    base_url = PORTAL_BASE

    def scrape(self) -> list[Event]:
        events = []
        try:
            for location_query, city_name in CITIES:
                city_events = self._scrape_city(location_query, city_name)
                events.extend(city_events)
                self.logger.info(
                    "Scraped %d events for %s", len(city_events), city_name
                )
        except Exception:
            self.logger.exception("Failed to scrape %s", self.name)
        return events

    def _scrape_city(self, location_query: str, city_name: str) -> list[Event]:
        """Fetch all pages for a single city and return Event objects."""
        events = []
        offset = 0
        today = datetime.now().strftime("%Y-%m-%d")

        while True:
            params = {
                "l": location_query,
                "sd": today,
                "n": PAGE_SIZE,
                "o": offset,
            }
            try:
                response = self.session.get(
                    f"{API_BASE}/api/search",
                    params=params,
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()
            except Exception:
                self.logger.exception(
                    "Failed to fetch page (offset=%d) for %s", offset, city_name
                )
                break

            items = data.get("data") or []
            for item in items:
                event = self._parse_event(item, city_name)
                if event:
                    events.append(event)

            total = data.get("total", 0)
            offset += len(items)
            if not items or offset >= total:
                break

        return events

    def _parse_event(self, item: dict, city_name: str) -> Event | None:
        """Convert a single API result dict into an Event."""
        title = (item.get("name") or "").strip()
        if not title:
            return None

        date_raw = item.get("date", "")
        date_start = self._parse_iso(date_raw)
        if not date_start:
            return None

        event_id = item.get("id", "")
        slug = item.get("slug", "")
        url = f"{PORTAL_BASE}/info/{event_id}-{slug}" if event_id and slug else PORTAL_BASE

        venue = item.get("venue") or {}
        venue_name = venue.get("name", "")
        venue_city = venue.get("city", city_name)
        location = venue_name
        if venue.get("street"):
            location = f"{venue_name}, {venue['street']}"

        description = item.get("description") or ""

        image_url = item.get("imagePath") or ""
        if not image_url:
            img = item.get("image") or {}
            path = img.get("path", "")
            # The path may be a srcset string like "url1 ..., url2 2x" — take the first URL
            image_url = path.split(",")[0].split()[0] if path else ""

        price_val = item.get("price")
        if price_val is not None and price_val > 0:
            price = f"{price_val:.2f} €".replace(".", ",")
        else:
            price = ""

        category = item.get("eventType") or ""

        return Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=url,
            description=description,
            location=location,
            city=venue_city,
            category=category,
            image_url=image_url,
            price=price,
        )

    @staticmethod
    def _parse_iso(date_str: str) -> datetime | None:
        """Parse an ISO 8601 datetime string (with optional timezone offset)."""
        if not date_str:
            return None
        # Normalise "+02:00" → strip timezone for naive datetime storage
        for fmt in (
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                dt = datetime.strptime(date_str[:25], fmt)
                # Return as naive local time (drop tzinfo)
                if dt.tzinfo is not None:
                    dt = dt.astimezone(tz=None).replace(tzinfo=None)
                return dt
            except ValueError:
                continue
        return None
