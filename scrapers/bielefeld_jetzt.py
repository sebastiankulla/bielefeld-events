"""Scraper for bielefeld.jetzt event listings."""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event

# How many days ahead to scrape
_DAYS_AHEAD = 30

# Max parallel detail-page fetches for events missing an image
_DETAIL_FETCH_WORKERS = 8

# Pattern for extracting the start time from a card time paragraph
# e.g. "09:30 Uhr, 14:00 Uhr" or "10:00 - 17:00 Uhr"
_RE_TIME = re.compile(r"(\d{1,2})[:\.](\d{2})")


class BielefeldJetztScraper(BaseScraper):
    """Scrapes events from bielefeld.jetzt (main Bielefeld event portal)."""

    name = "bielefeld_jetzt"
    base_url = "https://www.bielefeld.jetzt"

    def scrape(self) -> list[Event]:
        events = []
        seen = set()

        today = date.today()
        for offset in range(_DAYS_AHEAD):
            day = today + timedelta(days=offset)
            date_str = day.strftime("%Y-%m-%d")
            url = f"{self.base_url}/termine/datum/{date_str}"
            try:
                html = self._get_page(url)
                soup = BeautifulSoup(html, "lxml")
                day_events = self._extract_events(soup, day)
                for ev in day_events:
                    key = (ev.title, ev.date_start.date())
                    if key not in seen:
                        seen.add(key)
                        events.append(ev)
            except Exception:
                self.logger.exception("Failed to scrape %s", url)

        # Enrich events that have no image by fetching their detail page
        events = self._fill_missing_images(events)

        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    def _fill_missing_images(self, events: list[Event]) -> list[Event]:
        """Fetch detail pages in parallel for events that are missing an image."""
        missing = [(i, ev) for i, ev in enumerate(events) if not ev.image_url and ev.url]
        if not missing:
            return events

        self.logger.info(
            "Fetching detail pages for %d events without images", len(missing)
        )

        def fetch_image(index_event):
            idx, ev = index_event
            try:
                html = self._get_page(ev.url)
                soup = BeautifulSoup(html, "lxml")
                image_url = self._extract_detail_image(soup)
                return idx, image_url
            except Exception:
                self.logger.warning("Failed to fetch detail page for %s", ev.url)
                return idx, ""

        with ThreadPoolExecutor(max_workers=_DETAIL_FETCH_WORKERS) as executor:
            futures = {executor.submit(fetch_image, item): item for item in missing}
            for future in as_completed(futures):
                idx, image_url = future.result()
                if image_url:
                    events[idx] = Event(
                        title=events[idx].title,
                        date_start=events[idx].date_start,
                        date_end=events[idx].date_end,
                        source=events[idx].source,
                        url=events[idx].url,
                        description=events[idx].description,
                        location=events[idx].location,
                        city=events[idx].city,
                        category=events[idx].category,
                        image_url=image_url,
                        price=events[idx].price,
                        tags=events[idx].tags,
                    )

        return events

    def _extract_detail_image(self, soup: BeautifulSoup) -> str:
        """Extract the main event image from a detail page."""
        # Try the main content area first to avoid logo/nav images
        for container_sel in [
            ".node__content",
            "article",
            "main",
            ".field--name-field-bild",
            ".field--type-image",
        ]:
            container = soup.select_one(container_sel)
            if container:
                img = container.select_one("img[src], img[data-src]")
                if img:
                    url = self._absolute_url(
                        img.get("data-src", "") or img.get("src", "")
                    )
                    if url and not self._is_placeholder(url):
                        return url

        # Fallback: any content image on the page
        for img in soup.find_all("img"):
            src = img.get("data-src", "") or img.get("src", "")
            if src and not self._is_placeholder(src):
                url = self._absolute_url(src)
                if url:
                    return url

        return ""

    @staticmethod
    def _is_placeholder(url: str) -> bool:
        """Return True for UI icons, logos and other non-event images."""
        lowered = url.lower()
        return any(
            kw in lowered
            for kw in ("logo", "icon", "sprite", "data:image", "bielefeld-ui", "favicon")
        )

    def _extract_events(self, soup: BeautifulSoup, day: date) -> list[Event]:
        """Extract events from a per-day page."""
        events = []
        for card in soup.select(".veranstaltung.masonry-view-item"):
            event = self._parse_card(card, day)
            if event:
                events.append(event)
        return events

    def _parse_card(self, card, day: date) -> Event | None:
        # Title
        title_el = card.select_one("h2, h3, h4")
        if not title_el:
            title_el = card.select_one("a[href]")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        # Link
        link_el = card.select_one("a[href]")
        url = self._absolute_url(link_el["href"]) if link_el else ""

        # Date: use the day from the URL; parse start time from card text if present
        time_el = card.select_one(".bielefeld-ui-kalender")
        hour, minute = 0, 0
        if time_el and time_el.parent:
            time_text = time_el.parent.get_text(strip=True)
            m = _RE_TIME.search(time_text)
            if m:
                hour, minute = int(m.group(1)), int(m.group(2))
        date_start = datetime(day.year, day.month, day.day, hour, minute)

        # Description
        desc_el = card.select_one("p, .description, [class*='desc'], [class*='text']")
        description = desc_el.get_text(strip=True) if desc_el else ""

        # Location
        location = self._extract_location_from_card(card)

        # Image
        img_el = card.select_one("img[src], img[data-src]")
        if not img_el and card.parent:
            img_el = card.parent.select_one("img[src], img[data-src]")
        image_url = ""
        if img_el:
            image_url = self._absolute_url(
                img_el.get("data-src", "") or img_el.get("src", "")
            )

        # Category
        cat_el = card.select_one(
            ".category, .kategorie, [class*='category'], [class*='kategorie']"
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
