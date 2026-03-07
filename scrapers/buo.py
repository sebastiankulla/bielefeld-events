"""Scraper for Bühnen und Orchester der Stadt Bielefeld (BUO) event listings."""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event, parse_german_date

# Match dates like "So., 01.03.2026 19:30 Uhr" or "01.03.2026 19:30"
RE_BUO_DATE = re.compile(
    r"(?:Mo|Di|Mi|Do|Fr|Sa|So)\.\,?\s*"
    r"(\d{1,2})\.(\d{1,2})\.(\d{4})\s+"
    r"(\d{1,2}):(\d{2})\s*(?:Uhr)?",
    re.IGNORECASE,
)


class BuoScraper(BaseScraper):
    """Scrapes events from BUO Bielefeld (theater & orchestra).

    The calendar pages use a Tailwind-based grid layout.  Each event sits
    inside a ``div`` whose class list contains the ``thea`` or ``phil``
    theme token together with ``grid`` and ``grid-cols-12``.  The title is
    in ``h2 > a[href*='veranstaltung']``, and the date is found in the
    left-hand column text.
    """

    name = "buo"
    base_url = "https://www.buo-bielefeld.de"

    PATHS = [
        "/theater/kalender",
        "/philharmoniker/kalender",
    ]

    def scrape(self) -> list[Event]:
        events = []
        seen = set()

        for path in self.PATHS:
            try:
                html = self._get_page(f"{self.base_url}{path}")
                soup = BeautifulSoup(html, "lxml")
                page_events = self._extract_events(soup)
                for ev in page_events:
                    key = (ev.title, ev.date_start.date())
                    if key not in seen:
                        seen.add(key)
                        events.append(ev)
            except Exception:
                self.logger.exception(
                    "Failed to scrape %s%s", self.base_url, path,
                )

        events = self._fill_missing_images(events)

        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    def _fill_missing_images(self, events: list[Event]) -> list[Event]:
        """Fetch detail pages in parallel for events that are missing an image."""
        missing = [(i, ev) for i, ev in enumerate(events) if not ev.image_url and ev.url]
        if not missing:
            return events

        self.logger.info(
            "Fetching detail pages for %d BUO events without images", len(missing)
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

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(fetch_image, item): item for item in missing}
            for future in as_completed(futures):
                idx, image_url = future.result()
                if image_url:
                    ev = events[idx]
                    events[idx] = Event(
                        title=ev.title,
                        date_start=ev.date_start,
                        date_end=ev.date_end,
                        source=ev.source,
                        url=ev.url,
                        description=ev.description,
                        location=ev.location,
                        city=ev.city,
                        category=ev.category,
                        image_url=image_url,
                        price=ev.price,
                        tags=ev.tags,
                    )

        return events

    def _extract_detail_image(self, soup: BeautifulSoup) -> str:
        """Extract the main event image from a BUO detail page."""
        for container_sel in ["main", "article", ".content", "#content"]:
            container = soup.select_one(container_sel)
            if container:
                img = container.select_one("img[src], img[data-src]")
                if img:
                    url = self._absolute_url(
                        img.get("data-src", "") or img.get("src", "")
                    )
                    if url and not self._is_placeholder(url):
                        return url

        for img in soup.find_all("img"):
            src = img.get("data-src", "") or img.get("src", "")
            if src and not self._is_placeholder(src):
                url = self._absolute_url(src)
                if url:
                    return url

        return ""

    @staticmethod
    def _is_placeholder(url: str) -> bool:
        """Return True for logos, icons and other non-event images."""
        lowered = url.lower()
        return any(
            kw in lowered
            for kw in ("logo", "icon", "sprite", "data:image", "favicon")
        )

    def _extract_events(self, soup: BeautifulSoup) -> list[Event]:
        events = []

        # Find all grid containers that hold an event link.
        for grid_div in soup.select("div.grid"):
            link = grid_div.select_one(
                'a[href*="/theater/veranstaltung/"], '
                'a[href*="/philharmoniker/veranstaltung/"]'
            )
            if not link:
                continue

            # Only process leaf-level event grids (not parent wrappers).
            inner_grids = grid_div.select("div.grid")
            event_links_inside = sum(
                1 for g in inner_grids
                if g.select_one('a[href*="/veranstaltung/"]')
            )
            if event_links_inside > 1:
                continue

            event = self._parse_grid_event(grid_div)
            if event:
                events.append(event)

        return events

    def _parse_grid_event(self, container) -> Event | None:
        # Title
        title_link = container.select_one(
            'h2 a[href*="/veranstaltung/"]'
        )
        if not title_link:
            return None
        title = title_link.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        url = self._absolute_url(title_link["href"])

        # Subtitle / author
        subtitle_el = container.select_one("h3")
        description = subtitle_el.get_text(strip=True) if subtitle_el else ""

        # Date – search for "So., 01.03.2026 19:30 Uhr" in the text
        text = container.get_text(" ", strip=True)
        date_start = None
        m = RE_BUO_DATE.search(text)
        if m:
            day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
            hour, minute = int(m.group(4)), int(m.group(5))
            try:
                from datetime import datetime
                date_start = datetime(year, month, day, hour, minute)
            except ValueError:
                pass

        if not date_start:
            date_start = parse_german_date(text)
        if not date_start:
            return None

        # Location from text
        location = self._extract_location_from_card(container)
        if not location:
            location = "Theater Bielefeld"

        # Tags / category
        category = ""
        tags = container.select("ul li")
        tag_texts = [
            t.get_text(strip=True) for t in tags
            if t.get_text(strip=True) and len(t.get_text(strip=True)) < 30
        ]
        if tag_texts:
            category = " / ".join(tag_texts)

        # Image - try to find one in the card container
        image_url = ""
        img_el = container.select_one("img[src], img[data-src]")
        if img_el:
            image_url = self._absolute_url(
                img_el.get("data-src", "") or img_el.get("src", "")
            )
            if self._is_placeholder(image_url):
                image_url = ""

        return Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=url,
            description=description,
            location=location,
            category=category or "Theater & Musik",
            image_url=image_url,
        )
