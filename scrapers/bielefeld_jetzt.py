"""Scraper for bielefeld.jetzt event listings."""

from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event

# Max parallel detail-page fetches for events missing an image
_DETAIL_FETCH_WORKERS = 8


class BielefeldJetztScraper(BaseScraper):
    """Scrapes events from bielefeld.jetzt (main Bielefeld event portal)."""

    name = "bielefeld_jetzt"
    base_url = "https://www.bielefeld.jetzt"

    # Pages to scrape for broader event coverage
    PATHS = [
        "/termine/heute",
        "/termine/monat",
        "/termine/wochenende",
    ]

    def scrape(self) -> list[Event]:
        events = []
        seen_titles = set()

        for path in self.PATHS:
            try:
                html = self._get_page(f"{self.base_url}{path}")
                soup = BeautifulSoup(html, "lxml")
                page_events = self._extract_events(soup)
                for ev in page_events:
                    # Deduplicate across pages
                    key = (ev.title, ev.date_start.date())
                    if key not in seen_titles:
                        seen_titles.add(key)
                        events.append(ev)
            except Exception:
                self.logger.exception("Failed to scrape %s%s", self.base_url, path)

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

    def _extract_events(self, soup: BeautifulSoup) -> list[Event]:
        """Extract events from a page using multiple selector strategies."""
        events = []

        # Strategy 1: Try Drupal masonry layout (used by bielefeld.jetzt)
        containers = soup.select(".veranstaltung.masonry-view-item")

        # Strategy 2: Fall back to generic event containers
        if not containers:
            containers = soup.select(
                "article, .event-item, .event-card, .event, "
                ".termin, .termin-item, .termine-item, "
                "[class*='event'], [class*='termin'], "
                ".card, .list-item"
            )

        for card in containers:
            event = self._parse_card(card)
            if event:
                events.append(event)

        # Strategy 2: If no events found, look for links with dates
        if not events:
            events = self._extract_from_links(soup)

        return events

    def _parse_card(self, card) -> Event | None:
        # Find title – prefer headings (select_one returns first in
        # document order, so we search headings separately to avoid
        # matching a wrapping <a> or Drupal field div before the heading)
        title_el = card.select_one("h2, h3, h4")
        if not title_el:
            title_el = card.select_one(
                ".event-title, .title, .termin-title, [class*='title']"
            )
        if not title_el:
            title_el = card.select_one("a[href]")
        if not title_el:
            return None

        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        # Find link
        link_el = card.select_one("a[href]")
        url = self._absolute_url(link_el["href"]) if link_el else ""

        # Find date
        date_el = card.select_one(
            "time, .event-date, .date, .datum, .termin-datum, "
            "[class*='date'], [class*='datum'], [class*='time']"
        )
        date_start = self._parse_date_element(date_el)

        # If no date element, try to find date in the card text
        if not date_start:
            card_text = card.get_text()
            from scrapers.base import parse_german_date
            date_start = parse_german_date(card_text)

        if not date_start:
            return None

        # Description
        desc_el = card.select_one(
            "p, .description, .event-description, .text, .teaser, "
            "[class*='desc'], [class*='text']"
        )
        description = desc_el.get_text(strip=True) if desc_el else ""

        # Location - use the robust multi-strategy extraction
        location = self._extract_location_from_card(card)

        # Image – check card itself, then parent (masonry layouts put
        # images in a sibling wrapper, not inside the text container)
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
            ".category, .kategorie, .tag, .event-category, "
            "[class*='category'], [class*='kategorie']"
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

    def _extract_from_links(self, soup: BeautifulSoup) -> list[Event]:
        """Fallback: extract events from links that contain date information."""
        events = []
        from scrapers.base import parse_german_date

        for link in soup.select("a[href]"):
            text = link.get_text(strip=True)
            if not text or len(text) < 5:
                continue

            # Check parent element for date info
            parent = link.parent
            if parent:
                parent_text = parent.get_text()
                date = parse_german_date(parent_text)
                if date:
                    url = self._absolute_url(link.get("href", ""))
                    events.append(Event(
                        title=text,
                        date_start=date,
                        source=self.name,
                        url=url,
                    ))

        return events
