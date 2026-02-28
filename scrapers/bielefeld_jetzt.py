"""Scraper for bielefeld.jetzt event listings."""

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event


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

        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    def _extract_events(self, soup: BeautifulSoup) -> list[Event]:
        """Extract events from a page using multiple selector strategies."""
        events = []

        # Strategy 1: Look for structured event containers
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
        # Find title - try multiple selectors
        title_el = card.select_one(
            "h2, h3, h4, .event-title, .title, .termin-title, "
            "[class*='title'], [class*='name']"
        )
        if not title_el:
            # Try the first meaningful link
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

        # Image
        img_el = card.select_one("img[src]")
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
