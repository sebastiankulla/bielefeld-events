"""Scraper for Lenkwerk Bielefeld event listings."""

import re
from datetime import datetime

from bs4 import BeautifulSoup, Tag

from scrapers.base import BaseScraper, Event, parse_german_date, GERMAN_MONTHS

# English month abbreviations as used in the compact date display ("29Mar")
_ENGLISH_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# "29Mar" or "04Apr" compact date format
_RE_COMPACT_DATE = re.compile(
    r"(\d{1,2})\s*([A-Za-z]{3,})\s*(\d{4})?", re.IGNORECASE
)

# Time like "13:00" or "20:00 Uhr"
_RE_TIME = re.compile(r"(\d{1,2}):(\d{2})(?:\s*Uhr)?", re.IGNORECASE)

# Date range in URL slug: event-name-29032026  →  day=29 month=03 year=2026
_RE_SLUG_DATE = re.compile(r"(\d{2})(\d{2})(\d{4})(?:-\d+)?/?$")


def _parse_compact_date(text: str, fallback_year: int | None = None) -> datetime | None:
    """Parse compact date formats like '29Mar' or '4 April 2026'."""
    m = _RE_COMPACT_DATE.search(text.strip())
    if not m:
        return None
    day = int(m.group(1))
    month_str = m.group(2).lower()
    year_str = m.group(3)

    month = _ENGLISH_MONTHS.get(month_str[:3]) or GERMAN_MONTHS.get(month_str)
    if not month:
        return None

    year = int(year_str) if year_str else (fallback_year or datetime.now().year)
    try:
        return datetime(year, month, day)
    except ValueError:
        return None


def _date_from_url_slug(url: str) -> datetime | None:
    """Try to extract a date embedded in the URL slug (e.g. …-29032026/)."""
    m = _RE_SLUG_DATE.search(url)
    if not m:
        return None
    day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return datetime(year, month, day)
    except ValueError:
        return None


class LenkwerkScraper(BaseScraper):
    """Scrapes events from Lenkwerk Bielefeld (automotive event venue).

    The ``/event/`` archive page organises events into month sections
    (``<h2>`` headings like "März 2026") with one card per event.  Each
    card is an ``<a>`` element that links to a detail page and contains:

    * an ``<img>`` with the event poster,
    * a compact date label (e.g. "29Mar"),
    * an ``<h3>`` with the event title.

    When the listing page yields no results the scraper falls back to
    fetching individual detail pages collected from the listing.
    """

    name = "lenkwerk"
    base_url = "https://www.lenkwerk-bielefeld.de"

    def scrape(self) -> list[Event]:
        events: list[Event] = []
        try:
            html = self._get_page(f"{self.base_url}/event/")
            soup = BeautifulSoup(html, "lxml")

            events = self._extract_from_listing(soup)

            if not events:
                # Fallback: visit each detail page individually
                event_urls = self._collect_event_urls(soup)
                seen: set[str] = set()
                for url in event_urls:
                    if url in seen:
                        continue
                    seen.add(url)
                    ev = self._scrape_detail_page(url)
                    if ev:
                        events.append(ev)

        except Exception:
            self.logger.exception("Failed to scrape %s", self.name)

        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    # ------------------------------------------------------------------
    # Listing page parsing
    # ------------------------------------------------------------------

    def _extract_from_listing(self, soup: BeautifulSoup) -> list[Event]:
        """Parse event cards from the /event/ archive page."""
        events: list[Event] = []
        seen: set[tuple] = set()

        # Month sections provide the year context for compact dates
        current_year: int | None = None

        # Walk all elements in document order so that month headings
        # appear before the cards they belong to.
        for el in soup.find_all(True):
            if not isinstance(el, Tag):
                continue

            # Detect month/year heading  e.g. "März 2026" or "April 2026"
            if el.name in ("h2", "h3", "h4"):
                text = el.get_text(strip=True)
                year_m = re.search(r"\b(20\d{2})\b", text)
                if year_m:
                    current_year = int(year_m.group(1))

            # Event card: <a href="/event/…"> containing an <h3>
            if el.name == "a":
                href = el.get("href", "")
                if not re.search(r"/event/[^/]+/", href):
                    continue
                # Skip pure anchor/category filter links
                if not el.find("h3") and not el.find("h2"):
                    continue

                ev = self._parse_listing_card(el, href, current_year)
                if ev:
                    key = (ev.title, ev.date_start.date())
                    if key not in seen:
                        seen.add(key)
                        events.append(ev)

        return events

    def _parse_listing_card(
        self, card: Tag, href: str, fallback_year: int | None
    ) -> Event | None:
        # Title
        title_el = card.find("h3") or card.find("h2")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        # URL
        url = self._absolute_url(href)

        # Date – try multiple sources in priority order
        date_start = self._extract_date_from_card(card, url, fallback_year)
        if not date_start:
            return None

        # Image
        image_url = self._extract_image(card)

        # Category from the card text or surrounding content
        category = self._guess_category(title)

        return Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=url,
            location="Lenkwerk Bielefeld",
            category=category,
            image_url=image_url,
        )

    def _extract_date_from_card(
        self, card: Tag, url: str, fallback_year: int | None
    ) -> datetime | None:
        # 1. <time datetime="…"> element
        time_el = card.find("time")
        if time_el and isinstance(time_el, Tag):
            dt = self._parse_date_element(time_el)
            if dt:
                return dt

        # 2. itemprop="startDate"
        schema_el = card.find(attrs={"itemprop": "startDate"})
        if schema_el and isinstance(schema_el, Tag):
            dt = parse_german_date(
                schema_el.get("content", "") or schema_el.get_text(strip=True)
            )
            if dt:
                return dt

        # 3. Compact date label text like "29Mar" or "29. März 2026"
        card_text = card.get_text(" ", strip=True)
        dt = _parse_compact_date(card_text, fallback_year)
        if dt:
            return dt

        # 4. Date embedded in URL slug
        return _date_from_url_slug(url)

    def _extract_image(self, card: Tag) -> str:
        img = card.find("img")
        if not img or not isinstance(img, Tag):
            return ""
        src = (
            img.get("data-lazy-src", "")
            or img.get("data-src", "")
            or img.get("src", "")
        )
        return self._absolute_url(src) if src else ""

    # ------------------------------------------------------------------
    # Detail page fallback
    # ------------------------------------------------------------------

    def _collect_event_urls(self, soup: BeautifulSoup) -> list[str]:
        """Return all unique event detail URLs from the listing page."""
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.search(r"/event/[^/]+/", href):
                url = self._absolute_url(href)
                if url not in seen:
                    seen.add(url)
                    urls.append(url)
        return urls

    def _scrape_detail_page(self, url: str) -> Event | None:
        """Fetch an event detail page and extract the event data."""
        try:
            html = self._get_page(url)
        except Exception:
            self.logger.debug("Could not fetch detail page: %s", url)
            return None

        soup = BeautifulSoup(html, "lxml")

        # Title
        title_el = soup.find("h1")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        # Date – try JSON-LD first, then page text
        date_start = self._date_from_detail_jsonld(soup)
        if not date_start:
            date_start = _date_from_url_slug(url)
        if not date_start:
            date_start = parse_german_date(soup.get_text(" ", strip=True))
        if not date_start:
            return None

        # Time – look for HH:MM in the page text
        page_text = soup.get_text(" ", strip=True)
        time_m = _RE_TIME.search(page_text)
        if time_m:
            date_start = date_start.replace(
                hour=int(time_m.group(1)), minute=int(time_m.group(2))
            )

        # Image
        image_url = ""
        og_img = soup.find("meta", property="og:image")
        if og_img and isinstance(og_img, Tag):
            image_url = og_img.get("content", "")
        if not image_url:
            img = soup.find("article", class_=re.compile(r"post|event")) or soup
            img_el = img.find("img") if isinstance(img, Tag) else None  # type: ignore[arg-type]
            if img_el and isinstance(img_el, Tag):
                image_url = (
                    img_el.get("data-lazy-src", "")
                    or img_el.get("data-src", "")
                    or img_el.get("src", "")
                )
            image_url = self._absolute_url(image_url)

        # Description – first paragraph under article
        description = ""
        article = soup.find("article") or soup.find("main") or soup
        for p in article.find_all("p"):  # type: ignore[union-attr]
            text = p.get_text(strip=True)
            if text and len(text) > 30:
                description = text[:500]
                break

        # Price – look for "€" in page text
        price = ""
        price_m = re.search(r"(?:ab\s*)?([\d,]+)\s*€", page_text)
        if price_m:
            price = f"{price_m.group(1)} €"

        return Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=url,
            description=description,
            location="Lenkwerk Bielefeld",
            category=self._guess_category(title),
            image_url=image_url,
            price=price,
        )

    def _date_from_detail_jsonld(self, soup: BeautifulSoup) -> datetime | None:
        """Try to extract startDate from JSON-LD on a detail page."""
        import json
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") in ("Event", "MusicEvent", "SportsEvent",
                                             "ExhibitionEvent", "BusinessEvent"):
                        dt = parse_german_date(item.get("startDate", ""))
                        if dt:
                            return dt
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _guess_category(title: str) -> str:
        title_lower = title.lower()
        if "candlelight" in title_lower or "konzert" in title_lower:
            return "Konzert"
        if any(w in title_lower for w in ("pinball", "expo", "convention", "messe")):
            return "Convention"
        if any(w in title_lower for w in ("porsche", "mercedes", "bmw", "ferrari",
                                           "british", "italian", "automobil",
                                           "season opening", "season closing")):
            return "Automobil"
        return ""
