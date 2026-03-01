"""Scraper for NW.de (Neue Westfälische) event articles.

NW.de publishes editorial articles about events in the OWL (Ostwestfalen-Lippe)
region. The /events section curates event-related articles (concerts, comedy,
theater, parties). Actual event details (date, venue, price) are embedded in
the article body text in structured info boxes like:

    Samstag, 7.3., 21 Uhr, Hechelei, Bielefeld;
    Karten (ab 52,50 €): ...

The /events/city/<name> pages all serve the same regional content; the city
is determined from the article text instead.
"""

import re
from datetime import datetime, timedelta

from bs4 import BeautifulSoup

from scrapers.base import GERMAN_MONTHS, BaseScraper, Event, parse_german_date

# Cities in the OWL region covered by NW.de
OWL_CITIES = [
    "Bielefeld", "Gütersloh", "Herford", "Höxter", "Detmold",
    "Minden", "Paderborn", "Lemgo", "Bad Salzuflen", "Bünde",
    "Löhne", "Rheda-Wiedenbrück", "Bad Oeynhausen", "Porta Westfalica",
    "Lübbecke", "Espelkamp", "Halle", "Warburg", "Brakel",
    "Bad Driburg", "Blomberg", "Bad Lippspringe", "Delbrück",
    "Salzkotten", "Verl", "Harsewinkel", "Rietberg",
    "Enger", "Spenge", "Vlotho", "Kirchlengern",
    "Oerlinghausen", "Lage", "Leopoldshöhe",
]

# Info box pattern: "Samstag, 7.3., 21 Uhr, Hechelei, Bielefeld;"
# or: "Freitag, 6.3., 19.30 Uhr, Heristo Arena, Halle;"
# or: "Montag, 6.4., 20 Uhr, Rudolf-Oetker-Halle, Bielefeld."
RE_INFO_BOX = re.compile(
    r"(?:Montag|Dienstag|Mittwoch|Donnerstag|Freitag|Samstag|Sonntag)"
    r"[,\s]+(\d{1,2})\.(\d{1,2})\."
    r"[,\s]+(\d{1,2})(?:\.(\d{2}))?\s*Uhr"
    r"[,\s]+([^,;.]+)"      # venue
    r"[,\s]+([^,;.\n]+?)"   # city
    r"\s*[;.\n]",
    re.IGNORECASE,
)

# Price from info box: "Karten (ab 52,50 €)" or "Karten (18,02 €)"
RE_KARTEN_PRICE = re.compile(
    r"Karten\s*\(([^)]+?(\d+[,.]\d{2})\s*€)\)",
)

# Weekday + full date: "Freitag, 6. März 2026"
RE_WEEKDAY_FULL = re.compile(
    r"(?:Montag|Dienstag|Mittwoch|Donnerstag|Freitag|Samstag|Sonntag)"
    r"[,\s]+(\d{1,2})\.\s*(\w+)\s+(\d{4})"
    r"(?:[,\s]+(?:um\s+)?(\d{1,2})(?:[:\.](\d{2}))?\s*Uhr)?",
    re.IGNORECASE,
)

# Price fallback: "ab 52,50 Euro" / "18,02 Euro" / "ab 52,50 €"
RE_PRICE = re.compile(
    r"((?:ab|Ab)\s+)?(\d+[,.]\d{2})\s*(?:Euro|€)",
)


class NwEventsScraper(BaseScraper):
    """Scrapes event articles from NW.de (Neue Westfälische).

    The scraper collects article URLs from the events listing page and the
    first pages of the Kultur section, then visits each article to extract
    actual event details (date, venue, price) from the body text.
    """

    name = "nw_events"
    base_url = "https://www.nw.de"

    # Number of Kultur section pages to scrape (each has ~10 articles)
    KULTUR_PAGES = 3

    def scrape(self) -> list[Event]:
        events: list[Event] = []
        seen_urls: set[str] = set()
        article_urls: list[str] = []

        # Collect article URLs from the events listing page
        for path in ("/events", "/events/city/bielefeld"):
            try:
                urls = self._collect_article_urls(path)
                article_urls.extend(urls)
            except Exception:
                self.logger.exception("Failed to fetch listing %s", path)

        # Collect from paginated Kultur section for broader coverage
        for page in range(1, self.KULTUR_PAGES + 1):
            try:
                path = "/nachrichten/kultur/kultur"
                if page > 1:
                    path += f"?em_index_page={page}"
                urls = self._collect_article_urls(path)
                article_urls.extend(urls)
            except Exception:
                self.logger.exception("Failed to fetch Kultur page %d", page)

        # Deduplicate URLs
        unique_urls: list[str] = []
        for url in article_urls:
            if url not in seen_urls:
                seen_urls.add(url)
                unique_urls.append(url)

        self.logger.info("Found %d unique article URLs", len(unique_urls))

        # Visit each article and try to extract event information
        for url in unique_urls:
            try:
                event = self._parse_article(url)
                if event:
                    events.append(event)
            except Exception:
                self.logger.debug("Could not extract event from %s", url)

        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    # ------------------------------------------------------------------
    # Listing pages
    # ------------------------------------------------------------------

    def _collect_article_urls(self, path: str) -> list[str]:
        """Extract article URLs from a listing page."""
        html = self._get_page(f"{self.base_url}{path}")
        soup = BeautifulSoup(html, "lxml")
        urls: list[str] = []

        for link in soup.select("a[href]"):
            href = link.get("href", "")
            # NW article URLs: /nachrichten/.../12345678_Title.html
            if "/nachrichten/" in href and href.endswith(".html"):
                full = self._absolute_url(href)
                if full not in urls:
                    urls.append(full)

        return urls

    # ------------------------------------------------------------------
    # Article detail pages
    # ------------------------------------------------------------------

    def _parse_article(self, url: str) -> Event | None:
        """Visit an article and extract event information."""
        html = self._get_page(url)
        soup = BeautifulSoup(html, "lxml")

        # Title
        title_el = soup.select_one(
            "h1, .article-title, .headline, [itemprop='headline']"
        )
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 5:
            return None

        # Full page text for extraction
        body_el = soup.select_one(
            "article, .article-body, .article-content, .story-body, main"
        )
        body_text = (
            body_el.get_text(separator="\n") if body_el
            else soup.get_text(separator="\n")
        )

        # Strategy 1: Try structured info box (most reliable)
        # Format: "Samstag, 7.3., 21 Uhr, Hechelei, Bielefeld;"
        info = self._parse_info_box(body_text)
        if info:
            price = self._find_karten_price(body_text)
            return Event(
                title=title,
                date_start=info["date"],
                source=self.name,
                url=url,
                description=self._get_description(soup),
                location=info["venue"],
                city=info["city"],
                category=self._get_category(soup, body_text),
                image_url=self._get_image(soup),
                price=price,
            )

        # Strategy 2: Look for dates with German month names in text
        event_date = self._find_event_date(body_text)
        if not event_date:
            return None

        return Event(
            title=title,
            date_start=event_date,
            source=self.name,
            url=url,
            description=self._get_description(soup),
            location=self._find_venue(body_text),
            city=self._find_city(title, body_text),
            category=self._get_category(soup, body_text),
            image_url=self._get_image(soup),
            price=self._find_price(body_text),
        )

    # ------------------------------------------------------------------
    # Info box extraction (primary strategy)
    # ------------------------------------------------------------------

    def _parse_info_box(self, text: str) -> dict | None:
        """Parse the structured info box: 'Samstag, 7.3., 21 Uhr, Venue, City;'"""
        now = datetime.now()
        cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
            days=1
        )

        for m in RE_INFO_BOX.finditer(text):
            day = int(m.group(1))
            month = int(m.group(2))
            hour = int(m.group(3))
            minute = int(m.group(4)) if m.group(4) else 0
            venue = m.group(5).strip()
            city = m.group(6).strip()

            # Year is not in the short format; infer from current date
            year = now.year
            try:
                dt = datetime(year, month, day, hour, minute)
            except ValueError:
                continue

            # If date is >2 months in the past, assume next year
            if dt < now - timedelta(days=60):
                dt = dt.replace(year=year + 1)

            if dt < cutoff:
                continue

            return {"date": dt, "venue": venue, "city": city}

        return None

    @staticmethod
    def _find_karten_price(text: str) -> str:
        """Extract price from 'Karten (ab 52,50 €)' pattern."""
        m = RE_KARTEN_PRICE.search(text)
        if m:
            return m.group(1).replace("€", "Euro").strip()
        # Fallback to generic price pattern
        m = RE_PRICE.search(text)
        if m:
            prefix = m.group(1) or ""
            amount = m.group(2)
            return f"{prefix}{amount} Euro".strip()
        return ""

    # ------------------------------------------------------------------
    # Fallback extraction helpers
    # ------------------------------------------------------------------

    def _find_event_date(self, text: str) -> datetime | None:
        """Find the actual event date (not publication date) in article text."""
        now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff = now - timedelta(days=1)

        # 1) Weekday + full date ("Freitag, 6. März 2026, 19.30 Uhr")
        m = RE_WEEKDAY_FULL.search(text)
        if m:
            dt = self._parse_weekday_full_match(m)
            if dt and dt >= cutoff:
                return dt

        # 2) Keyword + date ("am 6. April 2026", "Beginn: 20.03.2026 19:30")
        keyword_patterns = [
            r"(?:am|Ab|Beginn|Start|Einlass)[:\s]+(\d{1,2}\.\s*\w+\s+\d{4}"
            r"(?:[,\s]+(?:um\s+)?\d{1,2}[:.]\d{2}\s*(?:Uhr)?)?)",
            r"(?:am|Ab|Beginn|Start|Einlass)[:\s]+(\d{1,2}\.\d{1,2}\.\d{4}"
            r"(?:\s+\d{1,2}[:.]\d{2})?)",
        ]
        for pattern in keyword_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                dt = parse_german_date(match.group(1))
                if dt and dt >= cutoff:
                    return dt

        # 3) Generic future date with German month name (last resort)
        for m in re.finditer(
            r"(\d{1,2})\.\s*(\w+)\s+(\d{4})"
            r"(?:[,\s]+(?:um\s+)?(\d{1,2})[:\.](\d{2})\s*(?:Uhr)?)?",
            text,
        ):
            dt = self._parse_german_date_match(m)
            if dt and dt >= cutoff:
                return dt

        return None

    @staticmethod
    def _parse_weekday_full_match(m: re.Match) -> datetime | None:
        day = int(m.group(1))
        month_str = m.group(2).lower()
        year = int(m.group(3))
        hour = int(m.group(4)) if m.group(4) else 0
        minute = int(m.group(5)) if m.group(5) else 0
        month = GERMAN_MONTHS.get(month_str)
        if month:
            try:
                return datetime(year, month, day, hour, minute)
            except ValueError:
                pass
        return None

    @staticmethod
    def _parse_german_date_match(m: re.Match) -> datetime | None:
        day = int(m.group(1))
        month_str = m.group(2).lower()
        year = int(m.group(3))
        hour = int(m.group(4)) if m.group(4) else 0
        minute = int(m.group(5)) if m.group(5) else 0
        month = GERMAN_MONTHS.get(month_str)
        if month:
            try:
                return datetime(year, month, day, hour, minute)
            except ValueError:
                pass
        return None

    @staticmethod
    def _find_city(title: str, body: str) -> str:
        combined = title + " " + body[:2000]
        for city in OWL_CITIES:
            if city in combined:
                return city
        return "Bielefeld"

    @staticmethod
    def _find_venue(text: str) -> str:
        # Regex for "in der/im <Venue>" patterns
        m = re.search(
            r"(?:in der|im|Ort:|Veranstaltungsort:)\s+"
            r"((?:[A-ZÄÖÜ][a-zäöüß]+[\s-]*){1,4}"
            r"(?:Halle|Forum|Theater|Museum|Stadion|Arena|Park|Kirche|Zentrum|Haus))",
            text,
        )
        if m:
            return m.group(1).strip()
        return ""

    @staticmethod
    def _find_price(text: str) -> str:
        m = RE_KARTEN_PRICE.search(text)
        if m:
            return m.group(1).replace("€", "Euro").strip()
        m = RE_PRICE.search(text)
        if m:
            prefix = m.group(1) or ""
            amount = m.group(2)
            return f"{prefix}{amount} Euro".strip()
        return ""

    def _get_description(self, soup: BeautifulSoup) -> str:
        meta = soup.select_one('meta[name="description"]')
        if meta and meta.get("content"):
            return meta["content"].strip()[:300]
        for p in soup.select("article p, .article-body p, main p"):
            text = p.get_text(strip=True)
            if text and len(text) > 20:
                return text[:300]
        return ""

    def _get_image(self, soup: BeautifulSoup) -> str:
        og = soup.select_one('meta[property="og:image"]')
        if og and og.get("content"):
            return og["content"]
        img = soup.select_one("article img[src], main img[src]")
        if img:
            return self._absolute_url(
                img.get("data-src", "") or img.get("src", "")
            )
        return ""

    @staticmethod
    def _get_category(soup: BeautifulSoup, text: str) -> str:
        meta = soup.select_one('meta[name="keywords"]')
        keywords = (meta.get("content", "") if meta else "").lower()
        combined = keywords + " " + text[:500].lower()
        categories = [
            ("konzert", "Konzert"), ("comedy", "Comedy"), ("kabarett", "Comedy"),
            ("theater", "Theater"), ("musical", "Musical"), ("oper", "Oper"),
            ("kino", "Kino"), ("film", "Kino"),
            ("party", "Party"), ("festival", "Festival"),
            ("lesung", "Lesung"), ("ausstellung", "Ausstellung"),
            ("kunst", "Kunst"),
        ]
        for keyword, cat in categories:
            if keyword in combined:
                return cat
        return "Kultur"
