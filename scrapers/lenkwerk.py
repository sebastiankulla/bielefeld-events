"""Scraper for Lenkwerk Bielefeld event listings."""

import re
from datetime import datetime

from bs4 import BeautifulSoup, Tag

from scrapers.base import BaseScraper, Event, GERMAN_MONTHS

# English 3-letter month abbreviations used in the date badges
_ENGLISH_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# "März2026" or "April 2026" – month name immediately followed by 4-digit year
_RE_MONTH_YEAR = re.compile(
    r"(" + "|".join(GERMAN_MONTHS) + r")\s*(\d{4})",
    re.IGNORECASE,
)

# Date embedded in URL slug: event-name-29032026 → day=29 month=03 year=2026
_RE_SLUG_DATE = re.compile(r"-(\d{2})(\d{2})(\d{4})(?:-\d+)?/?$")

# Time like "18:30 Uhr" or "20:00"
_RE_TIME = re.compile(r"\b(\d{1,2}):(\d{2})\s*(?:Uhr\b)?")


def _month_from_badge(text: str) -> int | None:
    """Resolve an English or German 3-letter month abbreviation to a number."""
    t = text.strip().lower()
    return _ENGLISH_MONTHS.get(t) or GERMAN_MONTHS.get(t)


class LenkwerkScraper(BaseScraper):
    """Scrapes events from Lenkwerk Bielefeld (automotive event venue).

    The ``/event/`` archive page is divided into ``div.month`` sections,
    each containing an ``h2`` heading ("März2026") and one or more
    ``div.event.cnt*`` cards.  Each card holds:

    * ``div.img > a.img > img[src]`` – event poster image
    * ``p.date > span.day`` + ``p.date > span.month`` – date badge (e.g. "29 Mar")
    * ``h3 > a`` – event title and detail-page URL

    After collecting all events from the listing the scraper enriches each
    event with the start time from its detail page (one GET per event).
    """

    name = "lenkwerk"
    base_url = "https://www.lenkwerk-bielefeld.de"

    def scrape(self) -> list[Event]:
        events: list[Event] = []
        try:
            html = self._get_page(f"{self.base_url}/event/")
            soup = BeautifulSoup(html, "lxml")
            events = self._extract_from_listing(soup)

            # Enrich with start time from detail pages
            for ev in events:
                self._enrich_with_time(ev)

        except Exception:
            self.logger.exception("Failed to scrape %s", self.name)

        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    # ------------------------------------------------------------------
    # Listing page
    # ------------------------------------------------------------------

    def _extract_from_listing(self, soup: BeautifulSoup) -> list[Event]:
        events: list[Event] = []
        seen: set[tuple] = set()

        for month_div in soup.find_all("div", class_="month"):
            year = self._year_from_month_div(month_div)
            for card in month_div.find_all("div", class_=lambda c: c and "event" in c.split() and any("cnt" in p for p in c.split())):
                ev = self._parse_card(card, year)
                if ev:
                    key = (ev.title, ev.date_start.date())
                    if key not in seen:
                        seen.add(key)
                        events.append(ev)

        return events

    def _year_from_month_div(self, month_div: Tag) -> int:
        """Extract the 4-digit year from the h2 heading inside a div.month."""
        h2 = month_div.find("h2")
        if h2:
            text = h2.get_text(strip=True)
            m = _RE_MONTH_YEAR.search(text)
            if m:
                return int(m.group(2))
        return datetime.now().year

    def _parse_card(self, card: Tag, year: int) -> Event | None:
        # Title
        h3 = card.find("h3")
        if not h3:
            return None
        title_a = h3.find("a")
        title = (title_a or h3).get_text(strip=True)
        if not title or len(title) < 2:
            return None

        # URL
        url = ""
        if title_a and title_a.get("href"):
            url = self._absolute_url(title_a["href"])
        else:
            img_a = card.select_one("div.img > a.img")
            if img_a:
                url = self._absolute_url(img_a.get("href", ""))

        # Date from badge spans
        date_start = self._date_from_badge(card, year, url)
        if not date_start:
            return None

        # Image – direct src on the poster img (no lazy loading on this site)
        image_url = ""
        img_el = card.select_one("div.img img")
        if img_el:
            image_url = self._absolute_url(
                img_el.get("data-lazy-src", "")
                or img_el.get("data-src", "")
                or img_el.get("src", "")
            )

        # Category from site's own taxonomy
        category = self._guess_category(title, card)

        return Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=url,
            location="Lenkwerk Bielefeld",
            category=category,
            image_url=image_url,
        )

    def _date_from_badge(self, card: Tag, year: int, url: str) -> datetime | None:
        """Build a date from the day/month badge spans in the card."""
        day_el = card.select_one("span.day")
        month_el = card.select_one("span.month")
        if day_el and month_el:
            try:
                day = int(day_el.get_text(strip=True))
                month = _month_from_badge(month_el.get_text(strip=True))
                if month:
                    return datetime(year, month, day)
            except (ValueError, TypeError):
                pass

        # Fallback: date in URL slug (e.g. …-29032026/)
        m = _RE_SLUG_DATE.search(url)
        if m:
            try:
                return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            except ValueError:
                pass

        return None

    # ------------------------------------------------------------------
    # Detail page enrichment (start time + better image fallback)
    # ------------------------------------------------------------------

    def _enrich_with_time(self, ev: Event) -> None:
        """Fetch the detail page and set the start time on the event."""
        if not ev.url:
            return
        try:
            html = self._get_page(ev.url)
        except Exception:
            self.logger.debug("Could not fetch detail page: %s", ev.url)
            return

        soup = BeautifulSoup(html, "lxml")
        page_text = soup.get_text(" ", strip=True)

        # Find the time that appears close to (or just after) the event date
        # Strategy: look for the first HH:MM Uhr pattern in the page
        for m in _RE_TIME.finditer(page_text):
            hour, minute = int(m.group(1)), int(m.group(2))
            # Skip obviously wrong times (0:00 = no time set; single-digit
            # values that are part of version numbers, etc.)
            if hour > 23 or minute > 59:
                continue
            if hour == 0 and minute == 0:
                continue
            ev.date_start = ev.date_start.replace(hour=hour, minute=minute)
            break

        # If image is missing, try first non-theme upload image
        if not ev.image_url:
            article = soup.find("article") or soup.find("main") or soup
            for img in article.find_all("img"):
                src = (
                    img.get("data-lazy-src", "")
                    or img.get("data-src", "")
                    or img.get("src", "")
                )
                if "uploads" in src and "themes" not in src:
                    ev.image_url = self._absolute_url(src)
                    break

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _guess_category(title: str, card: Tag | None = None) -> str:
        t = title.lower()
        if "candlelight" in t or "konzert" in t:
            return "Konzert"
        if any(w in t for w in ("pinball", "expo", "convention", "messe")):
            return "Convention"
        if any(w in t for w in (
            "porsche", "mercedes", "bmw", "ferrari", "british", "italian",
            "automobil", "season opening", "season closing",
        )):
            return "Automobil"
        return ""
