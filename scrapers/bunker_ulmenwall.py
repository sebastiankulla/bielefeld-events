"""Scraper for Bunker Ulmenwall Bielefeld event listings."""

import json
import re

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event, parse_german_date

# Pattern for German dates like "Fr 27. Februar 2026" or "Sa 28. März 2026"
RE_BUNKER_DATE = re.compile(
    r"(?:Mo|Di|Mi|Do|Fr|Sa|So)\s+(\d{1,2})\.\s*(\w+)\s+(\d{4})",
    re.IGNORECASE,
)


class BunkerUlmenwallScraper(BaseScraper):
    """Scrapes events from Bunker Ulmenwall (sociocultural venue).

    The main page uses Kadence blocks that are hard to parse.  The WordPress
    category archive ``/category/kalender/`` provides a standard
    ``<article>`` structure that is much more reliable.
    """

    name = "bunker_ulmenwall"
    base_url = "https://bunker-ulmenwall.org"

    CATEGORY_PATHS = [
        "/category/kalender/",
    ]

    def scrape(self) -> list[Event]:
        events = []
        seen = set()

        for path in self.CATEGORY_PATHS:
            try:
                html = self._get_page(f"{self.base_url}{path}")
                soup = BeautifulSoup(html, "lxml")
                page_events = self._extract_articles(soup)
                for ev in page_events:
                    key = (ev.title, ev.date_start.date())
                    if key not in seen:
                        seen.add(key)
                        events.append(ev)
            except Exception:
                self.logger.exception(
                    "Failed to scrape %s%s", self.base_url, path,
                )

        if not events:
            try:
                html = self._get_page(self.base_url)
                soup = BeautifulSoup(html, "lxml")
                events = self._extract_from_jsonld(soup)
            except Exception:
                self.logger.exception("Failed to scrape %s", self.name)

        self.logger.info("Scraped %d events from %s", len(events), self.name)
        return events

    def _extract_articles(self, soup: BeautifulSoup) -> list[Event]:
        """Extract events from standard WordPress article elements."""
        events = []
        for article in soup.select("article"):
            event = self._parse_article(article)
            if event:
                events.append(event)
        return events

    def _parse_article(self, article) -> Event | None:
        # Title
        title_el = article.select_one(
            "h2.entry-title, h2, h3, .entry-title"
        )
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        # URL
        link_el = title_el.select_one("a[href]")
        if not link_el:
            link_el = article.select_one("a[href]")
        url = self._absolute_url(link_el["href"]) if link_el else ""

        # Date – search in the article text for German date pattern
        date_start = None
        article_text = article.get_text()
        m = RE_BUNKER_DATE.search(article_text)
        if m:
            date_start = parse_german_date(
                f"{m.group(1)}. {m.group(2)} {m.group(3)}"
            )
        if not date_start:
            date_el = article.select_one(
                "time, .date, .datum, [class*='date']"
            )
            date_start = self._parse_date_element(date_el)
        if not date_start:
            date_start = parse_german_date(article_text)
        if not date_start:
            return None

        # Description
        desc_el = article.select_one(
            ".entry-summary, .entry-content, "
            ".excerpt, p, [class*='excerpt']"
        )
        description = desc_el.get_text(strip=True)[:500] if desc_el else ""

        # Category from CSS classes like "category-electronic-jazz"
        category = ""
        classes = article.get("class", [])
        cat_classes = [
            c.replace("category-", "").replace("-", " ").title()
            for c in classes if c.startswith("category-")
            and c != "category-kalender"
        ]
        if cat_classes:
            category = " / ".join(cat_classes)

        # Image
        img_el = article.select_one("img[src]")
        image_url = ""
        if img_el:
            image_url = self._absolute_url(
                img_el.get("data-src", "") or img_el.get("src", "")
            )

        return Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=url,
            description=description,
            location="Bunker Ulmenwall",
            category=category or "Kultur",
            image_url=image_url,
        )

    def _extract_from_jsonld(self, soup: BeautifulSoup) -> list[Event]:
        """Extract events from JSON-LD structured data."""
        events = []
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(script.string)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") in ("Event", "MusicEvent", "TheaterEvent"):
                        date = parse_german_date(item.get("startDate", ""))
                        if date:
                            image = item.get("image", "")
                            if isinstance(image, list):
                                image = image[0] if image else ""
                            elif isinstance(image, dict):
                                image = image.get("url", "")
                            events.append(Event(
                                title=item.get("name", ""),
                                date_start=date,
                                source=self.name,
                                url=item.get("url", ""),
                                description=item.get("description", ""),
                                location="Bunker Ulmenwall",
                                category="Kultur",
                                image_url=image,
                            ))
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue
        return events
