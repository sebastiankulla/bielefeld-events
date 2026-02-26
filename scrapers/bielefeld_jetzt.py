"""Scraper for bielefeld-jetzt.de event listings."""

from datetime import datetime

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event


class BielefeldJetztScraper(BaseScraper):
    """Scrapes events from bielefeld-jetzt.de."""

    name = "bielefeld_jetzt"
    base_url = "https://bielefeld-jetzt.de"

    def scrape(self) -> list[Event]:
        events = []
        try:
            html = self._get_page(f"{self.base_url}/events")
            soup = BeautifulSoup(html, "lxml")

            for card in soup.select("article.event, .event-item, .event-card"):
                event = self._parse_card(card)
                if event:
                    events.append(event)

            self.logger.info("Scraped %d events from %s", len(events), self.name)
        except Exception:
            self.logger.exception("Failed to scrape %s", self.name)
        return events

    def _parse_card(self, card) -> Event | None:
        title_el = card.select_one("h2, h3, .event-title, .title")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)

        link_el = card.select_one("a[href]")
        url = link_el["href"] if link_el else ""
        if url and not url.startswith("http"):
            url = self.base_url + url

        date_el = card.select_one("time, .event-date, .date")
        date_start = self._parse_date(date_el)
        if not date_start:
            return None

        desc_el = card.select_one("p, .event-description, .description")
        description = desc_el.get_text(strip=True) if desc_el else ""

        loc_el = card.select_one(".event-location, .location, .venue")
        location = loc_el.get_text(strip=True) if loc_el else ""

        img_el = card.select_one("img[src]")
        image_url = img_el["src"] if img_el else ""
        if image_url and not image_url.startswith("http"):
            image_url = self.base_url + image_url

        cat_el = card.select_one(".event-category, .category, .tag")
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

    def _parse_date(self, el) -> datetime | None:
        if not el:
            return None
        # Try datetime attribute first (e.g. <time datetime="2026-03-15">)
        dt_attr = el.get("datetime", "")
        if dt_attr:
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
                try:
                    return datetime.strptime(dt_attr[:19], fmt)
                except ValueError:
                    continue

        # Fall back to text content
        text = el.get_text(strip=True)
        for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(text[:10], fmt)
            except ValueError:
                continue
        return None
