"""Scraper for OWL Journal event listings."""

from datetime import datetime

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event


class OwlJournalScraper(BaseScraper):
    """Scrapes events from OWL Journal (regional news portal)."""

    name = "owl_journal"
    base_url = "https://www.owl-journal.de"

    def scrape(self) -> list[Event]:
        events = []
        try:
            html = self._get_page(f"{self.base_url}/veranstaltungen/bielefeld/")
            soup = BeautifulSoup(html, "lxml")

            for card in soup.select("article, .event-item, .veranstaltung"):
                event = self._parse_card(card)
                if event:
                    events.append(event)

            self.logger.info("Scraped %d events from %s", len(events), self.name)
        except Exception:
            self.logger.exception("Failed to scrape %s", self.name)
        return events

    def _parse_card(self, card) -> Event | None:
        title_el = card.select_one("h2, h3, .entry-title, .title")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title:
            return None

        link_el = card.select_one("a[href]")
        url = link_el["href"] if link_el else ""
        if url and not url.startswith("http"):
            url = self.base_url + url

        date_el = card.select_one("time, .event-date, .date, .datum")
        date_start = self._parse_date(date_el)
        if not date_start:
            return None

        desc_el = card.select_one("p, .entry-summary, .excerpt")
        description = desc_el.get_text(strip=True) if desc_el else ""

        loc_el = card.select_one(".event-location, .location, .ort")
        location = loc_el.get_text(strip=True) if loc_el else ""

        img_el = card.select_one("img[src]")
        image_url = img_el["src"] if img_el else ""
        if image_url and not image_url.startswith("http"):
            image_url = self.base_url + image_url

        return Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=url,
            description=description,
            location=location,
            image_url=image_url,
        )

    def _parse_date(self, el) -> datetime | None:
        if not el:
            return None
        dt_attr = el.get("datetime", "")
        if dt_attr:
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
                try:
                    return datetime.strptime(dt_attr[:19], fmt)
                except ValueError:
                    continue

        text = el.get_text(strip=True)
        for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(text[:10], fmt)
            except ValueError:
                continue
        return None
