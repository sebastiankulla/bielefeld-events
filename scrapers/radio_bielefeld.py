"""Scraper for Radio Bielefeld Veranstaltungstipps.

Events are loaded dynamically via a POST request to the vtipps AJAX API
at vtipps.amstools.de. The list endpoint returns HTML cards; the detail
endpoint returns full event info including time.

Strategy
--------
1. POST to the AJAX API with action=getList to get all event cards.
2. For each card, extract the article ID from the detail URL and POST
   again with action=getDetails to obtain the precise start time.
3. Collect title, date+time, image, location, category, and URL.
"""

import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event, parse_german_date

_AJAX_URL = "https://vtipps.amstools.de/vtipps/vtippsajax/ajaxget.php"
_BASE_PAGE = "https://www.radiobielefeld.de/service/veranstaltungstipps.html"
_BACKLINK = "https://www.radiobielefeld.de/service/veranstaltungstipps"
_SENDER_ID = 1

# Matches "07.03.2026, 09:00-13:00 Uhr" or "07.03.2026, 19:00 Uhr"
_RE_WHEN = re.compile(
    r"(\d{1,2})\.(\d{1,2})\.(\d{4})"
    r"(?:[,\s]+(\d{1,2}):(\d{2}))?"
)

# Extract article ID from URLs like ".../122745.html"
_RE_ARTICLE_ID = re.compile(r"/(\d+)\.html$")


def _parse_when(text: str) -> datetime | None:
    """Parse a 'when' string like '07.03.2026, 09:00-13:00 Uhr'."""
    m = _RE_WHEN.search(text)
    if not m:
        return None
    day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    hour = int(m.group(4)) if m.group(4) else 0
    minute = int(m.group(5)) if m.group(5) else 0
    try:
        return datetime(year, month, day, hour, minute)
    except ValueError:
        return None


class RadioBielefeldScraper(BaseScraper):
    """Scrapes events from Radio Bielefeld Veranstaltungstipps."""

    name = "radio_bielefeld"
    base_url = "https://www.radiobielefeld.de"

    def _ajax_post(self, data: dict) -> str:
        """POST to the vtipps AJAX endpoint and return the HTML data string."""
        base_data = {
            "ajaxConfig[SENDERID]": str(_SENDER_ID),
            "ajaxConfig[senderId]": str(_SENDER_ID),
            "ajaxConfig[EVENTS_PER_PAGE]": "200",
            "ajaxConfig[URL]": (
                "https://vtipps.amstools.de/calendar/index.php?s=event_single_serv"
            ),
            "ajaxConfig[BACKLINK]": _BACKLINK,
            "search[wannvon]": "",
            "search[wannbis]": "",
            "search[was]": "",
            "search[category_id]": "",
            "search[plz]": "",
            "search[distance]": "",
        }
        base_data.update(data)

        resp = self.session.post(
            _AJAX_URL,
            data=base_data,
            headers={
                "Referer": _BASE_PAGE,
                "Origin": self.base_url,
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("data", "")

    def _fetch_list_html(self) -> str:
        return self._ajax_post({
            "action": "getList",
            "vars[fromurl]": _BASE_PAGE,
            "vars[backlink]": _BACKLINK,
            "vars[maxNews]": "200",
            "vars[isPlugin]": "1",
        })

    def _fetch_detail_html(self, article_id: str, detail_url: str) -> str:
        return self._ajax_post({
            "action": "getDetails",
            "vars[fromurl]": detail_url,
            "vars[backlink]": _BACKLINK,
            "vars[articleid]": article_id,
        })

    def _parse_detail(self, article_id: str, detail_url: str) -> dict:
        """Return {'date_start', 'image_url', 'description', 'price'} from detail page."""
        result: dict = {}
        try:
            html = self._fetch_detail_html(article_id, detail_url)
            soup = BeautifulSoup(html, "lxml")
            det = soup.find("div", class_="vtipp_det")
            if not det:
                return result

            when_el = det.find(class_="when")
            if when_el:
                when_text = when_el.get_text(strip=True)
                dt = _parse_when(when_text)
                if dt:
                    result["date_start"] = dt

            ticket_el = det.find(class_="ticket")
            if ticket_el:
                result["price"] = ticket_el.get_text(strip=True)

            # Image from detail page (may be higher-res)
            img = det.find("img", src=lambda s: s and "vtipps" in s)
            if img:
                result["image_url"] = img["src"]

            # Description text
            text_el = det.find(class_="vtipp_text")
            if text_el:
                # Remove child divs that hold metadata fields
                for tag in text_el.find_all(
                    class_=["when", "where", "category", "ticket",
                            "additionallink", "vtipp_title", "vtipp_image",
                            "vtipp_infos"]
                ):
                    tag.decompose()
                desc = text_el.get_text(separator=" ", strip=True)
                if desc:
                    result["description"] = desc

        except Exception:
            self.logger.debug(
                "Could not fetch detail for article %s", article_id
            )
        return result

    def scrape(self) -> list[Event]:
        events: list[Event] = []
        try:
            html = self._fetch_list_html()
            soup = BeautifulSoup(html, "lxml")
            cards = soup.find_all("div", class_="vtipp")
            self.logger.info("Found %d event cards", len(cards))

            for card in cards:
                try:
                    event = self._parse_card(card)
                    if event:
                        events.append(event)
                except Exception:
                    self.logger.debug("Failed to parse card", exc_info=True)

            self.logger.info("Scraped %d events from %s", len(events), self.name)
        except Exception:
            self.logger.exception("Failed to scrape %s", self.name)
        return events

    def _parse_card(self, card) -> Event | None:
        link_el = card.find("a", href=True)
        detail_url = link_el["href"] if link_el else ""
        if not detail_url:
            return None

        # Make absolute
        if detail_url.startswith("/"):
            detail_url = self.base_url + detail_url

        m = _RE_ARTICLE_ID.search(detail_url)
        article_id = m.group(1) if m else ""

        title_el = card.find("div", class_="vtipp_title")
        title = title_el.get_text(strip=True) if title_el else ""
        if not title:
            return None

        # Date from list card (date only, no time)
        date_el = card.find("div", class_="vtipp_date")
        date_text = date_el.get_text(strip=True) if date_el else ""
        date_start = _parse_when(date_text) or parse_german_date(date_text)

        location_el = card.find("div", class_="vtipp_location")
        location = location_el.get_text(strip=True) if location_el else ""

        category_el = card.find("div", class_="vtipp_category")
        category = category_el.get_text(strip=True) if category_el else ""

        # Image from list card
        img_el = card.find("img", src=lambda s: s and "vtipps" in s)
        image_url = img_el["src"] if img_el else ""

        # Enrich with detail page (time, price, description)
        detail: dict = {}
        if article_id:
            detail = self._parse_detail(article_id, detail_url)

        date_start = detail.get("date_start") or date_start
        if date_start is None:
            self.logger.debug("Skipping '%s': no date", title)
            return None

        return Event(
            title=title,
            date_start=date_start,
            source=self.name,
            url=detail_url,
            description=detail.get("description", ""),
            location=location,
            city="Bielefeld",
            category=category,
            image_url=detail.get("image_url") or image_url,
            price=detail.get("price", ""),
        )
