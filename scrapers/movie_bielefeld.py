"""Scraper for Movie Bielefeld (Discothek & Live Club) event listings.

The Movie Bielefeld website (IONOS MyWebsite builder) publishes event flyers
as plain JPEG images — dates, times and event names are embedded inside the
image artwork itself and are NOT present in the HTML source.

Strategy
--------
1. Fetch the homepage and collect all full-size teaserbox image URLs.
2. Group images by their upload week (derived from the cache-busting ``t=``
   URL parameter).
3. For each group generate Event entries for the club's regular nights that
   fall within a configurable lookahead window:
       • Thursday  19:30 Uhr
       • Saturday  21:30 Uhr
       • Sunday    18:30 Uhr
4. The most recently uploaded image in each group is used as the event image.

Because actual dates/times are embedded in flyer artwork, this scraper
provides *approximate* dates based on the published opening-hours schedule.
"""

import re
from datetime import datetime, timedelta

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, Event

# Regular club nights with their canonical start times
# weekday: 0=Mon … 6=Sun
_CLUB_NIGHTS: list[tuple[int, int, int]] = [
    (3, 19, 30),   # Thursday  19:30
    (5, 21, 30),   # Saturday  21:30
    (6, 18, 30),   # Sunday    18:30
]

# How many days into the future we look for club nights
_LOOKAHEAD_DAYS = 90


def _upcoming_nights(
    from_date: datetime,
    days: int = _LOOKAHEAD_DAYS,
) -> list[datetime]:
    """Return all upcoming club-night datetimes within *days* of *from_date*."""
    results: list[datetime] = []
    end = from_date + timedelta(days=days)
    cursor = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
    while cursor <= end:
        for weekday, hour, minute in _CLUB_NIGHTS:
            if cursor.weekday() == weekday:
                dt = cursor.replace(hour=hour, minute=minute)
                if dt >= from_date:
                    results.append(dt)
        cursor += timedelta(days=1)
    return sorted(results)


def _extract_timestamp(url: str) -> int | None:
    """Parse the cache-busting ``t=<unix>`` parameter from a URL."""
    m = re.search(r"[?&]t=(\d+)", url)
    return int(m.group(1)) if m else None


class MovieBielefeldScraper(BaseScraper):
    """Scrapes event flyers from Movie Bielefeld (nightclub).

    Since event metadata is embedded in image artwork, this scraper derives
    approximate event dates from the club's published weekly schedule and
    attaches the most recently uploaded flyer image to each date.
    """

    name = "movie_bielefeld"
    base_url = "https://www.movie-bielefeld.de"

    def scrape(self) -> list[Event]:
        events: list[Event] = []
        try:
            html = self._get_page(self.base_url)
            soup = BeautifulSoup(html, "lxml")
            events = self._extract_events(soup)
            self.logger.info(
                "Scraped %d events from %s", len(events), self.name
            )
        except Exception:
            self.logger.exception("Failed to scrape %s", self.name)
        return events

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _collect_images(self, soup: BeautifulSoup) -> list[tuple[int, str]]:
        """Return list of (unix_timestamp, teaserbox_url) sorted newest-first.

        Each ``imageSubtitle`` block has:
        * A thumbnail:  ``cache_<id>.jpg?t=<ts>``
        * A lightbox link: ``teaserbox_<id>.jpg?t=<ts>``

        We prefer the full-size teaserbox URL so event cards show a
        high-quality image.  External hrefs (e.g. ticketing sites) are
        ignored and we fall back to the cached thumbnail instead.
        """
        seen_ids: set[str] = set()
        images: list[tuple[int, str]] = []

        for div in soup.select(".module-type-imageSubtitle"):
            link = div.select_one("a.imagewrapper[href]")
            img = div.select_one("img[src]")

            # Resolve image URL: prefer the lightbox teaserbox link, but
            # only if it points to the movie-bielefeld.de domain.
            url = ""
            if link:
                href = link["href"]
                if "movie-bielefeld.de" in href and "cc_images" in href:
                    url = href

            # Fall back to the thumbnail src (always on the same domain)
            if not url and img:
                src = img.get("src", "")
                if "cc_images" in src:
                    url = src

            if not url:
                continue

            if url.startswith("//"):
                url = "https:" + url

            # Upgrade thumbnail URL to teaserbox (higher quality)
            url = re.sub(r"/cache_(\d+)", r"/teaserbox_\1", url)

            # Deduplicate by numeric image ID
            id_match = re.search(r"(?:teaserbox|cache)_(\d+)", url)
            img_id = id_match.group(1) if id_match else url
            if img_id in seen_ids:
                continue
            seen_ids.add(img_id)

            ts = _extract_timestamp(url) or 0
            images.append((ts, url))

        images.sort(key=lambda x: x[0], reverse=True)
        return images

    def _extract_events(self, soup: BeautifulSoup) -> list[Event]:
        """Create one Event per upcoming club night, cycling through flyer images."""
        images = self._collect_images(soup)
        if not images:
            self.logger.warning("No images found on %s", self.base_url)
            return []

        now = datetime.now()
        upcoming = _upcoming_nights(now)

        if not upcoming:
            return []

        # Collect only distinct image URLs (newest first) for rotation
        image_urls = [url for _, url in images]

        events: list[Event] = []
        for idx, night in enumerate(upcoming):
            # Rotate through available flyer images
            image_url = image_urls[idx % len(image_urls)]

            events.append(Event(
                title="Movie Bielefeld",
                date_start=night,
                source=self.name,
                url=self.base_url,
                description=(
                    "Discothek und Live Club im Herzen Bielefelds. "
                    "Aktuelle Veranstaltungsinfos auf den Flyern."
                ),
                location="Movie Bielefeld, Am Bahnhof 6, 33602 Bielefeld",
                city="Bielefeld",
                category="Party / Club",
                image_url=image_url,
            ))

        return events
