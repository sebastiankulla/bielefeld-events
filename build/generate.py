#!/usr/bin/env python3
"""Static site generator: reads events from SQLite, writes JSON + HTML."""

import hashlib
import json
import logging
import re
import shutil
import sys
import unicodedata
import urllib.parse
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scrapers.database import get_all_events, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("generate")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SITE_DIR = PROJECT_ROOT / "site"
IMAGES_DIR = SITE_DIR / "images"
TEMPLATE_PATH = PROJECT_ROOT / "build" / "template.html"

_VALID_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"}

# Characters to strip when normalizing titles for dedup comparison
_RE_NON_ALNUM = re.compile(r"[^a-z0-9 ]+")
_RE_MULTI_SPACE = re.compile(r"\s+")


def _normalize_title(title: str) -> str:
    """Normalize a title for deduplication comparison.

    Lowercases, strips accents, removes non-alphanumeric chars, and collapses
    whitespace so that minor formatting differences don't prevent matching.
    """
    t = title.lower().strip()
    # Normalize unicode (e.g. ä -> a for comparison purposes)
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = _RE_NON_ALNUM.sub(" ", t)
    t = _RE_MULTI_SPACE.sub(" ", t).strip()
    return t


# Lower value = higher priority (preferred when merging duplicate events).
# Sources not listed here get priority 0 (highest).
_SOURCE_PRIORITY: dict[str, int] = {
    "nw_events": 10,       # second to last
    "bielefeld_jetzt": 20, # last
}


def _source_sort_key(ev: dict) -> int:
    return _SOURCE_PRIORITY.get(ev.get("source", ""), 0)


def deduplicate_events(events: list[dict]) -> list[dict]:
    """Merge events that appear on multiple sources into single entries.

    Groups events by normalised title + date (day only).  For each group the
    best available information is picked and all sources are collected in a
    ``sources`` list (each entry has ``source`` and ``url``).

    Within each group events are sorted by source priority so that preferred
    sources (venue pages, Kulturamt, …) are picked over aggregators like
    nw.de or bielefeld-jetzt.de.
    """
    groups: dict[tuple[str, str], list[dict]] = {}
    for ev in events:
        norm_title = _normalize_title(ev.get("title", ""))
        date_day = (ev.get("date_start") or "")[:10]  # YYYY-MM-DD
        key = (norm_title, date_day)
        groups.setdefault(key, []).append(ev)

    merged: list[dict] = []
    dedup_count = 0
    for _key, group in groups.items():
        # Sort by source priority so the most trusted source comes first
        group = sorted(group, key=_source_sort_key)

        if len(group) == 1:
            ev = group[0]
            ev["sources"] = [{"source": ev["source"], "url": ev.get("url", "")}]
            merged.append(ev)
            continue

        dedup_count += len(group) - 1

        # Pick the "best" value for each field across all duplicates
        primary = group[0]
        result = dict(primary)

        # Collect all sources
        result["sources"] = [
            {"source": e["source"], "url": e.get("url", "")}
            for e in group
        ]

        # Keep the primary source for gradient colours etc.
        result["source"] = group[0]["source"]

        # Prefer the longest / most complete description
        best_desc = max(group, key=lambda e: len(e.get("description") or ""))
        result["description"] = best_desc.get("description", "")

        # Prefer a non-empty image
        result["image_url"] = next(
            (e["image_url"] for e in group if e.get("image_url")), ""
        )

        # Prefer a non-empty location
        result["location"] = next(
            (e["location"] for e in group if e.get("location")), ""
        )

        # Prefer a non-empty category
        result["category"] = next(
            (e["category"] for e in group if e.get("category")), ""
        )

        # Prefer a non-empty price
        result["price"] = next(
            (e["price"] for e in group if e.get("price")), ""
        )

        merged.append(result)

    # Sort again by date after merging
    merged.sort(key=lambda e: e.get("date_start", ""))

    if dedup_count:
        logger.info(
            "Deduplicated %d duplicate entries -> %d unique events",
            dedup_count,
            len(merged),
        )

    return merged


def _create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    })
    return session


_MIN_IMAGE_BYTES = 5_000  # hotlink-blocker placeholder images are tiny


def _download_image(url: str, session: requests.Session, page_url: str = "") -> str:
    """Download an image to IMAGES_DIR and return its site-relative path.

    Uses ``page_url`` as the Referer header (simulating the browser loading
    the image while viewing the event page), which satisfies most
    same-domain hotlink-protection checks.  Falls back to the image URL's
    own origin when no page_url is given.  Returns an empty string if the
    download fails or the response looks like a hotlink-blocker placeholder.
    """
    if not url or url.startswith("data:"):
        return ""

    url_hash = hashlib.md5(url.encode()).hexdigest()
    parsed = urllib.parse.urlparse(url)
    ext = Path(parsed.path).suffix.lower()
    if ext not in _VALID_IMAGE_EXTENSIONS:
        ext = ".jpg"

    filename = f"{url_hash}{ext}"
    local_path = IMAGES_DIR / filename

    if local_path.exists():
        return f"images/{filename}"

    # Prefer the event's page URL as Referer; fall back to image origin.
    if page_url:
        referer = page_url
    else:
        referer = f"{parsed.scheme}://{parsed.netloc}/"

    try:
        response = session.get(url, timeout=15, headers={"Referer": referer})
        response.raise_for_status()
        content_type = response.headers.get("content-type", "")
        if "image" not in content_type and "octet-stream" not in content_type:
            logger.debug("Skipping non-image response for %s (%s)", url, content_type)
            return ""
        if len(response.content) < _MIN_IMAGE_BYTES:
            logger.debug(
                "Skipping suspiciously small image (%d bytes): %s",
                len(response.content), url,
            )
            return ""
        local_path.write_bytes(response.content)
        return f"images/{filename}"
    except Exception:
        logger.warning("Could not download image: %s", url)
        return ""


def build_json() -> list[dict]:
    """Export events to JSON file for Alpine.js to consume."""
    events = get_all_events()
    events = deduplicate_events(events)

    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    session = _create_session()
    downloaded = 0
    for ev in events:
        remote_url = ev.get("image_url", "")
        if remote_url and not remote_url.startswith("images/"):
            local = _download_image(remote_url, session, page_url=ev.get("url", ""))
            if local:
                ev["image_url"] = local
                downloaded += 1
            else:
                ev["image_url"] = ""
    if downloaded:
        logger.info("Downloaded %d event images to %s", downloaded, IMAGES_DIR)

    output_path = SITE_DIR / "events.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2)
    logger.info("Written %d events to %s", len(events), output_path)
    return events


def build_html() -> None:
    """Copy template as index.html (filters are now dynamic via Alpine.js)."""
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    output_path = SITE_DIR / "index.html"
    output_path.write_text(template, encoding="utf-8")
    logger.info("Written index.html to %s", output_path)


def main() -> int:
    init_db()
    SITE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    events = build_json()
    build_html()

    # Summary
    categories = set(e.get("category", "") for e in events if e.get("category"))
    sources = set(e.get("source", "") for e in events)
    logger.info(
        "Site generation complete: %d events, %d categories, %d sources",
        len(events), len(categories), len(sources),
    )
    logger.info("Output in %s", SITE_DIR)
    return 0


if __name__ == "__main__":
    sys.exit(main())
