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
from difflib import SequenceMatcher
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
# Connectors that should be treated as equivalent ("und", "and", "&", "+")
# Uses word boundaries so we don't break words like "wunderbar" or "andi"
_RE_CONNECTORS = re.compile(r"\b(?:und|and)\b")
# Some scrapers append the city name to the title (e.g. "Vivid Indie Bielefeld")
_RE_CITY_SUFFIX = re.compile(r"\s+bielefeld$")

# Minimum similarity ratio (0..1) for fuzzy title matching.  Two events on the
# same day whose normalised titles have a SequenceMatcher ratio >= this value
# are considered duplicates even if they don't match exactly.
_FUZZY_THRESHOLD = 0.82


def _normalize_title(title: str) -> str:
    """Normalize a title for deduplication comparison.

    Lowercases, strips accents, removes non-alphanumeric chars, and collapses
    whitespace so that minor formatting differences don't prevent matching.
    """
    t = title.lower().strip()
    # Normalize unicode (e.g. ä -> a for comparison purposes)
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if not unicodedata.combining(c))
    # Replace "&" and "+" with "und" before stripping non-alnum chars,
    # so that "beats & butterkeks" and "beats und butterkeks" both match.
    t = t.replace("&", " und ").replace("+", " und ")
    # Remove connectors ("und", "and") so they don't affect matching
    t = _RE_CONNECTORS.sub(" ", t)
    t = _RE_NON_ALNUM.sub(" ", t)
    t = _RE_MULTI_SPACE.sub(" ", t).strip()
    # Strip trailing city name that some scrapers append to the title
    t = _RE_CITY_SUFFIX.sub("", t)
    return t


# Lower value = higher priority (preferred when merging duplicate events).
# Sources not listed here get priority 0 (highest).
_SOURCE_PRIORITY: dict[str, int] = {
    "nw_events": 10,       # second to last
    "bielefeld_jetzt": 20, # last
}


def _source_sort_key(ev: dict) -> int:
    return _SOURCE_PRIORITY.get(ev.get("source", ""), 0)


def _has_real_time(date_str: str) -> bool:
    """Return True when date_str contains a time component that is not 00:00."""
    # ISO format stored by SQLite: "2026-03-10T19:30:00" or "2026-03-10 19:30:00"
    if len(date_str) <= 10:
        return False
    time_part = date_str[10:]
    return not re.search(r"[T ]00:00", time_part)


def _merge_group(group: list[dict]) -> dict:
    """Merge a group of duplicate events into a single result dict."""
    group = sorted(group, key=_source_sort_key)
    primary = group[0]
    result = dict(primary)

    result["sources"] = [
        {"source": e["source"], "url": e.get("url", "")}
        for e in group
    ]
    result["source"] = primary["source"]

    # Prefer date_start that carries a real time over midnight (00:00).
    # Some scrapers (e.g. Lokschuppen) only know the date, not the time.
    best_date_ev = next(
        (e for e in group if _has_real_time(e.get("date_start") or "")),
        group[0],
    )
    result["date_start"] = best_date_ev["date_start"]

    # Prefer the longest / most complete description
    best_desc = max(group, key=lambda e: len(e.get("description") or ""))
    result["description"] = best_desc.get("description", "")

    # Prefer non-empty values for optional fields
    for field in ("image_url", "location", "category", "price"):
        result[field] = next(
            (e[field] for e in group if e.get(field)), ""
        )

    return result


def _is_title_match(title_i: str, title_j: str) -> bool:
    """Return True if two normalised titles should be treated as duplicates.

    Matches when any of the following hold:

    1. SequenceMatcher ratio >= ``_FUZZY_THRESHOLD`` (catches minor typos /
       spelling differences).
    2. The shorter title is a **word-boundary prefix** of the longer one and
       covers >= 50 % of its length.  Catches "Mord am Mittwoch" vs
       "Mord am Mittwoch – Krimidinnershow" (subtitle appended).
    3. The shorter title is a **verbatim substring** of the longer one, the
       shorter title is >= 12 chars, and it covers >= 35 % of the longer
       title's length.  Catches "Mord am Mittwoch" inside
       "Lucia Leona: Mord am Mittwoch - Die Crime Show" where a performer
       name is prepended before the actual event title.
    """
    ratio = SequenceMatcher(None, title_i, title_j).ratio()
    if ratio >= _FUZZY_THRESHOLD:
        return True

    if title_i and title_j:
        shorter, longer = (
            (title_i, title_j) if len(title_i) <= len(title_j)
            else (title_j, title_i)
        )
        length_ratio = len(shorter) / len(longer)

        # Rule 2: word-boundary prefix match
        if (
            len(shorter) >= 8
            and length_ratio >= 0.5
            and (longer.startswith(shorter + " ") or longer == shorter)
        ):
            logger.info("Prefix-matched: %r  <->  %r", shorter, longer)
            return True

        # Rule 3: substring match (e.g. performer name prepended)
        if (
            len(shorter) >= 12
            and length_ratio >= 0.35
            and shorter in longer
        ):
            logger.info("Substring-matched: %r  <->  %r", shorter, longer)
            return True

    return False


def deduplicate_events(events: list[dict]) -> list[dict]:
    """Merge events that appear on multiple sources into single entries.

    Two-pass approach:
    1. **Exact match** – group by normalised title + date (day).
    2. **Fuzzy match** – within the same day, merge groups whose normalised
       titles have a SequenceMatcher ratio >= ``_FUZZY_THRESHOLD`` *or*
       where the shorter title is a word-boundary prefix of the longer one.

    This catches minor spelling differences, extra words, subtitles, or
    abbreviations that survive normalisation.
    """

    # --- Pass 1: exact grouping by (normalised title, date) ----------------
    groups: dict[tuple[str, str], list[dict]] = {}
    for ev in events:
        norm_title = _normalize_title(ev.get("title", ""))
        date_day = (ev.get("date_start") or "")[:10]  # YYYY-MM-DD
        key = (norm_title, date_day)
        groups.setdefault(key, []).append(ev)

    # --- Pass 2: fuzzy-merge groups that share the same day ----------------
    # Organise groups by date so we only compare titles within the same day.
    by_date: dict[str, list[tuple[str, list[dict]]]] = {}
    for (norm_title, date_day), group in groups.items():
        by_date.setdefault(date_day, []).append((norm_title, group))

    merged: list[dict] = []
    dedup_count = 0

    for date_day, title_groups in by_date.items():
        # Sort groups shortest-title-first so that a bare core title (e.g.
        # "Mord am Mittwoch") is always processed before decorated variants
        # ("Mord am Mittwoch – Krimishow", "Lucia Leona: Mord am Mittwoch …").
        # This ensures the short core title matches all variants in one pass,
        # avoiding transitivity failures in the greedy algorithm.
        title_groups = sorted(title_groups, key=lambda x: len(x[0]))

        # Union-Find style merging: greedily merge similar titles
        merged_flags = [False] * len(title_groups)
        for i in range(len(title_groups)):
            if merged_flags[i]:
                continue
            combined = list(title_groups[i][1])  # start with this group
            title_i = title_groups[i][0]
            for j in range(i + 1, len(title_groups)):
                if merged_flags[j]:
                    continue
                title_j = title_groups[j][0]
                if _is_title_match(title_i, title_j):
                    combined.extend(title_groups[j][1])
                    merged_flags[j] = True
                    ratio = SequenceMatcher(None, title_i, title_j).ratio()
                    logger.info(
                        "Fuzzy-matched (%.0f%%): %r  <->  %r",
                        ratio * 100,
                        title_i,
                        title_j,
                    )

            if len(combined) == 1:
                ev = combined[0]
                ev["sources"] = [
                    {"source": ev["source"], "url": ev.get("url", "")}
                ]
                merged.append(ev)
            else:
                dedup_count += len(combined) - 1
                merged.append(_merge_group(combined))

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
