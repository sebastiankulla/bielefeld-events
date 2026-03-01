#!/usr/bin/env python3
"""Static site generator: reads events from SQLite, writes JSON + HTML."""

import json
import logging
import re
import shutil
import sys
import unicodedata
from pathlib import Path

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
TEMPLATE_PATH = PROJECT_ROOT / "build" / "template.html"

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


def deduplicate_events(events: list[dict]) -> list[dict]:
    """Merge events that appear on multiple sources into single entries.

    Groups events by normalised title + date (day only).  For each group the
    best available information is picked and all sources are collected in a
    ``sources`` list (each entry has ``source`` and ``url``).
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


def build_json() -> list[dict]:
    """Export events to JSON file for Alpine.js to consume."""
    events = get_all_events()
    events = deduplicate_events(events)
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
