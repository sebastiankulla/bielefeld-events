#!/usr/bin/env python3
"""Static site generator: reads events from SQLite, writes JSON + HTML."""

import json
import logging
import shutil
import sys
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


def build_json() -> list[dict]:
    """Export events to JSON file for Alpine.js to consume."""
    events = get_all_events()
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
