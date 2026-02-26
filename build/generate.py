#!/usr/bin/env python3
"""Static site generator: reads events from SQLite, writes JSON + HTML."""

import json
import logging
import shutil
import sys
from pathlib import Path

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scrapers.database import get_all_events, get_categories, get_locations, init_db

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


def build_html(events: list[dict]) -> None:
    """Generate index.html from template."""
    categories = get_categories()
    locations = get_locations()

    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    # Inject filter options into template
    category_options = "\n".join(
        f'                            <option value="{c}">{c}</option>'
        for c in categories
    )
    location_options = "\n".join(
        f'                            <option value="{loc}">{loc}</option>'
        for loc in locations
    )

    html = template.replace("<!-- CATEGORY_OPTIONS -->", category_options)
    html = html.replace("<!-- LOCATION_OPTIONS -->", location_options)

    output_path = SITE_DIR / "index.html"
    output_path.write_text(html, encoding="utf-8")
    logger.info("Written index.html to %s", output_path)


def main() -> int:
    init_db()
    SITE_DIR.mkdir(parents=True, exist_ok=True)

    # Copy static assets
    assets_src = PROJECT_ROOT / "site" / "assets"
    if assets_src.exists():
        logger.info("Static assets directory exists at %s", assets_src)

    events = build_json()
    build_html(events)

    logger.info("Site generation complete. Output in %s", SITE_DIR)
    return 0


if __name__ == "__main__":
    sys.exit(main())
