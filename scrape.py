#!/usr/bin/env python3
"""Main entry point: scrape all sources and store events in SQLite."""

import logging
import sys

from scrapers.bielefeld_jetzt import BielefeldJetztScraper
from scrapers.database import init_db, upsert_events
from scrapers.owl_journal import OwlJournalScraper
from scrapers.stadtwerke_bielefeld import StadtwerkeBielefeldScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("scrape")

SCRAPERS = [
    BielefeldJetztScraper,
    StadtwerkeBielefeldScraper,
    OwlJournalScraper,
]


def main() -> int:
    logger.info("Initializing database...")
    init_db()

    total = 0
    for scraper_cls in SCRAPERS:
        scraper = scraper_cls()
        logger.info("Running scraper: %s", scraper.name)
        events = scraper.scrape()
        if events:
            count = upsert_events(events)
            logger.info("  -> %d events stored from %s", count, scraper.name)
            total += count
        else:
            logger.warning("  -> No events returned from %s", scraper.name)

    logger.info("Done. Total events stored: %d", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
