#!/usr/bin/env python3
"""Main entry point: scrape all sources and store events in SQLite."""

import logging
import sys
import time

from scrapers.bielefeld_jetzt import BielefeldJetztScraper
from scrapers.bunker_ulmenwall import BunkerUlmenwallScraper
from scrapers.buo import BuoScraper
from scrapers.cafe_europa import CafeEuropaScraper
from scrapers.database import init_db, upsert_events
from scrapers.kulturamt import KulturamtScraper
from scrapers.lokschuppen import LokschuppenScraper
from scrapers.nrzp import NrzpScraper
from scrapers.owl_journal import OwlJournalScraper
from scrapers.stadtwerke_bielefeld import StadtwerkeBielefeldScraper
from scrapers.stereo import StereoScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("scrape")

SCRAPERS = [
    BielefeldJetztScraper,
    StadtwerkeBielefeldScraper,
    KulturamtScraper,
    OwlJournalScraper,
    StereoScraper,
    LokschuppenScraper,
    CafeEuropaScraper,
    BunkerUlmenwallScraper,
    NrzpScraper,
    BuoScraper,
]


def main() -> int:
    logger.info("Initializing database...")
    init_db()

    total = 0
    failed = 0

    for scraper_cls in SCRAPERS:
        scraper = scraper_cls()
        logger.info("Running scraper: %s", scraper.name)

        start = time.monotonic()
        events = scraper.scrape()
        elapsed = time.monotonic() - start

        if events:
            count = upsert_events(events)
            logger.info(
                "  -> %d events stored from %s (%.1fs)",
                count, scraper.name, elapsed,
            )
            total += count
        else:
            logger.warning(
                "  -> No events returned from %s (%.1fs)",
                scraper.name, elapsed,
            )
            failed += 1

    logger.info(
        "Done. Total events stored: %d | Scrapers: %d OK, %d failed",
        total, len(SCRAPERS) - failed, failed,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
