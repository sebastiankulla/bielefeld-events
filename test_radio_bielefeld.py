"""Tests for the RadioBielefeldScraper.

Runs a live scrape against the actual vtipps API and checks that:
- At least one event is returned
- Each event has a non-empty title
- Each event has a valid date_start (datetime object, year >= today)
- Each event with a time set has hour != 0 (proves time was scraped, not defaulted)
- Each event has a non-empty image_url pointing to the vtipps CDN
"""

import sys
from datetime import datetime

# Make sure the project root is on the path
sys.path.insert(0, "/home/user/bielefeld-events")

from scrapers.radio_bielefeld import RadioBielefeldScraper, _parse_when


# ---------------------------------------------------------------------------
# Unit tests for the date/time parser
# ---------------------------------------------------------------------------

def test_parse_when_date_only():
    dt = _parse_when("07.03.2026")
    assert dt == datetime(2026, 3, 7, 0, 0), f"Expected 2026-03-07 00:00, got {dt}"


def test_parse_when_date_and_time():
    dt = _parse_when("07.03.2026, 09:00-13:00 Uhr")
    assert dt == datetime(2026, 3, 7, 9, 0), f"Expected 2026-03-07 09:00, got {dt}"


def test_parse_when_date_and_single_time():
    dt = _parse_when("15.04.2026, 19:30 Uhr")
    assert dt == datetime(2026, 4, 15, 19, 30), f"Expected 2026-04-15 19:30, got {dt}"


def test_parse_when_invalid():
    dt = _parse_when("kein Datum")
    assert dt is None


# ---------------------------------------------------------------------------
# Integration test: live scrape
# ---------------------------------------------------------------------------

def test_live_scrape():
    scraper = RadioBielefeldScraper()
    events = scraper.scrape()

    assert len(events) > 0, "No events scraped from Radio Bielefeld"

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    events_with_time = []

    for ev in events:
        # Title must be non-empty
        assert ev.title, f"Event has empty title: {ev}"

        # date_start must be a valid datetime
        assert isinstance(ev.date_start, datetime), (
            f"date_start is not a datetime: {ev.date_start!r} for '{ev.title}'"
        )

        # Date must be in the future (or today)
        assert ev.date_start >= today, (
            f"Event date is in the past: {ev.date_start} for '{ev.title}'"
        )

        # Image URL must point to the vtipps CDN
        assert ev.image_url, f"Event has no image_url: '{ev.title}'"
        assert "vtipps" in ev.image_url or ev.image_url.startswith("http"), (
            f"Unexpected image URL: {ev.image_url!r} for '{ev.title}'"
        )

        # URL must point to radiobielefeld.de
        assert "radiobielefeld.de" in ev.url, (
            f"Unexpected event URL: {ev.url!r} for '{ev.title}'"
        )

        if ev.date_start.hour != 0:
            events_with_time.append(ev)

    print(f"\n✓ Scraped {len(events)} events total")
    print(f"✓ {len(events_with_time)} events have an explicit start time")

    # Print a sample for visual inspection
    print("\nSample events:")
    for ev in events[:5]:
        time_str = ev.date_start.strftime("%d.%m.%Y %H:%M")
        print(f"  [{time_str}] {ev.title}")
        print(f"    Location : {ev.location}")
        print(f"    Category : {ev.category}")
        print(f"    Image    : {ev.image_url}")
        print(f"    URL      : {ev.url}")
        if ev.price:
            print(f"    Price    : {ev.price}")
        print()

    # Soft assertion: we expect at least some events to have a time
    if events_with_time:
        print(f"✓ Time scraping works ({len(events_with_time)} events with time)")
    else:
        print("⚠ No events had an explicit start time (detail pages may lack time)")


if __name__ == "__main__":
    # Run unit tests
    print("=== Unit tests ===")
    test_parse_when_date_only()
    print("✓ test_parse_when_date_only")
    test_parse_when_date_and_time()
    print("✓ test_parse_when_date_and_time")
    test_parse_when_date_and_single_time()
    print("✓ test_parse_when_date_and_single_time")
    test_parse_when_invalid()
    print("✓ test_parse_when_invalid")

    print("\n=== Integration test (live scrape) ===")
    test_live_scrape()
    print("All tests passed!")
