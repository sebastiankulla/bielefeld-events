"""SQLite database layer for storing scraped events."""

import logging
import sqlite3
from pathlib import Path

from scrapers.base import Event

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "events.db"


def get_connection() -> sqlite3.Connection:
    """Return a connection to the events database."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    """Create the events table if it doesn't exist."""
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            date_start TEXT NOT NULL,
            date_end TEXT,
            description TEXT DEFAULT '',
            location TEXT DEFAULT '',
            city TEXT DEFAULT 'Bielefeld',
            category TEXT DEFAULT '',
            image_url TEXT DEFAULT '',
            url TEXT DEFAULT '',
            price TEXT DEFAULT '',
            source TEXT NOT NULL,
            tags TEXT DEFAULT '',
            scraped_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(title, date_start, source)
        )
    """)
    conn.commit()
    conn.close()


def upsert_events(events: list[Event]) -> int:
    """Insert or update events. Returns number of new/updated rows."""
    conn = get_connection()
    count = 0
    for event in events:
        try:
            conn.execute(
                """
                INSERT INTO events
                    (title, date_start, date_end, description, location,
                     city, category, image_url, url, price, source, tags)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(title, date_start, source) DO UPDATE SET
                    description=excluded.description,
                    location=excluded.location,
                    category=excluded.category,
                    image_url=excluded.image_url,
                    url=excluded.url,
                    price=excluded.price,
                    tags=excluded.tags,
                    scraped_at=datetime('now')
                """,
                (
                    event.title,
                    event.date_start.isoformat(),
                    event.date_end.isoformat() if event.date_end else None,
                    event.description,
                    event.location,
                    event.city,
                    event.category,
                    event.image_url,
                    event.url,
                    event.price,
                    event.source,
                    ",".join(event.tags),
                ),
            )
            count += 1
        except sqlite3.Error:
            logger.warning("Failed to upsert event: %s", event.title, exc_info=True)
            continue
    conn.commit()
    conn.close()
    return count


def get_all_events() -> list[dict]:
    """Retrieve all future events, sorted by date."""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT * FROM events
        WHERE date_start >= date('now')
        ORDER BY date_start ASC
    """)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_categories() -> list[str]:
    """Retrieve all distinct categories."""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT DISTINCT category FROM events
        WHERE category != '' AND date_start >= date('now')
        ORDER BY category
    """)
    categories = [row["category"] for row in cursor.fetchall()]
    conn.close()
    return categories


def get_locations() -> list[str]:
    """Retrieve all distinct locations."""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT DISTINCT location FROM events
        WHERE location != '' AND date_start >= date('now')
        ORDER BY location
    """)
    locations = [row["location"] for row in cursor.fetchall()]
    conn.close()
    return locations
