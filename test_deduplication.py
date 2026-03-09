"""Tests for the deduplication logic in build/generate.py.

Covers:
- _normalize_title      (normalisation pipeline)
- _has_real_time        (time detection)
- _is_title_match       (all three matching rules)
- _merge_group          (field-merging strategy)
- deduplicate_events    (end-to-end)
"""

import sys
from datetime import datetime

sys.path.insert(0, "/home/user/bielefeld-events")

from build.generate import (
    _has_real_time,
    _is_title_match,
    _merge_group,
    _normalize_title,
    deduplicate_events,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ev(
    title: str,
    date_start: str = "2026-03-10T20:00:00",
    source: str = "test_source",
    *,
    url: str = "https://example.com",
    description: str = "",
    location: str = "Lokschuppen",
    category: str = "",
    image_url: str = "",
    price: str = "",
) -> dict:
    """Return a minimal event dict for use in tests."""
    return {
        "title": title,
        "date_start": date_start,
        "source": source,
        "url": url,
        "description": description,
        "location": location,
        "category": category,
        "image_url": image_url,
        "price": price,
    }


def _n(title: str) -> str:
    """Shorthand: normalise a title."""
    return _normalize_title(title)


# ===========================================================================
# _normalize_title
# ===========================================================================

def test_normalize_lowercase():
    assert _n("Rock KONZERT") == "rock konzert"


def test_normalize_strips_leading_trailing_whitespace():
    assert _n("  Jazz Night  ") == "jazz night"


def test_normalize_collapses_spaces():
    assert _n("Open  Air   Festival") == "open air festival"


def test_normalize_removes_special_chars():
    assert _n("Beats & Butterkeks!") == "beats butterkeks"


def test_normalize_ampersand_becomes_und_then_stripped():
    # "&" → " und " → connector stripped → just space
    assert _n("rock & roll") == "rock roll"


def test_normalize_plus_becomes_und_then_stripped():
    assert _n("A + B Konzert") == "a b konzert"


def test_normalize_connectors_und_stripped():
    assert _n("Mord und Totschlag") == "mord totschlag"


def test_normalize_connectors_and_stripped():
    assert _n("Fire and Ice") == "fire ice"


def test_normalize_strips_trailing_bielefeld():
    assert _n("Vivid Indie Bielefeld") == "vivid indie"


def test_normalize_does_not_strip_mid_bielefeld():
    # "Bielefeld" only stripped if it's the very last word
    result = _n("Bielefeld Open Air Festival")
    assert "bielefeld" in result


def test_normalize_umlaut_a():
    assert _n("Bärenstark") == "barenstark"


def test_normalize_umlaut_o():
    assert _n("Schön") == "schon"


def test_normalize_umlaut_u():
    assert _n("Über den Dächern") == "uber den dachern"


def test_normalize_dash_removed():
    assert _n("Mord am Mittwoch - Die Crime Show") == "mord am mittwoch die crime show"


def test_normalize_colon_removed():
    assert _n("Lucia Leona: Mord am Mittwoch") == "lucia leona mord am mittwoch"


def test_normalize_dash_and_umlaut():
    assert _n("Mord am Mittwoch – Krimidinnershow") == "mord am mittwoch krimidinnershow"


def test_normalize_empty_string():
    assert _n("") == ""


# ===========================================================================
# _has_real_time
# ===========================================================================

def test_has_real_time_iso_with_time():
    assert _has_real_time("2026-03-10T20:00:00") is True


def test_has_real_time_iso_with_midnight_is_false():
    assert _has_real_time("2026-03-10T00:00:00") is False


def test_has_real_time_space_separator_with_time():
    assert _has_real_time("2026-03-10 19:30:00") is True


def test_has_real_time_space_separator_midnight_is_false():
    assert _has_real_time("2026-03-10 00:00:00") is False


def test_has_real_time_date_only_is_false():
    assert _has_real_time("2026-03-10") is False


def test_has_real_time_empty_is_false():
    assert _has_real_time("") is False


# ===========================================================================
# _is_title_match
# ===========================================================================

# --- Rule 1: fuzzy ratio >= 0.82 ---

def test_title_match_identical():
    assert _is_title_match(_n("Mord am Mittwoch"), _n("Mord am Mittwoch")) is True


def test_title_match_minor_typo():
    # "Nightwash" vs "Nightwach" – one character difference
    assert _is_title_match(_n("Nightwash"), _n("Nightwach")) is True


def test_title_match_extra_whitespace_normalised():
    assert _is_title_match(_n("Open  Air"), _n("Open Air")) is True


def test_title_match_umlaut_vs_no_umlaut():
    # Both normalise to the same string (ä→a)
    assert _is_title_match(_n("Schön und Gut"), _n("Schon und Gut")) is True


# --- Rule 2: prefix match ---

def test_title_match_prefix_subtitle_appended():
    # "Mord am Mittwoch" is prefix of "Mord am Mittwoch – Krimidinnershow"
    assert _is_title_match(
        _n("Mord am Mittwoch"),
        _n("Mord am Mittwoch – Krimidinnershow"),
    ) is True


def test_title_match_prefix_dash_subtitle():
    assert _is_title_match(
        _n("Mord am Mittwoch"),
        _n("Mord am Mittwoch - Die Crime Show"),
    ) is True


def test_title_match_prefix_too_short():
    # Shorter title < 8 chars → Rule 2 does NOT apply
    # "Rock" (4 chars) in "Rock Konzert": should NOT match via prefix rule
    assert _is_title_match("rock", "rock konzert") is False


def test_title_match_prefix_coverage_below_50_percent():
    # "jazz" (4) in "jazz festival open air bielefeld" (32): 4/32 = 12.5 % → no match
    assert _is_title_match("jazz", "jazz festival open air bielefeld") is False


def test_title_match_prefix_requires_word_boundary():
    # Rule 2 requires the shorter title to be followed by a space (word boundary).
    # "abend show" (10 chars) is NOT followed by " " in "abend showkasse" –
    # it's a broken-word prefix.  Rule 3 doesn't apply (len < 12); fuzzy
    # ratio is ~0.80 (below the 0.82 threshold).  Expected: no match.
    assert _is_title_match("abend show", "abend showkasse") is False


# --- Rule 3: substring match ---

def test_title_match_performer_prepended():
    # Real case: nw.de prepends performer name before show title
    assert _is_title_match(
        _n("Mord am Mittwoch"),
        _n("Lucia Leona: Mord am Mittwoch - Die Crime Show"),
    ) is True


def test_title_match_substring_too_short():
    # Shorter title < 12 chars → Rule 3 does NOT apply
    assert _is_title_match("jazz night", "open jazz night bielefeld") is False


def test_title_match_substring_coverage_below_35_percent():
    # Shorter covers < 35% of longer → no match
    # "comedy abend" (12) in "großer comedy abend im theater am abend" (38): 12/38 = 0.31
    assert _is_title_match(
        "comedy abend",
        "groser comedy abend im theater am abend",
    ) is False


# --- No match ---

def test_title_no_match_completely_different():
    assert _is_title_match(_n("Jazz Festival"), _n("Techno Night")) is False


def test_title_no_match_same_genre_different_event():
    assert _is_title_match(_n("Kabarett Abend"), _n("Stand-up Comedy Night")) is False


# ===========================================================================
# _merge_group
# ===========================================================================

def test_merge_group_single_event():
    ev = _ev("Mord am Mittwoch", source="lokschuppen")
    result = _merge_group([ev])
    assert result["title"] == "Mord am Mittwoch"
    assert result["source"] == "lokschuppen"
    assert len(result["sources"]) == 1


def test_merge_group_sources_list_contains_all():
    group = [
        _ev("Konzert", source="lokschuppen", url="https://lok.de"),
        _ev("Konzert", source="nw_events",   url="https://nw.de"),
    ]
    result = _merge_group(group)
    sources = {s["source"] for s in result["sources"]}
    assert sources == {"lokschuppen", "nw_events"}


def test_merge_group_source_priority_low_priority_source_loses():
    # nw_events (priority 10) should lose to lokschuppen (priority 0)
    group = [
        _ev("Konzert", source="nw_events",   url="https://nw.de",  description="kurz"),
        _ev("Konzert", source="lokschuppen", url="https://lok.de", description="lang"),
    ]
    result = _merge_group(group)
    assert result["source"] == "lokschuppen"


def test_merge_group_bielefeld_jetzt_last_priority():
    # bielefeld_jetzt (priority 20) should lose to everything
    group = [
        _ev("Konzert", source="bielefeld_jetzt", url="https://bj.de"),
        _ev("Konzert", source="lokschuppen",     url="https://lok.de"),
    ]
    result = _merge_group(group)
    assert result["source"] == "lokschuppen"


def test_merge_group_prefers_real_time_over_midnight():
    group = [
        _ev("Konzert", date_start="2026-03-10T00:00:00", source="lokschuppen"),
        _ev("Konzert", date_start="2026-03-10T20:00:00", source="nw_events"),
    ]
    result = _merge_group(group)
    assert result["date_start"] == "2026-03-10T20:00:00"


def test_merge_group_keeps_primary_time_when_all_midnight():
    group = [
        _ev("Konzert", date_start="2026-03-10T00:00:00", source="lokschuppen"),
        _ev("Konzert", date_start="2026-03-11T00:00:00", source="nw_events"),
    ]
    result = _merge_group(group)
    # Should fall back to primary source's date_start
    assert result["date_start"] == "2026-03-10T00:00:00"


def test_merge_group_prefers_longest_description():
    group = [
        _ev("Konzert", source="lokschuppen", description="kurze Beschreibung"),
        _ev("Konzert", source="nw_events",   description="viel längere und detailliertere Beschreibung mit mehr Inhalt"),
    ]
    result = _merge_group(group)
    assert "detailliertere" in result["description"]


def test_merge_group_first_nonempty_image_wins():
    group = [
        _ev("Konzert", source="lokschuppen", image_url=""),
        _ev("Konzert", source="nw_events",   image_url="https://cdn.example.com/img.jpg"),
    ]
    result = _merge_group(group)
    assert result["image_url"] == "https://cdn.example.com/img.jpg"


def test_merge_group_url_comes_from_primary_source():
    group = [
        _ev("Konzert", source="lokschuppen", url="https://lok.de/event"),
        _ev("Konzert", source="nw_events",   url="https://nw.de/event"),
    ]
    result = _merge_group(group)
    assert result["url"] == "https://lok.de/event"


# ===========================================================================
# deduplicate_events  (end-to-end)
# ===========================================================================

def test_dedup_no_duplicates_unchanged():
    events = [
        _ev("Jazz Festival",  "2026-03-10T20:00:00", "src_a"),
        _ev("Techno Night",   "2026-03-10T22:00:00", "src_b"),
        _ev("Kabarett Abend", "2026-03-11T19:00:00", "src_a"),
    ]
    result = deduplicate_events(events)
    assert len(result) == 3


def test_dedup_exact_same_title_and_day_merged():
    events = [
        _ev("Mord am Mittwoch", "2026-03-10T00:00:00", "lokschuppen"),
        _ev("Mord am Mittwoch", "2026-03-10T20:00:00", "nw_events"),
    ]
    result = deduplicate_events(events)
    assert len(result) == 1
    assert result[0]["source"] == "lokschuppen"
    assert result[0]["date_start"] == "2026-03-10T20:00:00"


def test_dedup_same_title_different_days_not_merged():
    events = [
        _ev("Mord am Mittwoch", "2026-03-10T20:00:00", "lokschuppen"),
        _ev("Mord am Mittwoch", "2026-03-17T20:00:00", "nw_events"),
    ]
    result = deduplicate_events(events)
    assert len(result) == 2


def test_dedup_fuzzy_title_merged():
    # Minor spelling difference → ratio >= 0.82
    events = [
        _ev("Nightwash", "2026-03-18T20:00:00", "lokschuppen"),
        _ev("Nightwach",  "2026-03-18T19:00:00", "nw_events"),
    ]
    result = deduplicate_events(events)
    assert len(result) == 1


def test_dedup_prefix_title_merged():
    events = [
        _ev("Mord am Mittwoch",                    "2026-03-10T00:00:00", "lokschuppen"),
        _ev("Mord am Mittwoch - Die Crime Show",   "2026-03-10T20:00:00", "bielefeld_jetzt"),
    ]
    result = deduplicate_events(events)
    assert len(result) == 1
    assert result[0]["date_start"] == "2026-03-10T20:00:00"


def test_dedup_substring_title_merged():
    # Real-world case: nw.de prepends performer name
    events = [
        _ev("Mord am Mittwoch",                              "2026-03-10T00:00:00", "lokschuppen"),
        _ev("Lucia Leona: Mord am Mittwoch - Die Crime Show","2026-03-10T20:00:00", "nw_events"),
    ]
    result = deduplicate_events(events)
    assert len(result) == 1


def test_dedup_three_sources_all_merged():
    # The canonical "Mord am Mittwoch" scenario
    events = [
        _ev("Mord am Mittwoch",                              "2026-03-10T00:00:00", "lokschuppen"),
        _ev("Mord am Mittwoch - Die Crime Show",             "2026-03-10T20:00:00", "bielefeld_jetzt"),
        _ev("Lucia Leona: Mord am Mittwoch - Die Crime Show","2026-03-10T20:00:00", "nw_events"),
    ]
    result = deduplicate_events(events)
    assert len(result) == 1
    sources = {s["source"] for s in result[0]["sources"]}
    assert sources == {"lokschuppen", "bielefeld_jetzt", "nw_events"}
    assert result[0]["date_start"] == "2026-03-10T20:00:00"


def test_dedup_three_sources_title_from_highest_priority():
    events = [
        _ev("Mord am Mittwoch",                              "2026-03-10T00:00:00", "lokschuppen"),
        _ev("Mord am Mittwoch - Die Crime Show",             "2026-03-10T20:00:00", "bielefeld_jetzt"),
        _ev("Lucia Leona: Mord am Mittwoch - Die Crime Show","2026-03-10T20:00:00", "nw_events"),
    ]
    result = deduplicate_events(events)
    # lokschuppen is highest priority → its title wins
    assert result[0]["source"] == "lokschuppen"
    assert result[0]["title"] == "Mord am Mittwoch"


def test_dedup_cross_day_isolation():
    # Events on different days with similar names should NOT merge
    events = [
        _ev("Kabarett Night", "2026-03-10T20:00:00", "src_a"),
        _ev("Kabarett Night", "2026-03-11T20:00:00", "src_b"),
        _ev("Kabarett Night", "2026-03-12T20:00:00", "src_c"),
    ]
    result = deduplicate_events(events)
    assert len(result) == 3


def test_dedup_normalisation_merges_umlaut_variants():
    # "Schön" and "Schon" normalise identically
    events = [
        _ev("Schön und Gut", "2026-03-10T20:00:00", "src_a"),
        _ev("Schon und Gut", "2026-03-10T20:00:00", "src_b"),
    ]
    result = deduplicate_events(events)
    assert len(result) == 1


def test_dedup_sources_list_urls_preserved():
    events = [
        _ev("Konzert", "2026-03-10T20:00:00", "lokschuppen", url="https://lok.de/e1"),
        _ev("Konzert", "2026-03-10T20:00:00", "nw_events",   url="https://nw.de/e2"),
    ]
    result = deduplicate_events(events)
    urls = {s["url"] for s in result[0]["sources"]}
    assert "https://lok.de/e1" in urls
    assert "https://nw.de/e2" in urls


def test_dedup_empty_list():
    assert deduplicate_events([]) == []


def test_dedup_single_event():
    events = [_ev("Solo Event", "2026-03-10T20:00:00", "lokschuppen")]
    result = deduplicate_events(events)
    assert len(result) == 1
    assert result[0]["title"] == "Solo Event"


# ===========================================================================
# Manual runner (python test_deduplication.py)
# ===========================================================================

if __name__ == "__main__":
    import traceback

    tests = [
        (name, fn) for name, fn in sorted(globals().items())
        if name.startswith("test_")
    ]
    passed = failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  ✓  {name}")
            passed += 1
        except Exception as exc:
            print(f"  ✗  {name}")
            traceback.print_exc()
            failed += 1

    print(f"\n{passed} passed, {failed} failed out of {passed + failed} tests")
    if failed:
        sys.exit(1)
