"""Tests for src.transform.

The transform module exposes three public functions — ``build_resale_identifier``,
``dedupe_by_identifier``, ``add_hashed_identifier`` — so the tests exercise
those directly with small hand-crafted frames. Each test targets one identifier
component (block, price, month, town) or one post-build behaviour (collision
dedupe, hashing), so a failing test names the bug unambiguously.
"""

from __future__ import annotations

import hashlib

import pandas as pd
import pytest

from src.transform import (
    add_hashed_identifier,
    build_resale_identifier,
    dedupe_by_identifier,
    hash_identifier,
)


def _one_row(**overrides) -> pd.DataFrame:
    """Build a single-row frame with the columns build_resale_identifier needs.

    Defaults are chosen so every component has a non-trivial expected output,
    making the brief's worked example easy to adapt per test.
    """
    base = {
        "month": "2012-01",
        "town": "ANG MO KIO",
        "flat_type": "2 ROOM",
        "block": "406",
        "resale_price": 257_800.0,
    }
    base.update(overrides)
    return pd.DataFrame([base])


# ---------------------------------------------------------------------------
# build_resale_identifier — happy path and per-component behaviour
# ---------------------------------------------------------------------------


def test_brief_worked_example():
    """A singleton group so the group mean = the one row's price.
    S + "406" (block) + "25" (25..00 → first 2) + "01" (month) + "A" (town)
    → "S4062501A".
    """
    out = build_resale_identifier(_one_row())
    assert out.loc[0, "resale_identifier"] == "S4062501A"


def test_block_with_trailing_letter_keeps_leading_digits():
    """block "123A" → digits "123" → first 3 "123"."""
    out = build_resale_identifier(_one_row(block="123A"))
    assert out.loc[0, "resale_identifier"][1:4] == "123"


def test_short_block_is_left_padded_with_zeros():
    """block "19" → digits "19" → first 3 "019" (brief's own example)."""
    out = build_resale_identifier(_one_row(block="19"))
    assert out.loc[0, "resale_identifier"][1:4] == "019"


def test_block_with_leading_letter_takes_the_digits_after():
    """block "A7" → digits "7" → first 3 "007"."""
    out = build_resale_identifier(_one_row(block="A7"))
    assert out.loc[0, "resale_identifier"][1:4] == "007"


def test_block_with_no_digits_raises():
    """Silently emitting "000" for a digit-less block would collide with every
    other digit-less block, so the contract is to raise."""
    with pytest.raises(ValueError, match="no digits"):
        build_resale_identifier(_one_row(block="AAA"))


def test_price_digits_are_truncated_not_rounded():
    """234,567 → "23" (not "24"). Truncation is the brief's "1st and 2nd digit"."""
    out = build_resale_identifier(_one_row(resale_price=234_567.0))
    assert out.loc[0, "resale_identifier"][4:6] == "23"


def test_price_uses_the_group_mean_not_the_row_price():
    """Two rows in the same (month, town, flat_type) group must share the
    same price digits even when their individual prices differ."""
    df = pd.DataFrame(
        [
            {"month": "2014-07", "town": "BEDOK", "flat_type": "3 ROOM",
             "block": "123", "resale_price": 300_000.0},
            {"month": "2014-07", "town": "BEDOK", "flat_type": "3 ROOM",
             "block": "456", "resale_price": 320_000.0},
        ]
    )
    out = build_resale_identifier(df)
    # Group mean = 310_000 → first two digits "31".
    assert out.loc[0, "resale_identifier"][4:6] == "31"
    assert out.loc[1, "resale_identifier"][4:6] == "31"


def test_month_suffix_is_the_mm_portion():
    out = build_resale_identifier(_one_row(month="2014-07"))
    assert out.loc[0, "resale_identifier"][6:8] == "07"


def test_bad_month_format_raises():
    with pytest.raises(ValueError, match="YYYY-MM"):
        build_resale_identifier(_one_row(month="2014/07"))


def test_town_initial_is_upper_cased_first_char():
    out = build_resale_identifier(_one_row(town="  bedok "))
    assert out.loc[0, "resale_identifier"][-1] == "B"


def test_every_identifier_is_exactly_nine_chars():
    df = pd.DataFrame(
        [
            {"month": "2012-01", "town": "ANG MO KIO", "flat_type": "2 ROOM",
             "block": "406", "resale_price": 257_800.0},
            {"month": "2015-12", "town": "BEDOK", "flat_type": "4 ROOM",
             "block": "1", "resale_price": 5.0},   # tiny price stresses zero-padding
        ]
    )
    out = build_resale_identifier(df)
    assert (out["resale_identifier"].str.len() == 9).all()


# ---------------------------------------------------------------------------
# dedupe_by_identifier
# ---------------------------------------------------------------------------


def test_dedupe_keeps_highest_price_on_collision():
    df = pd.DataFrame(
        {
            "resale_identifier": ["S1234567A", "S1234567A", "S9999999B"],
            "resale_price": [300_000.0, 350_000.0, 500_000.0],
        }
    )
    kept, discarded = dedupe_by_identifier(df)

    assert len(kept) == 2
    assert len(discarded) == 1
    # Of the two collided rows, the 350k row survived.
    collided_kept = kept.loc[kept["resale_identifier"] == "S1234567A"].iloc[0]
    assert collided_kept["resale_price"] == 350_000.0
    assert (discarded["failure_reason"] == "identifier_collision_lower_price").all()


def test_dedupe_no_collisions_returns_empty_discarded():
    df = pd.DataFrame(
        {
            "resale_identifier": ["S0000001A", "S0000002B"],
            "resale_price": [100.0, 200.0],
        }
    )
    kept, discarded = dedupe_by_identifier(df)
    assert len(kept) == 2
    assert len(discarded) == 0
    # Shape-compatible: the failure_reason column is still present.
    assert "failure_reason" in discarded.columns


# ---------------------------------------------------------------------------
# Hashing
# ---------------------------------------------------------------------------


def test_hash_identifier_is_deterministic_sha256():
    digest = hash_identifier("S1234567A")
    assert digest == hashlib.sha256(b"S1234567A").hexdigest()
    assert len(digest) == 64


def test_add_hashed_identifier_preserves_uniqueness():
    df = pd.DataFrame(
        {"resale_identifier": ["S0000001A", "S0000002B", "S0000001A"]}
    )
    out = add_hashed_identifier(df)
    assert "resale_identifier_hashed" in out.columns
    # Same plaintext → same hash.
    assert out.loc[0, "resale_identifier_hashed"] == out.loc[2, "resale_identifier_hashed"]
    # Distinct plaintext → distinct hash.
    assert out.loc[0, "resale_identifier_hashed"] != out.loc[1, "resale_identifier_hashed"]
