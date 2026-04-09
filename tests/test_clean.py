"""Tests for src.clean.

Focus on the three pipeline steps individually (lease recompute, dedupe,
anomaly flag) and the end-to-end ``clean_master`` entrypoint. Lease
recomputation uses an explicit ``as_of`` parameter in every test so these
assertions don't drift as ``config.AS_OF_DATE`` moves.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from src.clean import (
    clean_master,
    composite_key_columns,
    deduplicate,
    flag_price_anomalies,
    recompute_remaining_lease,
)


# ---------------------------------------------------------------------------
# Lease recompute
# ---------------------------------------------------------------------------


def test_recompute_remaining_lease_basic_math():
    """A flat with lease commencing 1990 has expiry = 1990 + 99 = 2089-01-01.
    Evaluated on 2026-04-09 the remaining lease is:
        (2089 - 2026) * 12 + (1 - 4) - 1 (because day > 1)
        = 63*12 - 3 - 1 = 752 months = 62 years 8 months.
    """
    df = pd.DataFrame(
        {
            "lease_commence_date": [1990],
            "resale_price": [500000.0],
        }
    )
    kept, expired = recompute_remaining_lease(df, as_of=date(2026, 4, 9))
    assert len(expired) == 0
    assert kept.loc[0, "remaining_lease_computed_months"] == 752
    assert kept.loc[0, "remaining_lease_computed"] == "62 years 8 months"


def test_recompute_remaining_lease_splits_expired():
    """A lease from 1900 (expiry 1999) is fully expired in 2026."""
    df = pd.DataFrame(
        {
            "lease_commence_date": [1990, 1900],
            "resale_price": [500000.0, 123.0],
        }
    )
    kept, expired = recompute_remaining_lease(df, as_of=date(2026, 4, 9))
    assert len(kept) == 1
    assert len(expired) == 1
    assert (expired["failure_reasons"] == "lease_expired").all()


# ---------------------------------------------------------------------------
# Dedupe
# ---------------------------------------------------------------------------


def test_composite_key_columns_excludes_price_and_metadata():
    df = pd.DataFrame(
        columns=[
            "month", "town", "flat_type", "block",
            "resale_price", "source_file",
            "remaining_lease", "remaining_lease_years_original",
            "remaining_lease_computed", "remaining_lease_computed_months",
            "price_anomaly_flag", "price_anomaly_reason",
        ]
    )
    key = composite_key_columns(df)
    assert set(key) == {"month", "town", "flat_type", "block"}


def test_deduplicate_keeps_highest_price_on_composite_tie():
    df = pd.DataFrame(
        {
            "month": ["2014-07", "2014-07", "2014-07"],
            "town": ["ANG MO KIO", "ANG MO KIO", "BEDOK"],
            "flat_type": ["3 ROOM", "3 ROOM", "4 ROOM"],
            "block": ["123", "123", "456"],
            "resale_price": [300000.0, 350000.0, 500000.0],
        }
    )
    kept, losers = deduplicate(df)
    assert len(kept) == 2
    assert len(losers) == 1
    # The 350k row survived, the 300k row was dropped.
    amk_prices = set(
        kept.loc[kept["town"] == "ANG MO KIO", "resale_price"].tolist()
    )
    assert amk_prices == {350000.0}
    assert losers["resale_price"].tolist() == [300000.0]


# ---------------------------------------------------------------------------
# Anomaly flag
# ---------------------------------------------------------------------------


def test_flag_price_anomalies_below_lower_bound():
    """Eight normal rows plus one absurdly cheap outlier in the same group.
    The outlier must be flagged with the lower-bound reason."""
    normal_prices = [400_000.0] * 4 + [420_000.0] * 4
    prices = normal_prices + [10_000.0]  # absurdly cheap
    df = pd.DataFrame(
        {
            "month": ["2014-07"] * 9,
            "town": ["BEDOK"] * 9,
            "flat_type": ["4 ROOM"] * 9,
            "floor_area_sqm": [90.0] * 9,
            "resale_price": prices,
        }
    )
    out = flag_price_anomalies(df)
    assert out["price_anomaly_flag"].sum() == 1
    outlier = out.loc[out["price_anomaly_flag"]]
    assert outlier.iloc[0]["price_anomaly_reason"] == "below_lower_iqr_bound"
    assert outlier.iloc[0]["resale_price"] == 10_000.0


def test_flag_price_anomalies_singleton_group_never_flagged():
    """A (town, flat_type, year) group with only one row has IQR=0 and must
    not flag that row — we don't have anything to compare it to."""
    df = pd.DataFrame(
        {
            "month": ["2014-07"],
            "town": ["UNIQUETOWN"],
            "flat_type": ["3 ROOM"],
            "floor_area_sqm": [70.0],
            "resale_price": [300_000.0],
        }
    )
    out = flag_price_anomalies(df)
    assert not out["price_anomaly_flag"].any()


# ---------------------------------------------------------------------------
# clean_master end-to-end
# ---------------------------------------------------------------------------


def test_clean_master_stage_counts_are_consistent():
    df = pd.DataFrame(
        {
            "month": ["2014-07", "2014-07", "2014-07"],
            "town": ["BEDOK", "BEDOK", "BEDOK"],
            "flat_type": ["4 ROOM", "4 ROOM", "4 ROOM"],
            "block": ["123", "123", "456"],  # first two are composite-key dupes
            "floor_area_sqm": [90.0, 90.0, 85.0],
            "resale_price": [400_000.0, 450_000.0, 420_000.0],
            "lease_commence_date": [1990, 1990, 1985],
        }
    )
    result = clean_master(df, as_of=date(2026, 4, 9))
    sc = result.stage_counts
    assert sc["input_validation_passed"] == 3
    assert sc["expired_lease_dropped"] == 0
    assert sc["duplicates_dropped"] == 1
    assert sc["final_cleaned_rows"] == 2
    # Row identity: the 450k row won the dedupe, not the 400k row.
    assert 450_000.0 in result.cleaned_df["resale_price"].tolist()
    assert 400_000.0 not in result.cleaned_df["resale_price"].tolist()
