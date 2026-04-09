"""Unit tests for src.profile.

Uses a small synthetic DataFrame designed so each flag and statistic has a
known expected value, decoupling the tests from the live HDB data.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src import config, profile


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """6-row synthetic frame covering every code path in profile_dataset."""
    return pd.DataFrame(
        {
            # Numeric col with one NaN — exercises null counting and stats.
            "price": [100.0, 200.0, 300.0, 400.0, 500.0, np.nan],
            # Constant col — should trigger the `constant` flag.
            "currency": ["SGD", "SGD", "SGD", "SGD", "SGD", "SGD"],
            # Mostly-null col — should trigger `high_null` (>50%).
            "optional": [None, None, None, None, "x", "y"],
            # Object col with one whitespace-padded value.
            "town": ["BISHAN", "BISHAN ", "BEDOK", "BEDOK", "ANG MO KIO", "ANG MO KIO"],
            # Month col — exercises _month_range.
            "month": [
                "2012-01",
                "2012-02",
                "2013-06",
                "2014-12",
                "2015-03",
                "2016-12",
            ],
            # Lineage col — must be excluded from string profiling.
            "source_file": ["a.csv"] * 3 + ["b.csv"] * 3,
        }
    )


def test_shape_and_overview_row_count(sample_df: pd.DataFrame) -> None:
    report = profile.profile_dataset(sample_df)
    assert report.shape == (6, 6)
    # One overview row per column.
    assert len(report.overview) == 6
    assert set(report.overview["column"]) == set(sample_df.columns)


def test_overview_null_counts(sample_df: pd.DataFrame) -> None:
    report = profile.profile_dataset(sample_df)
    by_col = report.overview.set_index("column")
    assert by_col.loc["price", "null_count"] == 1
    assert by_col.loc["optional", "null_count"] == 4
    assert by_col.loc["currency", "null_count"] == 0


def test_numeric_stats(sample_df: pd.DataFrame) -> None:
    report = profile.profile_dataset(sample_df)
    # Only `price` is numeric.
    assert list(report.numeric.index) == ["price"]
    row = report.numeric.loc["price"]
    assert row["min"] == 100.0
    assert row["max"] == 500.0
    # Mean of 100..500 = 300.
    assert row["mean"] == pytest.approx(300.0)
    assert row["median"] == pytest.approx(300.0)


def test_string_lengths_excludes_source_file(sample_df: pd.DataFrame) -> None:
    report = profile.profile_dataset(sample_df)
    cols = set(report.string_lengths["column"])
    assert "source_file" not in cols
    # `town`, `currency`, `optional`, `month` are object dtype.
    assert "town" in cols


def test_top_values_present_for_string_cols(sample_df: pd.DataFrame) -> None:
    report = profile.profile_dataset(sample_df)
    assert "town" in report.string_top_values
    assert "source_file" not in report.string_top_values
    town_top = report.string_top_values["town"]
    # ANG MO KIO and BEDOK each appear twice; BISHAN/BISHAN<space> are distinct.
    assert town_top["count"].max() == 2


def test_month_range(sample_df: pd.DataFrame) -> None:
    report = profile.profile_dataset(sample_df)
    assert report.month_range == ("2012-01", "2016-12")


def test_flags_high_null(sample_df: pd.DataFrame) -> None:
    report = profile.profile_dataset(sample_df)
    flagged = report.flags[report.flags["flag"] == "high_null"]["column"].tolist()
    assert "optional" in flagged  # 4/6 nulls = 66% > 50%
    assert "price" not in flagged  # 1/6 = 16%


def test_flags_constant(sample_df: pd.DataFrame) -> None:
    report = profile.profile_dataset(sample_df)
    flagged = report.flags[report.flags["flag"] == "constant"]["column"].tolist()
    assert "currency" in flagged


def test_flags_whitespace(sample_df: pd.DataFrame) -> None:
    report = profile.profile_dataset(sample_df)
    flagged = report.flags[report.flags["flag"] == "whitespace"]["column"].tolist()
    assert "town" in flagged


def test_pure_function(sample_df: pd.DataFrame) -> None:
    """profile_dataset must not mutate its input."""
    before = sample_df.copy()
    profile.profile_dataset(sample_df)
    pd.testing.assert_frame_equal(sample_df, before)


def test_high_null_threshold_uses_config(sample_df: pd.DataFrame, monkeypatch) -> None:
    """Lowering the threshold should flag more columns; tunable lives in config."""
    monkeypatch.setattr(config, "PROFILE_HIGH_NULL_THRESHOLD", 0.10)
    report = profile.profile_dataset(sample_df)
    flagged = report.flags[report.flags["flag"] == "high_null"]["column"].tolist()
    # `price` is now flagged (1/6 ≈ 16% > 10%).
    assert "price" in flagged
