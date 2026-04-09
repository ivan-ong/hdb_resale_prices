"""Tests for src.validate.

Covers: spec derivation with categorical normalization, per-rule behavior
(structural format, set membership, numeric positivity), the NaN-as-failure
convention, and the end-to-end ``validate_master`` split.
"""

from __future__ import annotations

import pandas as pd

from src.validate import derive_spec, validate_master


def _good_row(**overrides):
    base = {
        "month": "2014-07",
        "town": "BEDOK",
        "flat_type": "4 ROOM",
        "flat_model": "Improved",        # case drift — must still pass
        "block": "123",
        "storey_range": "04 TO 06",
        "floor_area_sqm": 90.0,
        "resale_price": 400_000.0,
        "lease_commence_date": 1985,
        "remaining_lease": pd.NA,
        "remaining_lease_years_original": pd.NA,
        "source_file": "file.csv",
    }
    base.update(overrides)
    return base


def _frame(*rows):
    return pd.DataFrame(list(rows))


# ---------------------------------------------------------------------------
# Spec
# ---------------------------------------------------------------------------


def test_derive_spec_normalizes_categoricals():
    df = _frame(
        _good_row(flat_model="Improved"),
        _good_row(flat_model="IMPROVED"),
        _good_row(flat_model="  improved  "),
    )
    spec = derive_spec(df)
    # All three rows collapse to one canonical entry.
    assert spec.flat_models == frozenset({"IMPROVED"})


# ---------------------------------------------------------------------------
# Per-rule behavior via validate_master
# ---------------------------------------------------------------------------


def test_validate_master_passes_all_clean_rows():
    df = _frame(_good_row(), _good_row(block="456"))
    result = validate_master(df)
    assert len(result.passed_df) == 2
    assert len(result.failed_df) == 0


def test_validate_master_fails_bad_month_format():
    df = _frame(
        _good_row(),
        _good_row(month="2014/07"),   # slash, not dash
    )
    result = validate_master(df)
    assert len(result.failed_df) == 1
    reasons = result.failed_df["failure_reasons"].iloc[0]
    assert "month_format" in reasons


def test_validate_master_fails_bad_storey_range():
    df = _frame(
        _good_row(),
        _good_row(storey_range="06 TO 04"),  # hi <= lo
    )
    result = validate_master(df)
    assert len(result.failed_df) == 1
    assert "storey_range_format" in result.failed_df["failure_reasons"].iloc[0]


def test_validate_master_fails_non_positive_price():
    df = _frame(_good_row(), _good_row(resale_price=0))
    result = validate_master(df)
    assert len(result.failed_df) == 1
    assert "resale_price_positive" in result.failed_df["failure_reasons"].iloc[0]


def test_validate_master_fails_lease_year_before_min():
    df = _frame(
        _good_row(),
        _good_row(lease_commence_date=1900),  # below LEASE_MIN_YEAR
    )
    result = validate_master(df)
    assert len(result.failed_df) == 1
    assert (
        "lease_commence_year_in_range"
        in result.failed_df["failure_reasons"].iloc[0]
    )


def test_validate_master_treats_null_composite_key_as_failure():
    """NaN in a key column must be a failure, not a silent pass."""
    df = _frame(_good_row(), _good_row(block=None))
    result = validate_master(df)
    assert len(result.failed_df) == 1
    assert (
        "composite_key_no_nulls"
        in result.failed_df["failure_reasons"].iloc[0]
    )


def test_validate_master_report_shape():
    df = _frame(_good_row(), _good_row(resale_price=-1))
    result = validate_master(df)
    report = result.report
    assert {"rule", "rows_checked", "rows_failed", "fail_pct"}.issubset(
        report.columns
    )
    # The resale_price rule saw one failure.
    row = report.loc[report["rule"] == "resale_price_positive"].iloc[0]
    assert row["rows_failed"] == 1
    assert row["rows_checked"] == 2
