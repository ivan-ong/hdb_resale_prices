"""Stage 4: Validate the master DataFrame.

Hand-rolled validation rules. All allowed-value sets are derived from the
master at runtime rather than hardcoded — see the brief's "avoid hardcoding"
directive. The :func:`validate_master` entry point returns a structured
:class:`ValidationResult` containing passed rows, failed rows (with a
``failure_reasons`` column), and a per-rule summary report.

The split between :func:`derive_spec` and :func:`validate_master` is
deliberate: it lets a future batch be validated against a frozen master spec
without re-deriving the categorical sets. On the master itself, this is a
tautology; on a future batch it is the substantive check.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from src import config

# ---------------------------------------------------------------------------
# Regex constants
# ---------------------------------------------------------------------------

# `month` column structural format. The brief specifies YYYY-MM.
_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")

# `storey_range` structural format: two zero-padded integers separated by
# the literal " TO ". Compared after upper-casing so case drift in future
# vintages doesn't break the rule.
_STOREY_RE = re.compile(r"^(\d{2})\s+TO\s+(\d{2})$")


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------


def _normalize_categorical(s: pd.Series) -> pd.Series:
    """Strip surrounding whitespace and uppercase a string Series.

    The HDB CSVs have known case drift in ``flat_model`` ("Improved" vs
    "IMPROVED") across vintages. Normalizing to a single canonical form
    before set membership checks means the same logical value passes the
    rule regardless of which vintage it came from.
    """
    return s.astype("string").str.strip().str.upper()


# ---------------------------------------------------------------------------
# Spec derivation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ValidationSpec:
    """Allowed-value sets and numeric bounds derived from a reference frame.

    Attributes
    ----------
    month_min, month_max
        Inclusive lexicographic bounds on the ``month`` column (``YYYY-MM``).
    towns, flat_types, flat_models, storey_ranges
        Canonical (stripped, upper-cased) sets of observed values.
    floor_area_lower, floor_area_upper
        ``[Q1 - k*IQR, Q3 + k*IQR]`` bounds for ``floor_area_sqm``, with
        ``k = config.VALIDATE_FLOOR_AREA_IQR_MULT``. Used to *flag*, not
        fail, structurally valid but statistically extreme rows.
    """

    month_min: str
    month_max: str
    towns: frozenset[str]
    flat_types: frozenset[str]
    flat_models: frozenset[str]
    storey_ranges: frozenset[str]
    floor_area_lower: float
    floor_area_upper: float


def derive_spec(df: pd.DataFrame) -> ValidationSpec:
    """Build a :class:`ValidationSpec` from a reference DataFrame.

    Categorical sets are normalized before being frozen, so downstream rules
    can compare against canonical forms without re-normalizing the spec.
    """
    floor = df["floor_area_sqm"].dropna().astype(float)
    q1, q3 = floor.quantile([0.25, 0.75])
    iqr = q3 - q1
    mult = config.VALIDATE_FLOOR_AREA_IQR_MULT

    return ValidationSpec(
        month_min=str(df["month"].min()),
        month_max=str(df["month"].max()),
        towns=frozenset(_normalize_categorical(df["town"]).dropna().unique()),
        flat_types=frozenset(
            _normalize_categorical(df["flat_type"]).dropna().unique()
        ),
        flat_models=frozenset(
            _normalize_categorical(df["flat_model"]).dropna().unique()
        ),
        storey_ranges=frozenset(
            _normalize_categorical(df["storey_range"]).dropna().unique()
        ),
        floor_area_lower=float(q1 - mult * iqr),
        floor_area_upper=float(q3 + mult * iqr),
    )


# ---------------------------------------------------------------------------
# Per-rule helpers — each returns a boolean mask of failing rows.
# ---------------------------------------------------------------------------
#
# Convention: True in the returned mask means the row FAILS the rule.
# NaN/missing values are treated as failures (see _coerce_mask) so that an
# unevaluable row is never silently passed.


def _coerce_mask(mask: pd.Series) -> np.ndarray:
    """Convert a (possibly nullable) boolean mask to a plain numpy array.

    NaN entries become ``True`` (failure) — the safer default than passing
    rows that couldn't be evaluated.
    """
    return mask.fillna(True).to_numpy(dtype=bool)


def _rule_month_format(df: pd.DataFrame) -> pd.Series:
    """Fail rows whose ``month`` is not ``YYYY-MM`` or doesn't parse."""
    s = df["month"].astype("string")
    structural = s.str.match(_MONTH_RE).fillna(False)
    parses = pd.to_datetime(s, format="%Y-%m", errors="coerce").notna()
    return ~(structural & parses)


def _rule_month_in_range(df: pd.DataFrame, spec: ValidationSpec) -> pd.Series:
    """Fail rows whose ``month`` falls outside the observed master range.

    Tautological on the master itself, but the substantive guard for any
    future batch validated against this spec.
    """
    s = df["month"].astype("string")
    return ~((s >= spec.month_min) & (s <= spec.month_max))


def _rule_in_set(
    df: pd.DataFrame, col: str, allowed: frozenset[str]
) -> pd.Series:
    """Fail rows whose normalized ``col`` value is not in ``allowed``."""
    return ~_normalize_categorical(df[col]).isin(allowed)


def _rule_storey_range_format(df: pd.DataFrame) -> pd.Series:
    """Fail rows whose ``storey_range`` doesn't parse as ``NN TO NN`` with hi > lo."""
    s = _normalize_categorical(df["storey_range"])
    parts = s.str.extract(_STOREY_RE)
    lo = pd.to_numeric(parts[0], errors="coerce")
    hi = pd.to_numeric(parts[1], errors="coerce")
    structural_ok = lo.notna() & hi.notna() & (hi > lo)
    return ~structural_ok


def _rule_positive(df: pd.DataFrame, col: str) -> pd.Series:
    """Fail rows where ``col`` is not strictly greater than zero."""
    return ~(pd.to_numeric(df[col], errors="coerce") > 0)


def _rule_floor_area_within_iqr(
    df: pd.DataFrame, spec: ValidationSpec
) -> pd.Series:
    """Flag rows whose ``floor_area_sqm`` falls outside the IQR-derived band."""
    s = pd.to_numeric(df["floor_area_sqm"], errors="coerce")
    return ~((s >= spec.floor_area_lower) & (s <= spec.floor_area_upper))


def _rule_lease_commence_year(df: pd.DataFrame) -> pd.Series:
    """Fail rows whose ``lease_commence_date`` is outside [LEASE_MIN_YEAR, this year]."""
    s = pd.to_numeric(df["lease_commence_date"], errors="coerce")
    return ~((s >= config.LEASE_MIN_YEAR) & (s <= date.today().year))


def _rule_no_nulls_in_key(df: pd.DataFrame, key_cols: list[str]) -> pd.Series:
    """Fail rows with any null in the composite-key columns."""
    return df[key_cols].isna().any(axis=1)


# ---------------------------------------------------------------------------
# Composite key resolution
# ---------------------------------------------------------------------------


def _composite_key_columns(df: pd.DataFrame) -> list[str]:
    """All raw columns except the price and pipeline metadata.

    Mirrors the dedupe key the brief asks for in Stage 5: every column that
    identifies a transaction, but not the price (which is the value being
    deduped on) and not pipeline-added lineage columns.
    """
    excluded = {"resale_price", *config.PIPELINE_METADATA_COLUMNS}
    # remaining_lease columns are excluded from the key — they are absent
    # for the pre-2015 vintages by design and would otherwise fail every
    # such row on the no-null rule.
    excluded.update({"remaining_lease", "remaining_lease_years_original"})
    return [c for c in df.columns if c not in excluded]


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------


@dataclass
class ValidationResult:
    """Output of :func:`validate_master`.

    Attributes
    ----------
    passed_df
        Rows that passed every rule. Same columns as the input frame.
    failed_df
        Rows that failed at least one rule. Same columns as the input frame
        plus a ``failure_reasons`` column listing the failed rule names,
        semicolon-separated.
    report
        Per-rule summary DataFrame with columns ``rule, rows_checked,
        rows_failed, fail_pct, sample_failures``.
    spec
        The :class:`ValidationSpec` used during validation.
    """

    passed_df: pd.DataFrame
    failed_df: pd.DataFrame
    report: pd.DataFrame
    spec: ValidationSpec


# ---------------------------------------------------------------------------
# Top-level entrypoint
# ---------------------------------------------------------------------------


def validate_master(
    df: pd.DataFrame,
    spec: ValidationSpec | None = None,
) -> ValidationResult:
    """Run all validation rules and return a :class:`ValidationResult`.

    Pure function — does not mutate ``df``.

    Parameters
    ----------
    df
        DataFrame to validate. Typically the combined master from
        :func:`src.combine.combine_raw_files`.
    spec
        Optional precomputed :class:`ValidationSpec`. If ``None`` the spec is
        derived from ``df`` itself, which makes the categorical-set rules
        tautological on the master but well-defined when the same spec is
        later applied to a future batch.

    Returns
    -------
    ValidationResult
        Structured result with passed/failed splits and a summary report.
    """
    if spec is None:
        spec = derive_spec(df)

    key_cols = _composite_key_columns(df)

    # (rule_name, mask). Order matters only for failure_reasons display.
    rules: list[tuple[str, pd.Series]] = [
        ("month_format", _rule_month_format(df)),
        ("month_in_observed_range", _rule_month_in_range(df, spec)),
        ("town_in_observed_set", _rule_in_set(df, "town", spec.towns)),
        ("flat_type_in_observed_set", _rule_in_set(df, "flat_type", spec.flat_types)),
        ("flat_model_in_observed_set", _rule_in_set(df, "flat_model", spec.flat_models)),
        ("storey_range_format", _rule_storey_range_format(df)),
        (
            "storey_range_in_observed_set",
            _rule_in_set(df, "storey_range", spec.storey_ranges),
        ),
        ("floor_area_positive", _rule_positive(df, "floor_area_sqm")),
        (
            "floor_area_within_iqr_bounds",
            _rule_floor_area_within_iqr(df, spec),
        ),
        ("resale_price_positive", _rule_positive(df, "resale_price")),
        ("lease_commence_year_in_range", _rule_lease_commence_year(df)),
        ("composite_key_no_nulls", _rule_no_nulls_in_key(df, key_cols)),
    ]

    n = len(df)
    reason_lists: list[list[str]] = [[] for _ in range(n)]
    report_rows: list[dict[str, object]] = []

    for name, mask in rules:
        mask_arr = _coerce_mask(mask)
        failing_idx = np.where(mask_arr)[0]
        n_failed = int(failing_idx.size)

        if n_failed:
            sample = (
                df.iloc[failing_idx[: config.VALIDATE_SAMPLE_FAILURES]]
                .to_dict(orient="records")
            )
        else:
            sample = []

        report_rows.append(
            {
                "rule": name,
                "rows_checked": n,
                "rows_failed": n_failed,
                "fail_pct": (n_failed / n) if n else 0.0,
                "sample_failures": sample,
            }
        )

        for i in failing_idx:
            reason_lists[i].append(name)

    joined = np.array(["; ".join(rs) for rs in reason_lists], dtype=object)
    fail_mask = np.array([bool(rs) for rs in reason_lists], dtype=bool)

    passed_df = df.iloc[~fail_mask].copy()
    failed_df = df.iloc[fail_mask].copy()
    failed_df["failure_reasons"] = joined[fail_mask]

    report = pd.DataFrame(report_rows)
    return ValidationResult(
        passed_df=passed_df,
        failed_df=failed_df,
        report=report,
        spec=spec,
    )
