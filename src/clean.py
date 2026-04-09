"""Stage 5: Clean the validation-passing master DataFrame.

Three transformations, in order:

1. **Recompute remaining lease** as of ``config.AS_OF_DATE`` (default: today)
   from each row's ``lease_commence_date``. Adds two canonical columns:
   ``remaining_lease_computed_months`` (Int64) and
   ``remaining_lease_computed`` ("Y years M months"). Rows whose lease has
   already expired (computed months < 0) are split off and excluded from
   the cleaned output.
2. **Deduplicate** on the composite key. The composite key is every column
   that identifies a transaction — i.e. all raw columns except the price,
   lineage, vintage-specific lease columns, and any pipeline-added columns.
   On a tie, the row with the highest ``resale_price`` wins; the lower-price
   losers are returned for separate inspection.
3. **Flag price anomalies** via asymmetric IQR on price-per-sqm grouped by
   ``(town, flat_type, year)``. The bounds are
   ``[Q1 − k_lo·IQR, Q3 + k_hi·IQR]`` with ``k_lo < k_hi`` (config) — tighter
   on the cheap side because impossibly cheap flats are more suspicious than
   luxury outliers. Anomalies are *flagged*, never dropped.

This module is pure: it does not touch the filesystem. The notebook is
responsible for writing the cleaned frame and the various failed/flagged
side outputs.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from src import config


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------


@dataclass
class CleanResult:
    """Output of :func:`clean_master`.

    Attributes
    ----------
    cleaned_df
        The final cleaned dataset, including the recomputed lease columns
        and the price anomaly flag columns. Anomaly rows are *included* —
        the flag is the entire point.
    expired_df
        Rows dropped because their recomputed lease is negative. Carries a
        ``failure_reasons`` column with the value ``"lease_expired"`` so it
        can be appended to the validation failures file in the same shape.
    duplicate_losers_df
        Rows discarded by the dedupe step (same composite key as a kept row,
        but lower ``resale_price``).
    anomalies_df
        Subset of ``cleaned_df`` where ``price_anomaly_flag`` is True.
        Provided as a convenience for the notebook side-output.
    stage_counts
        Row counts at each step of the clean pipeline, for the end-of-run
        summary table.
    """

    cleaned_df: pd.DataFrame
    expired_df: pd.DataFrame
    duplicate_losers_df: pd.DataFrame
    anomalies_df: pd.DataFrame
    stage_counts: dict[str, int]


# ---------------------------------------------------------------------------
# Composite key
# ---------------------------------------------------------------------------


# Columns that are NOT part of the dedupe identity:
#   - resale_price: the value being deduped on, by definition not part of the key
#   - source_file: lineage metadata
#   - remaining_lease, remaining_lease_years_original: vintage-dependent;
#     the same transaction shows up with NaN in pre-2015 vintages
#   - all clean-stage additions
_NON_KEY_COLUMNS: frozenset[str] = frozenset(
    {
        "resale_price",
        "source_file",
        "remaining_lease",
        "remaining_lease_years_original",
        "remaining_lease_computed",
        "remaining_lease_computed_months",
        "price_anomaly_flag",
        "price_anomaly_reason",
    }
)


def composite_key_columns(df: pd.DataFrame) -> list[str]:
    """Return the dedupe key columns present in ``df``.

    Used by :func:`deduplicate`. Public so the notebook can show the user
    exactly which columns make up the key.
    """
    return [c for c in df.columns if c not in _NON_KEY_COLUMNS]


# ---------------------------------------------------------------------------
# Step 1: lease recompute
# ---------------------------------------------------------------------------


def _format_years_months(months_total: int) -> str:
    """Format an integer month count as ``"Y years M months"``.

    Negative inputs return ``pd.NA`` — they correspond to expired leases,
    which are filtered out of the cleaned output.
    """
    if months_total < 0:
        return pd.NA  # type: ignore[return-value]
    years, months = divmod(months_total, 12)
    return f"{years} years {months} months"


def recompute_remaining_lease(
    df: pd.DataFrame,
    as_of: date | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Add the recomputed-lease columns; split off expired rows.

    Parameters
    ----------
    df
        Input frame. Must contain a ``lease_commence_date`` column (year as
        an integer or coercible to one).
    as_of
        Reference date for the recomputation. Defaults to
        :data:`config.AS_OF_DATE`.

    Returns
    -------
    (kept_df, expired_df)
        ``kept_df`` is the input with two new columns:
        ``remaining_lease_computed_months`` (Int64) and
        ``remaining_lease_computed`` (string "Y years M months").
        ``expired_df`` holds the rows whose computed months were negative,
        annotated with a ``failure_reasons`` column for the failed-files
        side output.

    Notes
    -----
    The HDB CSV ``lease_commence_date`` column is a *year* (e.g. 1979), not
    a calendar date. We treat the lease as starting on January 1 of that
    year and expiring on January 1 of ``year + LEASE_TERM_YEARS``. The
    "round down to years and months" requirement from the brief is then a
    straightforward integer-month subtraction with a single day-of-month
    adjustment when ``as_of`` is past the 1st.
    """
    as_of = as_of or config.AS_OF_DATE
    df = df.copy()

    lc = pd.to_numeric(df["lease_commence_date"], errors="coerce").astype("Int64")
    expiry_year = lc + config.LEASE_TERM_YEARS

    # Expiry is the 1st of January, so the month-of-year delta is `1 - as_of.month`
    # and we subtract one extra month whenever as_of is past the 1st of its month
    # (because we have not yet completed the partial month).
    months_total = (expiry_year - as_of.year) * 12 + (1 - as_of.month)
    if as_of.day > 1:
        months_total = months_total - 1

    df["remaining_lease_computed_months"] = months_total.astype("Int64")
    df["remaining_lease_computed"] = (
        df["remaining_lease_computed_months"]
        .map(_format_years_months, na_action="ignore")
        .astype("string")
    )

    expired_mask = df["remaining_lease_computed_months"].fillna(-1) < 0
    expired_df = df.loc[expired_mask].copy()
    expired_df["failure_reasons"] = "lease_expired"
    kept_df = df.loc[~expired_mask].copy()
    return kept_df, expired_df


# ---------------------------------------------------------------------------
# Step 2: deduplicate
# ---------------------------------------------------------------------------


def deduplicate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Drop duplicate composite keys, keeping the highest ``resale_price``.

    Sort + ``duplicated(keep="first")`` is used rather than ``groupby.idxmax``
    so the discarded rows can be returned in the same shape as the kept ones,
    which is what the brief asks for (write losers to a side file for review).

    Returns
    -------
    (kept_df, losers_df)
        Both frames have the original column order; original row order
        within the kept frame is preserved (we sort once and re-sort by index
        before returning).
    """
    key = composite_key_columns(df)
    sorted_df = df.sort_values("resale_price", ascending=False, kind="mergesort")
    is_first = ~sorted_df.duplicated(subset=key, keep="first")
    kept = sorted_df.loc[is_first].sort_index()
    losers = sorted_df.loc[~is_first].sort_index()
    return kept, losers


# ---------------------------------------------------------------------------
# Step 3: anomaly flag
# ---------------------------------------------------------------------------


def flag_price_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``price_anomaly_flag`` and ``price_anomaly_reason`` columns.

    Heuristic: compute price-per-sqm, then within each
    ``(town, flat_type, year)`` group derive Q1 and Q3. Flag rows whose
    price-per-sqm falls outside ``[Q1 − k_lo·IQR, Q3 + k_hi·IQR]``. The
    multipliers are read from :data:`config.IQR_LOWER_MULT` and
    :data:`config.IQR_UPPER_MULT`; the lower side is intentionally tighter
    because impossibly cheap flats are more suspicious than luxury outliers.

    Singleton groups (one observation in a given (town, flat_type, year))
    have IQR = 0, so the bounds collapse to the single value and the row
    is never flagged. This is the safe behaviour: we do not flag a row as
    an outlier when there is nothing to compare it to.
    """
    df = df.copy()

    pps = df["resale_price"].astype(float) / df["floor_area_sqm"].astype(float)
    year = df["month"].astype("string").str[:4]

    grouped = pps.groupby([df["town"], df["flat_type"], year])
    q1 = grouped.transform(lambda s: s.quantile(0.25))
    q3 = grouped.transform(lambda s: s.quantile(0.75))
    iqr = q3 - q1
    lower = q1 - config.IQR_LOWER_MULT * iqr
    upper = q3 + config.IQR_UPPER_MULT * iqr

    below = pps < lower
    above = pps > upper

    df["price_anomaly_flag"] = (below | above).fillna(False)
    df["price_anomaly_reason"] = pd.Series(pd.NA, index=df.index, dtype="string")
    df.loc[below, "price_anomaly_reason"] = "below_lower_iqr_bound"
    df.loc[above, "price_anomaly_reason"] = "above_upper_iqr_bound"
    return df


# ---------------------------------------------------------------------------
# Top-level entrypoint
# ---------------------------------------------------------------------------


def clean_master(
    passed_df: pd.DataFrame,
    as_of: date | None = None,
) -> CleanResult:
    """Run lease recompute → dedupe → anomaly flag and return a :class:`CleanResult`.

    Pure function — does not mutate ``passed_df`` or write any files. The
    notebook orchestrates the file writes for the cleaned frame and the
    failed/flagged side outputs.

    Parameters
    ----------
    passed_df
        Validation-passing rows (typically ``ValidationResult.passed_df``
        from :mod:`src.validate`).
    as_of
        Reference date for the lease recomputation. Defaults to
        :data:`config.AS_OF_DATE`.
    """
    n_in = len(passed_df)

    after_lease, expired = recompute_remaining_lease(passed_df, as_of=as_of)
    n_after_lease = len(after_lease)

    after_dedupe, losers = deduplicate(after_lease)
    n_after_dedupe = len(after_dedupe)

    cleaned = flag_price_anomalies(after_dedupe)
    anomalies = cleaned.loc[cleaned["price_anomaly_flag"]].copy()

    return CleanResult(
        cleaned_df=cleaned,
        expired_df=expired,
        duplicate_losers_df=losers,
        anomalies_df=anomalies,
        stage_counts={
            "input_validation_passed": n_in,
            "expired_lease_dropped": len(expired),
            "after_lease_recompute": n_after_lease,
            "duplicates_dropped": len(losers),
            "after_dedupe": n_after_dedupe,
            "anomalies_flagged": len(anomalies),
            "final_cleaned_rows": len(cleaned),
        },
    )
