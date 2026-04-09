"""Stage 3: Custom data profiling.

Hand-rolled in preference to ``ydata-profiling`` so the report is small,
predictable, and tailored to what later stages (validation, cleaning) need
to know about the master DataFrame. The output is a :class:`ProfileReport`
holding pre-built DataFrames so the notebook can render each section with a
single ``display(...)`` call.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

import pandas as pd

from src import config

# Columns excluded from per-column string profiling. ``source_file`` is
# pipeline-added lineage metadata — by construction it has exactly one value
# per source file, so its top-values table is uninformative.
_STRING_PROFILE_EXCLUDE: tuple[str, ...] = ("source_file",)


# ---------------------------------------------------------------------------
# Report container
# ---------------------------------------------------------------------------


@dataclass
class ProfileReport:
    """Structured profiling output for a DataFrame.

    Attributes
    ----------
    shape
        ``(n_rows, n_cols)`` of the profiled DataFrame.
    overview
        One row per column with dtype, null count, null %, unique count, and
        a short string of sample values.
    numeric
        Per-numeric-column descriptive statistics (min/max/mean/median/std/
        q1/q3). Empty if the frame has no numeric columns.
    string_lengths
        Per-object-column min/max/mean string length. Empty if no eligible
        object columns.
    string_top_values
        Mapping ``column -> value_counts().head(N)`` DataFrame. One entry per
        eligible object column.
    month_range
        ``(min, max)`` of the ``month`` column as ``YYYY-MM`` strings.
        ``None`` if the frame has no ``month`` column.
    flags
        DataFrame with columns ``column, flag, detail`` listing every
        column-flag pair the profile triggered (high null %, constant,
        whitespace contamination).
    """

    shape: tuple[int, int]
    overview: pd.DataFrame
    numeric: pd.DataFrame
    string_lengths: pd.DataFrame
    string_top_values: dict[str, pd.DataFrame] = field(default_factory=dict)
    month_range: tuple[str, str] | None = None
    flags: pd.DataFrame = field(default_factory=pd.DataFrame)


# ---------------------------------------------------------------------------
# Per-section helpers
# ---------------------------------------------------------------------------


def _sample_values(series: pd.Series, n: int) -> str:
    """Return up to ``n`` distinct non-null sample values as a short string.

    Used purely for human display in the overview table; the goal is to give
    a reader a feel for what's in the column without dumping the full series.
    """
    distinct = series.dropna().drop_duplicates().head(n).tolist()
    return ", ".join(repr(v) for v in distinct)


def _overview_table(df: pd.DataFrame) -> pd.DataFrame:
    """Build the per-column overview table.

    One row per column. ``nunique(dropna=True)`` ensures nullable Int64
    behaves the same as plain int64.
    """
    n = len(df)
    rows = []
    for col in df.columns:
        s = df[col]
        nulls = int(s.isna().sum())
        rows.append(
            {
                "column": col,
                "dtype": str(s.dtype),
                "null_count": nulls,
                "null_pct": (nulls / n) if n else 0.0,
                "unique_count": int(s.nunique(dropna=True)),
                "sample_values": _sample_values(s, config.PROFILE_SAMPLE_VALUES),
            }
        )
    return pd.DataFrame(rows)


def _numeric_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Per-numeric-column min/max/mean/median/std/q1/q3.

    ``.describe()`` is avoided so we can control column order, skip the row
    count (already in the overview), and present nullable Int64 stats as
    plain floats for clean display.
    """
    numeric = df.select_dtypes(include="number")
    if numeric.empty:
        return pd.DataFrame()

    quartiles = numeric.quantile([0.25, 0.5, 0.75])
    out = pd.DataFrame(
        {
            "min": numeric.min(),
            "max": numeric.max(),
            "mean": numeric.mean(),
            "median": numeric.median(),
            "std": numeric.std(),
            "q1": quartiles.loc[0.25],
            "q3": quartiles.loc[0.75],
        }
    )
    # Cast to float for display: Int64 columns produce a mixed-dtype frame
    # otherwise, which renders inconsistently.
    return out.astype(float)


def _string_columns(df: pd.DataFrame, exclude: Iterable[str]) -> list[str]:
    """Object-dtype columns eligible for string profiling."""
    excluded = set(exclude)
    return [c for c in df.select_dtypes(include=["object", "string"]).columns if c not in excluded]


def _string_length_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Per-string-column length statistics (min/max/mean)."""
    cols = _string_columns(df, _STRING_PROFILE_EXCLUDE)
    if not cols:
        return pd.DataFrame()

    rows = []
    for col in cols:
        lengths = df[col].dropna().astype(str).str.len()
        rows.append(
            {
                "column": col,
                "min_len": int(lengths.min()) if not lengths.empty else 0,
                "max_len": int(lengths.max()) if not lengths.empty else 0,
                "mean_len": float(lengths.mean()) if not lengths.empty else 0.0,
            }
        )
    return pd.DataFrame(rows)


def _top_values(df: pd.DataFrame, n: int) -> dict[str, pd.DataFrame]:
    """``value_counts().head(n)`` per eligible string column."""
    out: dict[str, pd.DataFrame] = {}
    for col in _string_columns(df, _STRING_PROFILE_EXCLUDE):
        vc = df[col].value_counts(dropna=False).head(n)
        out[col] = vc.rename_axis(col).reset_index(name="count")
    return out


def _month_range(df: pd.DataFrame) -> tuple[str, str] | None:
    """Min/max of the ``month`` column.

    Lexicographic comparison is correct because the column is a ``YYYY-MM``
    string (zero-padded month component).
    """
    if "month" not in df.columns or df["month"].dropna().empty:
        return None
    return (str(df["month"].min()), str(df["month"].max()))


def _flag_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Surface columns that warrant human review.

    Three flag types are emitted:

    * ``high_null`` — null fraction exceeds ``PROFILE_HIGH_NULL_THRESHOLD``.
      For the in-scope master this is expected on
      ``remaining_lease`` / ``remaining_lease_years_original`` (pre-2015
      vintages don't carry the column), so the flag confirms a known schema
      gap rather than a defect.
    * ``constant`` — only one distinct non-null value. Such columns add no
      information and should usually be dropped or investigated.
    * ``whitespace`` — object column where at least one value differs from
      its ``.str.strip()`` form. Empty on the current in-scope master, but
      kept as a guard so any future vintage that introduces padded values
      surfaces it before it can poison Stage 4's allowed-set inference.
    """
    n = len(df)
    rows: list[dict[str, object]] = []

    for col in df.columns:
        s = df[col]
        null_pct = (s.isna().sum() / n) if n else 0.0
        if null_pct > config.PROFILE_HIGH_NULL_THRESHOLD:
            rows.append(
                {
                    "column": col,
                    "flag": "high_null",
                    "detail": f"{null_pct:.1%} nulls",
                }
            )
        if s.nunique(dropna=True) <= 1:
            rows.append(
                {
                    "column": col,
                    "flag": "constant",
                    "detail": f"unique_count={int(s.nunique(dropna=True))}",
                }
            )

    for col in _string_columns(df, _STRING_PROFILE_EXCLUDE):
        s = df[col].dropna().astype(str)
        if (s != s.str.strip()).any():
            n_dirty = int((s != s.str.strip()).sum())
            rows.append(
                {
                    "column": col,
                    "flag": "whitespace",
                    "detail": f"{n_dirty} values with leading/trailing whitespace",
                }
            )

    return pd.DataFrame(rows, columns=["column", "flag", "detail"])


# ---------------------------------------------------------------------------
# Top-level entrypoint
# ---------------------------------------------------------------------------


def profile_dataset(df: pd.DataFrame) -> ProfileReport:
    """Profile a DataFrame and return a :class:`ProfileReport`.

    Pure function — does not mutate ``df``. Composes the per-section
    helpers above into a single structured report.

    Parameters
    ----------
    df
        DataFrame to profile. Typically the combined master from
        :func:`src.combine.combine_raw_files`, but any frame is accepted.

    Returns
    -------
    ProfileReport
        Structured profile suitable for direct display in the notebook.
    """
    return ProfileReport(
        shape=df.shape,
        overview=_overview_table(df),
        numeric=_numeric_stats(df),
        string_lengths=_string_length_stats(df),
        string_top_values=_top_values(df, config.PROFILE_TOP_N_VALUES),
        month_range=_month_range(df),
        flags=_flag_columns(df),
    )
