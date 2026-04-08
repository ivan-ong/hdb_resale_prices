"""Stage 2: Combine raw CSVs into a single master DataFrame.

Reads every CSV in ``data/raw/``, normalizes headers, filters to the
configured scope window, tags each row with its source file, and unions the
frames into a single master. Schema union is taken automatically by
``pd.concat`` — files missing a column (e.g. ``remaining_lease`` in the
pre-2015 vintages) get NaN for that column in the master.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-file transforms
# ---------------------------------------------------------------------------


def _normalize_header(name: str) -> str:
    """Normalize a single CSV header.

    The HDB CSVs have historically drifted in case and whitespace across
    vintages (e.g. ``Month`` vs ``month``). Normalizing on read prevents the
    schema union from producing duplicate columns like ``Month`` and ``month``.
    """
    return name.strip().lower().replace(" ", "_")


def _read_normalized_csv(path: Path) -> pd.DataFrame:
    """Read a CSV and normalize its column headers in place."""
    df = pd.read_csv(path)
    df.columns = [_normalize_header(c) for c in df.columns]
    return df


def _filter_to_scope(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows whose ``month`` falls outside the configured scope window.

    The ``month`` column is a ``YYYY-MM`` string, so simple lexicographic
    comparison against ``SCOPE_START`` / ``SCOPE_END`` is correct.

    This is a no-op for files that are entirely in scope (Mar 2012–Dec 2014
    and Jan 2015–Dec 2016) and trims the 2000–Feb 2012 file down to its
    Jan–Feb 2012 tail. Applying it uniformly keeps the logic principled
    rather than special-casing one file.
    """
    return df[
        (df["month"] >= config.SCOPE_START) & (df["month"] <= config.SCOPE_END)
    ].copy()


def _normalize_remaining_lease(df: pd.DataFrame) -> pd.DataFrame:
    """Add a canonical ``remaining_lease_years_original`` integer column.

    For the in-scope vintages:

    * Pre-2015 files do not have a ``remaining_lease`` column at all. The
      canonical column is left absent on this frame; ``pd.concat`` will
      introduce it as NaN when frames are unioned.
    * The Jan 2015–Dec 2016 file stores ``remaining_lease`` as integer years
      already (verified by inspection: dtype int64, sample values like 70,
      65, 64). We copy it into the canonical column unchanged but cast to
      pandas' nullable ``Int64`` so that the unioned master can hold NaN for
      the pre-2015 rows without losing the integer dtype.

    The 2017+ file uses an ``"X years Y months"`` string format, but it is
    out of scope and not parsed here.
    """
    if "remaining_lease" in df.columns:
        df["remaining_lease_years_original"] = pd.to_numeric(
            df["remaining_lease"], errors="coerce"
        ).astype("Int64")
    return df


# ---------------------------------------------------------------------------
# Top-level entrypoint
# ---------------------------------------------------------------------------


def combine_raw_files(raw_dir: Path | None = None) -> pd.DataFrame:
    """Combine all CSVs in ``data/raw/`` into a single master DataFrame.

    Pipeline per file:

      1. Read with normalized headers.
      2. Filter to the scope window on the ``month`` column.
      3. Normalize ``remaining_lease`` into the canonical Int64 column.
      4. Tag with ``source_file`` for lineage.

    All frames are then concatenated with ``sort=False`` so the master takes
    the union of columns and preserves the order of first appearance.

    Parameters
    ----------
    raw_dir
        Directory to scan for CSVs. Defaults to :data:`config.RAW_DIR`.

    Returns
    -------
    pd.DataFrame
        The combined master dataset.
    """
    raw_dir = raw_dir or config.RAW_DIR
    csv_paths = sorted(raw_dir.glob("*.csv"))
    if not csv_paths:
        raise FileNotFoundError(f"No CSVs found in {raw_dir}")

    frames: list[pd.DataFrame] = []
    for path in csv_paths:
        df = _read_normalized_csv(path)
        raw_rows = len(df)
        df = _filter_to_scope(df)
        in_scope_rows = len(df)
        df = _normalize_remaining_lease(df)
        # Lineage tag — added last so it doesn't interfere with the schema-
        # union logic above. Always present, populated, and non-null.
        df["source_file"] = path.name
        logger.info(
            "%s: %d rows raw, %d in scope (%s..%s)",
            path.name,
            raw_rows,
            in_scope_rows,
            config.SCOPE_START,
            config.SCOPE_END,
        )
        frames.append(df)

    # `sort=False` keeps column order stable (order of first appearance)
    # rather than alphabetizing — easier for a human reader to scan.
    master = pd.concat(frames, ignore_index=True, sort=False)

    # After concat, the canonical lease column may have been promoted to
    # float64 because of NaN-merging across frames. Re-cast to nullable Int64
    # so it stays integer-valued where present.
    if "remaining_lease_years_original" in master.columns:
        master["remaining_lease_years_original"] = master[
            "remaining_lease_years_original"
        ].astype("Int64")

    logger.info(
        "Combined master: %d rows, %d columns from %d files",
        len(master),
        master.shape[1],
        len(frames),
    )
    return master
