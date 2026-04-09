"""Stage 6: Build the Resale Identifier, dedupe on it, and hash it.

Implements the brief's *Data Transformation Requirements*:

1. Build a 9-character ``resale_identifier`` for every cleaned row from
   ``block``, the per-group average ``resale_price``, ``month``, and ``town``.
2. Deduplicate on the identifier — because the identifier compresses
   ``block`` to 3 digits and price to 2 digits, it is intentionally lossy
   and different flats can collide on it. On a collision, keep the row
   with the highest ``resale_price`` (brief Transformation §2).
3. Hash the identifier with SHA-256 for the Hashed output group.

Pure module: no filesystem access. The notebook writes the Transformed
and Hashed output files.
"""

from __future__ import annotations

import hashlib
import re

import pandas as pd

from src import config

# ---------------------------------------------------------------------------
# Regex / constants
# ---------------------------------------------------------------------------

# Matches the YYYY-MM format used by the source data's `month` column.
_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")

# Pre-compiled for `extract_block_digits` — matches every non-digit char.
_NON_DIGIT_RE = re.compile(r"\D")

# Temporary column name used inside `build_resale_identifier` to carry the
# per-group average price. Private so callers don't come to depend on it.
_GROUP_AVG_COL = "_group_avg_price"


# ---------------------------------------------------------------------------
# Per-component extractors
# ---------------------------------------------------------------------------


def extract_block_digits(block: str) -> str:
    """Return the first ``IDENTIFIER_BLOCK_DIGITS`` digits of a block string.

    Strips every non-digit character first, takes the leading digits, then
    left-pads with zeros so the result is always exactly
    :data:`config.IDENTIFIER_BLOCK_DIGITS` characters.

    Parameters
    ----------
    block
        Raw value from the ``block`` column (e.g. ``"456A"``, ``"10"``).

    Returns
    -------
    str
        A 3-character numeric string.

    Raises
    ------
    ValueError
        If stripping non-digits leaves an empty string. This indicates a
        genuine data-quality defect (the block column should always contain
        at least one digit) and failing loudly beats silently emitting
        ``"000"``, which would collide with every other digit-less block.

    Examples
    --------
    >>> extract_block_digits("456A")
    '456'
    >>> extract_block_digits("10")
    '010'
    >>> extract_block_digits("2B")
    '002'
    >>> extract_block_digits("1234")
    '123'
    """
    digits = _NON_DIGIT_RE.sub("", str(block))
    if not digits:
        raise ValueError(f"block {block!r} contains no digits after stripping")
    # Take the first N, then left-pad. Padding is a no-op when len >= N.
    return digits[: config.IDENTIFIER_BLOCK_DIGITS].zfill(
        config.IDENTIFIER_BLOCK_DIGITS
    )


def extract_price_digits(average_price: float) -> str:
    """Return the first ``IDENTIFIER_PRICE_DIGITS`` digits of the integer price.

    Takes ``int(average_price)`` (truncation, not rounding — per the brief's
    wording "1st and 2nd digit of the average resale price"), stringifies,
    and returns the first N characters. Single-digit integer parts are
    left-padded with zero so the result is always exactly N characters.

    Parameters
    ----------
    average_price
        Per-group mean resale price (float).

    Returns
    -------
    str
        A 2-character numeric string.

    Examples
    --------
    >>> extract_price_digits(230000.0)
    '23'
    >>> extract_price_digits(234567.89)
    '23'
    >>> extract_price_digits(98000.0)
    '98'
    >>> extract_price_digits(5.0)
    '05'
    """
    n = config.IDENTIFIER_PRICE_DIGITS
    return str(int(average_price))[:n].zfill(n)


def extract_month_suffix(month: str) -> str:
    """Return the 2-digit month portion of a ``YYYY-MM`` string.

    Parameters
    ----------
    month
        Value from the ``month`` column.

    Returns
    -------
    str
        The two trailing characters (``"MM"``).

    Raises
    ------
    ValueError
        If ``month`` does not match ``YYYY-MM``. Validation is strict because
        this function is called on every row and any format drift would
        poison the identifier silently.
    """
    if not _MONTH_RE.match(str(month)):
        raise ValueError(f"month {month!r} does not match YYYY-MM")
    return str(month)[-2:]


def extract_town_initial(town: str) -> str:
    """Return the first character of a town name, stripped and upper-cased.

    Parameters
    ----------
    town
        Value from the ``town`` column.

    Returns
    -------
    str
        A single uppercase character.

    Raises
    ------
    ValueError
        If the town is empty or whitespace-only after stripping.
    """
    stripped = str(town).strip()
    if not stripped:
        raise ValueError(f"town {town!r} is empty after stripping")
    return stripped[0].upper()


# ---------------------------------------------------------------------------
# Group averages
# ---------------------------------------------------------------------------


def compute_group_averages(df: pd.DataFrame) -> pd.DataFrame:
    """Mean ``resale_price`` grouped by (``month``, ``town``, ``flat_type``).

    Returned shape is a long-format DataFrame with the three grouping columns
    plus a ``_group_avg_price`` column, suitable for :meth:`DataFrame.merge`
    onto the cleaned frame. This is computed *before* any identifier-collision
    dedupe so every transaction in a group contributes to its own average,
    which is what the brief asks for ("the average X in Y").

    Parameters
    ----------
    df
        Cleaned master DataFrame (Stage 5 output).
    """
    return (
        df.groupby(["month", "town", "flat_type"], as_index=False)["resale_price"]
        .mean()
        .rename(columns={"resale_price": _GROUP_AVG_COL})
    )


# ---------------------------------------------------------------------------
# Identifier construction
# ---------------------------------------------------------------------------


def _build_identifier_row(row: pd.Series) -> str:
    """Assemble one identifier from a row containing the required inputs.

    Kept as a small helper so :func:`build_resale_identifier` can use
    ``.apply`` without an opaque lambda. The row must carry ``block``,
    ``_group_avg_price``, ``month``, and ``town`` columns.
    """
    return (
        "S"
        + extract_block_digits(row["block"])
        + extract_price_digits(row[_GROUP_AVG_COL])
        + extract_month_suffix(row["month"])
        + extract_town_initial(row["town"])
    )


def build_resale_identifier(df: pd.DataFrame) -> pd.DataFrame:
    """Add a ``resale_identifier`` column to ``df`` and return the new frame.

    The identifier layout (brief Transformation §1):

    ========  =======  =============================================
    Position  Length   Source
    ========  =======  =============================================
    1         1        Literal ``"S"``
    2–4       3        First 3 digits of ``block`` (non-digits stripped, zero-padded)
    5–6       2        First 2 digits of ``int(group_mean(resale_price))``
    7–8       2        ``month[-2:]`` (``MM`` portion of ``YYYY-MM``)
    9         1        First character of ``town``, upper-cased
    ========  =======  =============================================

    Pure function — does not mutate ``df``. The temporary group-average
    column is stripped from the returned frame so the caller sees a clean
    ``cleaned_cols + [resale_identifier]`` schema.

    Parameters
    ----------
    df
        Cleaned master DataFrame from Stage 5.

    Returns
    -------
    pd.DataFrame
        ``df`` plus a new ``resale_identifier`` column.

    Raises
    ------
    AssertionError
        If any generated identifier is not exactly
        :data:`config.IDENTIFIER_LENGTH` characters — a safety net that
        catches component-extractor bugs before the downstream hashed output
        is produced.
    """
    averages = compute_group_averages(df)
    merged = df.merge(averages, on=["month", "town", "flat_type"], how="left")
    merged["resale_identifier"] = merged.apply(_build_identifier_row, axis=1)

    # Invariant: every identifier is exactly IDENTIFIER_LENGTH characters.
    # Violating it means one of the per-component extractors emitted a
    # shorter/longer string than its contract promised — fail loudly so
    # the hashed output cannot be produced from a half-broken identifier.
    bad = merged.loc[
        merged["resale_identifier"].str.len() != config.IDENTIFIER_LENGTH
    ]
    assert bad.empty, (
        f"{len(bad)} identifiers are not {config.IDENTIFIER_LENGTH} chars; "
        f"samples: {bad['resale_identifier'].head().tolist()}"
    )

    return merged.drop(columns=[_GROUP_AVG_COL])


# ---------------------------------------------------------------------------
# Identifier-level dedupe
# ---------------------------------------------------------------------------


def dedupe_by_identifier(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split ``df`` into kept / discarded frames on duplicate ``resale_identifier``.

    Because the identifier is intentionally lossy (block compressed to 3
    digits, price to 2 digits), genuinely different flats can collide on it.
    Brief Transformation §2 says "if there are any duplicate records, take
    the higher price and discard the lower price one" — we interpret that
    at the identifier level, which is where Transformation §1 introduces
    the new duplicates (Stage 5 composite-key dedupe already ran).

    On a collision, the row with the highest ``resale_price`` is kept. The
    discarded rows are returned in a second frame with a ``failure_reason``
    column holding ``"identifier_collision_lower_price"``, ready to append
    to the ``data/failed/`` outputs.

    Parameters
    ----------
    df
        Frame carrying a ``resale_identifier`` column.

    Returns
    -------
    (kept_df, discarded_df)
        ``discarded_df`` is empty (but shape-compatible, with the
        ``failure_reason`` column present) when there are no collisions.
    """
    sorted_df = df.sort_values(
        "resale_price", ascending=False, kind="mergesort"
    )
    is_first = ~sorted_df.duplicated(subset="resale_identifier", keep="first")
    kept = sorted_df.loc[is_first].sort_index()
    discarded = sorted_df.loc[~is_first].sort_index().copy()
    discarded["failure_reason"] = "identifier_collision_lower_price"
    return kept, discarded


# ---------------------------------------------------------------------------
# Hashing
# ---------------------------------------------------------------------------


def hash_identifier(identifier: str) -> str:
    """Return the SHA-256 hex digest of ``identifier`` (UTF-8 encoded).

    Wraps :func:`hashlib.sha256` so tests and documentation have a single
    named entry point, and so :data:`config.HASH_ALGORITHM` can drive the
    choice in a future extension.

    Parameters
    ----------
    identifier
        Plaintext resale identifier string.

    Returns
    -------
    str
        64-character lowercase hex digest.
    """
    return hashlib.sha256(identifier.encode("utf-8")).hexdigest()


def add_hashed_identifier(df: pd.DataFrame) -> pd.DataFrame:
    """Return a new frame with a ``resale_identifier_hashed`` column.

    The hashing is deterministic and element-wise: identical plaintext
    identifiers must hash to identical digests. We assert uniqueness
    preservation at runtime (``nunique(hashed) == nunique(plaintext)``) to
    catch any hash collision — at 256-bit output and ~100k rows the
    probability is ~10^-72 by the birthday bound, so a mismatch would
    almost certainly indicate a pipeline bug rather than a real collision.

    Parameters
    ----------
    df
        Frame carrying a ``resale_identifier`` column.

    Returns
    -------
    pd.DataFrame
        Copy of ``df`` with a new ``resale_identifier_hashed`` column.

    Raises
    ------
    AssertionError
        If hashing reduced the number of distinct values (hash collision or
        upstream mutation bug).
    """
    out = df.copy()
    out["resale_identifier_hashed"] = out["resale_identifier"].map(hash_identifier)

    n_plain = out["resale_identifier"].nunique()
    n_hashed = out["resale_identifier_hashed"].nunique()
    assert n_plain == n_hashed, (
        f"hash collided: {n_plain} distinct plaintext identifiers but only "
        f"{n_hashed} distinct hashes"
    )
    return out
