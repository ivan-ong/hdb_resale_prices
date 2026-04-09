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

# Matches the YYYY-MM format used by the source data's `month` column.
_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")

# Matches every non-digit character — used to strip letters off block values
# like "456A" → "456" before taking the leading digits.
_NON_DIGIT_RE = re.compile(r"\D")


# ---------------------------------------------------------------------------
# Identifier construction
# ---------------------------------------------------------------------------


def build_resale_identifier(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of ``df`` with a new ``resale_identifier`` column.

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

    Fully vectorized: every component is a pandas Series built with
    ``.str`` accessors, then concatenated elementwise. The group mean is
    computed with ``groupby.transform("mean")`` so the result aligns with
    the input index without a separate merge step.

    Parameters
    ----------
    df
        Cleaned master DataFrame from Stage 5. Must contain ``block``,
        ``month``, ``town``, ``flat_type``, and ``resale_price``.

    Returns
    -------
    pd.DataFrame
        ``df.copy()`` plus a ``resale_identifier`` column of 9-character strings.

    Raises
    ------
    ValueError
        If any ``block`` has no digits at all, any ``month`` is not ``YYYY-MM``,
        or any ``town`` is empty. These are data-quality defects, not edge
        cases — failing loudly beats emitting silent ``"000"`` or ``""``
        fragments that would collide on the hash output.
    """
    n_block = config.IDENTIFIER_BLOCK_DIGITS
    n_price = config.IDENTIFIER_PRICE_DIGITS

    # Per-group mean resale price: the "average X in Y" figure the brief asks
    # for ("group by year-month, town and flat_type"). `transform("mean")`
    # broadcasts each group's mean back to every row in that group, so the
    # result already aligns with `df`'s index — no merge needed.
    group_mean = df.groupby(["month", "town", "flat_type"])["resale_price"].transform(
        "mean"
    )

    # --- Per-component strings, all vectorized --------------------------------

    # Block: strip non-digit chars, take the first N digits, left-pad with zeros.
    block_digits = (
        df["block"]
        .astype("string")
        .str.replace(_NON_DIGIT_RE, "", regex=True)
    )
    empty_blocks = block_digits.isna() | (block_digits.str.len() == 0)
    if empty_blocks.any():
        samples = df.loc[empty_blocks, "block"].head().tolist()
        raise ValueError(
            f"{int(empty_blocks.sum())} block values have no digits after "
            f"stripping; samples: {samples}"
        )
    block_part = block_digits.str.slice(0, n_block).str.zfill(n_block)

    # Price: first N digits of the integer part of the group mean.
    # Truncation, not rounding — the brief says "1st and 2nd digit".
    price_part = (
        group_mean.astype("int64").astype("string").str.slice(0, n_price).str.zfill(n_price)
    )

    # Month: the MM portion of YYYY-MM. Strict format check — drift here would
    # silently poison the hashed output.
    month_str = df["month"].astype("string")
    bad_month = ~month_str.str.match(_MONTH_RE.pattern).fillna(False)
    if bad_month.any():
        raise ValueError(
            f"month values not in YYYY-MM format; samples: "
            f"{month_str[bad_month].head().tolist()}"
        )
    month_part = month_str.str.slice(-2)

    # Town: first character, stripped and upper-cased.
    town_stripped = df["town"].astype("string").str.strip()
    empty_towns = town_stripped.isna() | (town_stripped.str.len() == 0)
    if empty_towns.any():
        raise ValueError(
            f"{int(empty_towns.sum())} town values are empty after stripping"
        )
    town_part = town_stripped.str.slice(0, 1).str.upper()

    out = df.copy()
    out["resale_identifier"] = "S" + block_part + price_part + month_part + town_part

    # Safety net: every identifier must be exactly IDENTIFIER_LENGTH chars.
    # Anything else means one of the component builders above has a bug, so
    # fail loudly before the hashed output gets written.
    assert (
        out["resale_identifier"].str.len() == config.IDENTIFIER_LENGTH
    ).all(), "resale_identifier length invariant violated"
    return out


# ---------------------------------------------------------------------------
# Collision diagnostics (notebook display helper)
# ---------------------------------------------------------------------------


def collision_report(df: pd.DataFrame, top_n: int = 5) -> dict[str, object]:
    """Summarise identifier-collision statistics for notebook display.

    The identifier is intentionally lossy (block → 3 digits, price → 2 digits),
    so collisions are expected. A high collision rate is not a bug — it's a
    property of the construction spec. This helper exposes the numbers a
    reviewer cares about:

    * Compression ratio (total rows vs distinct identifiers).
    * Which identifiers attract the most collisions.
    * Intra-bucket price spread — a wide spread is the reviewer-visible proof
      that colliding rows are genuinely different flats, not latent duplicates
      the cleaning stage missed.

    Parameters
    ----------
    df
        Frame carrying ``resale_identifier`` and ``resale_price`` columns
        (the output of :func:`build_resale_identifier`).
    top_n
        Number of most-collided identifiers to include in the breakdown.

    Returns
    -------
    dict
        Keys: ``n_total``, ``n_distinct``, ``mean_rows_per_id``,
        ``n_collided_ids``, ``n_rows_in_collisions``, ``top_collisions``
        (a DataFrame with ``rows``, ``min_price``, ``max_price``,
        ``price_spread`` columns indexed by identifier).
    """
    id_counts = df["resale_identifier"].value_counts()
    n_total = len(df)
    n_distinct = int(id_counts.size)
    n_collided_ids = int((id_counts > 1).sum())
    n_rows_in_collisions = int(id_counts[id_counts > 1].sum())

    top_ids = id_counts.head(top_n).index
    top_collisions = (
        df[df["resale_identifier"].isin(top_ids)]
        .groupby("resale_identifier")["resale_price"]
        .agg(rows="count", min_price="min", max_price="max")
        .sort_values("rows", ascending=False)
    )
    top_collisions["price_spread"] = (
        top_collisions["max_price"] - top_collisions["min_price"]
    )

    return {
        "n_total": n_total,
        "n_distinct": n_distinct,
        "mean_rows_per_id": n_total / n_distinct if n_distinct else 0.0,
        "n_collided_ids": n_collided_ids,
        "n_rows_in_collisions": n_rows_in_collisions,
        "top_collisions": top_collisions,
    }


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
    the higher price and discard the lower price one" — we apply that at
    the identifier level, which is where Transformation §1 introduces the
    new duplicates (Stage 5 composite-key dedupe already ran upstream).

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
    # Stable sort by descending price → first occurrence per identifier is the
    # highest-price row. `duplicated(keep="first")` then marks the rest as losers.
    sorted_df = df.sort_values("resale_price", ascending=False, kind="mergesort")
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

    Thin wrapper around :func:`hashlib.sha256` so tests and documentation
    have a single named entry point for the algorithm choice.
    """
    return hashlib.sha256(identifier.encode("utf-8")).hexdigest()


def add_hashed_identifier(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of ``df`` with a new ``resale_identifier_hashed`` column.

    The hashing is deterministic and element-wise: identical plaintext
    identifiers must hash to identical digests. We assert uniqueness
    preservation at runtime (``nunique(hashed) == nunique(plaintext)``) to
    catch any hash collision — at 256-bit output and ~77k distinct identifiers
    the probability is ~10⁻⁶⁴ by the birthday bound, so a mismatch would
    almost certainly indicate a pipeline bug rather than a real collision.

    Parameters
    ----------
    df
        Frame carrying a ``resale_identifier`` column.

    Returns
    -------
    pd.DataFrame
        ``df.copy()`` with a ``resale_identifier_hashed`` column appended.

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
