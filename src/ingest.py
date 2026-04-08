"""Stage 1: Ingest raw CSVs from data.gov.sg collection 189.

Discovery is fully programmatic — no dataset IDs or filenames are hardcoded.
The pipeline:

  1. Reads the collection metadata endpoint to enumerate child dataset IDs.
  2. Reads each dataset's metadata to inspect its coverage window.
  3. Filters to datasets whose coverage overlaps the configured scope window.
  4. For each in-scope dataset, calls the poll-download endpoint to obtain a
     pre-signed S3 URL, then streams the CSV to ``data/raw/``.

Idempotency: a download is skipped if the local file already exists and its
size matches the ``datasetSize`` reported by the dataset metadata endpoint.
``force_refresh=True`` overrides the skip.

Robustness: if the discovery API is unreachable, the pipeline falls back to
whatever CSVs are already present in ``data/raw/`` so the rest of the notebook
can still run on a fresh clone without network access.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests

from src import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Low-level HTTP helpers
# ---------------------------------------------------------------------------


def _get_json(url: str) -> dict:
    """GET a data.gov.sg JSON endpoint and return the ``data`` payload.

    Raises ``RuntimeError`` if the API reports a non-zero ``code``.
    """
    response = requests.get(url, timeout=config.API_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    # data.gov.sg wraps every response in an envelope:
    #   {"code": 0, "data": {...}, "errorMsg": ""}
    # A non-zero `code` signals a logical API error even when the HTTP status
    # is 200, so we treat it as a hard failure here.
    if payload.get("code") != 0:
        raise RuntimeError(
            f"data.gov.sg API error at {url}: {payload.get('errorMsg')!r}"
        )
    return payload["data"]


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def _coverage_overlaps_scope(coverage_start: str, coverage_end: str) -> bool:
    """Return True if a dataset's coverage window overlaps the configured scope.

    The dataset's ``month`` column has month granularity, so we compare on the
    ``YYYY-MM`` prefix of the ISO timestamps returned by the metadata API.
    """
    # Take only the YYYY-MM prefix so we compare apples to apples with the
    # config's month-granularity scope window.
    ds_start = coverage_start[:7]
    ds_end = coverage_end[:7]
    # Standard interval-overlap test, expressed as the negation of the two
    # disjoint cases (dataset entirely before scope, or entirely after scope).
    return not (ds_end < config.SCOPE_START or ds_start > config.SCOPE_END)


def _fetch_dataset_metadata(dataset_id: str) -> dict:
    """Fetch normalized dataset metadata from the API.

    Returns a dict with the fields we care about: ``id``, ``name``,
    ``datasetSize``, ``expected_columns`` (in order), ``coverageStart``,
    ``coverageEnd``.
    """
    meta_url = config.DATASET_METADATA_URL.format(dataset_id=dataset_id)
    meta = _get_json(meta_url)
    # `columnMetadata.map` is keyed by opaque column codes (e.g.
    # "c_btsi3rr0vdd52mhwjxdho817g") and valued by the human column names.
    # We only need the names, in declaration order, for verification.
    column_map = meta.get("columnMetadata", {}).get("map", {})
    return {
        "id": dataset_id,
        "name": meta.get("name", dataset_id),
        "datasetSize": meta.get("datasetSize"),
        "expected_columns": list(column_map.values()),
        "coverageStart": meta.get("coverageStart", ""),
        "coverageEnd": meta.get("coverageEnd", ""),
    }


def discover_in_scope_datasets() -> list[dict]:
    """Resolve dataset IDs in the collection that overlap the scope window.

    Returns
    -------
    list of dict
        One entry per in-scope dataset (see :func:`_fetch_dataset_metadata`
        for the schema).
    """
    collection_url = config.COLLECTION_METADATA_URL.format(
        collection_id=config.DATAGOVSG_COLLECTION_ID
    )
    collection = _get_json(collection_url)
    # The collection envelope nests its dataset list under
    # `collectionMetadata.childDatasets` — a flat list of opaque dataset IDs.
    child_ids = collection["collectionMetadata"]["childDatasets"]
    logger.info(
        "Collection %s has %d child datasets",
        config.DATAGOVSG_COLLECTION_ID,
        len(child_ids),
    )

    # One metadata round-trip per child. We could parallelize, but the
    # collection only has a handful of datasets and the dataset-metadata
    # endpoint is not rate-limited like poll-download is.
    in_scope: list[dict] = []
    for dataset_id in child_ids:
        meta = _fetch_dataset_metadata(dataset_id)
        # Defensive: a dataset without a coverage window can't be filtered by
        # date overlap, so we exclude it rather than guess.
        if not (meta["coverageStart"] and meta["coverageEnd"]):
            logger.warning("Dataset %s missing coverage window; skipping", dataset_id)
            continue
        if _coverage_overlaps_scope(meta["coverageStart"], meta["coverageEnd"]):
            in_scope.append(meta)
    logger.info(
        "Filtered to %d in-scope datasets for %s..%s",
        len(in_scope),
        config.SCOPE_START,
        config.SCOPE_END,
    )
    return in_scope


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def _resolve_download_url(dataset_id: str) -> str:
    """Poll ``poll-download`` until the dataset is ready, return the S3 URL.

    Two retryable conditions are handled:

    * ``429 Too Many Requests`` — the endpoint is rate-limited. We honor any
      ``Retry-After`` header and otherwise back off by ``POLL_BACKOFF_SECONDS``.
    * ``status != DOWNLOAD_SUCCESS`` — the dataset is being generated
      server-side. We poll on the same backoff.

    Both retries share a single ``POLL_MAX_SECONDS`` deadline.
    """
    url = config.POLL_DOWNLOAD_URL.format(dataset_id=dataset_id)
    # Single deadline shared by both retry conditions (rate-limit and
    # not-yet-ready). Using a deadline rather than a max-attempts counter
    # means a few short waits don't crowd out a longer one.
    deadline = time.monotonic() + config.POLL_MAX_SECONDS
    last_status: str | None = None
    while True:
        # Note: we can't use _get_json here because we need to inspect the
        # raw response (specifically status_code 429 and Retry-After header).
        response = requests.get(url, timeout=config.API_TIMEOUT)

        if response.status_code == 429:
            # Honor server-supplied Retry-After when present; otherwise fall
            # back to the configured backoff. Cast to float so the header
            # parses whether it's "3" or "3.0".
            retry_after = float(
                response.headers.get("Retry-After", config.POLL_BACKOFF_SECONDS)
            )
            # Bail out early if even one more sleep would push us past the
            # deadline — saves a pointless wait followed by a guaranteed fail.
            if time.monotonic() + retry_after > deadline:
                raise TimeoutError(
                    f"poll-download for {dataset_id} rate-limited; "
                    f"deadline exceeded after {config.POLL_MAX_SECONDS}s"
                )
            logger.info(
                "poll-download rate-limited for %s; sleeping %.1fs",
                dataset_id,
                retry_after,
            )
            time.sleep(retry_after)
            continue

        response.raise_for_status()
        payload = response.json()
        # Same envelope handling as _get_json (duplicated because we couldn't
        # delegate this call).
        if payload.get("code") != 0:
            raise RuntimeError(
                f"data.gov.sg API error at {url}: {payload.get('errorMsg')!r}"
            )
        data = payload["data"]
        last_status = data.get("status")
        # The endpoint can return intermediate states like "GENERATING" while
        # the file is being prepared server-side. For the in-scope HDB resale
        # files this branch never fires in practice (DOWNLOAD_SUCCESS comes
        # back on the first call) but we keep it because the contract allows
        # it and a regression on data.gov.sg's side would otherwise hang.
        if last_status == "DOWNLOAD_SUCCESS":
            return data["url"]
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"poll-download for {dataset_id} did not reach DOWNLOAD_SUCCESS "
                f"within {config.POLL_MAX_SECONDS}s (last status={last_status!r})"
            )
        time.sleep(config.POLL_BACKOFF_SECONDS)


# Matches the canonical filename forms inside a Content-Disposition header:
#   filename="example.csv"
#   filename=example.csv
# Optional surrounding quotes; lazy match stops at the first " or ;.
_FILENAME_RE = re.compile(r'filename="?([^";]+?)"?(?:;|$)')


def _filename_from_presigned_url(s3_url: str, default: str) -> str:
    """Extract the canonical filename from the S3 pre-signed URL.

    The URL embeds ``response-content-disposition`` as a query parameter, e.g.
    ``response-content-disposition=attachment; filename="ResaleFlat...csv"``.
    ``urllib.parse.parse_qs`` handles the percent-decoding for us.
    """
    # parse_qs returns a dict[str, list[str]] with values already
    # percent-decoded. We expect at most one value for this parameter.
    qs = parse_qs(urlparse(s3_url).query)
    disposition = qs.get("response-content-disposition", [None])[0]
    if not disposition:
        # Defensive fallback: pre-signed URL without the disposition param
        # would be unusual, but we'd rather save under a stable derived name
        # than crash.
        return default
    match = _FILENAME_RE.search(disposition)
    return match.group(1) if match else default


def _stream_to_disk(url: str, dest: Path) -> int:
    """Stream a URL to ``dest`` in chunks. Returns bytes written."""
    bytes_written = 0
    # `stream=True` keeps the body off the heap; combined with iter_content
    # this lets us pull arbitrarily large files (the brief calls out >100 MB
    # transmission) without loading them into memory.
    with requests.get(url, stream=True, timeout=config.DOWNLOAD_TIMEOUT) as response:
        response.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=config.DOWNLOAD_CHUNK_SIZE):
                # iter_content can yield empty keep-alive chunks; skip them
                # so the byte count stays accurate.
                if chunk:
                    fh.write(chunk)
                    bytes_written += len(chunk)
    return bytes_written


def download_dataset(
    dataset: dict,
    *,
    force_refresh: bool = False,
) -> Path:
    """Download a single in-scope dataset to ``data/raw/``.

    Idempotent: if the local file exists and its size matches ``datasetSize``,
    the download is skipped unless ``force_refresh`` is True.
    """
    dataset_id = dataset["id"]
    expected_size = dataset.get("datasetSize")

    # Resolve S3 URL first so we can read the canonical filename out of its
    # disposition param. Doing this even on the skip path means a fresh local
    # file gets the same canonical name as a re-download would, regardless of
    # what we previously named it on disk.
    s3_url = _resolve_download_url(dataset_id)
    filename = _filename_from_presigned_url(s3_url, default=f"{dataset_id}.csv")
    dest = config.RAW_DIR / filename

    # Idempotency: data.gov.sg does not publish a checksum, so we use file
    # size against the API's authoritative datasetSize as the integrity check.
    # It's not bullet-proof against in-cell corruption but it's the strongest
    # guarantee available without a server-side hash.
    if dest.exists() and not force_refresh:
        local_size = dest.stat().st_size
        # If the API didn't report a size we can't compare, so trust presence.
        if expected_size is None or local_size == expected_size:
            logger.info("Skip %s (already present, %d bytes)", filename, local_size)
            return dest
        # Size disagrees → assume the local file is stale and re-download.
        logger.info(
            "Re-downloading %s (local %d bytes != expected %d bytes)",
            filename,
            local_size,
            expected_size,
        )

    bytes_written = _stream_to_disk(s3_url, dest)
    logger.info(
        "Downloaded %s -> %s (%d bytes)", dataset_id, dest, bytes_written
    )

    # Warn (rather than raise) on size mismatch: verify_raw() does the
    # authoritative check downstream and reports it as a structured failure.
    if expected_size is not None and bytes_written != expected_size:
        logger.warning(
            "Size mismatch for %s: wrote %d bytes, expected %d bytes",
            filename,
            bytes_written,
            expected_size,
        )

    return dest


# ---------------------------------------------------------------------------
# Top-level entrypoint
# ---------------------------------------------------------------------------


def ingest_all(*, force_refresh: bool = False) -> list[Path]:
    """Discover and download all in-scope datasets to ``data/raw/``.

    On API failure, logs a clear error and returns whatever CSVs are already
    present locally so the rest of the pipeline can still run.
    """
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)

    try:
        datasets = discover_in_scope_datasets()
    except Exception as exc:
        # Fallback path: API down, network blocked, etc. Because data/raw/ is
        # committed to the repo, a reviewer on a fresh clone can still run
        # the rest of the pipeline against the snapshot we shipped. We log
        # loudly so this isn't silent.
        logger.error(
            "Discovery failed (%s); falling back to local files in %s",
            exc,
            config.RAW_DIR,
        )
        return sorted(config.RAW_DIR.glob("*.csv"))

    downloaded: list[Path] = []
    for index, dataset in enumerate(datasets):
        # Polite delay between datasets. The poll-download endpoint is
        # aggressively rate-limited (429s observed when calling 3 in a row);
        # `_resolve_download_url` will also retry on 429, but adding a small
        # pause up front avoids the wasted round-trips.
        if index > 0:
            time.sleep(config.INTER_DATASET_PAUSE_SECONDS)
        try:
            path = download_dataset(dataset, force_refresh=force_refresh)
            downloaded.append(path)
        except Exception as exc:
            # Continue with the remaining datasets — partial success is more
            # useful than aborting the whole batch on one bad apple.
            logger.error("Failed to download %s: %s", dataset["id"], exc)
    return downloaded


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def verify_raw(datasets: list[dict] | None = None) -> list[dict]:
    """Verify each CSV in ``data/raw/`` against the API's authoritative metadata.

    For every CSV in the raw directory the function:

    * parses the file with pandas,
    * matches it to an in-scope dataset by date-range overlap (no filename
      coupling),
    * checks the file size equals ``datasetSize``,
    * checks the column names equal ``columnMetadata`` (in order),
    * checks the file's date range fits inside the dataset coverage window.

    It also reports any in-scope dataset for which no local file is present.

    Parameters
    ----------
    datasets
        Pre-fetched output of :func:`discover_in_scope_datasets`. If ``None``,
        discovery is performed (one API call per child dataset).

    Returns
    -------
    list of dict
        One row per local file plus one row per missing in-scope dataset.
        Suitable for ``pd.DataFrame(...)`` display in a notebook cell.
    """
    # Allow callers to pass a pre-fetched discovery result. Useful when the
    # notebook has already called discover_in_scope_datasets() upstream and
    # wants to avoid an extra round-trip-per-dataset to the metadata API.
    if datasets is None:
        datasets = discover_in_scope_datasets()

    local_files = sorted(config.RAW_DIR.glob("*.csv"))
    results: list[dict] = []
    # Track which datasets matched a local file so we can also report any
    # in-scope datasets that have NO local file at the end.
    matched_dataset_ids: set[str] = set()

    for path in local_files:
        # Initialize the result with sensible defaults so the schema is the
        # same on every code path (success, parse failure, no match, etc.).
        # This makes the eventual pd.DataFrame view have stable columns.
        result: dict = {
            "file": path.name,
            "matched_dataset": None,
            "rows": None,
            "cols": None,
            "min_month": None,
            "max_month": None,
            "actual_size": path.stat().st_size,
            "expected_size": None,
            "size_ok": False,
            "cols_ok": False,
            "dates_ok": False,
            "ok": False,
            "error": None,
        }
        try:
            df = pd.read_csv(path)
        except Exception as exc:
            result["error"] = f"parse error: {exc}"
            results.append(result)
            logger.error("[FAIL] %s: %s", path.name, result["error"])
            continue

        result["rows"], result["cols"] = df.shape

        if "month" not in df.columns:
            result["error"] = "missing 'month' column"
            results.append(result)
            logger.error("[FAIL] %s: %s", path.name, result["error"])
            continue

        months = sorted(df["month"].dropna().unique())
        if not months:
            result["error"] = "no values in 'month' column"
            results.append(result)
            logger.error("[FAIL] %s: %s", path.name, result["error"])
            continue
        result["min_month"], result["max_month"] = months[0], months[-1]

        # Match by coverage window rather than by filename. This keeps the
        # verification independent of how the file was named on disk, which
        # in turn means we can re-run verify_raw() against any snapshot of
        # data/raw/ — even one that pre-dates the canonical-naming change.
        candidates = [
            ds
            for ds in datasets
            if result["min_month"] >= ds["coverageStart"][:7]
            and result["max_month"] <= ds["coverageEnd"][:7]
        ]
        # We require exactly one match. Zero means a stray CSV in raw; more
        # than one would indicate overlapping coverage windows in the
        # collection (shouldn't happen, but worth catching loudly).
        if len(candidates) != 1:
            result["error"] = (
                f"no unique dataset match (found {len(candidates)} candidates)"
            )
            results.append(result)
            logger.error("[FAIL] %s: %s", path.name, result["error"])
            continue

        ds = candidates[0]
        matched_dataset_ids.add(ds["id"])
        result["matched_dataset"] = ds["id"]
        result["expected_size"] = ds["datasetSize"]
        # Three independent checks. Together they form the strongest integrity
        # envelope available without a server-side hash:
        #   * size_ok  → the file is byte-for-byte the same length as advertised
        #   * cols_ok  → the schema matches the API's column declaration in order
        #   * dates_ok → no rows have leaked outside the dataset's coverage window
        result["size_ok"] = result["actual_size"] == ds["datasetSize"]
        result["cols_ok"] = list(df.columns) == ds["expected_columns"]
        result["dates_ok"] = (
            result["min_month"] >= ds["coverageStart"][:7]
            and result["max_month"] <= ds["coverageEnd"][:7]
        )
        result["ok"] = result["size_ok"] and result["cols_ok"] and result["dates_ok"]

        level = logging.INFO if result["ok"] else logging.ERROR
        logger.log(
            level,
            "[%s] %s  rows=%d cols=%d  %s..%s  size_ok=%s cols_ok=%s dates_ok=%s",
            "OK" if result["ok"] else "FAIL",
            path.name,
            result["rows"],
            result["cols"],
            result["min_month"],
            result["max_month"],
            result["size_ok"],
            result["cols_ok"],
            result["dates_ok"],
        )
        results.append(result)

    # Report any in-scope dataset that has no corresponding local file. This
    # catches the inverse failure mode of an unmatched CSV: an expected file
    # is missing entirely (e.g. someone deleted it from data/raw/).
    for ds in datasets:
        if ds["id"] not in matched_dataset_ids:
            results.append(
                {
                    "file": None,
                    "matched_dataset": ds["id"],
                    "rows": None,
                    "cols": None,
                    "min_month": None,
                    "max_month": None,
                    "actual_size": None,
                    "expected_size": ds["datasetSize"],
                    "size_ok": False,
                    "cols_ok": False,
                    "dates_ok": False,
                    "ok": False,
                    "error": "missing local file for in-scope dataset",
                }
            )
            logger.error(
                "[FAIL] missing local file for in-scope dataset %s (%s)",
                ds["id"],
                ds["name"],
            )

    all_ok = bool(results) and all(r["ok"] for r in results)
    logger.info(
        "verify_raw: %d files checked, overall %s",
        len(local_files),
        "OK" if all_ok else "FAIL",
    )
    return results
