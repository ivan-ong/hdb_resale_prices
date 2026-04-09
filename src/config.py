"""Single source of truth for paths, scope, and tunables.

All magic values used by the pipeline live here. Modules import from this file
rather than hardcoding constants of their own.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
DATA_DIR: Path = PROJECT_ROOT / "data"
RAW_DIR: Path = DATA_DIR / "raw"
CLEANED_DIR: Path = DATA_DIR / "cleaned"
TRANSFORMED_DIR: Path = DATA_DIR / "transformed"
HASHED_DIR: Path = DATA_DIR / "hashed"
FAILED_DIR: Path = DATA_DIR / "failed"
REPORTS_DIR: Path = DATA_DIR / "reports"

# ---------------------------------------------------------------------------
# Scope
# ---------------------------------------------------------------------------

# Inclusive month bounds (YYYY-MM strings, matching the dataset's `month` column).
SCOPE_START: str = "2012-01"
SCOPE_END: str = "2016-12"

# ---------------------------------------------------------------------------
# data.gov.sg
# ---------------------------------------------------------------------------
#
# Dataset IDs are NOT hardcoded. Ingest discovers them at runtime by:
#   1. Reading collection metadata to get the list of child dataset IDs.
#   2. Reading each dataset's metadata to inspect its coverage window.
#   3. Filtering to datasets whose coverage overlaps SCOPE_START..SCOPE_END.

DATAGOVSG_COLLECTION_ID: int = 189

COLLECTION_METADATA_URL: str = (
    "https://api-production.data.gov.sg/v2/public/api/collections/{collection_id}/metadata"
)
DATASET_METADATA_URL: str = (
    "https://api-production.data.gov.sg/v2/public/api/datasets/{dataset_id}/metadata"
)
POLL_DOWNLOAD_URL: str = (
    "https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/poll-download"
)

# poll-download may need to be retried until the file is generated server-side,
# or because the endpoint is rate-limited (429 responses).
POLL_MAX_SECONDS: int = 60
POLL_BACKOFF_SECONDS: int = 3

# Pause between datasets to avoid hammering the rate-limited poll-download
# endpoint when downloading several files in quick succession.
INTER_DATASET_PAUSE_SECONDS: float = 2.0

# Streaming download chunk size (bytes).
DOWNLOAD_CHUNK_SIZE: int = 1 << 16  # 64 KB

# HTTP timeouts (seconds).
API_TIMEOUT: int = 30
DOWNLOAD_TIMEOUT: int = 120

# ---------------------------------------------------------------------------
# Lease recomputation
# ---------------------------------------------------------------------------

LEASE_TERM_YEARS: int = 99

# Reference date for the "remaining lease" recomputation (brief DQ §4).
# Frozen rather than `date.today()` so that re-runs of the pipeline against
# the same raw CSVs produce byte-identical cleaned outputs, which is a hard
# requirement for reproducible review. Bump this constant (and the committed
# cleaned/transformed/hashed CSVs) whenever the notebook is re-executed for
# submission. The notebook also prints the effective AS_OF_DATE so reviewers
# can spot the value without digging into config.
AS_OF_DATE: date = date(2026, 4, 9)

# ---------------------------------------------------------------------------
# Dedupe
# ---------------------------------------------------------------------------

# Lineage columns that are never part of the composite dedupe key. Consumed
# by `validate._composite_key_columns` and `clean.composite_key_columns`.
PIPELINE_METADATA_COLUMNS: list[str] = ["source_file"]

# ---------------------------------------------------------------------------
# Profiling
# ---------------------------------------------------------------------------

# A column with more than this fraction of nulls is flagged for review.
PROFILE_HIGH_NULL_THRESHOLD: float = 0.50

# How many top values to show per string column in the profile report.
PROFILE_TOP_N_VALUES: int = 10

# How many distinct sample values to display per column in the overview table.
PROFILE_SAMPLE_VALUES: int = 3

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

# Earliest plausible HDB lease commencement year. The first HDB blocks were
# completed in the early 1960s, so anything earlier is almost certainly bad
# data rather than a real flat.
LEASE_MIN_YEAR: int = 1960

# Symmetric IQR multiplier for the floor_area_sqm range check. Wider than
# the price-anomaly multipliers below because floor area is intrinsically
# bounded (you can't have a 1000 sqm HDB flat) and we want to flag, not fail,
# only the truly unbelievable values.
VALIDATE_FLOOR_AREA_IQR_MULT: float = 3.0

# How many failing rows to keep as illustrative samples per rule in the
# validation report.
VALIDATE_SAMPLE_FAILURES: int = 3

# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------

# Total length of the Resale Identifier (brief Transformation §1):
#   "S" + 3 block digits + 2 price digits + 2 month digits + 1 town initial = 9
IDENTIFIER_LENGTH: int = 9

# Number of leading digits taken from the block column, after stripping any
# non-digit characters. Brief: "Next 3 digits is the first 3 digits of the
# block column, after removing any characters."
IDENTIFIER_BLOCK_DIGITS: int = 3

# Number of leading digits taken from the integer part of the per-group
# average resale price. Brief: "Taking the 1st and 2nd digit of the average
# resale price, group by year-month, town and flat_type."
IDENTIFIER_PRICE_DIGITS: int = 2

# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------

# Asymmetric IQR multipliers on price-per-sqm, grouped by (town, flat_type, year).
# Tighter on the cheap side because impossibly cheap flats are more suspicious
# than luxury outliers.
IQR_LOWER_MULT: float = 1.5
IQR_UPPER_MULT: float = 3.0
