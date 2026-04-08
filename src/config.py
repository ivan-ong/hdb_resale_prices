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
AS_OF_DATE: date = date.today()

# ---------------------------------------------------------------------------
# Dedupe
# ---------------------------------------------------------------------------

# Composite key for dedupe = all raw master columns EXCEPT resale_price and
# pipeline-added metadata columns. Populated by combine.py once the raw master
# schema is known, OR overridden here after the master schema is locked.
COMPOSITE_KEY_COLUMNS: list[str] = []

PIPELINE_METADATA_COLUMNS: list[str] = ["source_file"]

# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------

# Asymmetric IQR multipliers on price-per-sqm, grouped by (town, flat_type, year).
# Tighter on the cheap side because impossibly cheap flats are more suspicious
# than luxury outliers.
IQR_LOWER_MULT: float = 1.5
IQR_UPPER_MULT: float = 3.0
