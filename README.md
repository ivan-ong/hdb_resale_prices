# HDB Resale Flat Prices ETL — Part 1

ETL pipeline producing a cleaned master dataset of HDB resale flat transactions
covering **January 2012 – December 2016**, sourced from
[data.gov.sg collection 189](https://data.gov.sg/collections/189/view).

This is **Part 1** of the HDB Senior Data Engineer technical test. It covers
ingestion through the Cleaned dataset. Resale Identifier generation, hashing,
and the Transformed/Hashed outputs will be added in a later pass.

## Quickstart

Tested on **Python 3.12** (any 3.10–3.12 should work; 3.13 is not yet
supported by the pinned dependency set). From the repo root:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
jupyter notebook notebook/hdb_etl.ipynb
```

Then **Kernel → Restart Kernel and Run All Cells**. Cell 0 (Setup) prints
the resolved Python and dependency versions and will fail loudly if any
import is missing, so a misconfigured env surfaces immediately rather than
mid-pipeline.

On a fresh clone the raw CSVs are already present in `data/raw/`, so the
pipeline runs end-to-end **without network access**. The download path is
still exercised on each run and is idempotent (files matching the API's
`datasetSize` are skipped).

### Reproducible install (optional)

`pyproject.toml` uses bounded ranges (e.g. `pandas>=2.0,<4`), which is
usually enough. For byte-identical reproducibility, install from the
committed lockfile instead:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.lock
pip install -e . --no-deps
```

### Tests

```bash
python -m pytest
```

## Directory layout

```
.
├── pyproject.toml
├── README.md
├── notebook/hdb_etl.ipynb   # Deliverable; orchestrates + displays
├── src/                            # Importable pipeline modules
│   ├── config.py                   # Single source of truth for paths/scope
│   ├── ingest.py                   # data.gov.sg streaming downloads
│   ├── combine.py                  # Header normalization, schema union
│   ├── profile.py                  # Custom data profiling
│   ├── validate.py                 # Hand-rolled validation rules
│   ├── clean.py                    # Dedupe, lease recompute, anomaly flag
│   └── transform.py                # Resale Identifier + SHA-256 hash
├── tests/                          # pytest unit tests
└── data/
    ├── raw/                        # Source CSVs (committed)
    ├── cleaned/                    # Stage 5 output: hdb_resale_cleaned.csv
    ├── transformed/                # Stage 6 output: cleaned + resale_identifier
    ├── hashed/                     # Stage 6 output: cleaned + hashed identifier
    ├── failed/                     # Records removed by a pipeline stage
    └── reports/                    # Review artifacts (flagged not removed)
```

## Source data: schema by vintage

The HDB resale CSVs in collection 189 are not schema-stable across vintages.
The pipeline handles three in-scope files, and **only the most recent one
carries `remaining_lease`**:

| Vintage | File | Columns | Has `remaining_lease`? |
|---|---|---:|:---:|
| 2000 – Feb 2012 (Approval Date) | `ResaleFlatPricesBasedonApprovalDate2000Feb2012.csv` | 10 | no |
| Mar 2012 – Dec 2014 (Registration Date) | `ResaleFlatPricesBasedonRegistrationDateFromMar2012toDec2014.csv` | 10 | no |
| Jan 2015 – Dec 2016 (Registration Date) | `ResaleFlatPricesBasedonRegistrationDateFromJan2015toDec2016.csv` | **11** | **yes** |

Implications for the pipeline:

- **Combine** takes the **union** of columns. Pre-2015 rows get `NaN` for
  `remaining_lease` and for the canonical `remaining_lease_years_original`
  column we derive from it. This is correct, not a defect — those files
  simply don't carry the data.
- The 2015–2016 file stores `remaining_lease` as **integer years** already
  (verified by inspection: dtype `int64`, sample values like 70, 65, 64).
  No string parsing is needed for in-scope data.
- The 2017+ vintage (out of scope) uses an `"X years Y months"` string
  format. This format is intentionally **not** parsed by the current
  pipeline; the `_normalize_remaining_lease` function in `src/combine.py`
  documents this and would need to be extended if the scope is later
  widened to include 2017+.
- **Dedupe (Stage 5)** explicitly excludes `remaining_lease` and the
  derived `remaining_lease_years_original` from the composite key — they
  are `NaN` for every pre-2015 row by design, and including them would
  prevent the same logical transaction across vintages from collapsing.
  See `src/clean.py:_NON_KEY_COLUMNS`.

## Pipeline stages

| Stage | Module | Output |
|---|---|---|
| 1. Ingest | `src/ingest.py` | `data/raw/*.csv` (committed) |
| 2. Combine | `src/combine.py` | in-memory `master` DataFrame (schema union) |
| 3. Profile | `src/profile.py` | in-notebook `ProfileReport` dataclass |
| 4. Validate | `src/validate.py` | `data/failed/validation_failures.csv` |
| 5. Clean | `src/clean.py` | `data/cleaned/hdb_resale_cleaned.csv` plus side files in `data/failed/` and `data/reports/` |
| 6. Transform | `src/transform.py` | `data/transformed/hdb_resale_transformed.csv`, `data/hashed/hdb_resale_hashed.csv`, `data/failed/identifier_collision_duplicates.csv` |

### Stage 5 — Clean: anomaly detection heuristic

Resale-price anomalies are flagged (not dropped) by an **asymmetric IQR
rule on price-per-sqm**, grouped by `(town, flat_type, year)`:

```
ppsm = resale_price / floor_area_sqm
flag if ppsm < Q1 − 1.5·IQR  (below_lower_iqr_bound)
flag if ppsm > Q3 + 3.0·IQR  (above_upper_iqr_bound)
```

Multipliers live in `src/config.py` (`IQR_LOWER_MULT=1.5`,
`IQR_UPPER_MULT=3.0`). The asymmetry is deliberate: a flat priced at half
the going rate is far more suspicious than one at double — the cheap-side
tail is dominated by data-entry slips and intra-family transfers, the
expensive-side tail by genuine luxury units (penthouses, large
maisonettes) that are rare but real. Singleton groups (one observation in
a given (town, flat_type, year)) have IQR = 0 and so are never flagged,
which is the safe behaviour: we don't flag rows we have nothing to
compare against.

Anomalies are written to `data/reports/price_anomalies_flagged.csv` for
review *and* retained in the cleaned output with `price_anomaly_flag=True`
and a `price_anomaly_reason` so downstream consumers can decide whether
to filter them out. The brief reserves `data/failed/` for records that
were *removed* (DQ §5 duplicates, §3 validation failures); §6 anomalies
are flagged-not-removed and so live under `reports/`.

### Validation framing

Stage 4's categorical rules (Town, Flat Type, Flat Model, storey_range)
derive their allowed sets from the master at runtime. On the master
itself they are tautological by construction; the substantive use is to
**gate future batches** against a frozen master spec. On the in-scope
master, the only rule that ever fires is `floor_area_within_iqr_bounds`
— a small handful of structurally valid but statistically extreme floor
areas in the upper tail.

### Stage 6 — Transform: Resale Identifier

The Resale Identifier is a 9-character string derived entirely from
already-present columns. The construction follows brief *Transformation §1*
literally; each component and its edge-case handling is documented below.

| Position | Length | Source | Example |
|---:|:---:|---|---|
| 1 | 1 | Literal `"S"` | `S` |
| 2–4 | 3 | First 3 digits of `block`, non-digit characters stripped, zero-padded | `"456A"` → `456`; `"19"` → `019`; `"2B"` → `002` |
| 5–6 | 2 | First 2 digits of `int(mean(resale_price))`, grouped by (`month`, `town`, `flat_type`) | mean `$234,567` → `23` |
| 7–8 | 2 | `month[-2:]` (the `MM` portion of `YYYY-MM`) | `"2012-01"` → `01` |
| 9 | 1 | First character of `town`, stripped + upper-cased | `"Ang Mo Kio"` → `A` |

Total: 9 characters. `build_resale_identifier` asserts this at runtime as
a guard against component-extractor bugs silently emitting the wrong
length.

**Key assumptions** (`src/transform.py`):

- **Block digits**: we strip non-digit chars, take the leading 3, zero-pad
  if fewer remain. A block with zero digits after stripping raises
  `ValueError` rather than emitting `"000"` — a zero-digit block should
  have been caught upstream and silently collapsing all such rows to the
  same prefix would be worse than failing loudly.
- **Price digits**: `int(average_price)` uses Python truncation, not
  rounding. The brief says "the 1st and 2nd digit of the average resale
  price" and its worked example (`$230,000` → `23`) is consistent with
  truncation; rounding would produce the same answer on that example but
  differ at the edges.
- **Group averages** are computed on the cleaned data *before* the
  identifier-collision dedupe. Every transaction in a group contributes
  to its own average, which is what "the average X in Y" unambiguously
  means.
- **Town initial** uses the first character of the stripped + upper-cased
  town name. Whitespace drift in the source data is already normalized
  by Stage 2, but stripping here is defensive.

### Stage 6 — Transform: identifier-collision dedupe

The identifier is intentionally lossy: `block` compresses to 3 digits
(several blocks share a prefix) and the price component is a 2-digit
average over (month, town, flat_type), so different flats in the same
group with the same block prefix generate the same identifier. Our run
produces **77,246 distinct identifiers from 90,938 cleaned rows**, so
~15% of rows land in a collision bucket.

Brief *Transformation §2* says *"if there are any duplicate records, take
the higher price and discard the lower price one."* Stage 5 has already
deduplicated on the composite key of raw columns, so any *remaining*
duplicates can only come from §1 — the identifier construction itself.
We apply §2 at the identifier level: rows sharing a `resale_identifier`
are the duplicate class, the highest `resale_price` wins, and losers are
written to `data/failed/identifier_collision_duplicates.csv` with a
`failure_reason` column consistent with the other `failed/` files.

### Stage 6 — Transform: hashing algorithm (SHA-256)

The identifier is hashed with **SHA-256** (`hashlib.sha256`), configured
via `config.HASH_ALGORITHM`. Properties, and why each matters here:

- **Cryptographically irreversible.** One-way function — given only the
  hash, recovering the plaintext identifier is computationally infeasible.
  This is the requirement the brief ("irreversible hashing algorithm")
  asks for directly.
- **256-bit output space.** Collision probability for ~77k distinct
  identifiers is approximately `77000² / 2^257 ≈ 2·10⁻⁶³` by the birthday
  bound — negligible. We don't rely on the math alone: `add_hashed_identifier`
  asserts `nunique(hash) == nunique(plaintext)` at runtime.
- **Deterministic.** Same input always produces the same digest, so the
  pipeline is reproducible across runs and the Hashed output diff is
  stable.
- **FIPS-approved, industry standard.** Defensible choice for a
  production pipeline with no case-specific tradeoffs.
- **Output format.** 64-character lowercase hex digest. Fits CSV cleanly
  and is easy to eyeball.

**Alternatives considered.** MD5 would also preserve uniqueness at this
scale (128-bit output, birthday-bound collision probability still below
`10⁻²⁹`) and is faster, but MD5 is cryptographically broken (chosen-prefix
attacks have been practical since 2008) and is no longer defensible for
a pipeline where the hash is the only public-facing identifier. SHA-1 has
the same problem. BLAKE2/BLAKE3 are excellent modern alternatives but add
no material benefit over SHA-256 for this workload. SHA-256 is the
boring, correct choice.

**Hashed output contents.** Per the brief, the Hashed output group is
"Cleaned Data + Hashed Identifier Column". We therefore **drop the
plaintext `resale_identifier` before writing** `data/hashed/hdb_resale_hashed.csv`
— the whole point of hashing is that the plaintext is not exposed next
to the digest.

## Known limitations

- The 2017+ vintage uses an `"X years Y months"` string format for
  `remaining_lease` which is intentionally not parsed (out of scope).
- Recomputed lease assumes a January-1 lease start, since the source
  `lease_commence_date` is a year, not a calendar date.
- The price-anomaly heuristic is unsupervised. It does not distinguish
  intra-family transfers from data-entry errors from genuine outliers; it
  surfaces all three for review.

## Out of scope (this pass)

- Part 2 (AWS architecture for data ingestion and exploitation).
