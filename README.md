# HDB Resale Flat Prices ETL — Part 1

ETL pipeline producing cleaned, transformed, and hashed datasets of HDB
resale flat transactions covering **January 2012 – December 2016**,
sourced from [data.gov.sg collection 189](https://data.gov.sg/collections/189/view).

This is **Part 1** of the HDB Senior Data Engineer technical test. It
covers ingestion through to the Hashed output group, end to end. Part 2
(AWS architecture) is not in this repo.

## Quickstart

Developed and tested on **Python 3.12**. `pyproject.toml` declares
`requires-python = ">=3.10,<3.13"` so 3.10 and 3.11 should also work, but
only 3.12 is actively exercised. From the repo root:

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

## General assumptions

Design decisions that apply across the whole pipeline. A reviewer who
disagrees with one of these would reasonably arrive at different
pipeline outputs, so they are surfaced here rather than buried in a
single stage's section.

1. **"Avoid hardcoding".** The brief asks us to avoid
   manual interactions and hardcoding where possible. We therefore:
   discover dataset IDs at runtime from the collection metadata endpoint
   (Stage 1); take the schema union with `pd.concat(sort=False)` rather
   than pinning an expected column list (Stage 2); derive every
   categorical validation set from the master at runtime (Stage 4); and
   pull all tunables (scope window, IQR multipliers, identifier lengths,
   hash algorithm) into `src/config.py` so nothing tunable lives in a
   module body.

2. **Pure functions in `src/`, I/O in the notebook.** Every `src/`
   module is import-safe and side-effect-free except `ingest.py` (which
   downloads by definition). The notebook is the only place that writes
   files. This makes every stage independently testable and means a
   reviewer can rerun any stage in isolation.

3. **"Failed" means removed, "reports" means flagged.** The brief
   defines the `Failed` output group as *"records that were removed"*
   (validation failures, duplicates). We therefore put only removed
   records under `data/failed/` and send review-only artifacts (e.g.
   flagged price anomalies) to `data/reports/`. A reviewer reading the
   brief literally will expect this split.

4. **Custom profiling and validation instead of open-source libraries.**
   The brief permits either. We hand-roll both (`src/profile.py`,
   `src/validate.py`) because `ydata-profiling` would add ~50 MB of
   dependencies for a report we largely ignore, and `great_expectations`
   imposes a project layout that would dominate a codebase this small.
   The hand-rolled versions are ~100–300 lines each and directly
   inspectable.

5. **Scope window enforced programmatically, not by file selection.**
   `SCOPE_START`/`SCOPE_END` in `config.py` drive both the ingest
   filter (pick in-scope datasets) and the combine filter (drop rows
   whose `month` falls outside the window). The 2000–Feb 2012 file is
   trimmed down to just its Jan–Feb 2012 tail rather than special-cased
   — a uniform rule is easier to audit than a one-off.

6. **Reproducibility is a first-class property.** A committed
   `requirements.lock` gives byte-identical installs, `raw/` is
   committed so the notebook runs offline, the download path is
   idempotent (`datasetSize` check skips already-present files), and
   SHA-256 (deterministic) keeps the Hashed output stable across runs.

7. **Everything is documented where it lives.** Every function in
   `src/` has a type hint and a NumPy-style docstring; every magic
   value in `config.py` has a comment explaining why it has the value
   it does; every notebook section has a markdown cell framing the
   brief requirement it addresses.

## Pipeline stages

| Stage | Module | Output |
|---|---|---|
| 1. Ingest | `src/ingest.py` | `data/raw/*.csv` (committed) |
| 2. Combine | `src/combine.py` | in-memory `master` DataFrame (schema union) |
| 3. Profile | `src/profile.py` | in-notebook `ProfileReport` dataclass |
| 4. Validate | `src/validate.py` | `data/failed/validation_failures.csv` |
| 5. Clean | `src/clean.py` | `data/cleaned/hdb_resale_cleaned.csv` plus side files in `data/failed/` and `data/reports/` |
| 6. Transform | `src/transform.py` | `data/transformed/hdb_resale_transformed.csv`, `data/hashed/hdb_resale_hashed.csv`, `data/failed/identifier_collision_duplicates.csv` |

### Stage 1 — Ingest

Downloads the in-scope HDB resale CSVs from data.gov.sg collection 189
into `data/raw/`. Dataset IDs are **discovered** at runtime by walking
the collection metadata endpoint, reading each child dataset's coverage
window, and filtering to those that overlap `SCOPE_START..SCOPE_END` —
no IDs or filenames are hardcoded in the module body. Downloads stream
from the `poll-download` endpoint's pre-signed S3 URLs with 429
retry/backoff, and are idempotent: files whose size already matches the
API's `datasetSize` are skipped. `verify_raw()` cross-checks each local
file against the API metadata on three axes (byte size, column list,
month range) for an end-to-end integrity envelope.

**Assumptions.**

- **`datasetSize`-based idempotency is sufficient.** We skip a
  re-download when the local file size matches the API's `datasetSize`
  exactly. There is no server-published checksum on the endpoint we
  could use instead. In practice this is the strongest integrity check
  available and any silent mid-file corruption would be caught by
  `verify_raw()`'s column/row cross-check.
- **Fall back to local files if the API is unreachable.** Because `raw/`
  is committed, the notebook must run offline on a fresh clone. The
  ingest function gracefully degrades to "use whatever is already in
  `data/raw/`" rather than raising.

### Stage 2 — Combine

Reads each CSV in `data/raw/`, normalizes headers
(`.strip().lower().replace(" ", "_")`), filters to the scope window on
the `month` column, tags each row with a `source_file` lineage column,
and concatenates into a single `master` DataFrame with `sort=False`
(union of columns, preserved in order of first appearance). The pre-2015
vintages do not carry `remaining_lease` at all, so those rows get `NaN`
after the schema union — which is correct, not a defect.

**Assumptions.**

- **Header normalization happens on read.** Even though the current
  three in-scope files use consistent lowercase headers today, historical
  drift (`Month` vs `month`) is the first thing to break a schema union.
  Normalizing unconditionally is cheap insurance.
- **Scope filter is lexicographic on a zero-padded `YYYY-MM` string.**
  This works because the month component is always two digits. We do
  not parse `month` into a datetime here — string comparison is faster
  and matches the shape of the column as stored.
- **Canonical lease column for in-scope data is an `Int64`.** The 2015–
  2016 file stores `remaining_lease` as integer years (e.g. `70`, `65`,
  `64`), so we cast to nullable `Int64` and land it in the master as
  `remaining_lease_years_original`. The 2017+ `"X years Y months"`
  string format is explicitly not parsed — it's out of scope, and
  extending the parser without the data to test it would be speculative.

### Stage 3 — Profile

Hand-rolled profiling (no `ydata-profiling`) that returns a
`ProfileReport` dataclass with per-column overview, numeric statistics,
string-column length statistics, top-N value counts, and a flag table
covering high-null, constant, and whitespace-contaminated columns. The
output is a set of pre-built DataFrames so the notebook renders each
section with a single `display(...)` call. The module is pure and does
not mutate its input.

**Assumptions.**

- **`source_file` is excluded from string profiling by design** — it is
  pipeline-added lineage metadata with exactly one value per source
  file, so its top-values table is uninformative.
- **50% is the "high null" threshold** (`PROFILE_HIGH_NULL_THRESHOLD`).
  This is a review nudge, not a validation rule. On the current master
  it correctly flags the two `remaining_lease*` columns (~60% null,
  expected because pre-2015 vintages don't carry the column) and
  nothing else.

### Stage 4 — Validate

Twelve hand-rolled rules in `src/validate.py`. Each returns a boolean
mask of failing rows; NaN/unevaluable cells are treated as failures
rather than silently passed. The five categorical rules required by the
brief (Date, Town, Flat Type, Flat Model, storey_range) derive their
allowed sets from the master at runtime via `ValidationSpec` — no
hardcoded town or flat-type lists. Seven additional rules cover
structural (month/storey format), numeric (positive price, positive
area, area-IQR bounds, lease year ∈ [1960, today]), and composite-key
no-null checks.

**Framing.** On the master itself the categorical rules are
**tautological by construction** — they can't fail on the data they
were derived from. Their substantive use is to **gate a future batch**
against a frozen master spec. The split between `derive_spec(df)` and
`validate_master(df, spec=…)` is what makes that use case possible
without modifying a single line of rule code. On the in-scope master
the only rule that ever fires in practice is
`floor_area_within_iqr_bounds` (6 rows in the upper tail).

**Assumptions.**

- **Strip-and-upper canonicalization** is the single normalization rule
  applied to every categorical column before set membership. This
  absorbs the known case drift in `flat_model` (`"Improved"` vs
  `"IMPROVED"`) across vintages — the canonical set holds one entry,
  not two.
- **Categorical sets frozen via `frozenset`** because `ValidationSpec`
  is `@dataclass(frozen=True)`. A spec that can't be mutated after
  derivation makes "validate a future batch against this exact spec"
  trivial to reason about.
- **NaN is a failure, not a pass.** If a rule can't evaluate a cell
  (e.g. `floor_area_sqm` is `NaN` on the positivity check), the row
  fails. This is strictly safer than letting un-evaluable rows slip
  through.
- **`remaining_lease*` is excluded from the composite-key-no-nulls
  check** because it's vintage-absent by design (see Stage 2). Failing
  ~60% of rows on a known schema gap would make the report unreadable.

### Stage 5 — Clean

Three transforms over the validation-passing rows:

1. **Recompute remaining lease as of today.** `LEASE_TERM_YEARS = 99`
   years minus `(AS_OF_DATE - lease_commence_date)`, rounded down to
   years and months, stored as both an integer month count
   (`remaining_lease_computed_months`) and a `"Y years M months"` string
   (`remaining_lease_computed`). Rows whose computed lease is negative
   are split off as expired and excluded from the cleaned output.
2. **Deduplicate on the composite key.** All raw columns except
   `resale_price`, `source_file`, and the vintage-specific
   `remaining_lease*` columns. On a tie, highest `resale_price` wins;
   losers go to `data/failed/duplicate_lower_price.csv`.
3. **Flag anomalous resale prices** (see heuristic below). Flags, never
   drops.

**Lease recomputation assumptions.**

- **`lease_commence_date` is treated as January 1 of the stored year.**
  The source column is a year (integer), not a calendar date. The
  "round down to years and months" requirement from the brief is then
  a straightforward integer-month subtraction with a single day-of-month
  adjustment when `as_of` is past the 1st.
- **`config.AS_OF_DATE` is frozen** (currently `2026-04-09`) rather than
  using `date.today()`, so re-running the pipeline against the same raw
  CSVs produces byte-identical cleaned outputs. Bump the constant (and
  re-run the notebook) to refresh the as-of date for submission.

**Composite-key dedupe assumptions.**

- **`remaining_lease*` is excluded from the key.** Including it would
  prevent the same logical transaction across vintages from collapsing,
  because pre-2015 rows carry `NaN` in those columns by design.
  `resale_price` is excluded because it is the value being deduped on.
  `source_file` is excluded because it is lineage metadata. See
  `src/clean.py:_NON_KEY_COLUMNS`.
- **Tie-break is deterministic.** We sort by `resale_price` descending
  with `kind="mergesort"` (stable), so on identical prices the original
  row order wins. Documented here so a reviewer isn't surprised by a
  specific pick.

**Price anomaly heuristic.**

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
were *removed* (DQ §5 duplicates, §3 validation failures); DQ §6
anomalies are flagged-not-removed and so live under `reports/`.

**Assumptions.**

- **"Flag, don't remove."** DQ §6 says "identify any potentially
  anomalous resale price". "Identify" plus "heuristics" is hedge
  language — a false positive would silently destroy a real
  transaction. We therefore flag, write a review file, and let the
  consumer decide. See the price-anomaly paragraph above for the full rationale.
- **Grouping on (town, flat_type, year)** is the finest partition the
  data supports without most groups collapsing to singletons. A
  per-month partition would leave too many groups with IQR = 0.
- **Year is extracted as `month[:4]`.** Again, no datetime parsing
  required — the column is already a `YYYY-MM` string.

### Stage 6 — Transform

Stage 6 implements the brief's *Data Transformation Requirements*. It
has three logically distinct steps — identifier construction, collision
dedupe, and hashing — each described below.

**Resale Identifier construction.**

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

**Assumptions.**

- **Block digits**: strip non-digit chars, take the leading 3, zero-pad
  if fewer remain. A block with zero digits after stripping raises
  `ValueError` rather than emitting `"000"` — a zero-digit block should
  have been caught upstream, and silently collapsing all such rows to
  the same prefix would be worse than failing loudly.
- **Price digits**: `int(average_price)` uses Python truncation, not
  rounding. The brief says "the 1st and 2nd digit of the average resale
  price" and its worked example (`$230,000` → `23`) is consistent with
  truncation; rounding would produce the same answer on that example
  but differ at the edges.
- **Group averages** are computed on the cleaned data *before* the
  identifier-collision dedupe. Every transaction in a group contributes
  to its own average, which is what "the average X in Y" unambiguously
  means.
- **Town initial** uses the first character of the stripped +
  upper-cased town name. Whitespace drift in the source data is already
  normalized by Stage 2, but stripping here is defensive.

**Identifier-collision dedupe.**

The identifier is intentionally lossy: `block` compresses to 3 digits
(several blocks share a prefix) and the price component is a 2-digit
average over (month, town, flat_type), so different flats in the same
group with the same block prefix generate the same identifier. Our run
produces **77,246 distinct identifiers from 90,938 cleaned rows**, so
~15% of rows land in a collision bucket.

**Assumptions.**

- **Brief §2 is applied at the identifier level.** Brief *Transformation
  §2* says *"if there are any duplicate records, take the higher price
  and discard the lower price one."* Stage 5 has already deduplicated
  on the composite key of raw columns, so any *remaining* duplicates
  can only come from §1 — the identifier construction itself. We
  therefore apply §2 where the duplicates come from: rows sharing a
  `resale_identifier`.
- **Highest `resale_price` wins** on collision; losers go to
  `data/failed/identifier_collision_duplicates.csv` with a
  `failure_reason` column consistent with the other `failed/` files.

**Hashing algorithm (SHA-256).**

The identifier is hashed with **SHA-256** via `hashlib.sha256`, wrapped
by `transform.hash_identifier` as the single named entry point.
Properties, and why each matters here:

- **Cryptographically irreversible.** One-way function — given only the
  hash, recovering the plaintext identifier is computationally infeasible.
  This is the requirement the brief ("irreversible hashing algorithm")
  asks for directly.
- **256-bit output space.** Collision probability for ~77k distinct
  identifiers is approximately `77000² / 2^257 ≈ 2·10⁻⁶³` by the birthday
  bound — negligible. We don't rely on the math alone:
  `add_hashed_identifier` asserts `nunique(hash) == nunique(plaintext)`
  at runtime.
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

**Assumptions.**

- **Hashed output drops the plaintext identifier.** Per the brief, the
  Hashed output group is "Cleaned Data + Hashed Identifier Column". We
  therefore drop `resale_identifier` before writing
  `data/hashed/hdb_resale_hashed.csv` — the whole point of hashing is
  that the plaintext is not exposed next to the digest.
- **UTF-8 encoding on input.** The identifier is ASCII-only by
  construction (`S` + digits + uppercase letter), so UTF-8 and ASCII
  produce identical bytes; specifying UTF-8 is defensive against
  future identifier schemes that might include non-ASCII characters.

## Known limitations

- Recomputed lease assumes a January-1 lease start, since the source
  `lease_commence_date` is a year, not a calendar date.
- The price-anomaly heuristic is unsupervised. It does not distinguish
  intra-family transfers from data-entry errors from genuine outliers;
  it surfaces all three for review.
- The Resale Identifier is intentionally lossy. ~15% of cleaned rows
  collide on the identifier and are dropped by Stage 6b. If the
  downstream consumer needs a lossless join back to the cleaned row,
  the composite key from Stage 5 should be used, not the identifier.
