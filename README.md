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
│   └── io_utils.py                 # Read/write helpers
├── tests/                          # pytest unit tests
└── data/
    ├── raw/                        # Source CSVs (committed)
    ├── cleaned/                    # Pipeline outputs
    ├── failed/                     # Rejected records
    └── reports/                    # Anomaly review etc.
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
- **Dedupe (Stage 5) needs to be designed with this schema difference in
  mind.** The brief defines the composite key as "all columns except
  resale_price"; the schema difference means the key will include
  `remaining_lease`, which is `NaN` for all pre-2015 rows. Pandas treats
  `NaN == NaN` as equal for the purpose of `duplicated()`, and the three
  vintages don't share time windows, so this is workable — but the decision
  will be revisited and documented when dedupe is implemented.

## Design decisions and assumptions

To be filled in as each stage is implemented. Topics to cover:

- Why `data/raw/` is committed to the repo
- Why hand-rolled validation (no `great_expectations`) and custom profiling
  (no `ydata-profiling`)
- Lease recomputation rule and `as_of_date` handling
- Dedupe policy and composite key definition
- Anomaly detection heuristic and acknowledged blind spots
- Validation framing: rules derived from the master are forward-batch gates;
  on the master itself they primarily catch normalization defects

## Known limitations

To be filled in.

## Out of scope (Part 1, this pass)

- Resale Identifier column
- Hashed Identifier column
- `data/transformed/` and `data/hashed/` outputs
- Part 2 architecture diagrams
