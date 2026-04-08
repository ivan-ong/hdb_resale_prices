# HDB Resale Flat Prices ETL — Part 1

ETL pipeline producing a cleaned master dataset of HDB resale flat transactions
covering **January 2012 – December 2016**, sourced from
[data.gov.sg collection 189](https://data.gov.sg/collections/189/view).

This is **Part 1** of the HDB Senior Data Engineer technical test. It covers
ingestion through the Cleaned dataset. Resale Identifier generation, hashing,
and the Transformed/Hashed outputs will be added in a later pass.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Run

Open and execute `notebook/hdb_etl_part1.ipynb` top to bottom. On a fresh clone
the raw CSVs are already present in `data/raw/`, so the pipeline runs without
network access.

## Tests

```bash
pytest
```

## Directory layout

```
.
├── pyproject.toml
├── README.md
├── notebook/hdb_etl_part1.ipynb   # Deliverable; orchestrates + displays
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
