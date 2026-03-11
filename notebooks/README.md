# Notebooks

This folder contains Jupyter notebooks used during development, debugging, and analysis of the AfrAmr program-inventory pipeline.

These notebooks are provided for transparency and for readers who want to understand the exploratory workflow and intermediate reasoning.

If your goal is to **reproduce the pipeline**, prefer the command-line scripts and documentation:
- Pipeline overview: `docs/01_overview_pipeline.md`
- Inputs + merge/enrichment: `docs/02_inputs_and_merge.md`
- Stage A crawl: `docs/03_v15_webscrape.md` and `scripts/v15simple_program_inventory.py`
- Stage B parser/bucketing: `docs/04_parser_and_bucketing.md` and `scripts/webscrape_parser.py`
- Stage C 2013→current comparison: `docs/05_2013_current_comparison.md` and `scripts/2013_current_comparison.py`
- Stage E NCES characteristics: `docs/06_nces_characteristics.md` and `scripts/run_nces_characteristics_batches.py`
- Worked CLI example: `examples/RUN_MANIFEST.md`

## Notebook inventory

### `Carnegie_ed_processing.ipynb`

**Purpose:** Initial data preparation and exploratory merges.

**Why it’s included:**
- Documents the original unitid-first preparation decisions and intermediate files.
- Useful for understanding how the Carnegie/ACE institution table and the 2013 cohort list were combined.

### `carnegie_processing_fullruns.ipynb`

**Purpose:** Full-run version of the input preparation workflow.

**Why it’s included:**
- Captures the steps used to generate the canonical interim “handoff” tables under `data/interim/`.
- Shows how and why certain intermediate CSVs exist.

### `parser.ipynb`

**Purpose:** Development and validation of Stage B parsing/bucketing.

**Why it’s included:**
- Demonstrates expected outputs and sanity-check logic.
- Provides additional context for how bucket vocab and evidence tables were iterated.

### `postCrawl_analysis.ipynb`

**Purpose:** Post-crawl QA, review splits, and summary analysis.

**Why it’s included:**
- Demonstrates the downstream analysis used to interpret the Stage B/C outputs.
- Produces review-oriented breakdowns that informed reporting.

## Notes

- Notebooks may contain hard-coded paths used during development. CLI scripts are the authoritative reproducible entrypoints.
- For a small, self-contained example run, use the `examples/` workflow documented in `examples/RUN_MANIFEST.md`.
