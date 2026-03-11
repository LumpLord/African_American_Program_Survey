# Data

This directory contains the datasets used and produced by the pipeline. It is organized to separate **source inputs** (raw), **intermediate handoffs** (interim), and **final analysis-ready outputs** (processed).

> Tip: If you are trying to reproduce the workflow end-to-end, start in `docs/01_overview_pipeline.md` and use the runnable commands in `examples/RUN_MANIFEST.md`.

## Directory layout

- `raw/` — **Source datasets** used to seed the workflow.
  - Canonical Carnegie/ACE institutional classifications (`ace-institutional-classifications.csv`).
  - The 2013 comparison cohort list (`unitid_name_2013comp.csv`).
  - See `raw/README.md` for provenance, capture dates, and exact headers.

- `interim/` — **Intermediate handoff tables** between pipeline stages.
  - Website-enriched inputs (adds `Web_address`).
  - Stage A crawl outputs (v15) and their batch/progress artifacts.
  - Stage B parsing/bucketing products that are used downstream.
  - Stage E per-batch NCES profile characteristics augmentation outputs.
  - See `interim/README.md` for the authoritative inventory and how to interpret each file.

- `processed/` — **Final outputs** intended for analysis and reporting.
  - A “current IPEDS” run (non‑2013 cohort) outputs.
  - A “2013 cohort + comparisons” run outputs, including program bucketing details and downstream splits.
  - See `processed/README.md` for what is considered final, known limitations, and recommended usage patterns.

## What’s required vs optional

- **Required for reproduction (core crawl + bucketing):** `raw/` inputs + the scripts in `scripts/`.
- **Optional:** the 2013-vs-current comparison steps (Stage C) only apply to the 2013 cohort and can be skipped if you are running a different baseline.

## Notes on shipped vs re-created joins

For convenience and reproducibility, this repository includes a pre-joined handoff file containing the 2013 baseline label column (`2013_program_name`) for the 2013 cohort. If you want to swap/extend the baseline list, generate your own baseline label column and join on `unitid`.

## Large files and repository footprint

Some intermediate and processed CSVs can be large. This repository includes a curated snapshot sufficient to understand the workflow and reproduce an example run. For a minimal reproducible run, use the `examples/` folder.