# AfrAmr Program Inventory Pipeline (v15)

This repository builds a structured “program inventory” dataset for U.S. institutions by:
1) discovering each institution’s best “program listing” URL,
2) extracting program titles (and related signals),
3) normalizing/bucketing titles into categories relevant to Africana / Black / Ethnic studies,
4) comparing discovered titles against a 2013 baseline list,
5) optionally enriching institutions with NCES IPEDS profile characteristics.

**Important interpretation warning:**  
A missing program title in the scrape output does **not** prove the program does not exist. It may indicate blocking, wrong hub selection, heavy JavaScript rendering, insufficient page structure, or naming differences. Use the quality flags and review splits.

---

## Pipeline at a glance

Stage 0 — Input enrichment (adds institution website + optional 2013 label)  
- Script: `scripts/institution_webaddresses_get.py` (adds `Web_address` via NCES IPEDS profile pages)  
- Baseline label (`2013_program_name`) for the 2013 comparison cohort is provided in the shipped handoff file `data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL_plus2013name.csv` (so you do not need to recreate the join).  
- Output: `*__plusURL.csv` and `*__plusURL_plus2013name.csv`  
- Docs: `docs/02_inputs_and_merge.md`

Stage A — Web scrape + “best inventory URL” selection  
- Script: `scripts/v15simple_program_inventory.py`  
- Output: `*__webscrape__v15simple*.csv`  
- Docs: `docs/03_v15_webscrape.md`

Stage B — Parse + bucket program titles and signals  
- Script: `scripts/webscrape_parser.py`  
- Output: `*__bucketed_programs.csv` plus long-form tables  
- Docs: `docs/04_parser_and_bucketing.md`

Stage C (optional; 2013 cohort only) — Compare 2013 baseline vs current discovered titles  
- Script: `scripts/2013_current_comparison.py`  
- Output: `*__2013_current_matches.csv`  
- Docs: `docs/05_2013_current_comparison.md`

Stage D (manual QA)

The original workflow generated review splits in notebooks (see `notebooks/postCrawl_analysis.ipynb`). A dedicated QA-splits notebook is planned but not included in this repo.

Stage E (optional) — NCES IPEDS Profile Characteristics (Selenium + XLSX parsing)  
- Scripts: `scripts/additional_institution_characteristics.py`, `scripts/run_nces_characteristics_batches.py`  
- Output: `ALL_BATCHES__nces_profile_characteristics__merged.csv`  
- Docs: `docs/06_nces_characteristics.md`

More detail: see `docs/01_overview_pipeline.md`.

---

### Final annotated datasets

The easiest entry point for most users is the folder `FINAL_annotated_data/`.

- `FINAL_annotated_data/FINAL_manual_subset.csv` — manually curated subset (the primary end-user table).
- `FINAL_annotated_data/FINAL_all_institutions.csv` — all institutions (full scraped/enriched table).

These are renamed copies of the corresponding `data/processed/Final_annotated/` outputs. The file `FINAL_annotated_data/README.md` documents provenance and interpretation notes.

## Quickstart (10-row demo)

### 1) Create / activate environment

This repo is tested with **Python 3.9.6** using a conda environment defined in `environment.yml`.

```bash
# Create the environment (first time only)
conda env create -f environment.yml

# Activate
conda activate aframr-runtime

# Optional (Stage E): install XLSX reader
conda install -c conda-forge openpyxl
```

For a fully worked example run (commands + sanity checks), see `examples/RUN_MANIFEST.md`.

### 2) Run Stage 0: website enrichment (adds Web_address)

(This demo uses the ACE-only example input and does not attach the optional 2013 baseline label. For the 2013 comparison cohort workflow, see `docs/02_inputs_and_merge.md`.)

```bash
python scripts/institution_webaddresses_get.py \
  --input  examples/inputs/ace_first20.csv \
  --output examples/interim/ace_first20__plusURL.csv \
  --progress examples/interim/checkpoints/ace_first20__plusURL__progress.csv \
  --headless
```

### 3) Run Stage A: v15 scrape (2 batches for the example)

```bash
python scripts/v15simple_program_inventory.py \
  --input  examples/interim/ace_first20__plusURL.csv \
  --output examples/interim/ace_first20__webscrape__v15simple.csv \
  --batches 2 \
  --workers 2 \
  --checkpoint-every 3 \
  --compact-every 5
```

Output created:  
- `examples/interim/ace_first20__webscrape__v15simple.csv`

How to interpret: `docs/03_v15_webscrape.md`.

### 4) Run Stage B: parse + bucket titles (and signals)

```bash
python scripts/webscrape_parser.py \
  --input examples/interim/ace_first20__webscrape__v15simple.csv \
  --outdir examples/interim/
```

Outputs created (typical):  
- `examples/interim/ace_first20__webscrape__v15simple__bucketed_programs.csv`  
- `examples/interim/ace_first20__webscrape__v15simple__bucketed_programs__long.csv`  
- `examples/interim/ace_first20__webscrape__v15simple__bucketed_programs__long_programs.csv`  
- `examples/interim/ace_first20__webscrape__v15simple__bucketed_programs__long_signals.csv`

If you only need the final tables, start in `FINAL_annotated_data/`.

How to interpret: `docs/04_parser_and_bucketing.md`.

### 5) (Optional; 2013 cohort only) Run Stage C: compare 2013 baseline vs current discovered titles

Stage C requires a `2013_program_name` column. This repo ships a baseline-labeled handoff for the 2013 cohort (`...plusURL_plus2013name.csv`). If you want to use a different baseline list, generate your own `2013_program_name` column and join it on `unitid` before running Stage C.

```bash
python scripts/2013_current_comparison.py \
  --input examples/interim/ace_first20__webscrape__v15simple__bucketed_programs.csv \
  --output examples/interim/ace_first20__bucketed_programs__2013_current_matches.csv
```

Output created:  
- `examples/interim/ace_first20__bucketed_programs__2013_current_matches.csv`

How to interpret: `docs/05_2013_current_comparison.md`.

### Stage D (manual QA)

The original workflow generated review splits in notebooks (see `notebooks/postCrawl_analysis.ipynb`). A dedicated QA-splits notebook is planned but not included in this repo.

### Stage E (optional): NCES IPEDS profile characteristics

This step is slower and uses Selenium + file downloads; run it after you’ve validated Stage A–D.

Planned notebook reference:  
- `notebooks/postCrawl_analysis.ipynb`

Or run from CLI (batch mode):  
- `scripts/run_nces_characteristics_batches.py` (batch runner + merge)

A worked example (including the glob pattern used to find Stage A batch progress files and copying the merged output to `examples/outputs/`) is recorded in `examples/RUN_MANIFEST.md`.

More detail: `docs/06_nces_characteristics.md`.

---

## Documentation map

- `docs/01_overview_pipeline.md` (pipeline overview)  
- `docs/02_inputs_and_merge.md` (input data and schema)  
- `docs/03_v15_webscrape.md` (Stage A web scraping)  
- `docs/04_parser_and_bucketing.md` (Stage B parsing and bucketing)  
- `docs/05_2013_current_comparison.md` (Stage C comparison)  
- `docs/06_nces_characteristics.md` (Stage E NCES enrichment)  
- `docs/07_outputs_data_dictionary.md` (output columns and data dictionary)  
- `notebooks/Carnegie_ed_processing.ipynb`  
- `notebooks/carnegie_processing_fullruns.ipynb`  
- `notebooks/parser.ipynb`  
- `notebooks/postCrawl_analysis.ipynb`