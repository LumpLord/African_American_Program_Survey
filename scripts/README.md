# Scripts

This folder contains the command-line entrypoints for the AfrAmr program-inventory pipeline.

For an end-to-end worked example (commands + sanity checks + expected files), see:
- `examples/RUN_MANIFEST.md`

For high-level pipeline context, see:
- `docs/01_overview_pipeline.md`

## Quickstart (example run)

From the repo root:

```bash
# Environment (first time)
conda env create -f environment.yml

# Activate
conda activate aframr-runtime

# Optional (Stage E): XLSX reader
conda install -c conda-forge openpyxl
```

## Script inventory

### Stage 0 — Website enrichment (adds `Web_address`)

**Script:** `institution_webaddresses_get.py`

Populates a seed website (`Web_address`) for each institution by loading the NCES IPEDS institution profile page for a `unitid` and extracting a likely institution website from the rendered page text.

Common usage (checkpointed):

```bash
python scripts/institution_webaddresses_get.py \
  --input  examples/inputs/ace_first20.csv \
  --output examples/interim/ace_first20__plusURL.csv \
  --progress examples/interim/checkpoints/ace_first20__plusURL__progress.csv \
  --headless
```

Key flags:
- `--progress PATH` (checkpoint/resume file)
- `--checkpoint-every N` (flush cadence)
- `--recycle-driver-every N` (driver recycling)
- `--start-fresh` (delete progress file)
- `--resume/--no-resume`
- `--dedupe-unitid/--no-dedupe-unitid`

### Stage 0 — Baseline label attachment (adds `2013_program_name`)

**Status:** not currently implemented as a standalone CLI script in `scripts/`.

The 2013 comparison workflow requires a `2013_program_name` column (baseline label) for Stage C.
Historically, this column was attached by parsing it from `data/raw/unitid_name_2013comp.csv` and left-joining on `unitid`.

If you need to reproduce the 2013 baseline label attachment step, implement a small join script (recommended name: `scripts/attach_2013_program_name.py`) or reproduce the join in a notebook using `notebooks/carnegie_processing_fullruns.ipynb` as a reference. These data must be generated from the source: https://www.alkalimat.org/356%20alkalimat%20et%20al%202013%20black%20studies%20survey%20june%207%202013.pdf, OR use the provided input.

### Stage A — Program inventory crawl (v15)

**Script:** `v15simple_program_inventory.py`

Discovers a likely institution “program inventory hub” and extracts candidate program titles using a crawl + scoring heuristic. Writes a single merged Stage A output plus per-batch progress artifacts.

Example run (2 batches for demonstration):

```bash
python scripts/v15simple_program_inventory.py \
  --input  examples/interim/ace_first20__plusURL.csv \
  --output examples/interim/ace_first20__webscrape__v15simple.csv \
  --batches 2 \
  --workers 2 \
  --checkpoint-every 3 \
  --compact-every 5
```

Key flags:
- `--batches N` / `--batch-size N`
- `--workers N`
- `--checkpoint-every N`
- `--compact-every N`
- `--head N` (0 = full run)

### Stage B — Parser + bucketing

**Script:** `webscrape_parser.py`

Parses Stage A extracted titles and assigns “bucket” categories (e.g., `program_bucket__black`, `program_bucket__africana`, etc.). Produces a wide table and several long-form evidence tables.

```bash
python scripts/webscrape_parser.py \
  --input examples/interim/ace_first20__webscrape__v15simple.csv \
  --outdir examples/interim/
```

Outputs (derived from input name):
- `*__bucketed_programs.csv`
- `*__bucketed_programs__long.csv`
- `*__bucketed_programs__long_programs.csv`
- `*__bucketed_programs__long_signals.csv`
- `*__bucketed_programs__long_bucket_summary.csv`
- `*__bucketed_programs__long_programs_agg.csv`

### Stage C — 2013 vs current comparison

**Script:** `2013_current_comparison.py`

Compares the 2013 baseline label (`2013_program_name`) to discovered titles (crawl + CollegeVine) and emits match/diagnostic columns.

```bash
python scripts/2013_current_comparison.py \
  --input  examples/interim/ace_first20__webscrape__v15simple__bucketed_programs.csv \
  --output examples/interim/ace_first20__bucketed_programs__2013_current_matches.csv
```

### Stage E — Additional institution characteristics

**Scripts:**
- `additional_institution_characteristics.py` (core enrichment logic)
- `run_nces_characteristics_batches.py` (batch runner + merge)

Batch mode is the recommended entrypoint. It consumes Stage A batch progress files and writes per-batch outputs plus a merged table.

Example (worked run):

```bash
python scripts/run_nces_characteristics_batches.py \
  --root . \
  --pattern "examples/interim/*__batch_*__progress.csv" \
  --outdir "examples/interim/interim_batch_files_additional_characteristics" \
  --full \
  --skip-existing \
  --keep-xlsx

# Copy merged output for easy review
cp -v examples/interim/interim_batch_files_additional_characteristics/ALL_BATCHES__nces_profile_characteristics__merged.csv \
      examples/outputs/ALL_BATCHES__nces_profile_characteristics__merged.csv
```

## Conventions

- All scripts support `-h/--help`.
- Prefer writing example artifacts under `examples/interim/` and final handoff files under `examples/outputs/`.
- For repo-scale runs, write intermediate artifacts under `data/interim/` and final outputs under `data/processed/`.

## Notes on naming

- `institution_webaddresses_get.py` performs **website enrichment**: it scrapes the NCES IPEDS institution profile page to extract a likely institution base website (`Web_address`). It does **not** attach `2013_program_name`.