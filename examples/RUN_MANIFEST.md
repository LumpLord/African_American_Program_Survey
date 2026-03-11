# Example Run Manifest

This manifest records the commands and parameters used to generate the example artifacts under `examples/`.

## Environment

- Conda env: `aframr-runtime`
- Python: 3.9.6
- Environment definition: `environment.yml` (repo root)

### Optional dependency for Stage E (additional characteristics)

Stage E (NCES/IPEDS characteristics parsing) requires `openpyxl` for reading downloaded XLSX files.

```bash
# Activate the env
conda activate aframr-runtime

# Install into the existing env (preferred)
conda install -c conda-forge openpyxl

# Sanity check
python -c "import openpyxl; print('openpyxl import: OK')"
```

> Note: Record `python --version` and the current git commit hash when available.

## Inputs

- Seed input (first 20 Carnegie/ACE rows): `examples/inputs/ace_first20.csv`
  - Source: `data/raw/ace-institutional-classifications.csv` (first 20 rows)

## Outputs (so far)

- Website-enriched example input (Stage 0): `examples/interim/ace_first20__plusURL.csv`
- Website enrichment progress (checkpoint): `examples/interim/checkpoints/ace_first20__plusURL__progress.csv`
- Stage E per-batch characteristics outputs: `examples/interim/interim_batch_files_additional_characteristics/`
- Stage E merged characteristics output (copied to outputs): `examples/outputs/ALL_BATCHES__nces_profile_characteristics__merged.csv`

## Commands executed

### 1) Stage 0 — Website enrichment (adds `Web_address`)

This step enriches the seed input by fetching NCES IPEDS institution profiles and extracting an institution website from rendered page text.

```bash
# Help / CLI verification
python scripts/institution_webaddresses_get.py -h

# Run (checkpointed)
python scripts/institution_webaddresses_get.py \
  --input  examples/inputs/ace_first20.csv \
  --output examples/interim/ace_first20__plusURL.csv \
  --progress examples/interim/checkpoints/ace_first20__plusURL__progress.csv \
  --headless
```

### 2) Stage A — v15 webscrape (program inventory discovery) with batching

This step runs the Stage A crawler against the website-enriched input and writes:
- merged Stage A output to `examples/interim/`
- per-batch progress + batch snapshots to `examples/interim/interim_batch_files_webscrape/`

```bash
# Help / CLI verification
python scripts/v15simple_program_inventory.py -h

# Run Stage A (20 rows split into exactly 2 batches)
python scripts/v15simple_program_inventory.py \
  --input examples/interim/ace_first20__plusURL.csv \
  --output examples/interim/ace_first20__webscrape__v15simple.csv \
  --batches 2 \
  --workers 2 \
  --checkpoint-every 3 \
  --compact-every 5

# Optional: move batch/progress artifacts into the example batch folder (mirrors repo layout)
mkdir -p examples/interim/interim_batch_files_webscrape
mv examples/interim/ace_first20__webscrape__v15simple__batch_* \
   examples/interim/interim_batch_files_webscrape/ 2>/dev/null || true
```

Observed artifacts (expected):
- Merged Stage A output:
  - `examples/interim/ace_first20__webscrape__v15simple.csv`
- Batch artifacts:
  - `examples/interim/interim_batch_files_webscrape/ace_first20__webscrape__v15simple__batch_001__progress.csv`
  - `examples/interim/interim_batch_files_webscrape/ace_first20__webscrape__v15simple__batch_001.csv`
  - `examples/interim/interim_batch_files_webscrape/ace_first20__webscrape__v15simple__batch_002__progress.csv`
  - `examples/interim/interim_batch_files_webscrape/ace_first20__webscrape__v15simple__batch_002.csv`

Sanity checks used:

```bash
# Confirm merged output exists and has 20 rows
head -n 2 examples/interim/ace_first20__webscrape__v15simple.csv
python - << 'PY'
import pandas as pd
p='examples/interim/ace_first20__webscrape__v15simple.csv'
df=pd.read_csv(p, dtype=str, keep_default_na=False)
print('rows:', len(df), 'cols:', len(df.columns))
print('nonempty best_guess_inventory_url:', (df.get('best_guess_inventory_url','').astype(str).str.strip()!='').sum() if 'best_guess_inventory_url' in df.columns else 'n/a')
PY

# Confirm batch artifacts present
ls -lh examples/interim/interim_batch_files_webscrape | head -40
```

### 3) Stage B — Parser + bucketing (webscrape_parser)

This step parses Stage A extracted titles and produces:
- a wide per-institution table with `program_bucket__*` columns
- long-form evidence tables (programs/signals) for QA

```bash
# Help / CLI verification
python scripts/webscrape_parser.py -h

# Run Stage B (write outputs into examples/interim)
python scripts/webscrape_parser.py \
  --input examples/interim/ace_first20__webscrape__v15simple.csv \
  --outdir examples/interim/
```

Observed artifacts (expected):
- Wide output:
  - `examples/interim/ace_first20__webscrape__v15simple__bucketed_programs.csv`
- Long-form outputs (names derived from input):
  - `...__bucketed_programs__long.csv`
  - `...__bucketed_programs__long_programs.csv`
  - `...__bucketed_programs__long_signals.csv`
  - `...__bucketed_programs__long_bucket_summary.csv`
  - `...__bucketed_programs__long_programs_agg.csv`

Note: Bucket columns may be empty for many institutions; this is expected when no titles/signals match the bucket vocab.

Sanity checks used:

```bash
# Confirm wide output exists and has 20 rows
head -n 2 examples/interim/ace_first20__webscrape__v15simple__bucketed_programs.csv
python - << 'PY'
import pandas as pd
p='examples/interim/ace_first20__webscrape__v15simple__bucketed_programs.csv'
df=pd.read_csv(p, dtype=str, keep_default_na=False)
print('rows:', len(df), 'cols:', len(df.columns))
print('has program_bucket__black:', 'program_bucket__black' in df.columns)
# quick hit-rate summary (any bucket hit)
if 'program_buckets_hit' in df.columns:
    hits = (df['program_buckets_hit'].astype(str).str.strip()!='').sum()
    print('nonempty program_buckets_hit:', hits, '/', len(df))
PY

# Confirm long-form outputs exist
ls -lh examples/interim | grep "__bucketed_programs__" | head -50
```
Observed artifacts:
- `examples/interim/checkpoints/ace_first20__plusURL__progress.csv`
- `examples/interim/ace_first20__plusURL.csv`


Quick validation used:

```bash
head -n 2 examples/interim/ace_first20__plusURL.csv
ls -lh examples/interim/checkpoints | head
```


### 4) Stage E — Additional characteristics (batched NCES/IPEDS profile parsing)

This step parses NCES/IPEDS profile XLSX exports for each Stage A batch progress file and writes:
- per-batch characteristics tables to `examples/interim/interim_batch_files_additional_characteristics/`
- a merged table `ALL_BATCHES__nces_profile_characteristics__merged.csv`

Note: In this example run, the Stage A batch progress files were matched directly from `examples/interim/`.

```bash
# Help / CLI verification
python scripts/run_nces_characteristics_batches.py -h

# Run Stage E (batched, full)
python scripts/run_nces_characteristics_batches.py \
  --root . \
  --pattern "examples/interim/*__batch_*__progress.csv" \
  --outdir "examples/interim/interim_batch_files_additional_characteristics" \
  --full \
  --skip-existing \
  --keep-xlsx

# Copy merged output to examples/outputs for easy review
cp -v examples/interim/interim_batch_files_additional_characteristics/ALL_BATCHES__nces_profile_characteristics__merged.csv \
      examples/outputs/ALL_BATCHES__nces_profile_characteristics__merged.csv
```

Sanity checks used:

```bash
# Confirm per-batch outputs exist
ls -lh examples/interim/interim_batch_files_additional_characteristics | head -40

# Confirm merged output exists and has expected header
head -n 2 examples/interim/interim_batch_files_additional_characteristics/ALL_BATCHES__nces_profile_characteristics__merged.csv

# Confirm copied output exists
ls -lh examples/outputs | head -40
```
