# data/interim

This folder contains **intermediate artifacts** produced during the unitid-first pipeline run (see `docs/02_inputs_and_merge.md` and `notebooks/carnegie_processing_fullruns.ipynb`). These artifacts are not the final “deliverables,” but they are essential for:
- reproducing the run step-by-step,
- auditing decisions (joins, URL enrichment, crawl outcomes),
- resuming long-running steps (batching/progress), and
- debugging failure modes.

Conventions:
- `data/raw/` = upstream inputs
- `data/interim/` = between-stage artifacts (this folder)
- `data/processed/` = curated handoff outputs intended to be re-used or shared

---

## Folder layout

Large runs generate many batch/progress files. To keep the top-level readable:

- `interim_batch_files_webscrape/` — Stage A (v15) webscrape batch + progress files
- `interim_batch_files_additional_characteristics/` — NCES/IPEDS enrichment progress and per-batch outputs
- `checkpoints/` — resumable snapshots from slow/brittle steps (e.g., NCES website enrichment)

The **merged, canonical outputs** for each stage (single CSVs that represent the combined result) remain in the top-level `data/interim/` directory.

---

## Top-level files currently present

These files reflect the core end-to-end chain from unitid joins → URL enrichment → webscrape → parsing/bucketing → 2013/current comparison.

### 1) Unitid join diagnostics

#### `unitid_join__inner_on_unitid.csv`
Inner join output on `unitid` for the initial comparison set.

#### `unitid_join__2013comp_only_on_unitid.csv`
Rows present in the 2013 comparison input but missing from the reference table on `unitid`.

#### `unitid_join__unit_name_only_on_unitid.csv`
Rows present in the reference unitid/name table but missing from the 2013 comparison input on `unitid`.

**Headers-only diagnostics:**
- If `unitid_join__2013comp_only_on_unitid.csv` and/or `unitid_join__unit_name_only_on_unitid.csv` contain only headers, this indicates **zero unmatched rows** for that diagnostic (i.e., full coverage on `unitid`).

---

### 2) ACE × 2013 comparison merges (unitid-first)

#### `ace_unitid_merge__ace_x_2013comp__inner_on_unitid.csv`
Core inner merge of ACE metadata with the 2013 comparison table on `unitid`.

#### `ace_unitid_merge__2013comp_only_on_unitid.csv`
2013 comparison rows that did not match ACE on `unitid`.

#### `ace_unitid_merge__ace_only_on_unitid.csv`
ACE rows that did not match the 2013 comparison table on `unitid` (an “ACE-only” cohort).

---

### 3) NCES/IPEDS website enrichment handoffs

#### `ace_unitid_merge__ace_only_on_unitid_plusURL.csv`
ACE-only cohort after adding `Web_address` from NCES/IPEDS institution profiles.

#### `ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL.csv`
ACE×2013comp cohort after adding `Web_address` from NCES/IPEDS institution profiles. This is the **canonical handoff** into Stage A crawling.

#### `ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL_plus2013name.csv`
ACE×2013comp cohort with `Web_address` and an explicit 2013 baseline label column (`2013_program_name`) included for downstream matching. **This baseline-labeled handoff is shipped with the repo, so you do not need to recreate the join for typical use.**

Notes:
- Website enrichment is performed by `scripts/institution_webaddresses_get.py`, which loads NCES IPEDS institution profiles: `https://nces.ed.gov/ipeds/institution-profile/<unitid>`.
- Failures at this step usually indicate missing/invalid `unitid`, Selenium/Chrome setup issues, rate limiting, or short/atypical profile pages where no institution website can be extracted.

Baseline swapping/extending: if you want to use a different baseline list for comparison, generate your own `2013_program_name` column and left-join it on `unitid` onto the website-enriched handoff (`...plusURL.csv`) before running Stage C.

---

### 4) Stage A output (v15 webscrape)

#### `ace_unitid_merge__ace_x_2013comp__webscrape__v15simple.csv`
Merged Stage A crawl output containing best-guess program inventory URLs (`best_guess_inventory_url`), extracted title candidates, and quality/error flags.

#### `ace_unitid_merge__ace_only_webscrape__v15simple.csv`
Merged Stage A crawl output for the ACE-only cohort.

---

### 5) Stage B outputs (parser/bucketing)

#### `ace_unitid_merge__ace_x_2013comp__webscrape__v15simple__bucketed_programs.csv`
Wide per-institution bucket output.

#### Long-form tables
- `ace_unitid_merge__ace_x_2013comp__webscrape__v15simple__bucketed_programs__long.csv`
- `ace_unitid_merge__ace_x_2013comp__webscrape__v15simple__bucketed_programs__long_bucket_summary.csv`
- `ace_unitid_merge__ace_x_2013comp__webscrape__v15simple__bucketed_programs__long_programs.csv`
- `ace_unitid_merge__ace_x_2013comp__webscrape__v15simple__bucketed_programs__long_programs_agg.csv`
- `ace_unitid_merge__ace_x_2013comp__webscrape__v15simple__bucketed_programs__long_signals.csv`

---

### 6) Stage C output (2013 vs current comparison)

#### `ace_unitid_merge__ace_x_2013comp__webscrape__v15simple__bucketed_programs__2013_current_matches.csv`
Adds match columns describing whether the 2013 baseline program label aligns with the currently discovered titles/signals. This file is produced only for the ACE×2013 comparison cohort. **Stage C is optional and only applies to the 2013 cohort workflow** (it does not apply to the ACE-only cohort).

---

## Subfolders

### `checkpoints/`
Holds resumable snapshots used to recover from slow or brittle steps.

- This directory may be empty in a clean checkout; it is used only when a long-running step is checkpointed for resume/debugging.

### `interim_batch_files_webscrape/`
Batch and progress files from Stage A webscrape runs.

- Contains `...__batch_###.csv` and `...__batch_###__progress.csv` for both the ACE×2013comp cohort and the ACE-only cohort.
- The merged outputs are kept at top-level as:
  - `ace_unitid_merge__ace_x_2013comp__webscrape__v15simple.csv`
  - `ace_unitid_merge__ace_only_webscrape__v15simple.csv`

### `interim_batch_files_additional_characteristics/`
Progress and per-batch outputs for NCES/IPEDS profile characteristics.

- Contains per-batch progress outputs such as `...__progress__nces_profile_characteristics.csv`.
- May also contain temporary progress files from website enrichment (e.g., `...__progressTEMP.csv`).

---
