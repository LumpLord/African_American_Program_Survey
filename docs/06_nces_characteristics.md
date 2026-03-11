

# Stage E: NCES/IPEDS Profile Characteristics Enrichment

This document describes **Stage E** of the pipeline: enriching institutions with characteristics scraped from **NCES IPEDS Institution Profile** pages (e.g., tuition, enrollment, race/ethnicity distributions).

Stage E is optional and is typically run **after** Stages A–D.

- Scripts:
  - `scripts/additional_institution_characteristics.py` (single-run enrichment)
  - `scripts/run_nces_characteristics_batches.py` (batch runner + merge)
- Primary output (merged): `ALL_BATCHES__nces_profile_characteristics__merged.csv`

This write-up is intended for readers who want to **reproduce** enrichment runs, **interpret** outputs, or **modify** the enrichment workflow.

If you have not installed the environment, start with `docs/INSTALL.md`.

---

## What Stage E does

For each institution (`unitid`), Stage E:

1) Opens the NCES IPEDS Institution Profile page:
   - `https://nces.ed.gov/ipeds/institution-profile/<unitid>`
2) Extracts institutional characteristics.
3) Writes results to per-run or per-batch CSV outputs.
4) Optionally merges multiple batch outputs into one consolidated file.

**Important:** The NCES profile site can change over time, and the website/characteristics fields can drift. Treat the capture date and extraction version as part of dataset provenance.

---

## Inputs

Stage E generally expects a CSV containing at least:

| Column | Meaning |
|---|---|
| `unitid` | IPEDS UNITID (stable key) |
| `name` | Institution name (readability; optional but strongly recommended) |

Typical input patterns in this repository:

- Enrich a cohort built during merging:
  - `data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL.csv`
  - `data/interim/ace_unitid_merge__ace_only_on_unitid_plusURL.csv`

- Enrich a crawl output (Stage A) or comparison output (Stage C):
  - `data/interim/ace_unitid_merge__ace_only_webscrape__v15simple.csv`
  - `data/interim/...__2013_current_matches.csv`

In practice, Stage E is often run as a separate enrichment pass and then joined back onto analysis tables by `unitid`.

---

## Dependencies and prerequisites

Stage E uses Selenium + page interaction and may also parse exported spreadsheets.

### Required
- Python environment: `aframr-runtime` (see `docs/INSTALL.md`)
- Selenium (installed in environment)
- Google Chrome installed separately (system dependency; see `docs/INSTALL.md`)

### Required Python package for spreadsheet parsing
Stage E requires `openpyxl`.

If you have not installed it yet:

```bash
conda activate aframr-runtime
conda install -c conda-forge openpyxl -y
```

### Recommended operational setup
- Use a dedicated download directory (do not use your default Downloads folder).
- Prefer small test runs before scaling.

---

## Running enrichment

### A) Single-run usage (`additional_institution_characteristics.py`)

Single-run enrichment is implemented by `scripts/additional_institution_characteristics.py` (core logic). In this repository, the **recommended reproducible entrypoint** is the batch runner (below), which provides a stable CLI, resumability via `--skip-existing`, and a merged output artifact.

If you need a one-off run on a small cohort, prefer using the batch runner with a narrow `--pattern` (e.g., matching a single batch progress file) rather than calling the module directly.

### B) Batch mode (`run_nces_characteristics_batches.py`)

Batch mode is recommended for large runs because the workflow can hang or fail intermittently. The batch runner:

- finds batch progress files by filename pattern
- runs enrichment per batch
- writes per-batch outputs into an output directory
- merges all batch outputs into one consolidated CSV

Example:

```bash
python scripts/run_nces_characteristics_batches.py \
  --root . \
  --pattern "data/interim/interim_batch_files_webscrape/*__batch_*__progress.csv" \
  --outdir "data/interim/interim_batch_files_additional_characteristics" \
  --full \
  --skip-existing \
  --keep-xlsx
```

Example-run variant (for the small worked example under `examples/`):

```bash
python scripts/run_nces_characteristics_batches.py \
  --root . \
  --pattern "examples/interim/*__batch_*__progress.csv" \
  --outdir "examples/interim/interim_batch_files_additional_characteristics" \
  --full \
  --skip-existing \
  --keep-xlsx
```

A fully worked example (commands, sanity checks, and copying the merged output to `examples/outputs/`) is recorded in `examples/RUN_MANIFEST.md`.

---

## Outputs

Stage E generally produces:

1) **Per-batch (or per-run) CSVs**
   - e.g., `ace_unitid_merge__ace_only_webscrape__v15simple__batch_###__progress__nces_profile_characteristics.csv`
   - keyed by `unitid`

2) **Merged output across batches**
   - `ALL_BATCHES__nces_profile_characteristics__merged.csv`
   - `examples/outputs/ALL_BATCHES__nces_profile_characteristics__merged.csv` (example run)

3) **Optionally: enriched analysis tables**
   - example from this repo:
     - `...__2013_current_matches__nces_profile_characteristics.csv`

### How to use outputs

- Join characteristics onto any analysis table (Stage A/B/C outputs) by `unitid`.
- Keep the merged characteristics table as a separate artifact so it can be re-used across cohorts.

---

## What fields are extracted

The exact set of extracted fields can vary by script version and by changes in NCES/IPEDS pages. For this repository’s current run, the extracted schema is explicit in the output headers below.

### Exact headers (current run)

#### A) Merged characteristics output (all batches)

- File: `data/processed/IPEDS_anti2013subset/ALL_BATCHES__nces_profile_characteristics__merged.csv`
- Header:
  - `unitid,name,Web_address,city,state,control,Institutional Classification,Student Access and Earnings Classification,Research Activity Designation,Award Level Focus,Academic Mix,Graduate Academic Program Mix,Size,Campus Setting,Highest Degree Awarded,Community Engagement,Leadership for Public Purpose,best_guess_inventory_url,best_guess_inventory_reason,url_tag,alt_candidate_urls,any_control_found,struct_hits_union,>0_struct_hits_found,program_title_count,program_titles_found,pdf_hits,total_controls_found,controls_sufficiency,status,error_detail,college_vine_site,college_vine_url,college_vine_ctrl_status,college_vine_program_title_count,college_vine_program_titles_found,afri_matches,ethnic_matches,black_matches,race_matches,anthropology,math,linguistics,chem,architect,economics,psychology,sociology,history,english,political_science,philosophy,computer_science,engineering,physics,geology,statistics,neuroscience,nces_profile_xlsx_downloaded,nces_profile_xlsx_filename,nces_profile_xlsx_dir,nces_profile_xlsx_parse_ok,nces_profile_xlsx_error,tuition_fees_ug_2024_25,tuition_fees_grad_2024_25,enrollment_total,enrollment_men,enrollment_women,pct_American Indian or Alaska Native,pct_Asian,pct_Black or African American,pct_Hispanic,pct_Native Hawaiian or Other Pacific Islander,pct_White,pct_Two or more races,pct_Race/ethnicity unknown,pct_U.S. Nonresident,__source_file`

#### B) Per-batch characteristics outputs

- Example file: `data/interim/interim_batch_files_additional_characteristics/ace_unitid_merge__ace_only_webscrape__v15simple__batch_001__progress__nces_profile_characteristics.csv`
- Header:
  - `unitid,name,Web_address,city,state,control,Institutional Classification,Student Access and Earnings Classification,Research Activity Designation,Award Level Focus,Academic Mix,Graduate Academic Program Mix,Size,Campus Setting,Highest Degree Awarded,Community Engagement,Leadership for Public Purpose,best_guess_inventory_url,best_guess_inventory_reason,url_tag,alt_candidate_urls,any_control_found,struct_hits_union,>0_struct_hits_found,program_title_count,program_titles_found,pdf_hits,total_controls_found,controls_sufficiency,status,error_detail,college_vine_site,college_vine_url,college_vine_ctrl_status,college_vine_program_title_count,college_vine_program_titles_found,afri_matches,ethnic_matches,black_matches,race_matches,anthropology,math,linguistics,chem,architect,economics,psychology,sociology,history,english,political_science,philosophy,computer_science,engineering,physics,geology,statistics,neuroscience,nces_profile_xlsx_downloaded,nces_profile_xlsx_filename,nces_profile_xlsx_dir,nces_profile_xlsx_parse_ok,nces_profile_xlsx_error,tuition_fees_ug_2024_25,tuition_fees_grad_2024_25,enrollment_total,enrollment_men,enrollment_women,pct_American Indian or Alaska Native,pct_Asian,pct_Black or African American,pct_Hispanic,pct_Native Hawaiian or Other Pacific Islander,pct_White,pct_Two or more races,pct_Race/ethnicity unknown,pct_U.S. Nonresident`

#### C) Enriched analysis table (2013 cohort)

- File: `data/processed/2013subset_with_IPEDS_comps/ace_unitid_merge__ace_x_2013comp__webscrape__v15simple__bucketed_programs__2013_current_matches__nces_profile_characteristics.csv`
- Header:
  - `unitid,name,2013_program_name,Web_address,city,state,control,Institutional Classification,Student Access and Earnings Classification,Research Activity Designation,Award Level Focus,Academic Mix,Graduate Academic Program Mix,Size,Campus Setting,Highest Degree Awarded,Community Engagement,Leadership for Public Purpose,best_guess_inventory_url,best_guess_inventory_reason,url_tag,alt_candidate_urls,any_control_found,struct_hits_union,>0_struct_hits_found,program_title_count,program_titles_found,pdf_hits,total_controls_found,controls_sufficiency,status,error_detail,college_vine_site,college_vine_url,college_vine_ctrl_status,college_vine_program_title_count,college_vine_program_titles_found,afri_matches,ethnic_matches,black_matches,race_matches,anthropology,math,linguistics,chem,architect,economics,psychology,sociology,history,english,political_science,philosophy,computer_science,engineering,physics,geology,statistics,neuroscience,program_bucket__black,program_bucket__black__crawl,program_bucket__black__cv,program_bucket__africana,program_bucket__africana__crawl,program_bucket__africana__cv,program_bucket__mena,program_bucket__mena__crawl,program_bucket__mena__cv,program_bucket__african,program_bucket__african__crawl,program_bucket__african__cv,program_bucket__minority,program_bucket__minority__crawl,program_bucket__minority__cv,program_bucket__ethnic,program_bucket__ethnic__crawl,program_bucket__ethnic__cv,program_bucket__race,program_bucket__race__crawl,program_bucket__race__cv,program_bucket__other,program_bucket__other__crawl,program_bucket__other__cv,real_nonprogram_signals,program_buckets_hit,match_2013__best_title,match_2013__best_source,match_2013__best_kind,match_2013__match_level,match_2013__match_score,match_2013__detail,match_2013__is_signal_marker_in_2013,debug__recombined_candidates_added,discovered__program_titles__crawl,discovered__program_titles__cv,discovered__signal_titles,discovered__all_titles,discovered__new_titles_unmatched,discovered__new_program_titles_when_best_signal,nces_profile_xlsx_downloaded,nces_profile_xlsx_filename,tuition_fees_ug_2024_25,tuition_fees_grad_2024_25,enrollment_total,enrollment_men,enrollment_women,pct_American Indian or Alaska Native,pct_Asian,pct_Black or African American,pct_Hispanic,pct_Native Hawaiian or Other Pacific Islander,pct_White,pct_Two or more races,pct_Race/ethnicity unknown,pct_U.S. Nonresident`

### Field groups (how to interpret)

The NCES enrichment fields appear in a few clear groups:

1) **Extraction diagnostics**
- `nces_profile_xlsx_downloaded`, `nces_profile_xlsx_filename`, `nces_profile_xlsx_dir`, `nces_profile_xlsx_parse_ok`, `nces_profile_xlsx_error`

2) **Tuition/fees fields**
- `tuition_fees_ug_2024_25`, `tuition_fees_grad_2024_25`

3) **Enrollment fields**
- `enrollment_total`, `enrollment_men`, `enrollment_women`

4) **Race/ethnicity distribution fields (percentages)**
- `pct_American Indian or Alaska Native`
- `pct_Asian`
- `pct_Black or African American`
- `pct_Hispanic`
- `pct_Native Hawaiian or Other Pacific Islander`
- `pct_White`
- `pct_Two or more races`
- `pct_Race/ethnicity unknown`
- `pct_U.S. Nonresident`

5) **Provenance fields**
- `__source_file` (present in the merged-all-batches output)

All other columns (institution metadata, Stage A diagnostics, Stage B/C columns) are carried through from upstream tables and provide the context needed to interpret missingness and failures.

---

## How to interpret Stage E results

### 1) Treat as an enrichment join, not a ground-truth audit

Stage E pulls fields from an external web source. Differences from other datasets may reflect:
- capture date differences
- NCES updates
- local parsing changes

### 2) Expect missingness and intermittent failures

Even in successful runs, you may see missing fields for specific institutions due to:
- temporary network failures
- page layout differences
- exported spreadsheet download failures

Prefer batching + resumable progress outputs.

---

## Known limitations and failure modes

Stage E is the most operationally brittle part of the pipeline.

Common issues:

1) **Hangs / timeouts at scale**
   - Selenium runs can hang on individual institutions.
   - Use batching and timeouts.

2) **Download workflow fragility**
   - If the script relies on downloaded XLSX exports, failures can occur due to:
     - blocked downloads
     - inconsistent filenames
     - browser permission prompts

3) **Site changes**
   - NCES page structure and labels can change.

4) **OS/browser variability**
   - Headless mode can behave differently by OS.

5) **Inefficiency in current architecture**
   - Stage E runs as a separate enrichment pass.
   - In a future refactor, it could be integrated upstream (shared session/crawl logic), reducing repeated work.

---

## Parameters and knobs (what to change)

The enrichment scripts typically expose parameters controlling:
- headless vs visible browser
- download directory
- wait times/timeouts
- resume/skip-existing behavior

Recommended defaults:
- headless mode for large runs
- conservative timeouts
- explicit download directory
- always write per-batch progress files

---

## Reproducibility checklist

To reproduce a Stage E run:

1) Record the input cohort file (and row count):
   - e.g., `data/interim/ace_unitid_merge__ace_only_webscrape__v15simple.csv`
2) Record capture metadata:
   - capture date, script version (git commit)
3) Record operational parameters:
   - headless setting, download directory, timeouts, batching scheme
4) Preserve batch/progress artifacts:
   - keep per-batch outputs under `data/interim/interim_batch_files_additional_characteristics/`
5) Preserve the merged output:
   - `ALL_BATCHES__nces_profile_characteristics__merged.csv`

---

## Where this stage fits

Stage E is typically used to enrich the final analysis tables:
- join characteristics onto Stage C outputs (2013→current matches)
- join characteristics onto ACE-only scrape outputs

See:
- `data/processed/README.md` (curated cohort outputs)
- `data/interim/README.md` (batch/progress layout)
- `docs/07_outputs_data_dictionary.md` (field definitions)