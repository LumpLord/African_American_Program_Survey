# data/raw

This folder contains **upstream source inputs** for the unitid-first pipeline run (see `docs/02_inputs_and_merge.md` and `notebooks/carnegie_processing_fullruns.ipynb`). These files are the starting point for the merge/enrichment steps that produce the canonical inputs used by downstream crawling and analysis.

**Key principle:** all merges and joins are performed on **`unitid` (IPEDS UNITID)**. Name-based joins are intentionally avoided because institution names are not stable identifiers.

---

## Files in this folder

### `ace-institutional-classifications.csv`
Carnegie/ACE institutional metadata table.

Source: Carnegie Classification / ACE-NSC (downloaded from `https://carnegieclassifications.acenet.edu/institutions/`; accessed Feb 2026).

- **Role:** Provides institutional metadata keyed by `unitid` (and typically includes `name`).
- **Minimum required columns:**
  - `unitid`
- **Strongly recommended columns:**
  - `name` (human readability / debugging)
- **Typical downstream use:**
  - merged against the 2013 comparison set to form ACE×2013comp cohorts
  - used to identify an “ACE-only” cohort (ACE rows not present in the comparison set)

---

### `unitid_name_2013comp.csv`
2013 comparison input (baseline comparison table).

Source: Alkalimat et al. (2013) survey list (see `https://www.alkalimat.org/356%20alkalimat%20et%20al%202013%20black%20studies%20survey%20june%207%202013.pdf`).

- **Role:** Defines the baseline comparison set keyed by `unitid`.
- **Minimum required columns:**
  - `unitid`
- **Recommended columns (for downstream matching/QA):**
  - `name` (institution name)
  - `2013_program_name` (baseline program label)

Notes:
- In the historical workflow, the 2013 program label was sometimes embedded in a string column (e.g., `"Institution Name, Program Name"`). For professional deployment, prefer an explicit `2013_program_name` column.
- `unitid` values should be unique (one row per institution).

---

## Provenance and capture metadata

### IPEDS / NCES website field provenance

Some fields used downstream (especially `Web_address`) may be sourced from NCES IPEDS institution profiles.

When reproducing or auditing runs, record the following metadata alongside any exported/derived files:

- **Source:** NCES IPEDS Institution Profile
- **Base URL pattern:** `https://nces.ed.gov/ipeds/institution-profile/<unitid>`
- **Field(s) pulled:** `Web_address` (institution website)
- **How extracted in this repo:** `scripts/institution_webaddresses_get.py` loads the profile page via Selenium and extracts a candidate website from rendered page text.
- **Capture date:** varies by run (record the date you execute `scripts/institution_webaddresses_get.py`)
- **Notes:** The IPEDS website field can change over time (redirects, rebranding, domain changes). Treat the capture date as part of the dataset provenance.

---

## Deployment notes

- Treat `data/raw/` as **inputs only**. Outputs produced by merges, enrichment, crawling, parsing, and matching should be written to `data/interim/` (between-stage artifacts) and/or `data/processed/` (curated handoffs).
- Before running the pipeline, validate raw inputs for:
  - required columns present
  - non-empty, numeric-ish `unitid`
  - uniqueness of `unitid`

See also:
- `docs/01_overview_pipeline.md` (pipeline overview)
- `docs/02_inputs_and_merge.md` (input contract and merge steps)
- `data/interim/README.md` (between-stage artifacts and run outputs)
