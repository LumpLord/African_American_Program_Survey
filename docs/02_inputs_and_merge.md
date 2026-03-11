# Inputs, Merge, and Validation (unitid-first)

This document defines the **input contract** for the pipeline and documents how we build the canonical, unitid-keyed inputs used by downstream stages.

This write-up reflects the **unitid-first full-run workflow** (see `carnegie_processing_fullruns.ipynb`) and the current repository data layout under `data/raw/`, `data/interim/`, and `data/processed/`.

If you have not installed the environment yet, start with `docs/INSTALL.md`.

---

## Key principle

All merges and joins are performed on **`unitid` (IPEDS UNITID)**.

- `unitid` is the canonical stable identifier.
- Institution names are treated as *human-readable metadata* only.
- Name-based joins are intentionally avoided because names vary across sources and over time.

---

## What “canonical input” means

Downstream pipeline stages (Stage A web scrape and beyond) assume a single CSV with **one row per institution** and a consistent set of columns.

At minimum, the canonical input must provide:
- an institution identifier (`unitid`)
- an institution name (`name`) for readability
- a seed website (`Web_address`)

For the 2013 comparison cohort, we also carry:
- a baseline program label (`2013_program_name`) when available

Stage C (2013 vs current comparison) is optional and only applies to the 2013 cohort.

---

## Upstream input files (unitid-first)

These are the primary upstream inputs used to build canonical crawl inputs.

For professional deployment and auditability, treat the two upstream sources above as the authoritative provenance for institutional metadata (Carnegie/ACE) and the baseline comparison cohort (2013 survey).

### 1) `data/raw/ace-institutional-classifications.csv`
Carnegie/ACE institutional metadata table.

**Exact headers (as of our current raw file):**
- `unitid,name,city,state,control,Institutional Classification,Student Access and Earnings Classification,Research Activity Designation,Award Level Focus,Academic Mix,Graduate Academic Program Mix,Size,Campus Setting,Highest Degree Awarded,Community Engagement,Leadership for Public Purpose`

- **Role:** Provides institutional metadata keyed by `unitid` (often including `name`).
- **Source/provenance:** downloaded from `https://carnegieclassifications.acenet.edu/institutions/` (accessed Feb 2026). Additional field definitions and context are available on the Carnegie site.

**Minimum required columns (used by our pipeline):**
- `unitid` — IPEDS UNITID (stable join key)

**Strongly recommended columns (improves readability/QA):**
- `name` — institution name

**Common additional columns (carried through if present):**
In our current export, these include: `city`, `state`, `control`, and multiple Carnegie classification descriptors (e.g., Institutional Classification, Student Access and Earnings Classification, Research Activity Designation, Award Level Focus, Academic Mix, Graduate Academic Program Mix, Size, Campus Setting, Highest Degree Awarded, Community Engagement, Leadership for Public Purpose). We carry these columns through merges so they are available for downstream analysis.

**Common formatting issues:**
- missing or non-numeric `unitid`
- duplicate `unitid`

---

### 2) `data/raw/unitid_name_2013comp.csv`
2013 comparison cohort input (baseline comparison table).

**Exact headers (as of our current raw file):**
- `unitid,name`

- **Role:** Defines the 2013 comparison cohort keyed by `unitid`.
- **Source/provenance:** derived from the 2013 Black Studies Survey (Alkalimat et al., 2013): `https://www.alkalimat.org/356%20alkalimat%20et%20al%202013%20black%20studies%20survey%20june%207%202013.pdf`.

**Minimum required columns (used by our pipeline):**
- `unitid` — IPEDS UNITID (stable join key)

**Recommended columns (improves matching and interpretability):**
- `name` — institution name (human readability)
- `2013_program_name` — baseline program label from the 2013 survey (not present in `unitid_name_2013comp.csv` in this repo snapshot; instead, we ship a baseline-labeled handoff file for the 2013 cohort — see Section D).

**Important note on historical formats:**
- In earlier drafts, the 2013 program label sometimes appeared embedded in a single string field (e.g., `"Institution Name, Program Name"`).
- For reproducible deployment, prefer an explicit `2013_program_name` column.

**Current state:**
- The file currently contains only `unitid` and `name`. The baseline program label must be attached as an explicit `2013_program_name` column before (or during) the merge step if 2013-vs-current matching is desired.

**Common formatting issues:**
- missing or non-numeric `unitid`
- duplicate `unitid`
- baseline program label stored in the wrong column due to delimiter issues

---

## What we produce during merging/enrichment

The unitid-first workflow produces a set of intermediate artifacts in `data/interim/` and curated handoffs in `data/processed/`.

### A) Unitid join diagnostics (initial cohort coverage)

Produced files (examples):
- `data/interim/unitid_join__inner_on_unitid.csv`
- `data/interim/unitid_join__2013comp_only_on_unitid.csv`
- `data/interim/unitid_join__unit_name_only_on_unitid.csv`

**Purpose:** verify that the comparison cohort and the reference table align on `unitid`.

**Interpretation note:**
- Some “only” diagnostics may be **headers-only**. This indicates **zero unmatched rows** for that diagnostic (full coverage on `unitid`).

---

### B) ACE × 2013 comparison merges (unitid-first)

Produced files (examples):
- `data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid.csv`
- `data/interim/ace_unitid_merge__2013comp_only_on_unitid.csv`
- `data/interim/ace_unitid_merge__ace_only_on_unitid.csv`

**Purpose:**
- create the main ACE×2013comp cohort for downstream crawling
- identify an ACE-only cohort (institutions not in the 2013 comparison cohort)

---

### C) Website enrichment (adds `Web_address`)

Stage A crawling requires a seed institution website in `Web_address`.

In the current workflow, `Web_address` is populated during input preparation using an institution-website enrichment step.

Produced files (examples):
- `data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL.csv`
- `data/interim/ace_unitid_merge__ace_only_on_unitid_plusURL.csv`

**Purpose:** add a seed website (`Web_address`) used by Stage A crawling.

**Implementation:**
- Script: `scripts/institution_webaddresses_get.py`

**Failure modes:**
- missing/invalid `unitid`
- source website field missing or blank for a subset of institutions
- institution website drift over time (redirects, rebranding)

---

### D) Baseline label attachment (`2013_program_name`)

Stage C (2013 vs current comparison) is optional and only applies to the 2013 cohort. For this cohort, we ship a baseline-labeled handoff file that already includes `2013_program_name`, so you do not need to recreate the join.

**Inputs:**
- Comparison cohort list: `data/raw/unitid_name_2013comp.csv`
  - **Required columns:** `unitid`, `name`
  - Historical note: `name` may embed the baseline label as `"<institution name>, <2013_program_name>"`.
- Base handoff (ACE×2013comp + URL): `data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL.csv`

**Output:**
- `data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL_plus2013name.csv` (shipped)

**Purpose:** ensure the 2013 baseline label exists as an explicit `2013_program_name` column used by downstream matching.

**Note (swap/extend the baseline):** If you want to use a different baseline list, generate your own `2013_program_name` column and left-join it on `unitid` onto the website-enriched handoff (`...plusURL.csv`) before running Stage C.

---

## Canonical input schema (for Stage A and beyond)

Stage A expects a CSV with **one row per institution**.

### Required columns

| Column | Type | Meaning |
|---|---|---|
| `unitid` | string/int-like | IPEDS UNITID (join key) |
| `name` | string | Institution name (readability) |
| `Web_address` | string | Seed website/domain or URL |

### Recommended columns (for the 2013 comparison cohort)

| Column | Type | Meaning |
|---|---|---|
| `2013_program_name` | string | Baseline program label (may be empty) |

### URL normalization rules

To reduce crawl failures:
- strip leading/trailing whitespace
- prefer a hostname/domain or a full URL
- if missing a scheme, downstream code may add `https://`

---

## Validation (planned: `scripts/validate_inputs.py`)

We will implement a dedicated validator that fails fast before crawling.

### Validation responsibilities

**Schema checks (hard errors):**
- required columns present: `unitid`, `name`, `Web_address`
- CSV is readable

**Content checks (errors or warnings depending on strictness):**
- `unitid` is non-empty and numeric-ish
- `name` is non-empty
- `Web_address` is non-empty and can be normalized
- duplicate `unitid` (warn or error; policy must be defined)

**Outputs:**
- concise console summary (row counts, missingness, duplicates)
- optional machine-readable report (CSV/JSON) listing failing rows

---

## Merge script (planned: `scripts/merge_inputs.py`)

We will formalize the notebook steps into a CLI script that:
- reads `data/raw/ace-institutional-classifications.csv`
- reads `data/raw/unitid_name_2013comp.csv`
- performs unitid-first joins and produces diagnostics
- performs website enrichment to populate `Web_address` (see `scripts/institution_webaddresses_get.py`)
- (optional; 2013 cohort only) uses the shipped baseline-labeled handoff (`...plusURL_plus2013name.csv`) or performs an equivalent `unitid` join to add `2013_program_name` before Stage C.
- writes canonical crawl inputs (ACE×2013comp and ACE-only cohorts)
- writes a merge report (row counts, unmatched keys, duplicates)

---

## Where to look for concrete examples

- `data/raw/README.md` — raw input descriptions and provenance
- `data/interim/README.md` — between-stage artifacts (including batch/progress layout)
- `data/processed/README.md` — curated cohort outputs intended for analysis

---

## Quick review checklist (before crawling)

1) Raw inputs
- `data/raw/ace-institutional-classifications.csv` contains unique, numeric-ish `unitid`
- `data/raw/unitid_name_2013comp.csv` contains unique, numeric-ish `unitid`

2) After website enrichment
- `Web_address` populated for most rows in `...plusURL.csv`
- failures are inspected for invalid `unitid` or missing/invalid source website values

3) Before Stage A
- canonical crawl input(s) have: `unitid`, `name`, `Web_address`
- for the optional 2013 comparison workflow (Stage C), use the shipped baseline-labeled handoff: `...inner_on_unitid_plusURL_plus2013name.csv` (explicit `2013_program_name`)