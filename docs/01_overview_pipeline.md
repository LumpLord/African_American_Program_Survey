# Pipeline Overview

This document provides the **big-picture flow** of the AfrAmr Program Inventory Pipeline (v15): what each stage does, which files/scripts are involved, what artifacts are produced, and where to find deeper documentation.

If you want a runnable walkthrough, start with the **Installation** guide in `docs/INSTALL.md`, then follow the **Quickstart** in the root `README.md`.

---

## What this pipeline produces

At the end of a standard run (Stages A–D), you will have:

1) A **per-institution crawl result** that attempts to identify the best “program inventory” URL and extract program titles.
2) A **parsed/bucketed** version of those titles (wide + long forms), including “signals” (departments/centers/etc.) that can help interpret ambiguous cases.
3) A **2013 vs current** comparison table that describes match quality and failure modes.
4) A set of **review splits** (CSV files) to support manual QA.

Optionally (Stage E), you can enrich institutions with **NCES IPEDS profile characteristics** (via Selenium + exported XLSX parsing).

---

## End-to-end flow

```text
Input CSV (one row per institution)
  |
  v
Stage A: v15 scrape
  - Select best inventory URL
  - Extract titles + quality flags
  => Artifact: *__webscrape__v15simple*.csv
  |
  v
Stage B: parse + bucket
  - Normalize titles
  - Create buckets + signals
  => Artifacts: *__bucketed_programs*.csv + long-form tables
  |
  v
Stage C: 2013 vs current comparison
  - Match ladder + scores
  => Artifact: *__2013_current_matches.csv
  |
  v
Stage D: review splits (manual QA)
  - QA-ready CSV subsets
  => Artifacts: 01..07 review CSVs
  |
  v
Stage E (optional): NCES enrichment
  - Selenium + XLSX parse
  => Artifact: ALL_BATCHES__nces_profile_characteristics__merged.csv
```

---

## Stage-by-stage summary

### Stage 0 — Inputs (and optional merge/cleaning)

**Goal:** Produce a single “canonical” input CSV with the required columns for downstream steps.

**Primary file(s):**
- Canonical input CSV (user-provided or merged from multiple sources)

**Planned scripts/docs:**
- Script: `scripts/institution_webaddresses_get.py` — website enrichment: add institution `Web_address` values used to seed Stage A crawling
- Baseline label (`2013_program_name`) for the 2013 comparison cohort is provided via a shipped handoff file (see below); no join script is required for typical reproduction.
- Script: `validate_inputs.py` (planned) — validate schema and basic hygiene
- Script: `merge_inputs.py` (planned) — combine initial sources into canonical input
- Docs: `docs/02_inputs_and_merge.md`

**Artifacts:**
- Canonical Stage A input CSV (example: ACE×URL handoff, optionally with `2013_program_name`)
- Optional merge report(s) if you do a join across sources

**Common unitid-first handoff files:**
- `data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid.csv` (ACE×2013 comparison cohort, pre-website enrichment)
- `data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL.csv` (website enrichment applied: adds `Web_address`)
- `data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL_plus2013name.csv` (includes shipped baseline label: adds `2013_program_name`; you do not need to recreate this join)
- `data/interim/ace_unitid_merge__ace_only_on_unitid_plusURL.csv` (ACE-only cohort after website enrichment)

**Two key Stage 0 enrichment steps:**

1) **Website enrichment (adds `Web_address`)**
- Source: institution website field(s) assembled during the unitid-first input preparation workflow
- Output: a `...plusURL.csv` handoff used by Stage A
- Implementation: `scripts/institution_webaddresses_get.py`

2) **Baseline label (`2013_program_name`) for the 2013 comparison cohort (Stage C optional)**
- Source: `data/raw/unitid_name_2013comp.csv` (2013 survey cohort list)
- Shipped handoff: `data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL_plus2013name.csv` (includes `2013_program_name`)
- Note: Stage C (2013 vs current comparison) is optional and only applies to the 2013 cohort.
- If you want to swap/extend the baseline list, generate your own `2013_program_name` column and join it on `unitid` before running Stage C.

---

### Stage A — v15 web scrape (program inventory discovery)

**Goal:** For each institution, identify a best “program inventory” URL and extract program title candidates, along with quality flags.

**Script:**
- `v15simple_program_inventory.py`

**Typical output:**
- `*__webscrape__v15simple*.csv`

**How to interpret (high-level):**
- `best_guess_inventory_url` is the pipeline’s selected “hub/listing” URL.
- `program_title_count == 0` does **not** prove absence of a program; it often indicates blocking, JS-heavy pages, wrong hub selection, or insufficient structure.
- Quality / failure indicators:
  - `controls_sufficiency`, `any_control_found`, `struct_hits_union` (confidence that the page is a real listing)
  - `url_tag`, `status`, `error_detail` (blocking/errors)

**Deeper doc:**
- `docs/03_v15_webscrape.md`

---

### Stage B — Parse + bucket titles (and signals)

**Goal:** Normalize extracted title strings and classify them into buckets (e.g., black/africana/ethnic/etc.), producing both wide and long-form tables.

**Script:**
- `webscrape_parser.py`

**Typical outputs:**
- Wide per-institution file:
  - `*__bucketed_programs.csv`
- Long-form tables (one row per discovered title / signal):
  - `*__bucketed_programs__long.csv`
  - `*__bucketed_programs__long_programs.csv`
  - `*__bucketed_programs__long_signals.csv`
  - (optional rollups) `*__bucketed_programs__long_bucket_summary.csv`, `*__bucketed_programs__long_programs_agg.csv`

**Key interpretability concept:**
- “Programs” are candidate offerings (majors/minors/BA/BS/etc.).
- “Signals” are nearby non-program entities (departments, centers, institutes, etc.) that help interpret ambiguous listings or missing program titles.

**Deeper doc:**
- `docs/04_parser_and_bucketing.md`

---

### Stage C (optional; 2013 cohort only) — Compare 2013 baseline vs current titles

**Goal:** Determine whether the 2013 baseline program name aligns with currently discovered program titles (or supporting signals), and characterize the match quality.

**Script:**
- `2013_current_comparison.py`

**Typical output:**
- `*__2013_current_matches.csv`

**How to interpret (high-level):**
- The output adds match columns such as:
  - best matched title, source (crawl vs CollegeVine vs signals), match tier/level, score, and details.
- Matches can be strict, fuzzy, or “rescued” via category mapping / aliases (depending on settings).

**Deeper doc:**
- `docs/05_2013_current_comparison.md`

---

### Stage D — Review splits (manual QA)

**Goal:** Produce review-ready subsets (CSV) that help triage outcomes (good matches vs ambiguous vs likely failures).

**Notebook (planned):**
- `notebooks/02_review_and_QA.ipynb`

**Typical outputs (saved under `out/review_splits/`):**
- `01_inadequate_controls.csv`
- `02_adequate_controls_base.csv`
- `03_strict_match.csv`
- `04_non_strict_match.csv`
- `05_no_match__2013_empty__now_something.csv`
- `06_no_match__2013_something__now_nothing.csv`
- `07_no_match__2013_something__now_something_else.csv`

**Deeper docs:**
- `docs/05_2013_current_comparison.md` (match logic that informs splits)
- `docs/07_outputs_data_dictionary.md` (columns)

---

### Stage E (optional) — NCES IPEDS profile characteristics

**Goal:** Enrich institutions with characteristics scraped from NCES IPEDS profiles (tuition/enrollment/race-ethnicity distributions).

**Scripts:**
- `additional_institution_characteristics.py`
- `run_nces_characteristics_batches.py` (batch runner + merge)

**Typical output:**
- `ALL_BATCHES__nces_profile_characteristics__merged.csv`

**Known caveats:**
- This step is slower and brittle at scale because it uses Selenium + file downloads.
- It can hang on large sets; use batching and resumable outputs.
- Architecturally, it should have been integrated upstream (so it can share crawling/session logic), but currently runs as a separate enrichment pass.

**Deeper doc:**
- `docs/06_nces_characteristics.md`

---

## Suggested execution order

For a first-time run:

1) Install dependencies and activate the environment (see `docs/INSTALL.md`)
2) Prepare canonical input (unitid-first): website enrichment (`Web_address`), and optionally attach baseline label (`2013_program_name`) for the 2013 comparison cohort (see `docs/02_inputs_and_merge.md`)
3) Run Stage A (`v15simple_program_inventory.py`)
4) Run Stage B (`webscrape_parser.py`)
5) (Optional; 2013 cohort only) Run Stage C (`2013_current_comparison.py`)
6) Run Stage D (review splits notebook)
7) Optionally Stage E (NCES enrichment)

For large runs (e.g., ~3k institutions), use batching for Stage A and Stage E.

---

## Common failure modes (and where to look)

### 1) “Nothing found” does not mean “does not exist”
- Zero extracted titles often indicates a crawl failure, wrong hub, or JS-heavy content.
- Always inspect `url_tag`, `status`, `error_detail`, and structure flags.

See: `docs/03_v15_webscrape.md`.

### 2) Wrong hub selection
- The chosen URL may be a marketing landing page, a school directory, or a catalog index that does not list programs.

See: `docs/03_v15_webscrape.md` and `docs/04_parser_and_bucketing.md`.

### 3) Ambiguous naming / entity type confusion
- Programs vs departments vs centers can be conflated; “signals” help, but do not solve all ambiguity.

See: `docs/04_parser_and_bucketing.md` and `docs/05_2013_current_comparison.md`.

### 4) Scale + blocking
- Large runs increase the chance of 403/429 and bot detection.
- Prefer batching, caching, and conservative concurrency.

See: `docs/03_v15_webscrape.md`.

### 5) Selenium fragility (NCES)
- Downloads, timeouts, and rendering issues can cause hangs.

See: `docs/06_nces_characteristics.md`.

---

## Where to go next

- Inputs and merge/validation: `docs/02_inputs_and_merge.md`
- Website enrichment (Web_address): `scripts/institution_webaddresses_get.py` (Stage 0)
- Baseline label (`2013_program_name`) for the 2013 cohort: shipped in `data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL_plus2013name.csv` (no join script required for typical use)
- v15 scrape details: `docs/03_v15_webscrape.md`
- Parser/bucketing details: `docs/04_parser_and_bucketing.md`
- 2013 vs current matching: `docs/05_2013_current_comparison.md`
- NCES enrichment: `docs/06_nces_characteristics.md`
- Output data dictionary: `docs/07_outputs_data_dictionary.md`