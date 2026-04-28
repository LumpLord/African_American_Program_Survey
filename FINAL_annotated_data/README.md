

# FINAL annotated data

This folder is a convenience “final delivery” location intended for most end users. It contains two **renamed copies** of upstream outputs produced by the webscraping + NCES/IPEDS enrichment pipeline and subsequent manual curation.

## Files in this folder

### `FINAL_manual_subset.csv`
**What it is**
- The primary, human-curated dataset for analysis and reporting (**591 institutions**).
- This file is a renamed copy of:
  - `data/processed/Final_annotated/Carla_annotations__with_nces_profile_characteristics.csv`

**What it contains**
- A manually selected subset of institutions and manually curated program fields (maintained by Author **CD Martin**).
- Pipeline-derived institution metadata (unitid, name, city/state, control, Carnegie/ACE classifications).
- Manually curated program fields (e.g., “program name 2026”, program website(s), contacts, and whether the institution appears in the 2013 report), plus enriched NCES/IPEDS profile characteristics (tuition/enrollment/race/ethnicity).

**What it is likely used for**
- Most downstream use cases: manual review, reporting, and analysis focusing on the curated subset.
- Joining to other sources by `unitid`.

**Key join key**
- `unitid` (NCES/IPEDS Unit ID).

---

### `FINAL_all_institutions.csv`
**What it is**
- The full, automated dataset produced by the pipeline (**3,927 institutions**).
- This file is a renamed copy of:
  - `data/processed/Final_annotated/all__nces_profile_characteristics__CAT.csv`

**What it contains**
- A union of:
  - the “2013 cohort” outputs (institutions referenced by the 2013 baseline list), and
  - the “anti-2013 / broader IPEDS” run outputs.
- For each institution: scraped “best guess” program inventory hub URL, extracted titles, heuristic signals, and enriched NCES/IPEDS profile characteristics (tuition/enrollment/race/ethnicity where available).

**What it is likely used for**
- Broad coverage analyses across all institutions. Note that this institution set likely includes many instituions a potential user looking at African American Studies would like to exclude (eg Blacksmithing, Massage trade schools)
- Auditing pipeline behavior (coverage, failures, and where program inventories were/weren’t discovered).
- Re-running or extending the pipeline (as a reference output for sanity checks).

**Key join key**
- `unitid` (NCES/IPEDS Unit ID).

## Important notes and caveats

- **Renamed copies:** The two files in this folder are provided for easier consumption but are **not new computations**. They are renamed copies of the upstream files listed above.
- **Race/ethnicity coverage:** For a subset of institutions, IPEDS profile page behavior (including redirects/reroutes) prevented automated capture of race/ethnicity percentages. Missing values in these columns do **not** necessarily indicate missing data at the source.
- **Tuition coverage:** Tuition and fee fields were not captured for all institutions. Missing values do **not** imply that the institution lacks tuition information; it may reflect scraping limitations that can be improved in future versions.

## Which file should I use?

- Use **`FINAL_manual_subset.csv`** if you want the curated subset with manual program fields and enriched NCES/IPEDS characteristics (this is the typical “analysis-ready” deliverable).
- Use **`FINAL_all_institutions.csv`** if you want the full automated run across all institutions (coverage/auditing/broad analyses).

## Provenance

- Manual curation: Author **CD Martin** (details to be expanded).
- Automated enrichment and scraping: derived from the repository pipeline (scripts and documentation under `scripts/` and `docs/`).