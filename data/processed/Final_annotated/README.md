

# Final annotated outputs

This folder contains the final, analysis-ready tables that combine:
- manual curation ("Carla" annotations), and
- automated web-scraping + NCES/IPEDS profile enrichment outputs.


## Files

This directory currently contains:

- `Carla_annotations.csv`
- `all__nces_profile_characteristics__CAT.csv`
- `Carla_annotations__with_nces_profile_characteristics.csv`
- `README.md`

### `Carla_annotations.csv`
Manually curated institution selection and program-level annotations provided by **CD Martin**.

- **Purpose:** Human-reviewed tracking table used to drive/organize manual review.
- **Source:** Derived from the repository’s web-scraping pipeline outputs.
- **Notes:** Add additional provenance details (who/when/how the manual curation was performed) in a future update.

### `all__nces_profile_characteristics__CAT.csv`
Concatenated (“CAT”) master table combining *all institutions* across:
- the **2013 cohort** (with 2013 baseline label + comparison fields), and
- the **non-2013 IPEDS cohort** (anti-2013 set).

This file aggregates the automated pipeline outputs, including web-scrape discovery fields and NCES/IPEDS profile characteristics (tuition, enrollment, race/ethnicity percentages).

Important caveats:
- **Ethnicity fields:** For 30 institutions, IPEDS profile reroutes prevented automatic extraction of race/ethnicity percentages; these values were added manually.
- **Tuition fields:** Tuition values are missing for some institutions. **Missing values do not imply the institution lacks tuition information**; they indicate the pipeline did not capture it for that case. This may be improved in future versions.

### `Carla_annotations__with_nces_profile_characteristics.csv`
Primary “final” file for downstream use.

- **What it is:** `Carla_annotations.csv` enriched by joining in NCES/IPEDS profile characteristic fields (e.g., tuition, enrollment totals, race/ethnicity percentages) keyed on `unitid`.
- **Recommended use:** This is the main table for external users who want the manually curated view plus standardized NCES/IPEDS characteristics.

## Keys and joins

- **Primary key:** `unitid` (NCES/IPEDS Unit ID). All automated enrichments and merges are keyed on `unitid`.

## Versioning notes

These outputs reflect the pipeline state and manual additions as of the file timestamps in this directory. Future pipeline versions may improve completeness (e.g., IPEDS reroute handling, tuition extraction robustness).