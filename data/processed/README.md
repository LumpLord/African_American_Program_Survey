# data/processed

This folder contains **curated, shareable outputs** from the unitid-first pipeline run. These are intended to be the **stable handoff artifacts** for analysis and reporting.

- If you want **inputs**: see `data/raw/`.
- If you want **between-stage artifacts** (joins, URL enrichment, batch/progress files, crawl intermediates): see `data/interim/`.

---

## What is in `data/processed/`

Outputs are organized by **cohort**:

1) **`2013subset_with_IPEDS_comps/`**
   - Institutions in the 2013 comparison cohort, carried through scraping, bucketing, and 2013→current matching.

2) **`IPEDS_anti2013subset/`**
   - Institutions not in the 2013 comparison cohort (“ACE-only / anti-2013”), carried through Stage A scraping and NCES characteristics enrichment.

---

## 2013subset_with_IPEDS_comps/

### Primary analysis tables

#### `ace_unitid_merge__ace_x_2013comp__webscrape__v15simple__bucketed_programs__2013_current_matches.csv`
**Use this file when:** you want the core, analysis-ready 2013→current comparison results.

**What it contains (high-level):** one row per institution with:
- identifiers: `unitid`, `name`, `Web_address`
- webscrape outcome: `best_guess_inventory_url`, quality flags (controls/sufficiency/error tags), extracted titles
- bucketing outputs: bucket counts and bucketed titles/signals (wide format)
- comparison outputs: fields describing how the 2013 label matched (or did not match) the current discovered titles/signals

**Typical uses:**
- compute match rates (strict/non-strict/no-match)
- review ambiguous cases where “signals” exist but program titles are missing
- audit crawl quality vs match outcomes (e.g., low controls → likely false negatives)

---

#### `ace_unitid_merge__ace_x_2013comp__webscrape__v15simple__bucketed_programs__2013_current_matches__nces_profile_characteristics.csv`
**Use this file when:** you want the same 2013→current match table enriched with institutional characteristics.

**What it contains:** all columns from the primary match table above **plus** NCES/IPEDS profile characteristics joined by `unitid` (e.g., enrollment, tuition, race/ethnicity distributions; exact fields depend on the NCES extraction script).

**Typical uses:**
- stratify match outcomes by institutional characteristics
- QA whether certain institution types systematically fail scraping or matching

---

### Supporting / interpretability materials

#### `2013_to_now_file_scheme.png`
**Use this file when:** onboarding someone to the matching logic.

**What it contains:** a visual schematic of the 2013→current matching flow.

---

#### `simplified_splits/`
**Use this folder when:** manual QA or targeted review is needed.

**What it contains:** subsets of the primary match table grouped into review categories (e.g., strict matches, non-strict matches, no-match cases, inadequate controls). These are derived from the match table and should be reproducible from it.

---

#### `program_bucketing_details/`
**Use this folder when:** interpreting or auditing the bucketing rules.

**What it contains:** supporting documentation/materials describing bucketing logic and how specific titles/signals are assigned to buckets.

---

## IPEDS_anti2013subset/

### Primary analysis tables

#### `ace_unitid_merge__ace_only_webscrape__v15simple.csv`
**Use this file when:** you want the Stage A scrape results for the ACE-only cohort.

**What it contains (high-level):** one row per institution with:
- identifiers: `unitid`, `name`, and the seed website (`Web_address`)
- selected hub/listing URL: `best_guess_inventory_url` (+ reason)
- extracted program titles (if any): `program_titles_found`, `program_title_count`
- quality/error flags: controls/sufficiency, HTTP/blocking tags, error details

**Interpretation note:** this is a *raw scrape result* even though it is stored here as a canonical cohort output. Titles may be noisy and “no titles found” does not imply a program does not exist.

---

#### `ALL_BATCHES__nces_profile_characteristics__merged.csv`
**Use this file when:** you need NCES/IPEDS profile characteristics for enrichment joins.

**What it contains:** a merged table of NCES/IPEDS profile characteristics across all processed batches, keyed by `unitid`.

**Typical uses:**
- join onto scrape outputs (ACE-only or 2013 cohort) by `unitid`
- cohort profiling and stratified analyses

---

## Limitations and interpretation notes

### General caveat: “no titles found” is not proof of absence

Across cohorts, Stage A is a best-effort heuristic crawl. A row with `program_title_count = 0` or empty `program_titles_found` does **not** prove that an institution has no relevant program. It indicates that, given the seed website (`Web_address`) and the crawler’s discovery heuristics, a stable “program inventory” page and extractable titles were not found.

### ACE-only / anti-2013 cohort: why many rows have sparse program evidence

In `IPEDS_anti2013subset/`, many institutions yield little or no extractable program inventory evidence. This is expected and is driven by structural differences in the cohort:

- **Less standardized websites:** many smaller institutions do not maintain a centralized majors/minors inventory page that the crawler can reliably discover.
- **Specialized institutions:** trade, vocational, or religious institutions often organize offerings differently (certificates, tracks, ministry training, apprenticeships), and “majors/minors” language may be absent.
- **Third-party coverage gaps:** the CollegeVine fallback source is not comprehensive. Many small or specialized institutions are missing from CollegeVine or have incomplete majors listings, reducing corroborating signal.

As a result, “no program titles found” is more common in this cohort and should be interpreted as a discovery/coverage limitation rather than a definitive classification.

### Quality flags matter

When interpreting downstream analyses, use crawl quality fields such as `controls_sufficiency`, `url_tag`, `status`, and `error_detail` to distinguish likely true negatives from crawl failures (e.g., blocked/rate-limited sites, non-academic subsites, or insufficient control-term hits).

---

## How to reproduce these outputs

A minimal reproduction path:

1) Install and activate the environment:
   - `docs/INSTALL.md`
2) Review the end-to-end process:
   - `docs/01_overview_pipeline.md`
3) Prepare/validate inputs (unitid-first):
   - `docs/02_inputs_and_merge.md`
   - `data/raw/README.md`
4) Execute the pipeline stages (and batch runs if needed):
   - `data/interim/README.md` documents the intermediate artifacts and batch/progress layout.
5) Interpret output columns:
   - `docs/07_outputs_data_dictionary.md`
