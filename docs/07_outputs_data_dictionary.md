# Output Data Dictionary

This document is the **column reference** for the major pipeline outputs. It is intended to help readers interpret outputs without reading source code.

Scope:
- Stage A: v15 scrape output (`*__webscrape__v15simple.csv`)
- Stage B: parser/bucketing outputs (`*__bucketed_programs*.csv`)
- Stage C: 2013 vs current match output (`*__2013_current_matches.csv`)
- Stage E: NCES/IPEDS characteristics outputs (`*__nces_profile_characteristics*.csv`, merged outputs)

For end-to-end process documentation, see:
- `docs/01_overview_pipeline.md`
- `docs/03_v15_webscrape.md`
- `docs/04_parser_and_bucketing.md`
- `docs/05_2013_current_comparison.md`
- `docs/06_nces_characteristics.md`

---

## Conventions

- **Keys**: `unitid` is the stable join key used across all stages.
- **Cohorts**: outputs may exist for multiple cohorts (e.g., ACE×2013comp vs ACE-only). Column meanings are the same.
- **Carry-through**: later-stage tables often retain upstream columns for QA and interpretability.

---

## Stage A output: `*__webscrape__v15simple.csv`

**Row grain:** one row per institution (`unitid`).

### Exact headers (current run)

- **ACE×2013comp cohort (includes `2013_program_name`):**
  - `unitid,name,2013_program_name,Web_address,city,state,control,Institutional Classification,Student Access and Earnings Classification,Research Activity Designation,Award Level Focus,Academic Mix,Graduate Academic Program Mix,Size,Campus Setting,Highest Degree Awarded,Community Engagement,Leadership for Public Purpose,best_guess_inventory_url,best_guess_inventory_reason,url_tag,alt_candidate_urls,any_control_found,struct_hits_union,>0_struct_hits_found,program_title_count,program_titles_found,pdf_hits,total_controls_found,controls_sufficiency,status,error_detail,college_vine_site,college_vine_url,college_vine_ctrl_status,college_vine_program_title_count,college_vine_program_titles_found,afri_matches,ethnic_matches,black_matches,race_matches,anthropology,math,linguistics,chem,architect,economics,psychology,sociology,history,english,political_science,philosophy,computer_science,engineering,physics,geology,statistics,neuroscience`

- **ACE-only cohort:**
  - `unitid,name,Web_address,city,state,control,Institutional Classification,Student Access and Earnings Classification,Research Activity Designation,Award Level Focus,Academic Mix,Graduate Academic Program Mix,Size,Campus Setting,Highest Degree Awarded,Community Engagement,Leadership for Public Purpose,best_guess_inventory_url,best_guess_inventory_reason,url_tag,alt_candidate_urls,any_control_found,struct_hits_union,>0_struct_hits_found,program_title_count,program_titles_found,pdf_hits,total_controls_found,controls_sufficiency,status,error_detail,college_vine_site,college_vine_url,college_vine_ctrl_status,college_vine_program_title_count,college_vine_program_titles_found,afri_matches,ethnic_matches,black_matches,race_matches,anthropology,math,linguistics,chem,architect,economics,psychology,sociology,history,english,political_science,philosophy,computer_science,engineering,physics,geology,statistics,neuroscience`

### Identifier fields

| Column | Type | Meaning |
|---|---|---|
| `unitid` | int-like | IPEDS UNITID (join key) |
| `name` | string | Institution name |
| `Web_address` | string | Seed website/domain or URL used to discover program inventory pages |

### Inventory URL selection

| Column | Type | Meaning |
|---|---|---|
| `best_guess_inventory_url` | string | Selected “program inventory” hub/listing URL |
| `best_guess_inventory_reason` | string | Scoring/explanation string for why the URL was selected |
| `alt_candidate_urls` | string | Other candidate URLs considered (serialized) |

### Extracted titles

| Column | Type | Meaning |
|---|---|---|
| `program_title_count` | int | Count of extracted candidate titles |
| `program_titles_found` | string | Extracted titles (serialized; treat as raw text) |

### Structure/quality signals (“controls”)

| Column | Type | Meaning |
|---|---|---|
| `any_control_found` | bool | Whether any structural/listing controls were detected |
| `struct_hits_union` | string | Summary of which structure tests fired |
| `>0_struct_hits_found` | bool | Convenience: any structural hits |
| `total_controls_found` | int | Count of control signals detected |
| `controls_sufficiency` | string | Heuristic summary of whether controls are “enough” to trust the page |

### Failure / status fields

| Column | Type | Meaning |
|---|---|---|
| `url_tag` | string | High-level tag for outcome (success/blocked/timeout/parse failure; vocabulary is run-dependent) |
| `status` | string | Script status label (implementation-defined) |
| `error_detail` | string | Exception/failure detail (truncated) |

### Optional external source fields (if present)

| Column | Type | Meaning |
|---|---|---|
| `college_vine_site` | string | External source site label |
| `college_vine_url` | string | External source URL |
| `college_vine_ctrl_status` | string | External source control/status label |
| `college_vine_program_title_count` | int | Count of external-source titles |
| `college_vine_program_titles_found` | string | External-source titles (serialized) |

### Optional token counters (if present)

These are quick string-match counters computed from extracted text.

| Column | Type | Meaning |
|---|---|---|
| `afri_matches` | int | Count of “afri*” token matches |
| `black_matches` | int | Count of “black*” token matches |
| `ethnic_matches` | int | Count of “ethnic*” token matches |
| `race_matches` | int | Count of “race*” token matches |

---

## Stage B outputs: `*__bucketed_programs*.csv`

Stage B produces both a wide per-institution output and multiple long-form tables.

### Exact headers (current run)

#### Wide output (`*__bucketed_programs.csv`)

- `unitid,name,2013_program_name,Web_address,city,state,control,Institutional Classification,Student Access and Earnings Classification,Research Activity Designation,Award Level Focus,Academic Mix,Graduate Academic Program Mix,Size,Campus Setting,Highest Degree Awarded,Community Engagement,Leadership for Public Purpose,best_guess_inventory_url,best_guess_inventory_reason,url_tag,alt_candidate_urls,any_control_found,struct_hits_union,>0_struct_hits_found,program_title_count,program_titles_found,pdf_hits,total_controls_found,controls_sufficiency,status,error_detail,college_vine_site,college_vine_url,college_vine_ctrl_status,college_vine_program_title_count,college_vine_program_titles_found,afri_matches,ethnic_matches,black_matches,race_matches,anthropology,math,linguistics,chem,architect,economics,psychology,sociology,history,english,political_science,philosophy,computer_science,engineering,physics,geology,statistics,neuroscience,program_bucket__black,program_bucket__black__crawl,program_bucket__black__cv,program_bucket__africana,program_bucket__africana__crawl,program_bucket__africana__cv,program_bucket__mena,program_bucket__mena__crawl,program_bucket__mena__cv,program_bucket__african,program_bucket__african__crawl,program_bucket__african__cv,program_bucket__minority,program_bucket__minority__crawl,program_bucket__minority__cv,program_bucket__ethnic,program_bucket__ethnic__crawl,program_bucket__ethnic__cv,program_bucket__race,program_bucket__race__crawl,program_bucket__race__cv,program_bucket__other,program_bucket__other__crawl,program_bucket__other__cv,real_nonprogram_signals,program_buckets_hit`

#### Long-form outputs

- `*__bucketed_programs__long.csv`:
  - `row_index,unitid,source,raw_title,title_kind,is_program_title,bucket,canonical_title,program_conf,program_conf_reason`

- `*__bucketed_programs__long_programs.csv`:
  - `unitid,source,bucket,canonical_title,supporting_raw_titles,program_conf_max,program_conf_reasons`
  - (one row per `(unitid, source, bucket, canonical_title)` aggregate; not one row per raw string)

- `*__bucketed_programs__long_signals.csv`:
  - `row_index,unitid,source,raw_title,title_kind,is_program_title,bucket,canonical_title,program_conf,program_conf_reason`

### B1) Wide output: `*__bucketed_programs.csv`

**Row grain:** one row per institution.

**Bucket summary columns:**

Stage B writes bucket hit counts and/or serialized content by bucket. In the current workflow, bucket columns include:

- `program_bucket__black`, `program_bucket__black__crawl`, `program_bucket__black__cv`
- `program_bucket__africana`, `program_bucket__africana__crawl`, `program_bucket__africana__cv`
- `program_bucket__mena`, `program_bucket__mena__crawl`, `program_bucket__mena__cv`
- `program_bucket__african`, `program_bucket__african__crawl`, `program_bucket__african__cv`
- `program_bucket__minority`, `program_bucket__minority__crawl`, `program_bucket__minority__cv`
- `program_bucket__ethnic`, `program_bucket__ethnic__crawl`, `program_bucket__ethnic__cv`
- `program_bucket__race`, `program_bucket__race__crawl`, `program_bucket__race__cv`
- `program_bucket__other`, `program_bucket__other__crawl`, `program_bucket__other__cv`

Additional summarizers:

| Column | Type | Meaning |
|---|---|---|
| `real_nonprogram_signals` | string | Serialized list of signal entities (departments/centers/etc.) |
| `program_buckets_hit` | string/int-like | Summary of which buckets were hit (implementation-specific) |

**Carry-through fields:** Stage B typically retains upstream Stage A fields (URL selection, controls, errors) and institution metadata.

### B2) Long-form outputs

These files provide detailed, row-level evidence for QA and review.

| File | Row grain | Purpose |
|---|---|---|
| `*__bucketed_programs__long.csv` | one row per extracted string | full per-string evidence view (programs + signals) |
| `*__bucketed_programs__long_signals.csv` | one row per extracted string | signals-only evidence view |
| `*__bucketed_programs__long_programs.csv` | one row per `(unitid, source, bucket, canonical_title)` | programs-only aggregates with supporting raw titles |
| `*__bucketed_programs__long_bucket_summary.csv` | one row per institution | bucket summary derived from evidence tables |
| `*__bucketed_programs__long_programs_agg.csv` | one row per institution | aggregated program rollups (see file header for exact fields) |

Common long-form columns are listed above under “Exact headers (current run)”. For interpretation, focus on: `unitid`, `source`, `raw_title`, `title_kind`, `is_program_title`, and `bucket`.

---

## Stage C output: `*__2013_current_matches.csv`

**Row grain:** one row per institution.

Stage C adds `match_2013__...` and `discovered__...` fields to the Stage B wide table.

### Baseline field

| Column | Type | Meaning |
|---|---|---|
| `2013_program_name` | string | Baseline program label from the 2013 survey (may be empty) |

### Best match selection

| Column | Type | Meaning |
|---|---|---|
| `match_2013__best_title` | string | Selected best matching “current” string (may be empty) |
| `match_2013__best_source` | string | Source of best title (crawl vs cv vs signals) |
| `match_2013__best_kind` | string | Whether best match was a `program` title or a `signal` |

### Match assessment

| Column | Type | Meaning |
|---|---|---|
| `match_2013__match_level` | string | Match tier label (strict/fuzzy/rescue/no_match; vocabulary depends on run) |
| `match_2013__match_score` | float | Similarity score (typically 0–1) |
| `match_2013__detail` | string | Short explanation for review/debugging |

### Additional Stage C flags

| Column | Type | Meaning |
|---|---|---|
| `match_2013__is_signal_marker_in_2013` | bool/int | Heuristic flag: baseline label suggests a signal-style entity |
| `debug__recombined_candidates_added` | bool/int/string | Debug indicator: candidate pools recombined/augmented |

### Candidate pools written by Stage C (current workflow)

| Column | Type | Meaning |
|---|---|---|
| `discovered__program_titles__crawl` | string | Current program-title candidates from crawl (serialized) |
| `discovered__program_titles__cv` | string | Current program-title candidates from external source (serialized) |
| `discovered__signal_titles` | string | Current signal candidates (serialized) |
| `discovered__all_titles` | string | Union of all candidates (serialized) |
| `discovered__new_titles_unmatched` | string | Candidate strings not matching baseline (serialized) |
| `discovered__new_program_titles_when_best_signal` | string | Program titles present when best match is a signal (serialized) |

---

## Stage E outputs: `*__nces_profile_characteristics*.csv`

Stage E extracts NCES/IPEDS characteristics and writes per-batch outputs plus a merged table.

### Extraction diagnostics

| Column | Type | Meaning |
|---|---|---|
| `nces_profile_xlsx_downloaded` | bool/int | Whether an export XLSX was downloaded |
| `nces_profile_xlsx_filename` | string | Downloaded XLSX filename |
| `nces_profile_xlsx_dir` | string | Directory where XLSX was downloaded |
| `nces_profile_xlsx_parse_ok` | bool/int | Whether parsing succeeded |
| `nces_profile_xlsx_error` | string | Parse error detail (if any) |

### Tuition/fees

| Column | Type | Meaning |
|---|---|---|
| `tuition_fees_ug_2024_25` | numeric/string | Undergraduate tuition/fees for the indicated year |
| `tuition_fees_grad_2024_25` | numeric/string | Graduate tuition/fees for the indicated year |

### Enrollment

| Column | Type | Meaning |
|---|---|---|
| `enrollment_total` | numeric/string | Total enrollment |
| `enrollment_men` | numeric/string | Men enrollment |
| `enrollment_women` | numeric/string | Women enrollment |

### Race/ethnicity distributions (percent)

| Column | Type | Meaning |
|---|---|---|
| `pct_American Indian or Alaska Native` | numeric/string | Percentage |
| `pct_Asian` | numeric/string | Percentage |
| `pct_Black or African American` | numeric/string | Percentage |
| `pct_Hispanic` | numeric/string | Percentage |
| `pct_Native Hawaiian or Other Pacific Islander` | numeric/string | Percentage |
| `pct_White` | numeric/string | Percentage |
| `pct_Two or more races` | numeric/string | Percentage |
| `pct_Race/ethnicity unknown` | numeric/string | Percentage |
| `pct_U.S. Nonresident` | numeric/string | Percentage |

### Provenance

| Column | Type | Meaning |
|---|---|---|
| `__source_file` | string | Source batch file used for the merged table (present in merged-all-batches output) |

---


## Exact schemas for this repository’s current run

This repository captures exact header strings in the stage docs:
- Stage A: `docs/03_v15_webscrape.md`
- Stage B: `docs/04_parser_and_bucketing.md`
- Stage C: `docs/05_2013_current_comparison.md`
- Stage E: `docs/06_nces_characteristics.md`

The “Exact headers (current run)” blocks above provide the authoritative column lists for the current outputs included in this repo.

