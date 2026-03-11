# Stage C: 2013 vs Current Comparison

This document describes **Stage C** of the pipeline: comparing a baseline 2013 program label against currently discovered program titles/signals and producing a structured assessment of match quality.

- Script: `scripts/2013_current_comparison.py`
- Primary input: `*__bucketed_programs.csv` (Stage B wide output)
- Primary output: `*__2013_current_matches.csv`


**Scope note:** Stage C (2013 vs current comparison) is **optional** and only applies to the **2013 cohort** workflow. It requires a baseline label column (`2013_program_name`). This repo ships a baseline-labeled handoff file (`data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL_plus2013name.csv`), so you do not need to recreate the baseline join for typical use. If you want to use a different baseline list, generate your own `2013_program_name` column and left-join it on `unitid` before running Stage C.

This write-up is intended for readers who want to **reproduce** a comparison run, **interpret** match outputs, or **modify** matching behavior.

If you have not installed the environment, start with `docs/INSTALL.md`.

---

## What Stage C does

Starting from the Stage B wide table (one row per institution), Stage C:

1) Reads the baseline program label (`2013_program_name`) if present.
2) Collects “current” candidate titles from available sources (crawl titles, optional CollegeVine titles, and/or signals).
3) Normalizes strings (basic whitespace/case cleanup).
4) Computes match candidates and chooses a **best match**.
5) Writes match diagnostics that support both:
   - automatic summaries (strict/non-strict/no-match)
   - manual review (why a match was or was not found)

Stage C is designed to be **explainable**: it emits both a final match label and the evidence used to reach that label.

---

## Inputs

### Required input file

Stage C expects the Stage B wide output:
- `*__bucketed_programs.csv`

Typical example in this repo:
- `data/interim/ace_unitid_merge__ace_x_2013comp__webscrape__v15simple__bucketed_programs.csv`
- `examples/interim/ace_first20__webscrape__v15simple__bucketed_programs.csv`
- 2013 cohort handoff (shipped; includes baseline label): `data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL_plus2013name.csv`

### Required columns (minimum)

| Column | Meaning |
|---|---|
| `unitid` | IPEDS UNITID (join key) |
| `name` | institution name (readability) |
| `2013_program_name` | baseline 2013 label (2013 cohort only; may be empty; determines whether matching is expected) |

### Input columns typically carried through

Stage C operates on the Stage B wide table and generally **preserves upstream columns** so the comparison can be interpreted in context. In the current workflow, the Stage C output retains:

- **Institution metadata** carried from Carnegie/ACE (e.g., `city`, `state`, `control`, and the Carnegie classification descriptor columns).
- **Stage A crawl diagnostics** (e.g., `best_guess_inventory_url`, `best_guess_inventory_reason`, `url_tag`, `any_control_found`, `program_title_count`, `controls_sufficiency`, `status`, `error_detail`, and optional CollegeVine fields).
- **Stage B bucketing summaries** (e.g., `program_bucket__*` and `program_bucket__*__crawl/__cv`, `real_nonprogram_signals`, `program_buckets_hit`).

These carry-through fields are not produced by Stage C itself, but they are essential for QA (e.g., separating true “no match” outcomes from crawl failures).

### Current-title candidate sources

Stage C may use one or more of the following, depending on which columns exist in the input:

- **Crawl program titles** (from Stage A → Stage B processing)
- **Signals** (departments/centers/etc.) if provided and configured
- **Optional external sources** (e.g., CollegeVine) if present

Because runs differ, the comparison output includes fields indicating the **source** of the selected best match.

---

## Running the script

### Minimal CLI example

```bash
python scripts/2013_current_comparison.py \
  --input examples/interim/ace_first20__webscrape__v15simple__bucketed_programs.csv \
  --output examples/interim/ace_first20__bucketed_programs__2013_current_matches.csv
```

### Common knobs

- `--fuzzy-threshold` (default ~0.80)
  - higher threshold = fewer fuzzy matches (more conservative)
  - lower threshold = more fuzzy matches (risk more false positives)

- `--allow-category-mapping`
  - if enabled, allows category labels (e.g., from an external taxonomy) to participate in “rescue/backstop” style matching.

Recommendation:
- For new deployments, keep defaults and validate on a small set before changing thresholds.

---

## Outputs

Stage C writes a per-institution table (one row per institution) containing the original columns plus match diagnostics.

### Primary output file

#### `*__2013_current_matches.csv`

**Use this file when:** you want the analysis-ready 2013→current comparison results.

**What it contains (high-level):**
- baseline field(s): `2013_program_name`
- discovered “current” title pools (implementation-defined columns)
- best match selection (title + source)
- match level / score / details

### Exact output header (current run)

The following header reflects the current Stage C output as generated in the unitid-first workflow:

- `unitid,name,2013_program_name,Web_address,city,state,control,Institutional Classification,Student Access and Earnings Classification,Research Activity Designation,Award Level Focus,Academic Mix,Graduate Academic Program Mix,Size,Campus Setting,Highest Degree Awarded,Community Engagement,Leadership for Public Purpose,best_guess_inventory_url,best_guess_inventory_reason,url_tag,alt_candidate_urls,any_control_found,struct_hits_union,>0_struct_hits_found,program_title_count,program_titles_found,pdf_hits,total_controls_found,controls_sufficiency,status,error_detail,college_vine_site,college_vine_url,college_vine_ctrl_status,college_vine_program_title_count,college_vine_program_titles_found,afri_matches,ethnic_matches,black_matches,race_matches,anthropology,math,linguistics,chem,architect,economics,psychology,sociology,history,english,political_science,philosophy,computer_science,engineering,physics,geology,statistics,neuroscience,program_bucket__black,program_bucket__black__crawl,program_bucket__black__cv,program_bucket__africana,program_bucket__africana__crawl,program_bucket__africana__cv,program_bucket__mena,program_bucket__mena__crawl,program_bucket__mena__cv,program_bucket__african,program_bucket__african__crawl,program_bucket__african__cv,program_bucket__minority,program_bucket__minority__crawl,program_bucket__minority__cv,program_bucket__ethnic,program_bucket__ethnic__crawl,program_bucket__ethnic__cv,program_bucket__race,program_bucket__race__crawl,program_bucket__race__cv,program_bucket__other,program_bucket__other__crawl,program_bucket__other__cv,real_nonprogram_signals,program_buckets_hit,match_2013__best_title,match_2013__best_source,match_2013__best_kind,match_2013__match_level,match_2013__match_score,match_2013__detail,match_2013__is_signal_marker_in_2013,debug__recombined_candidates_added,discovered__program_titles__crawl,discovered__program_titles__cv,discovered__signal_titles,discovered__all_titles,discovered__new_titles_unmatched,discovered__new_program_titles_when_best_signal`

---

## Key output fields (how to interpret)

The script writes a family of match columns prefixed with `match_2013__...`.

### Best match selection

| Column | Meaning |
|---|---|
| `match_2013__best_title` | the selected best matching current string (may be empty) |
| `match_2013__best_source` | where the best title came from (crawl vs CV vs signals, depending on available pools) |
| `match_2013__best_kind` | whether the best match was a `program` title or a `signal` |

### Match assessment

| Column | Meaning |
|---|---|
| `match_2013__match_level` | match tier/level label (e.g., strict/fuzzy/rescue/no_match) |
| `match_2013__match_score` | numeric similarity score (range depends on implementation; typically 0–1) |
| `match_2013__detail` | short explanation string for debugging/review |

### Additional Stage C flags

| Column | Meaning |
|---|---|
| `match_2013__is_signal_marker_in_2013` | indicates whether the 2013 label contained a marker suggesting the baseline refers to a signal/department-style entity (implementation-specific heuristic) |
| `debug__recombined_candidates_added` | debug field indicating whether candidate pools were recombined/augmented before selecting the best match |

### Candidate pools

The output also includes columns describing what was considered “current,” typically with a `discovered__...` prefix.

These fields are useful for auditing:
- “Did we consider crawl titles at all?”
- “Were there signals but no program titles?”
- “Did CollegeVine titles exist?”

In the current output, the candidate pools are explicitly written as:
- `discovered__program_titles__crawl`
- `discovered__program_titles__cv`
- `discovered__signal_titles`
- `discovered__all_titles`
- `discovered__new_titles_unmatched`
- `discovered__new_program_titles_when_best_signal`

---

## How to interpret match levels (conceptual)

Stage C is designed to separate confident matches from ambiguous cases.

A typical conceptual mapping:

- **Strict match**
  - a near-exact match between `2013_program_name` and a current program title

- **Fuzzy match**
  - string similarity above the threshold (e.g., “Africana Studies” vs “Africana Studies (BA)”)

- **Rescue / backstop match**
  - match obtained via category mapping or weaker evidence (often signals)

- **No match**
  - no sufficiently similar current string found

The exact tier names used by `match_2013__match_level` are implementation-defined; use the field values in your output as the authoritative vocabulary.

---

## Known limitations

1) **Baseline uncertainty**
   - The 2013 survey label may be incomplete or may not reflect program naming used in catalogs.

2) **Program vs signal ambiguity**
   - Departments/centers can suggest a program but do not guarantee one exists.

3) **String-based matching**
   - This is not ontology-based; it relies on textual similarity and heuristics.

4) **Naming drift and degree variants**
   - “Black Studies,” “African American Studies,” “Africana Studies,” minors/majors, BA/BS variants.

These limitations are why Stage D produces review splits and why Stage C emits explainable diagnostics.

---

## Practical guidance

### When to trust a match

A match is most trustworthy when:
- the best match kind is a **program** (not only a signal)
- Stage A quality is high (controls sufficient, no blocking)
- the match level indicates strict/high-confidence

### When to flag for manual review

Flag rows for manual review when:
- Stage A extracted no titles but signals exist
- match relies on signals only
- match score is near the threshold
- the baseline label is very broad (“Ethnic Studies”) and can map many ways

---

## Reproducibility checklist

To reproduce Stage C:

1) Record the Stage B input file and row count:
   - `...__bucketed_programs.csv`
2) Record comparison parameters:
   - fuzzy threshold, category mapping flag
3) Record environment:
   - `aframr-runtime` and git commit hash
4) Keep the full output:
   - `...__2013_current_matches.csv`

---

## Where this stage fits next

- Stage D (manual QA splits): typically implemented in a notebook and produces review subsets.

See:
- `docs/06_nces_characteristics.md` (Stage E: add NCES/IPEDS characteristics)
- `docs/07_outputs_data_dictionary.md` (output columns and interpretation)
- `data/processed/README.md` (final cohort outputs)