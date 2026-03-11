# Stage B: Parser and Program Bucketing

This document describes **Stage B** of the pipeline: transforming Stage A webscrape outputs into normalized, analysis-ready tables of **program titles** and **signals**, and classifying those strings into topical **buckets** (e.g., Black Studies, Africana, Ethnic Studies).

- Script: `scripts/webscrape_parser.py`
- Primary inputs: `*__webscrape__v15simple.csv` (Stage A output)
- Primary outputs: `*__bucketed_programs*.csv` (wide) and `*__bucketed_programs__long*.csv` (long)

This write-up is intended for readers who want to **reproduce** parsing/bucketing, **interpret** outputs, or **modify** the bucketing logic.

If you have not installed the environment, start with `docs/INSTALL.md`.

---

## What Stage B does

Starting from the Stage A crawl table (one row per institution), Stage B:

1) **Parses** serialized title fields (e.g., `program_titles_found`) into a clean list of candidate strings.
2) **Normalizes** strings (whitespace cleanup, case/Unicode normalization, basic de-noising).
3) Separates strings into two conceptual groups:
   - **Programs:** likely academic offerings (majors, minors, BA/BS, certificates)
   - **Signals:** nearby entities that are not programs but are informative (departments, centers, institutes, committees)
4) Assigns each string to exactly one **bucket** using ordered regex rules.
5) Produces:
   - a **wide** per-institution table summarizing bucket hits and extracted text
   - multiple **long-form** tables (one row per extracted string) for analysis and QA

Stage B is designed for interpretability: it preserves raw strings while adding structured labels.

---

## Inputs

### Required input file

Stage B expects the Stage A output table:
- `*__webscrape__v15simple.csv`

Typical examples in this repo:
- `data/interim/ace_unitid_merge__ace_x_2013comp__webscrape__v15simple.csv`
- `data/interim/ace_unitid_merge__ace_only_webscrape__v15simple.csv` (ACE-only cohort)
- `examples/interim/ace_first20__webscrape__v15simple.csv` (example run)

### Required columns (minimum)

Stage B assumes these columns exist in the Stage A output:

| Column | Meaning |
|---|---|
| `unitid` | IPEDS UNITID (join key) |
| `name` | institution name (readability) |
| `program_titles_found` | serialized list of extracted candidate titles |

The Stage A output includes many additional quality fields (controls, url_tag, etc.). Stage B typically carries those through and/or uses them for QA filtering.

### Optional inputs

Depending on your run history, the Stage A file may also include:
- `college_vine_program_titles_found` (if CollegeVine integration was used)
- token-match counters (e.g., `afri_matches`, `black_matches`)

Stage B can incorporate these if present, but they are not required for the core outputs.

---

## Running the parser

### Minimal CLI example

```bash
python scripts/webscrape_parser.py \
  --input data/interim/ace_unitid_merge__ace_x_2013comp__webscrape__v15simple.csv \
  --outdir data/interim/
```

### Recommended: small test run

Run parsing on a small subset first (e.g., 10 institutions) to verify output schemas:

- Use a small Stage A input (e.g., the example run under `examples/interim/`) to verify schemas before running full cohorts.

---

## Outputs

Stage B writes multiple outputs. Filenames are derived from the input prefix and output directory.

### 1) Wide per-institution table

#### `*__bucketed_programs.csv`

**Use this file when:** you want a per-institution summary suitable for merges and high-level reporting.

**What it contains (high-level):** one row per institution including:
- identifiers: `unitid`, `name`
- carry-through fields from Stage A (optional): `best_guess_inventory_url`, `controls_sufficiency`, `url_tag`, etc.
- bucket summary fields (counts and/or serialized lists by bucket)

This file is typically used as the input to Stage C (2013 vs current matching).

---

### 2) Long-form tables

Long-form tables are essential for QA and analysis because they preserve one row per extracted string.

#### `*__bucketed_programs__long.csv`
**Use this file when:** you want the complete per-string view.

**Typical columns include:**
- `unitid`, `name`
- `source` (where the string came from: crawl/CV/signals, if available)
- `raw_text` (the extracted string)
- `normalized_text` (after cleaning)
- `bucket` (assigned category)
- flags indicating whether the row is treated as a **program** or **signal**

#### `*__bucketed_programs__long_programs.csv`
Subset of the long table for rows treated as **program** candidates.

#### `*__bucketed_programs__long_signals.csv`
Subset of the long table for rows treated as **signals** (departments/centers/etc.).

#### `*__bucketed_programs__long_bucket_summary.csv`
Per-institution bucket summary derived from the long table.

#### `*__bucketed_programs__long_programs_agg.csv`
Aggregated/rolled-up program view (implementation-specific; often used for summary metrics).

---

## Buckets and precedence

Stage B assigns each extracted string to **exactly one** bucket using ordered pattern rules.

### Why precedence matters

Many titles match multiple categories (e.g., “Africana and Ethnic Studies”). To avoid double-counting, Stage B uses a precedence order.

A typical precedence order in this project is:

1) **black**
2) **africana**
3) **mena**
4) **african**
5) **minority**
6) **ethnic**
7) **race**
8) **other**

Interpretation:
- If a title matches **black** and **ethnic**, it is assigned to **black**.
- “other” is the fallback bucket.

### Bucket definitions

Buckets are implemented as regex patterns. If you need to modify bucketing:
- search in `scripts/webscrape_parser.py` for the bucket regex definitions and precedence list
- update patterns carefully and re-run Stage B

---

## Programs vs signals

A key design feature is distinguishing:

- **Programs:** offerings such as “Major,” “Minor,” “BA,” “BS,” “Certificate,” or clearly program-like titles.
- **Signals:** departments/centers/institutes and related entities.

Why this matters:
- Some institutions do not list programs explicitly, but signals (e.g., “Department of Africana Studies”) provide evidence.
- Stage C matching can use signals for “rescued” matches or interpretability, while still flagging them as non-program entities.

---

## How to interpret Stage B results

### 1) Buckets are heuristic labels

A bucket label is a classification of a string, not proof of an official program.
- Program rows are “best effort” candidates extracted from web listings.
- Signals provide context, not guaranteed offerings.

### 2) Watch for common noise

Common false positives include:
- marketing/landing text captured as “titles”
- navigation fragments (“Academics”, “Programs”, “Undergraduate”)
- duplicated headings across pages

Use:
- Stage A quality fields (controls, url_tag)
- long-form tables
- manual review splits (Stage D)

to separate reliable extractions from noise.

---

## Known limitations

1) **String-based heuristics:** parsing and bucketing operate on extracted strings, not a structured catalog API.
2) **Ambiguity:** departments vs programs can be conflated when titles are vague.
3) **Overlapping categories:** precedence mitigates double counting but can hide multi-category programs.
4) **Formatting variance:** the same program may appear in many textual forms (BA vs B.A. vs Bachelor of Arts).

---

## Parameters and knobs (what to change)

Stage B is primarily governed by:
- normalization rules (how text is cleaned)
- program-vs-signal heuristics
- bucket regex patterns and precedence

Recommended workflow when changing bucketing:
1) change one bucket rule at a time
2) re-run Stage B on a small test set
3) inspect `__long_programs.csv` and `__long_signals.csv` for regressions
4) only then re-run full cohorts

---

## Reproducibility checklist

To reproduce Stage B outputs:

1) Record the Stage A input file and row count:
   - e.g., `data/interim/ace_unitid_merge__ace_x_2013comp__webscrape__v15simple.csv`
2) Record the parser version (git commit) and any modified bucket rules.
3) Keep all Stage B outputs together:
   - wide table (`__bucketed_programs.csv`)
   - long tables (`__long*.csv`)
4) If you create cohort-specific bucketing variants, record which cohort and why.

---

## Where this stage fits next

- Stage C (2013→current matching): `scripts/2013_current_comparison.py`
  - uses the wide `__bucketed_programs.csv` table

See:
- `docs/05_2013_current_comparison.md`