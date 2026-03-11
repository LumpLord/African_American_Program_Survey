# Stage A (v15): Webscrape Program Inventory Discovery

This document describes **Stage A** of the pipeline: discovering each institution’s best “program inventory” URL and extracting candidate program titles.

- Script: `scripts/v15simple_program_inventory.py`
- Primary output: `*__webscrape__v15simple.csv`

This write-up is intended for readers who want to **reproduce** a run, **interpret** outputs, or **modify** the scraper behavior.

If you have not installed the environment, start with `docs/INSTALL.md`.

---

## What Stage A does

For each institution (one row per `unitid`):

1) Uses a seed website (`Web_address`) to generate and evaluate candidate “program inventory” pages.
2) Scores candidate URLs using **structure signals** (“controls”) that suggest a real program listing.
3) Selects a single `best_guess_inventory_url`.
4) Fetches the selected page and extracts **candidate program titles**.
5) Records extensive **quality flags** and **failure tags** so downstream steps can interpret results.

Stage A is deliberately conservative: it aims to capture evidence that a program inventory exists and to extract plausible titles, while explicitly flagging cases where the scrape is unreliable.

---

## Inputs

Stage A expects a CSV with one row per institution.

### Required columns

| Column | Meaning |
|---|---|
| `unitid` | IPEDS UNITID (stable key) |
| `name` | Institution name (readability) |
| `Web_address` | Seed website/domain or URL used to discover program inventory pages |

### Recommended columns

| Column | Meaning |
|---|---|
| `2013_program_name` | Baseline label (used later in Stage C; Stage A carries it through) |

### Where these inputs come from

For the unitid-first workflow, canonical crawl inputs are produced during merge/enrichment and stored under `data/interim/`:
- `ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL.csv` (2013 comparison cohort)
- `ace_unitid_merge__ace_only_on_unitid_plusURL.csv` (ACE-only cohort)

See `docs/02_inputs_and_merge.md` and `data/interim/README.md`.

---

## Running the script

### Minimal CLI example

```bash
python scripts/v15simple_program_inventory.py \
  --input data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL.csv \
  --output out/ace_x_2013comp__webscrape__v15simple.csv \
  --workers 4
```

### Recommended: small test run

Before running thousands of institutions:

```bash
python scripts/v15simple_program_inventory.py \
  --input data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL.csv \
  --output out/test10__webscrape__v15simple.csv \
  --head 10 \
  --workers 2
```

### Large runs (batching)

Stage A supports batch processing and checkpointing via CLI flags. In large runs, the script writes per-batch artifacts:
- `...__batch_###.csv` (final compact snapshot of that batch)
- `...__batch_###__progress.csv` (append-only progress with periodic compaction)

At the end of the run, all batch outputs are concatenated into the final output CSV.

**Batching-related CLI flags:**
- `--batches N` — force exactly N batches (overrides `--batch-size`)
- `--batch-size N` — institutions per batch (ignored if `--batches` is set)
- `--checkpoint-every N` — flush buffered rows to the progress file every N institutions
- `--compact-every N` — compact/dedupe the progress file every N institutions (0 disables)

Example: run 20 rows as 2 batches of ~10:

```bash
python scripts/v15simple_program_inventory.py \
  --input  examples/interim/ace_first20__plusURL.csv \
  --output examples/interim/ace_first20__webscrape__v15simple.csv \
  --batches 2 \
  --workers 2 \
  --checkpoint-every 3 \
  --compact-every 5
```

Batch artifacts can be organized under `data/interim/interim_batch_files_webscrape/` (repo-scale) or `examples/interim/` (example run). See `data/interim/README.md` and `examples/RUN_MANIFEST.md`.

---

## Outputs

Stage A outputs a single table with one row per institution. The output is designed to support:
- downstream parsing/bucketing (Stage B)
- downstream 2013→current matching (Stage C)
- manual QA (Stage D)

### Key output fields (grouped)

#### A) Best-guess inventory selection

| Column | Meaning |
|---|---|
| `best_guess_inventory_url` | Selected program listing “hub” URL |
| `best_guess_inventory_reason` | Scoring/explanation string describing why this URL won |
| `alt_candidate_urls` | Other candidate URLs considered (for debugging) |

#### B) Extracted titles

| Column | Meaning |
|---|---|
| `program_title_count` | Number of extracted candidate titles |
| `program_titles_found` | Extracted titles (serialized list/pipe-delimited string; treat as raw text) |

#### C) Structure/quality signals (“controls”)

Stage A uses structural signals to decide whether a page is a *true inventory/listing* vs a generic marketing page.

| Column | Meaning |
|---|---|
| `any_control_found` | Whether any structure signals were detected |
| `total_controls_found` | Count of control signals detected |
| `controls_sufficiency` | Heuristic summary of whether controls are “enough” to trust the page |
| `struct_hits_union` | Summary of which structure tests fired |
| `>0_struct_hits_found` | Convenience boolean: any structural hits |

#### D) Failure tags and debugging

| Column | Meaning |
|---|---|
| `url_tag` | High-level tag for what happened (success / blocked / timeout / parse failure, etc.) |
| `status` | Status label used by the script (implementation-defined) |
| `error_detail` | Exception or failure detail (truncated) |

#### E) Optional auxiliary signals

Depending on configuration and run history, outputs may include:
- token-match counters (e.g., `afri_matches`, `ethnic_matches`, `black_matches`) derived from title text
- optional “CollegeVine” columns if that integration was used in the run

These are not required for Stage B/C, but can be useful for quick triage.

---

## How to interpret Stage A results

### 1) “No titles found” does not mean “no program exists”

A common failure mode is:
- `program_title_count == 0`

This **does not** prove the institution lacks the program. It often indicates:
- the page is blocked (403/429) or bot-protected
- the hub selection was wrong (generic landing page)
- the program inventory is rendered via JavaScript
- the catalog is behind a search UI that is not visible in static HTML

Use quality fields (`controls_sufficiency`, `url_tag`, `error_detail`) to decide whether results are trustworthy.

### 2) Confidence rubric (practical)

A simple interpretation rubric:

- **High confidence:**
  - `controls_sufficiency` indicates adequate structure
  - `any_control_found == True`
  - `program_title_count > 0`

- **Medium confidence:**
  - structure signals present but limited
  - titles extracted but look noisy (e.g., many non-program entities)

- **Low confidence / needs review:**
  - blocked/timeout tags
  - structure signals missing
  - titles empty

Downstream Stage B (“signals” vs “programs”) and Stage D review splits are designed to help triage these cases.

---

## Known limitations

Stage A is designed for broad coverage, not perfect extraction.

Common limitations:

1) **Bot detection / blocking** (403/429)
2) **JavaScript-heavy catalogs** (content not present in initial HTML)
3) **Ambiguous hubs** (catalog index pages, school landing pages)
4) **Entity ambiguity** (departments/centers vs programs)
5) **Website drift** over time (domains change; redirects change crawl behavior)

These limitations are why Stage A emits explicit quality flags and why Stage D includes manual QA splits.

---

## Parameters and knobs (what to change)

Stage A exposes CLI flags intended to control crawl behavior. The most commonly tuned:

- `--workers` — parallelism; higher increases speed but also increases blocking risk
- `--head` — limit rows for a test run
- `--subsite-penalty` — penalty for selecting a candidate URL on a different subdomain/subsite
- `--progtitle-strictness` — how strict title extraction is
- `--batches` / `--batch-size` — batch sizing controls for large runs
- `--checkpoint-every` / `--compact-every` — progress flushing and compaction cadence

Recommendation:
- Prefer conservative `--workers` (2–6) for reliability.
- Use batching for large runs.

---

## Reproducibility checklist

To reproduce a Stage A run:

1) Record the input file used (and its row count):
   - e.g., `data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL.csv`
2) Record CLI parameters:
   - workers, strictness knobs, batch definitions
3) Record environment:
   - `environment.yml` / conda env name (`aframr-runtime`)
4) Keep merged outputs and batch/progress artifacts:
   - merged `...__webscrape__v15simple.csv`
   - batch parts under `data/interim/interim_batch_files_webscrape/`

---

## Where this stage fits next

- Stage B (parser/bucketing): `scripts/webscrape_parser.py` → produces `...__bucketed_programs*.csv`
- Stage C (2013→current matching): `scripts/2013_current_comparison.py` → produces `...__2013_current_matches.csv`

See:
- `docs/04_parser_and_bucketing.md`
- `docs/05_2013_current_comparison.md`