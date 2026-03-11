# Data Interim

This directory contains intermediate “handoff” tables between pipeline stages, including website-enriched inputs, Stage A crawl outputs, Stage B bucketing outputs, and batch/progress artifacts.

Subdirectories include:

- `checkpoints/` — optional progress and resume CSV files (e.g., from website enrichment). This directory may be empty in this repository snapshot.
- `interim_batch_files_webscrape/` — batch artifacts produced during Stage A (website crawling), organized by batch.
- `interim_batch_files_additional_characteristics/` — Stage E per-batch outputs adding further characteristics.

For detailed provenance and interpretation notes for these files, see the respective README files within each subdirectory.