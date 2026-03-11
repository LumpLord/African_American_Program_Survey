# examples/interim

This folder mirrors the repository's `data/interim/` layout, but only for the small example run.
It contains between-stage artifacts (Stage A/B/C outputs) plus batch/progress subfolders.

- Stage A output: `*__webscrape__v15simple.csv`
- Stage B outputs: `*__bucketed_programs*.csv`
- Stage C output: `*__2013_current_matches.csv`

Batch/progress files (if created) live under:
- `interim_batch_files_webscrape/`
- `interim_batch_files_additional_characteristics/`

Checkpoints (if created) live under:
- `checkpoints/`
