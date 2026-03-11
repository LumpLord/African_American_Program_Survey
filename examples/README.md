

# Examples

This folder contains a small, self-contained example run of the pipeline.

Start here:
- `examples/RUN_MANIFEST.md` — authoritative commands, parameters, sanity checks, and expected artifacts.

## Layout

- `examples/inputs/` — small input CSVs used to run the example pipeline.
- `examples/interim/` — intermediate artifacts produced by each stage (including batch/progress files).
- `examples/outputs/` — final “handoff” outputs copied from the interim stage for easy review.

For full-scale runs against the complete cohorts, see the main documentation in `docs/` and the corresponding repo-scale outputs under `data/`.