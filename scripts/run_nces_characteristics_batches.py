"""
Batch runner for NCES IPEDS profile characteristics extraction.

- Finds ace_unitid_merge__ace_only_webscrape__v15simple__batch_0*__progress.csv
- Runs additional_institution_characteristics.py on each input sequentially
- Writes outputs into a child folder
- Merges all batch outputs into a single combined CSV

Run:
  python run_nces_characteristics_batches.py \
    --root /path/to/project \
    --pattern "ace_unitid_merge__ace_only_webscrape__v15simple__batch_0*__progress.csv" \
    --outdir "nces_profile_characteristics" \
    --full \
    --output-suffix "__nces_profile_characteristics" \
    --keep-xlsx

Notes:
- This script expects `additional_institution_characteristics.py` to be in the same folder
  (or importable on PYTHONPATH).
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd

import additional_institution_characteristics as aic

DEFAULT_PATTERN = "ace_unitid_merge__ace_only_webscrape__v15simple__batch_0*__progress.csv"
DEFAULT_OUTDIR = "nces_profile_characteristics"
DEFAULT_NOTEBOOK_ARGS = ["--root", ".", "--full", "--skip-existing", "--keep-xlsx"]


def _running_in_ipython() -> bool:
    return "ipykernel" in sys.modules or "IPython" in sys.modules


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--root", type=str, default=".", help="Directory to search for input batch CSVs.")
    ap.add_argument(
        "--pattern",
        type=str,
        default=DEFAULT_PATTERN,
        help="Glob pattern for input batch files.",
    )
    ap.add_argument(
        "--outdir",
        type=str,
        default=DEFAULT_OUTDIR,
        help="Child folder (under root) to store per-batch outputs + merged output.",
    )
    ap.add_argument(
        "--full",
        action="store_true",
        help="If set, run full extraction (test_n=0).",
    )
    ap.add_argument(
        "--debug",
        action="store_true",
        help="If set, enable DEBUG_XLSX_DISCOVERY logs.",
    )
    ap.add_argument(
        "--skip-existing",
        action="store_true",
        help="If set, skip batches whose output already exists in outdir.",
    )

    # Pass-through knobs for additional_institution_characteristics (aic)
    ap.add_argument(
        "--output-suffix",
        type=str,
        default="__nces_profile_characteristics",
        help="Suffix appended to each per-batch output filename.",
    )
    ap.add_argument(
        "--test-n",
        type=int,
        default=None,
        help="Override aic.TEST_N for non---full runs (if set).",
    )

    # XLSX retention / cleanup
    xlsx_group = ap.add_mutually_exclusive_group()
    xlsx_group.add_argument(
        "--delete-xlsx",
        action="store_true",
        help="Delete downloaded XLSX files after parsing (default behavior in aic).",
    )
    xlsx_group.add_argument(
        "--keep-xlsx",
        action="store_true",
        help="Do NOT delete downloaded XLSX files after parsing (debug).",
    )

    ap.add_argument(
        "--xlsx-dir",
        type=str,
        default=None,
        help="Optional directory for downloaded XLSX files (if supported by aic).",
    )

    if argv is None and _running_in_ipython():
        argv = DEFAULT_NOTEBOOK_ARGS.copy()

    args, _unknown = ap.parse_known_args(argv)
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print("[INFO] run_nces_characteristics_batches args:", vars(args))

    root = Path(args.root).resolve()
    if not root.exists():
        print(f"[ERR] root does not exist: {root}")
        return 2

    outdir = (root / args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    inputs = sorted(root.glob(args.pattern))
    if not inputs:
        print(f"[ERR] No files matched pattern under {root}: {args.pattern}")
        return 2

    # Set module knobs
    aic.DEBUG_XLSX_DISCOVERY = bool(args.debug)

    # XLSX retention behavior
    if args.keep_xlsx:
        aic.DELETE_DOWNLOADED_XLSX = False
    elif args.delete_xlsx:
        aic.DELETE_DOWNLOADED_XLSX = True

    # Optional download directory (only if the module exposes a knob)
    if args.xlsx_dir:
        if hasattr(aic, "XLSX_DOWNLOAD_DIR"):
            aic.XLSX_DOWNLOAD_DIR = str(Path(args.xlsx_dir).resolve())
        elif hasattr(aic, "DOWNLOAD_DIR"):
            aic.DOWNLOAD_DIR = str(Path(args.xlsx_dir).resolve())
        else:
            print("[WARN] --xlsx-dir was provided but aic does not expose XLSX_DOWNLOAD_DIR/DOWNLOAD_DIR; ignoring")

    produced = []
    for i, in_path in enumerate(inputs, start=1):
        print(f"\n=== [{i}/{len(inputs)}] Processing: {in_path.name} ===")

        # Point the module at this input file
        aic.INPUT_PATH = in_path

        # Make module write into a child folder, but keep filename based on input stem.
        # (We override by temporarily changing INPUT_PATH to "outdir/<original_name>" while reading still uses real file.)
        # Easiest: set OUTPUT_SUFFIX and later move the file into outdir.
        aic.OUTPUT_SUFFIX = str(args.output_suffix)

        # Run full or test mode
        if args.full:
            test_n = 0
        else:
            test_n = int(args.test_n) if args.test_n is not None else int(aic.TEST_N)

        # Compute destination path for output file
        dest = outdir / (in_path.stem + aic.OUTPUT_SUFFIX + ".csv")

        if args.skip_existing and dest.exists():
            print(f"[SKIP] Output exists: {dest}")
            produced.append(dest)
            continue

        # Run extraction (writes output next to input by default)
        aic.run(test_n=test_n)

        # Move output into outdir
        out_path = in_path.with_name(in_path.stem + aic.OUTPUT_SUFFIX + ".csv")
        if not out_path.exists():
            print(f"[WARN] Expected output not found: {out_path}")
            continue

        try:
            # replace if exists
            if dest.exists():
                dest.unlink()
            out_path.rename(dest)
        except Exception:
            # fallback copy
            import shutil
            shutil.copy2(out_path, dest)
            try:
                out_path.unlink()
            except Exception:
                pass

        produced.append(dest)
        print(f"[OK] Wrote: {dest}")

    if not produced:
        print("[ERR] No outputs produced; aborting merge.")
        return 2

    # Merge all produced outputs
    dfs = []
    for p in produced:
        df = pd.read_csv(p, dtype=str, keep_default_na=False)
        df["__source_file"] = p.name
        dfs.append(df)

    merged = pd.concat(dfs, ignore_index=True)

    merged_path = outdir / "ALL_BATCHES__nces_profile_characteristics__merged.csv"
    merged.to_csv(merged_path, index=False)
    print(f"\n[DONE] Merged {len(produced)} files -> {merged_path}")
    print(f"[DONE] Rows: {len(merged):,}  Cols: {len(merged.columns):,}")

    return 0


def run_notebook() -> int:
    """Convenience entrypoint for notebooks using DEFAULT_NOTEBOOK_ARGS."""
    return main(DEFAULT_NOTEBOOK_ARGS.copy())


if __name__ == "__main__":
    raise SystemExit(main())