#!/usr/bin/env python3
"""scripts/institution_webaddresses_get.py

Website enrichment (Stage 0): add `Web_address` to an institution table (unitid-first).

This script enriches a CSV containing `unitid` (and usually `name`) by fetching the
NCES IPEDS Institution Profile page for each unitid via Selenium and extracting a
candidate institution website from the rendered BODY text.

NCES profile URL pattern:
  https://nces.ed.gov/ipeds/institution-profile/<unitid>

Extraction strategy (rendered BODY text):
1) First match of a `www.`-style domain
2) Fallback: first `.edu` domain (with or without scheme)

Operational behavior:
- Reuses a single Selenium Chrome driver for all rows.
- Uses explicit waits (BODY text length threshold) plus a short post-get sleep.
- If extraction returns empty, retries once with a longer post-get sleep.
- Polite delay between requests to reduce rate limiting.

Inputs
------
- A CSV with at least `unitid`.

Outputs
-------
- A CSV identical to input but with a `Web_address` column inserted immediately
  after the `name` column (or appended if `name` is not present).

Example
-------
python scripts/institution_webaddresses_get.py \
  --input  examples/inputs/ace_first20.csv \
  --output examples/interim/ace_first20__plusURL.csv \
  --head 20 \
  --headless

"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


DEFAULT_INPUT = Path("data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid.csv")
DEFAULT_OUTPUT = Path("data/interim/ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL.csv")

DEFAULT_UNITID_COL = "unitid"
DEFAULT_NAME_COL = "name"
DEFAULT_NEW_COL = "Web_address"

NCES_PROFILE_URL = "https://nces.ed.gov/ipeds/institution-profile/{}"

# Defaults tuned for stability while still being reasonably fast
DEFAULT_WAIT_TIMEOUT_SEC = 20
DEFAULT_MIN_SLEEP_AFTER_GET_SEC = 0.5
DEFAULT_RETRY_SLEEP_AFTER_GET_SEC = 10.0
DEFAULT_SLEEP_BETWEEN_REQUESTS_SEC = 1.0
DEFAULT_WINDOW_SIZE = "1400,900"

# Checkpoint / recycle defaults
DEFAULT_CHECKPOINT_EVERY = 30
DEFAULT_RECYCLE_DRIVER_EVERY = 250

WWW_REGEX = re.compile(r"\bwww\.[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?:/[^\s]*)?", re.IGNORECASE)
EDU_REGEX = re.compile(r"\b(?:https?://)?(?:www\.)?[A-Za-z0-9.-]+\.edu(?:/[^\s]*)?", re.IGNORECASE)


def make_driver(*, headless: bool, window_size: str) -> webdriver.Chrome:
    """Create a Chrome webdriver with options appropriate for headless scraping."""
    options = webdriver.ChromeOptions()
    if headless:
        # Chrome >= 109 supports the new headless mode
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument(f"--window-size={window_size}")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--blink-settings=imagesEnabled=false")
    return webdriver.Chrome(options=options)


def extract_web_address_from_body_text(body_text: str) -> str:
    """Extract a likely institution website from rendered body text."""
    if not body_text:
        return ""

    m = WWW_REGEX.search(body_text)
    if m:
        return m.group(0).strip()

    m = EDU_REGEX.search(body_text)
    if m:
        val = m.group(0).strip()
        # Normalize by stripping scheme for consistency with existing outputs
        val = re.sub(r"^https?://", "", val, flags=re.IGNORECASE)
        return val

    return ""


def _load_and_extract(
    driver: webdriver.Chrome,
    unitid: str,
    *,
    post_get_sleep_sec: float,
    wait_timeout_sec: float,
    min_body_chars: int = 200,
) -> str:
    """Load NCES profile for unitid and extract website with a post-get sleep + explicit wait."""
    url = NCES_PROFILE_URL.format(unitid)
    driver.get(url)
    if post_get_sleep_sec:
        time.sleep(post_get_sleep_sec)

    wait = WebDriverWait(driver, wait_timeout_sec)
    wait.until(lambda d: len(d.find_element(By.TAG_NAME, "body").text.strip()) > min_body_chars)

    body_text = driver.find_element(By.TAG_NAME, "body").text
    return extract_web_address_from_body_text(body_text)


def get_web_address_with_retry(
    driver: webdriver.Chrome,
    unitid: str,
    *,
    min_sleep_after_get_sec: float,
    retry_sleep_after_get_sec: float,
    wait_timeout_sec: float,
) -> str:
    """Fast attempt; if empty, retry once with longer post-load sleep."""
    web = _load_and_extract(
        driver,
        unitid,
        post_get_sleep_sec=min_sleep_after_get_sec,
        wait_timeout_sec=wait_timeout_sec,
    )
    if web:
        return web

    return _load_and_extract(
        driver,
        unitid,
        post_get_sleep_sec=retry_sleep_after_get_sec,
        wait_timeout_sec=wait_timeout_sec,
    )


def _insert_after_name(df: pd.DataFrame, *, name_col: str, new_col: str, values: list[str]) -> pd.DataFrame:
    """Insert new_col after name_col if present; else append."""
    if new_col in df.columns:
        df[new_col] = values
        return df

    if name_col in df.columns:
        idx = df.columns.get_loc(name_col) + 1
        df.insert(idx, new_col, values)
    else:
        df[new_col] = values
    return df


# --- Progress/resume/dedupe helpers ---
def normalize_unitid(val: str) -> str:
    if val is None:
        return ""
    v = str(val).strip()
    if v.lower() in {"", "nan", "none"}:
        return ""
    return v


def resolve_default_progress_path(output_path: Path) -> Path:
    """Default progress path: prefer sibling checkpoints/ folder if present."""
    out_dir = output_path.parent
    ckpt_dir = out_dir / "checkpoints"
    stem = output_path.stem

    if ckpt_dir.exists() and ckpt_dir.is_dir():
        return ckpt_dir / f"{stem}__progress.csv"
    return out_dir / f"{stem}__progress.csv"


def load_progress(progress_path: Path, *, unitid_col: str, new_col: str) -> dict[str, str]:
    """Load existing progress CSV into dict {unitid: web_address}."""
    if not progress_path.exists():
        return {}

    prog = pd.read_csv(progress_path, dtype=str, keep_default_na=False)
    if prog.empty:
        return {}

    if unitid_col not in prog.columns or new_col not in prog.columns:
        raise ValueError(f"Progress file exists but missing required cols: {progress_path}")

    prog[unitid_col] = prog[unitid_col].astype(str).map(normalize_unitid)
    prog = prog[prog[unitid_col].ne("")].copy()
    prog = prog.drop_duplicates(subset=[unitid_col], keep="first")

    return dict(zip(prog[unitid_col].tolist(), prog[new_col].tolist()))


def append_progress_rows(
    progress_path: Path,
    *,
    unitid_col: str,
    new_col: str,
    rows: list[tuple[str, str]],
) -> None:
    """Append (unitid, web) rows to progress CSV, creating header if needed."""
    if not rows:
        return

    df_new = pd.DataFrame(rows, columns=[unitid_col, new_col])
    write_header = not progress_path.exists()
    df_new.to_csv(progress_path, mode="a", header=write_header, index=False)


def _ordered_unique(values: list[str]) -> list[str]:
    """Return list of unique values preserving first-seen order."""
    seen = set()
    out = []
    for v in values:
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="institution_webaddresses_get.py",
        description="Website enrichment: add Web_address by loading NCES IPEDS institution profiles (unitid-first).",
    )

    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT, help=f"Input CSV (default: {DEFAULT_INPUT})")
    ap.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help=f"Output CSV (default: {DEFAULT_OUTPUT})")

    ap.add_argument("--unitid-col", default=DEFAULT_UNITID_COL, help=f"Unitid column (default: {DEFAULT_UNITID_COL})")
    ap.add_argument("--name-col", default=DEFAULT_NAME_COL, help=f"Name column (default: {DEFAULT_NAME_COL})")
    ap.add_argument("--new-col", default=DEFAULT_NEW_COL, help=f"New column to add (default: {DEFAULT_NEW_COL})")

    ap.add_argument("--head", type=int, default=0, help="Limit rows for quick testing (0 = full run).")

    # Selenium / pacing knobs
    ap.add_argument("--wait-timeout", type=float, default=DEFAULT_WAIT_TIMEOUT_SEC, help="Explicit wait timeout (seconds).")
    ap.add_argument("--min-sleep-after-get", type=float, default=DEFAULT_MIN_SLEEP_AFTER_GET_SEC, help="Post-get sleep for fast path (seconds).")
    ap.add_argument("--retry-sleep-after-get", type=float, default=DEFAULT_RETRY_SLEEP_AFTER_GET_SEC, help="Post-get sleep for retry path if extraction is empty (seconds).")
    ap.add_argument("--sleep-between-requests", type=float, default=DEFAULT_SLEEP_BETWEEN_REQUESTS_SEC, help="Polite delay between institutions (seconds).")
    ap.add_argument("--window-size", default=DEFAULT_WINDOW_SIZE, help="Chrome window size, e.g. 1400,900")

    headless_group = ap.add_mutually_exclusive_group()
    headless_group.add_argument("--headless", action="store_true", help="Run Chrome headless (recommended).")
    headless_group.add_argument("--no-headless", action="store_true", help="Run Chrome with a visible window.")

    ap.add_argument("--dry-run", action="store_true", help="Fetch and extract, but do not write output CSV.")

    # Checkpoint / resume / dedupe
    ap.add_argument(
        "--progress",
        type=Path,
        default=None,
        help="Progress CSV for checkpoint/resume. Default is derived from --output.",
    )
    ap.add_argument(
        "--checkpoint-every",
        type=int,
        default=DEFAULT_CHECKPOINT_EVERY,
        help=f"Flush newly processed unitids to progress every N (default {DEFAULT_CHECKPOINT_EVERY}).",
    )
    ap.add_argument(
        "--recycle-driver-every",
        type=int,
        default=DEFAULT_RECYCLE_DRIVER_EVERY,
        help=f"Recycle Selenium driver every N newly processed unitids (0 disables; default {DEFAULT_RECYCLE_DRIVER_EVERY}).",
    )
    ap.add_argument(
        "--start-fresh",
        action="store_true",
        help="Delete progress file before running (start fresh).",
    )

    resume_group = ap.add_mutually_exclusive_group()
    resume_group.add_argument(
        "--resume",
        action="store_true",
        help="Resume from progress file if it exists (default behavior).",
    )
    resume_group.add_argument(
        "--no-resume",
        action="store_true",
        help="Do not resume even if progress file exists.",
    )

    dedupe_group = ap.add_mutually_exclusive_group()
    dedupe_group.add_argument(
        "--dedupe-unitid",
        action="store_true",
        help="Scrape each unitid once even if input has duplicates (default behavior).",
    )
    dedupe_group.add_argument(
        "--no-dedupe-unitid",
        action="store_true",
        help="Do not dedupe; scrape duplicates independently.",
    )

    args = ap.parse_args(argv)

    if not args.input.exists():
        print(f"[ERROR] input file not found: {args.input}", file=sys.stderr)
        return 2

    df = pd.read_csv(args.input, dtype=str, keep_default_na=False)

    if args.unitid_col not in df.columns:
        print(f"[ERROR] missing required column '{args.unitid_col}' in {args.input}", file=sys.stderr)
        return 3

    if args.head and int(args.head) > 0:
        df = df.head(int(args.head)).copy()

    headless = True
    if args.no_headless:
        headless = False
    elif args.headless:
        headless = True

    # Resolve progress path
    progress_path = args.progress if args.progress is not None else resolve_default_progress_path(args.output)

    # Determine resume + dedupe defaults
    resume_enabled = True
    if args.no_resume:
        resume_enabled = False

    dedupe_enabled = True
    if args.no_dedupe_unitid:
        dedupe_enabled = False

    # Start fresh
    if args.start_fresh and progress_path.exists():
        try:
            progress_path.unlink()
            print(f"[INFO] --start-fresh: deleted progress file: {progress_path}")
        except Exception as e:
            print(f"[WARN] could not delete progress file {progress_path}: {e}")

    # Load progress
    progress_map: dict[str, str] = {}
    if resume_enabled and progress_path.exists():
        try:
            progress_map = load_progress(progress_path, unitid_col=args.unitid_col, new_col=args.new_col)
        except Exception as e:
            print(f"[ERROR] failed to load progress file {progress_path}: {e}", file=sys.stderr)
            return 4

    # Build unitid list in input order
    unitids_in_order = [normalize_unitid(u) for u in df[args.unitid_col].tolist()]

    # Build scrape list (unique unitids, preserve order)
    scrape_candidates = [u for u in unitids_in_order if u]
    if dedupe_enabled:
        scrape_candidates = _ordered_unique(scrape_candidates)

    # Apply resume skipping
    if resume_enabled and progress_map:
        before = len(scrape_candidates)
        scrape_candidates = [u for u in scrape_candidates if u not in progress_map]
        skipped = before - len(scrape_candidates)
    else:
        skipped = 0

    checkpoint_every = int(args.checkpoint_every) if int(args.checkpoint_every) > 0 else 0
    recycle_every = int(args.recycle_driver_every) if int(args.recycle_driver_every) > 0 else 0

    print("[INFO] Website enrichment")
    print(f"  input   : {args.input} (rows={len(df):,})")
    print(f"  output  : {args.output}")
    print(f"  progress: {progress_path} (resume={resume_enabled} | loaded={len(progress_map):,})")
    print(f"  dedupe_unitid={dedupe_enabled} | to_scrape={len(scrape_candidates):,} | skipped={skipped:,}")
    print(
        f"  headless={headless} | wait_timeout={args.wait_timeout}s | sleep_between={args.sleep_between_requests}s | "
        f"checkpoint_every={checkpoint_every} | recycle_driver_every={recycle_every}"
    )

    # Prepare driver
    driver: Optional[webdriver.Chrome] = None
    processed_this_driver = 0
    newly_processed = 0

    pending_rows: list[tuple[str, str]] = []

    def ensure_driver() -> None:
        nonlocal driver, processed_this_driver
        if driver is None:
            driver = make_driver(headless=headless, window_size=args.window_size)
            processed_this_driver = 0

    def recycle_driver() -> None:
        nonlocal driver, processed_this_driver
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass
        driver = make_driver(headless=headless, window_size=args.window_size)
        processed_this_driver = 0

    try:
        for unitid in scrape_candidates:
            ensure_driver()

            if recycle_every and processed_this_driver >= recycle_every:
                print(f"[INFO] Recycling driver after {processed_this_driver} newly processed unitids on this driver...")
                recycle_driver()

            try:
                web = get_web_address_with_retry(
                    driver,
                    unitid,
                    min_sleep_after_get_sec=float(args.min_sleep_after_get),
                    retry_sleep_after_get_sec=float(args.retry_sleep_after_get),
                    wait_timeout_sec=float(args.wait_timeout),
                )
            except Exception as e:
                web = ""
                # keep going; record blank
                print(f"[WARN] unitid={unitid}: failed to fetch/parse web address ({type(e).__name__}: {e})")

            progress_map[unitid] = web
            pending_rows.append((unitid, web))

            newly_processed += 1
            processed_this_driver += 1

            # polite delay
            if args.sleep_between_requests:
                time.sleep(max(float(args.sleep_between_requests), 0.0))

            # checkpoint flush
            if checkpoint_every and (newly_processed % checkpoint_every == 0):
                if not args.dry_run:
                    progress_path.parent.mkdir(parents=True, exist_ok=True)
                    append_progress_rows(progress_path, unitid_col=args.unitid_col, new_col=args.new_col, rows=pending_rows)
                print(f"[CHECKPOINT] appended {len(pending_rows):,} rows (newly_processed={newly_processed:,}) -> {progress_path}")
                pending_rows.clear()

    finally:
        # final checkpoint flush
        if pending_rows:
            if not args.dry_run:
                progress_path.parent.mkdir(parents=True, exist_ok=True)
                append_progress_rows(progress_path, unitid_col=args.unitid_col, new_col=args.new_col, rows=pending_rows)
            print(f"[CHECKPOINT] final flush appended {len(pending_rows):,} rows (newly_processed={newly_processed:,}) -> {progress_path}")
            pending_rows.clear()

        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

    # Build output column for each row from progress_map
    web_values: list[str] = []
    for u in unitids_in_order:
        if not u:
            web_values.append("")
        else:
            web_values.append(progress_map.get(u, ""))

    # Report fill
    filled = sum(1 for v in web_values if str(v).strip() != "")
    print(f"[INFO] Extracted {args.new_col}: {filled:,} / {len(df):,}")

    df = _insert_after_name(df, name_col=args.name_col, new_col=args.new_col, values=web_values)

    if args.dry_run:
        print("[DRY-RUN] Not writing output.")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False)
    print(f"[WROTE] {args.output} | rows={df.shape[0]:,} cols={df.shape[1]:,}")
    print(f"[INFO] Progress file: {progress_path} | entries now = {len(progress_map):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())