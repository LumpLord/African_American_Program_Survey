#!/usr/bin/env python3
"""
Add Web_address to a small example input using NCES IPEDS institution profile HTML.

Usage:
  python scripts/make_example_input_from_nces.py \
    --input examples/inputs/ace_first20.csv \
    --output examples/inputs/ace_first20_plus_web.csv \
    --sleep 0.5
"""

from __future__ import annotations
import argparse, re, time
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

NCES_PROFILE_URL = "https://nces.ed.gov/ipeds/institution-profile/{}"

@dataclass
class FetchResult:
    web_address: Optional[str]
    status: str
    detail: str

def _extract_web_address_from_html(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    # Look for labels like "Website" / "Web address" and then grab a nearby link.
    text_nodes = soup.find_all(string=re.compile(r"\bWeb\b|Website|Web address", re.I))
    for t in text_nodes:
        node = t
        for _ in range(4):
            parent = getattr(node, "parent", None)
            if parent is None:
                break
            a = parent.find("a", href=True)
            if a and a.get("href", "").strip().startswith("http"):
                href = a["href"].strip()
                if "nces.ed.gov" not in href:
                    return href
            node = parent

    # Fallback: first external http(s) link that isn't nces.
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("http") and "nces.ed.gov" not in href:
            return href

    return None

def fetch_web_address(unitid: str, timeout: float = 30.0) -> FetchResult:
    url = NCES_PROFILE_URL.format(unitid)
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return FetchResult(None, f"http_{r.status_code}", url)
        web = _extract_web_address_from_html(r.text)
        if not web:
            return FetchResult(None, "no_web_found", url)
        return FetchResult(web.strip(), "ok", url)
    except Exception as e:
        return FetchResult(None, "exception", f"{type(e).__name__}: {e}")

def main():
    p = argparse.ArgumentParser(description="Add Web_address to an example CSV using NCES IPEDS profiles")
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--sleep", type=float, default=0.5)
    p.add_argument("--timeout", type=float, default=30.0)
    p.add_argument("--max-rows", type=int, default=None)
    args = p.parse_args()

    df = pd.read_csv(args.input)
    if "unitid" not in df.columns:
        raise SystemExit("Input must contain column: unitid")
    if "name" not in df.columns:
        df["name"] = ""

    if args.max_rows is not None:
        df = df.head(args.max_rows).copy()

    # Ensure canonical columns for Stage A.
    if "2013_program_name" not in df.columns:
        df.insert(df.columns.get_loc("name") + 1, "2013_program_name", "")
    if "Web_address" not in df.columns:
        df.insert(df.columns.get_loc("name") + 1, "Web_address", "")

    statuses, details = [], []
    for i, row in df.iterrows():
        unitid = str(row["unitid"]).strip()
        if not unitid or unitid.lower() == "nan":
            df.at[i, "Web_address"] = ""
            statuses.append("missing_unitid")
            details.append("")
            continue
        res = fetch_web_address(unitid, timeout=args.timeout)
        df.at[i, "Web_address"] = res.web_address or ""
        statuses.append(res.status)
        details.append(res.detail)
        time.sleep(max(args.sleep, 0.0))

    df["nces_webaddress_status"] = statuses
    df["nces_webaddress_detail"] = details
    df.to_csv(args.output, index=False)

    ok = sum(1 for s in statuses if s == "ok")
    print(f"Wrote {args.output} (rows={len(df)})")
    print(f"Web_address ok={ok} missing={len(df)-ok}")

if __name__ == "__main__":
    main()
