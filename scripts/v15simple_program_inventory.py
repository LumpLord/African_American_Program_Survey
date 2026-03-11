#!/usr/bin/env python3
"""
v15simple: program-inventory discovery + scoring + tagging (clean)

v14 additions / changes (relative to v13):
- Robust fetch-status tagging via a per-institution status tally:
    * Deterministic row-level `url_tag` derived from all attempted institution-site fetches
    * Explicit buckets for 403, 429, any 5xx, request exceptions (timeouts/connection errors),
      and a catch-all for other 4xx (excluding 403/429)
    * Status tally is reset between the institution crawl and CollegeVine to keep `url_tag`
      institution-only

- Program-title extraction accuracy improvements (cleaning/postprocessing):
    * Stronger course detection (e.g., "AAAS 115a", "AAAS/WGS 125a")
    * Negative-context / announcement filtering (news/events/job postings/platform links)
    * Salvage of common boilerplate prefixes ("Welcome to", "About the program in", etc.)
    * Breadcrumb splitting, degree-suffix normalization, scoring, and deduping

- Expanded target token family for matching and outputs:
    * Africa/African/Africana, Pan-African, African-American, Afro-American, Africology,
      plus Ethnic/Ethnicity, Black, Race/Racial
    * Fixed output token buckets: afri / ethnic / black / race

- Hub confidence proxy from control majors:
    * `total_controls_found` = sum(found_<control>) across the CONTROL_TERMS list
    * `controls_sufficiency` = "warning_<X>_MajorsFound" if total_controls_found < X else "sufficient majors"
    * X is controlled by `MIN_CONTROL_HITS_FOR_CONFIDENT_HUB`

- Canonical-hub preference + JS-hub handling:
    * Canonical hubs can win tie-ish scores (CANONICAL_TIE_THRESHOLD)
    * JS-rendered thin hubs trigger earlier sitemap expansion (looks_like_js_hub)

- CollegeVine (3rd-party truth source) integration:
    * Builds a small slug candidate set for each school name and probes
      `https://www.collegevine.com/schools/hub/all/d/<slug>/majors`
    * Outputs `college_vine_url` (traceability), `college_vine_site` (1/0),
      `college_vine_control_hits`, and CollegeVine-derived program titles
      (extracted with the same heuristics + cleaning as institution pages)
    * Optional control threshold flag: `college_vine_controls_ge_<X>`

- Concordance checks (exact normalized matches; may be expanded later):
    * institution programs vs CollegeVine programs
    * institution programs vs `2013_program_name`
    * CollegeVine programs vs `2013_program_name`

Key features (overall):
- Candidate discovery: path templates + homepage link prioritization + best-first crawl + sitemap assist
- Page scoring: hubness signatures + structured term hits + control-term proxy + penalties
- Row tagging for best_guess_inventory_url: Correct hub / Correct-ish but too specific / Wrong subsite / Wrong non-academic
- CLI knobs for sweep runs from ipynb:
    --subsite-penalty (int): penalty applied to subsite-like pages
    --progtitle-strictness (int): controls how strict program title extraction is
    --head (int): limit rows for quick testing
    --out-suffix (str): appended to output filename stem
    --input / --output: file paths

Notebook-friendly:
- TEST_HEAD_N provides default subset size when --head is not provided. Use --head 0 for full run.
- DEFAULT_WORKERS provides a top-level default for --workers.

INPUT:
- ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL_plus2013name.csv (unitid, name, Web_address)

OUTPUT:
- Carnegie_Carla_2013_subset_majors_scan_v15simple.csv (default)
"""

from __future__ import annotations

import argparse
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import heapq
import re
import time
import unicodedata
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urldefrag
import csv
import math

import pandas as pd
import requests
from bs4 import BeautifulSoup

import difflib

# ============================================================
# Thread-local requests sessions (for safe parallel fetching)
# ============================================================
_thread_local = threading.local()


def get_thread_session() -> requests.Session:
    """Return a per-thread requests.Session (requests.Session is not thread-safe)."""
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        _thread_local.session = s
    return s

# ============================================================
# Per-institution fetch-status tally (403-aware)
# ============================================================
# We process institutions sequentially, but fetch pages in parallel per institution.
# This shared tally lets us tag rows when any attempted fetch hit a 403 (or similar).
_STATUS_LOCK = threading.Lock()
_STATUS_COUNTS: Dict[int, int] = {}


def reset_status_tally() -> None:
    with _STATUS_LOCK:
        _STATUS_COUNTS.clear()


def record_http_status(code: int) -> None:
    if not code:
        return
    with _STATUS_LOCK:
        _STATUS_COUNTS[code] = _STATUS_COUNTS.get(code, 0) + 1


# New: Record request exceptions for row-level tagging
def record_request_exception() -> None:
    """Record that a request raised an exception (timeout, connection error, etc.)."""
    with _STATUS_LOCK:
        _STATUS_COUNTS[-1] = _STATUS_COUNTS.get(-1, 0) + 1


def fetch_status_tag(best_url: str) -> str:
    """Row tag describing fetch status / blocks encountered during candidate discovery."""
    with _STATUS_LOCK:
        counts = dict(_STATUS_COUNTS)

    # Prioritize explicit blocks
    if counts.get(403, 0) > 0:
        return "403_forbidden_seen"
    if counts.get(429, 0) > 0:
        return "429_rate_limited_seen"

    # Any server errors encountered
    if any(isinstance(c, int) and 500 <= c <= 599 for c in counts.keys()):
        return "5xx_seen"

    # Request exceptions (timeouts, connection errors, etc.)
    if counts.get(-1, 0) > 0:
        return "exceptions_seen"

    # Any other 4xx (excluding 403/429)
    if any(isinstance(c, int) and 400 <= c <= 499 and c not in (403, 429) for c in counts.keys()):
        return "4xx_seen"

    # If nothing worked
    if not best_url or best_url == "N/A":
        return "no_candidate"

    return "ok"

# ============================================================
# CollegeVine fetch-status tally (separate from institution crawl)
# ============================================================
# CollegeVine can rate-limit (429) or block (403) separately from institution sites.
# Track these separately so `url_tag` remains institution-only.
_CV_STATUS_LOCK = threading.Lock()
_CV_STATUS_COUNTS: Dict[int, int] = {}


def reset_cv_status_tally() -> None:
    with _CV_STATUS_LOCK:
        _CV_STATUS_COUNTS.clear()


def record_cv_http_status(code: int) -> None:
    if not code:
        return
    with _CV_STATUS_LOCK:
        _CV_STATUS_COUNTS[code] = _CV_STATUS_COUNTS.get(code, 0) + 1


def record_cv_request_exception() -> None:
    """Record that a CollegeVine request raised an exception."""
    with _CV_STATUS_LOCK:
        _CV_STATUS_COUNTS[-1] = _CV_STATUS_COUNTS.get(-1, 0) + 1


def cv_status_snapshot() -> Dict[int, int]:
    with _CV_STATUS_LOCK:
        return dict(_CV_STATUS_COUNTS)


def cv_block_or_ratelimit_seen() -> bool:
    """True if we saw a clear CV-side block/rate-limit signal."""
    snap = cv_status_snapshot()
    return (snap.get(429, 0) > 0) or (snap.get(403, 0) > 0)

# ============================================================
# Notebook-friendly default test mode
# ============================================================
# If you run from an ipynb as:  !python v15simple_program_inventory.py
# it will default to processing TEST_HEAD_N rows unless you pass --head.
# Use --head 0 for full run.
TEST_HEAD_N = 0

# ============================================================
# Batch processing + checkpoint/resume (v14 stability patch)
# ============================================================
# Process input in batches in a single run:
#   batch 1 -> reset batch state -> batch 2 -> ... until EOF
#
# Each batch writes:
#   - a progress file (append-only + periodic compact)
#   - a batch output file (final compact snapshot of progress)
#
# At the end, script concatenates all batch outputs into the final output CSV.

START_FRESH_RUN = False

BATCH_SIZE = 100               # “first N entries”, default 100
CHECKPOINT_EVERY_N = 10        # user-requested: flush buffered rows every 10 institutions
COMPACT_EVERY_N = 50           # rewrite progress dropping duplicates every 50 institutions

# ============================================================
# Top-level parallelism defaults
# ============================================================
# You can override via CLI: --workers
# Keep MAX_WORKERS small to stay polite to university sites.

DEFAULT_WORKERS = 8
MAX_WORKERS = 8

# ============================================================
# Control-term coverage threshold (majors proxy)
# ============================================================
# We use the number of CONTROL_TERMS matched as a rough proxy for whether we landed on a
# real programs/majors hub page. If total controls found is below this threshold, we flag.
# (Do NOT reintroduce found_bio; this counts only the current CONTROL_TERMS list.)
MIN_CONTROL_HITS_FOR_CONFIDENT_HUB = 10

# ============================================================
# CollegeVine (3rd-party truth source): static majors list + control hits
# ============================================================
# We previously attempted to scrape majors from the rendered majors page or via
# content-modules APIs. The robust route is CollegeVine's static data blob
# referenced in CV.pathInfo.endpoints as `schools_static_data_url`.
#
# Flow:
#   1) Bootstrap a CV session by fetching a public hub page and extracting:
#        - CSRF token (meta tag)
#        - CV.pathInfo.endpoints (JSON-like object embedded in HTML)
#   2) Resolve the school id via POST /schools/search
#   3) Fetch the static JSON blob (schools_static_data_url)
#   4) Map the school's `majors` CIP-code list to human names via majorsMap
#   5) Derive:
#        - control hits proxy (distinct CONTROL_TERMS present among offered majors)
#        - target program titles (subset of majors matching TARGET_ANY_REGEX)
#
# Majors pages follow:
#   https://www.collegevine.com/schools/hub/all/d/<school-slug>/majors
# We still output that URL for traceability, but we do not rely on its HTML.

COLLEGEVINE_BASE = "https://www.collegevine.com/schools/hub/all/d"
COLLEGEVINE_BOOTSTRAP_URL = "https://www.collegevine.com/schools/hub/all"
COLLEGEVINE_TIMEOUT_SEC = 20

# In-memory cache for the very large static JSON blob (per run)
_COLLEGEVINE_STATIC_CACHE: Optional[dict] = None
_COLLEGEVINE_STATIC_CACHE_URL: str = ""


def slugify_collegevine_school_name(name: str) -> str:
    """Convert a school name to CollegeVine's expected hyphenated slug."""
    s = normalize_unicode_text(name or "").lower()

    # Replace '&' with 'and' to match common slug conventions
    s = s.replace("&", " and ")

    # Drop apostrophes/quotes; keep alnum + spaces + hyphens
    s = re.sub(r"[\"']", "", s)

    # Collapse non-alphanumeric into spaces, then hyphenate
    s = re.sub(r"[^a-z0-9\s-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace(" ", "-")

    # Collapse duplicate hyphens
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def collegevine_slug_candidates(name: str) -> List[str]:
    """Generate a small set of plausible CollegeVine slugs for a school name."""
    base = normalize_unicode_text(name or "")
    if not base:
        return []

    cands: List[str] = []

    def _add(nm: str) -> None:
        s = slugify_collegevine_school_name(nm)
        if s and s not in cands:
            cands.append(s)

    # 1) direct
    _add(base)

    # 2) drop common campus qualifiers / parentheticals
    nm2 = re.sub(r"\s*\([^)]*\)\s*", " ", base)
    nm2 = re.sub(r"\b(main\s+campus|downtown\s+campus|online|global\s+campus)\b", " ", nm2, flags=re.I)
    nm2 = normalize_unicode_text(nm2)
    _add(nm2)

    # 3) remove common suffix tokens (but keep order)
    nm3 = re.sub(
        r"\b(university|college|institute|school|state\s+university|community\s+college|polytechnic|campus)\b",
        " ",
        base,
        flags=re.I,
    )
    nm3 = normalize_unicode_text(nm3)
    _add(nm3)

    # 4) remove leading 'the'
    nm4 = re.sub(r"^the\s+", "", base, flags=re.I)
    nm4 = normalize_unicode_text(nm4)
    _add(nm4)

    return cands[:4]


def count_control_hits_in_text(text: str) -> int:
    """Count how many distinct CONTROL_TERMS match in the provided text."""
    blob = normalize_unicode_text(text or "")
    return int(sum(1 for pat in CONTROL_PATS.values() if pat.search(blob)))


def _extract_balanced_braces_object(html: str, marker: str) -> str:
    """Extract a JSON-like object literal that follows `marker` using brace balancing."""
    if not html or not marker:
        return ""
    i = html.find(marker)
    if i < 0:
        return ""

    # Find first '{' after marker
    j = html.find("{", i)
    if j < 0:
        return ""

    depth = 0
    in_str = False
    esc = False
    for k in range(j, len(html)):
        ch = html[k]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue

        if ch == '"':
            in_str = True
            continue

        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return html[j : k + 1]

    return ""


def _bootstrap_collegevine_session(session: requests.Session, bootstrap_urls: Optional[List[str]] = None) -> Tuple[str, Dict[str, str]]:
    """Fetch a public CollegeVine page and extract (csrf_token, endpoints_map).

    IMPORTANT: Not all CV pages embed `window.CV.pathInfo.endpoints = {...}`.
    In practice, the per-school majors page (or school page) is often the most reliable.

    We therefore try a small list of candidate bootstrap URLs (in order) until we can
    extract an endpoints map.
    """
    urls = [COLLEGEVINE_BOOTSTRAP_URL]
    for u in (bootstrap_urls or []):
        if u and u not in urls:
            urls.append(u)

    for url in urls:
        try:
            r = session.get(url, headers=HEADERS, timeout=COLLEGEVINE_TIMEOUT_SEC, allow_redirects=True)
        except Exception:
            try:
                record_cv_request_exception()
            except Exception:
                pass
            continue

        try:
            record_cv_http_status(getattr(r, "status_code", 0) or 0)
        except Exception:
            pass

        # React to CV rate limiting / blocking: don't keep probing bootstrap URLs.
        if getattr(r, "status_code", 0) in (403, 429):
            return "", {}

        if getattr(r, "status_code", 0) != 200:
            continue

        try:
            html = r.text or ""
        except Exception:
            try:
                html = (r.content or b"").decode("utf-8", errors="replace")
            except Exception:
                html = ""

        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        csrf = ""
        m = soup.find("meta", attrs={"name": "csrf-token"})
        if m and m.get("content"):
            csrf = normalize_unicode_text(m.get("content"))

        # endpoints object: window.CV.pathInfo.endpoints = {...}
        # Use a more specific marker so we don't accidentally start brace-balancing
        # inside an unrelated JS function body.
        marker_candidates = [
            "window.CV.pathInfo.endpoints =",
            "window.CV.pathInfo.endpoints=",
        ]

        obj_txt = ""
        for marker in marker_candidates:
            obj_txt = _extract_balanced_braces_object(html, marker)
            if obj_txt:
                break

        if not obj_txt:
            # try next bootstrap URL
            continue

        try:
            endpoints = json.loads(obj_txt)
        except Exception:
            # Sometimes the extracted blob can include trailing semicolons; try to sanitize.
            obj2 = obj_txt.strip().rstrip(";")
            try:
                endpoints = json.loads(obj2)
            except Exception:
                continue

        if not isinstance(endpoints, dict):
            continue

        # Ensure string -> string
        out: Dict[str, str] = {}
        for k, v in endpoints.items():
            if isinstance(k, str) and isinstance(v, str):
                out[k] = v

        if out:
            return csrf, out

    return "", {}


def _collegevine_request_headers(csrf_token: str) -> Dict[str, str]:
    """Headers for CV XHR-ish POST endpoints."""
    h = dict(HEADERS)
    h.update(
        {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
        }
    )
    if csrf_token:
        h["X-CSRF-Token"] = csrf_token
    return h


def _resolve_collegevine_school(session: requests.Session, csrf: str, endpoints: Dict[str, str], school_name: str) -> Tuple[str, str, str]:
    """Return (school_id, school_slug, school_display_name) from /schools/search.

    Why this can fail:
      - /schools/search may return a large, noisy list for short/slug-like queries.
      - Some school slugs are easily confused (e.g., "augustana" vs "asa").

    Strategy:
      1) Try multiple queries (slug candidates AND the raw school name).
      2) Prefer exact slug matches.
      3) For fuzzy picks, require overlap on at least one "key token" from the school name
         (e.g., "augustana", "albion", "amherst") to avoid false positives.
    """

    search_path = endpoints.get("schools_search_path") or "/schools/search"

    # --- Key tokens: remove generic stopwords and institutional suffixes ---
    name_norm = normalize_unicode_text(school_name or "").lower()
    name_norm = re.sub(r"[^a-z0-9\s-]", " ", name_norm)
    name_norm = re.sub(r"\s+", " ", name_norm).strip()

    stop = {
        "the", "of", "in", "for", "and", "to", "a", "an", "at",
        "university", "college", "institute", "school", "campus",
        "state", "community", "polytechnic",
    }
    key_tokens = {t for t in name_norm.replace("-", " ").split() if t and t not in stop and len(t) >= 4}

    target_slug = slugify_collegevine_school_name(school_name)

    # Query strategy (expanded):
    #   1) Raw name first (best precision)
    #   2) Key-token-only fallbacks (e.g., "albion")
    #   3) Slug candidates (slug + spaced slug)
    #   4) Remaining normalized variants
    queries: List[str] = []

    def _add_q(q: str) -> None:
        qn = normalize_unicode_text(q)
        if qn and qn not in queries:
            queries.append(qn)

    # 1) Raw name first
    _add_q(school_name)
    _add_q(name_norm)

    # 2) Key-token-only fallbacks (longer/more distinctive first)
    if key_tokens:
        for tok in sorted(key_tokens, key=lambda x: (-len(x), x)):
            _add_q(tok)

    # 3) Slug candidates
    for c in collegevine_slug_candidates(school_name):
        if not c:
            continue
        _add_q(c)
        _add_q(c.replace("-", " "))

    # 4) Lightly de-suffixed variant (sometimes CV names omit "College"/"University")
    nm_simple = re.sub(
        r"\b(university|college|institute|school|state\s+university|community\s+college|polytechnic|campus)\b",
        " ",
        school_name or "",
        flags=re.I,
    )
    nm_simple = normalize_unicode_text(nm_simple)
    if nm_simple and nm_simple.lower() != (school_name or "").lower():
        _add_q(nm_simple)

    # Cap attempts higher: some schools need a few different query shapes.
    for q in queries[:12]:
        try:
            r = session.post(
                urljoin("https://www.collegevine.com", search_path),
                headers=_collegevine_request_headers(csrf),
                json={"q": q},
                timeout=COLLEGEVINE_TIMEOUT_SEC,
            )
        except Exception:
            try:
                record_cv_request_exception()
            except Exception:
                pass
            continue

        try:
            record_cv_http_status(getattr(r, "status_code", 0) or 0)
        except Exception:
            pass

        # React to CV rate limiting / blocking: stop trying further queries.
        if getattr(r, "status_code", 0) in (403, 429):
            break

        if getattr(r, "status_code", 0) != 200:
            continue

        try:
            arr = r.json()
        except Exception:
            continue

        if not isinstance(arr, list) or not arr:
            continue

        # 1) Exact slug match wins immediately.
        for rec in arr:
            if not isinstance(rec, dict):
                continue
            if str(rec.get("slug") or "").strip().lower() == target_slug:
                sid = rec.get("id") or ""
                return str(sid), str(rec.get("slug") or ""), str(rec.get("name") or "")

        # 2) Fuzzy: require key token overlap to avoid false positives like ASA vs Augustana.
        scored: List[Tuple[float, dict]] = []
        for rec in arr:
            if not isinstance(rec, dict):
                continue

            nm = normalize_unicode_text(str(rec.get("name") or ""))
            sl = normalize_unicode_text(str(rec.get("slug") or ""))
            if not nm and not sl:
                continue

            blob = (nm + " " + sl).lower()
            if key_tokens:
                if not any(tok in blob for tok in key_tokens):
                    continue

            score = 0.0
            if sl:
                score = max(score, difflib.SequenceMatcher(None, target_slug, sl.lower()).ratio())
            if nm:
                score = max(score, difflib.SequenceMatcher(None, name_norm, nm.lower()).ratio())

            scored.append((score, rec))

        if not scored:
            continue

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best = scored[0]

        # With token-overlap gating, we can accept slightly lower thresholds.
        if best_score >= 0.74:
            sid = str(best.get("id") or "")
            sl = str(best.get("slug") or "")
            nm = str(best.get("name") or "")
            return sid, sl, nm

    return "", "", ""


def _load_collegevine_static_data(session: requests.Session, endpoints: Dict[str, str]) -> Optional[dict]:
    """Fetch and cache the large CollegeVine static JSON blob.

    Patches implemented here:
      - Record CV status + exceptions during the static-blob fetch (not just bootstrap/search)
      - Support relative `schools_static_data_url` via urljoin
      - Guard against 200-with-HTML interstitials / bot walls; fail cleanly (don’t cache garbage)
      - Track parse / content-type surprises in the CV status tally for diagnostics
    """
    global _COLLEGEVINE_STATIC_CACHE, _COLLEGEVINE_STATIC_CACHE_URL

    static_url = endpoints.get("schools_static_data_url") or ""
    static_url = normalize_unicode_text(static_url)
    if not static_url:
        return None

    # Some deployments return a relative URL.
    if not re.match(r"^https?://", static_url, flags=re.I):
        static_url = urljoin("https://www.collegevine.com", static_url)

    # Use the resolved absolute URL as the cache key.
    if _COLLEGEVINE_STATIC_CACHE is not None and static_url == _COLLEGEVINE_STATIC_CACHE_URL:
        return _COLLEGEVINE_STATIC_CACHE

    try:
        r = session.get(static_url, headers=HEADERS, timeout=COLLEGEVINE_TIMEOUT_SEC, allow_redirects=True)
    except Exception:
        # Timeouts / connection errors / etc.
        try:
            record_cv_request_exception()
        except Exception:
            pass
        return None

    # Always record the HTTP status for this static-blob fetch.
    try:
        record_cv_http_status(getattr(r, "status_code", 0) or 0)
    except Exception:
        pass

    code = getattr(r, "status_code", 0) or 0

    # If CV is blocking or rate limiting, treat as unavailable.
    if code in (403, 429):
        return None

    if code != 200:
        return None

    # Guard: sometimes CV returns HTML (bot wall / interstitial) with a 200.
    # Prefer to fail cleanly rather than caching non-JSON.
    try:
        ctype = (getattr(r, "headers", {}) or {}).get("Content-Type", "")
    except Exception:
        ctype = ""

    # If content-type claims non-JSON, sniff the body before bailing (some servers mislabel JSON).
    if ctype and ("json" not in ctype.lower()):
        try:
            head = (r.text or "").lstrip()[:400].lower()
        except Exception:
            head = ""
        if head.startswith("<!doctype") or head.startswith("<html") or "<head" in head:
            # Diagnostic bucket: 200-but-HTML interstitial
            try:
                record_cv_http_status(-3)
            except Exception:
                pass
            return None

    # Even without ctype, sniff for obvious HTML shells.
    try:
        head2 = (r.text or "").lstrip()[:200].lower()
    except Exception:
        head2 = ""
    if head2.startswith("<!doctype") or head2.startswith("<html"):
        try:
            record_cv_http_status(-3)
        except Exception:
            pass
        return None

    # Parse JSON (prefer response.json; fallback to json.loads(text)).
    try:
        data = r.json()
    except Exception:
        try:
            data = json.loads(r.text)
        except Exception:
            # Diagnostic bucket: parse failure
            try:
                record_cv_http_status(-2)
            except Exception:
                pass
            return None

    if not isinstance(data, dict):
        # Diagnostic bucket: unexpected top-level type
        try:
            record_cv_http_status(-2)
        except Exception:
            pass
        return None

    _COLLEGEVINE_STATIC_CACHE = data
    _COLLEGEVINE_STATIC_CACHE_URL = static_url
    return data


def _build_majors_lookup(static_data: dict) -> Dict[str, str]:
    """Return mapping CIP-code -> major name from CollegeVine static majorsMap."""
    majors_map = static_data.get("majorsMap")
    out: Dict[str, str] = {}

    if isinstance(majors_map, dict):
        # already keyed by code
        for k, v in majors_map.items():
            if isinstance(v, dict) and v.get("name"):
                out[str(k)] = str(v.get("name"))
        return out

    if isinstance(majors_map, list):
        # entries like {"cipCode": "05.0101", "name": "African Studies", ...}
        for rec in majors_map:
            if not isinstance(rec, dict):
                continue
            nm = rec.get("name")
            if not nm:
                continue

            code = rec.get("cipCode")
            if code is None:
                code = rec.get("cip")
            if code is None:
                code = rec.get("code")
            if code is None:
                code = rec.get("id")
            if code is None:
                continue

            out[str(code)] = str(nm)

    return out


def fetch_collegevine_majors_page(
    school_name: str,
    session: Optional[requests.Session] = None,
    progtitle_strictness: int = 2,
    *args,
    **kwargs,
) -> Tuple[str, int, int, int, List[str]]:
    """CollegeVine majors integration via static data.

    Return: (collegevine_url, college_vine_site, college_vine_control_hits,
             college_vine_program_title_count, college_vine_program_titles)

    - college_vine_site is 1 if we can resolve the school and map majors from the static blob.
    - control hits are computed against offered major *names*.
    - program titles are the subset of offered major names that match TARGET_ANY_REGEX,
      optionally passed through clean_program_titles for consistency.

    NOTE: This fetch is intentionally isolated from the per-institution _STATUS_COUNTS
    used for tagging university-site 403/429s.
    """
    # --- Signature tolerance / backwards-compat ---
    # Some wrappers historically called this with (school_name, progtitle_strictness, session)
    # or omitted the session. Normalize without throwing.
    if session is None:
        try:
            session = get_thread_session()
        except Exception:
            session = requests.Session()

    # Handle accidental swap: session passed where strictness should be.
    if isinstance(session, int) and isinstance(progtitle_strictness, requests.Session):
        session, progtitle_strictness = progtitle_strictness, session

    # If strictness arrives via kwargs, prefer it.
    if "progtitle_strictness" in kwargs:
        try:
            progtitle_strictness = int(kwargs.get("progtitle_strictness"))
        except Exception:
            pass

    try:
        progtitle_strictness = int(progtitle_strictness)
    except Exception:
        progtitle_strictness = 2

    # Normalize college_vine_site to an int (0/1) consistently.
    def _cv_site_flag(x: object) -> int:
        try:
            return 1 if int(x) == 1 else 0
        except Exception:
            return 1 if bool(x) else 0

    # Reset CV-only status tally for this institution's CollegeVine attempt.
    try:
        reset_cv_status_tally()
    except Exception:
        pass
    # 1) Bootstrap session for endpoints + csrf
    # Try to bootstrap from pages that reliably embed CV.pathInfo.endpoints.
    slugs_for_bootstrap = collegevine_slug_candidates(school_name)
    # Always keep a traceable majors URL slug handy (avoid empty school_slug => "//majors").
    trace_slug = slugs_for_bootstrap[0] if slugs_for_bootstrap else slugify_collegevine_school_name(school_name)

    bootstrap_urls: List[str] = []
    for slug in slugs_for_bootstrap:
        bootstrap_urls.append(f"{COLLEGEVINE_BASE}/{slug}/majors")
        bootstrap_urls.append(f"https://www.collegevine.com/schools/{slug}")

    csrf, endpoints = _bootstrap_collegevine_session(session, bootstrap_urls=bootstrap_urls)
    if not endpoints:
        # If CV is rate-limiting or blocking us, avoid further probing.
        if cv_block_or_ratelimit_seen():
            if trace_slug:
                return f"{COLLEGEVINE_BASE}/{trace_slug}/majors", _cv_site_flag(0), 0, 0, []
            return "", _cv_site_flag(0), 0, 0, []
        # fall back to old behavior: attempt HTML majors page fetch (best-effort)
        slugs = slugs_for_bootstrap
        if not slugs:
            return "", _cv_site_flag(0), 0, 0, []
        last_url = f"{COLLEGEVINE_BASE}/{trace_slug}/majors"
        for slug in slugs:
            cv_url = f"{COLLEGEVINE_BASE}/{slug}/majors"
            last_url = cv_url
            try:
                r = session.get(cv_url, headers=HEADERS, timeout=COLLEGEVINE_TIMEOUT_SEC, allow_redirects=True)
            except Exception:
                try:
                    record_cv_request_exception()
                except Exception:
                    pass
                continue

            try:
                record_cv_http_status(getattr(r, "status_code", 0) or 0)
            except Exception:
                pass

            # React to CV rate limiting / blocking.
            if getattr(r, "status_code", 0) in (403, 429):
                break

            if getattr(r, "status_code", 0) != 200:
                continue
            try:
                html = r.text or ""
            except Exception:
                html = ""
            if not html:
                continue
            parsed = parse_html_to_parsedpage(html, page_url=cv_url)
            if is_soft_404(parsed):
                continue
            ctrl_hits = count_control_hits_in_text(parsed.corpus_any)
            raw_titles: Set[str] = set()
            for txt in (parsed.anchor_texts + parsed.anchor_attr_texts + parsed.title_like):
                if txt and looks_like_program_title(txt, progtitle_strictness=progtitle_strictness, context="hub"):
                    raw_titles.add(txt)
            for txt in [getattr(parsed, "h1", ""), getattr(parsed, "html_title", "")]:
                if txt and looks_like_program_title(txt, progtitle_strictness=progtitle_strictness, context="hub"):
                    raw_titles.add(txt)
            cleaned_titles = clean_program_titles(list(raw_titles), progtitle_strictness=progtitle_strictness)
            return cv_url, _cv_site_flag(1), ctrl_hits, int(len(cleaned_titles)), cleaned_titles
        return last_url, _cv_site_flag(0), 0, 0, []

    # 2) Resolve school id + slug
    school_id, school_slug, _school_display_name = _resolve_collegevine_school(session, csrf, endpoints, school_name)
    # Ensure slug is never empty (prevents "//majors" URLs).
    if not school_slug:
        school_slug = trace_slug
    slug_for_url = school_slug or trace_slug
    # If CV is rate-limiting or blocking us, avoid further probing.
    if cv_block_or_ratelimit_seen():
        if slug_for_url:
            return f"{COLLEGEVINE_BASE}/{slug_for_url}/majors", _cv_site_flag(0), 0, 0, []
        return "", _cv_site_flag(0), 0, 0, []
    if not school_id:
        # Fallback: try to resolve directly from the static blob by slug/name (avoids /schools/search failures).
        static_data = _load_collegevine_static_data(session, endpoints)
        if isinstance(static_data, dict):
            slug_guess = slugify_collegevine_school_name(school_name)
            name_norm = normalize_unicode_text(school_name or "").lower()
            name_norm = re.sub(r"[^a-z0-9\s-]", " ", name_norm)
            name_norm = re.sub(r"\s+", " ", name_norm).strip()

            # Build key tokens (same logic as resolver) to prevent wrong picks.
            stop = {
                "the", "of", "in", "for", "and", "to", "a", "an", "at",
                "university", "college", "institute", "school", "campus",
                "state", "community", "polytechnic",
            }
            key_tokens = {t for t in name_norm.replace("-", " ").split() if t and t not in stop and len(t) >= 4}

            best_rec = None
            best_score = 0.0

            for rec in (static_data.get("staticSchools") or []):
                if not isinstance(rec, dict):
                    continue

                # Some versions of the blob include `slug`; others may not.
                sl = normalize_unicode_text(str(rec.get("slug") or rec.get("schoolSlug") or ""))
                nm = normalize_unicode_text(str(rec.get("name") or rec.get("displayName") or rec.get("schoolName") or ""))

                # Exact slug match if available.
                if sl and sl.lower() == slug_guess:
                    best_rec = rec
                    best_score = 1.0
                    break

                blob = (nm + " " + sl).lower()
                if key_tokens:
                    if not any(tok in blob for tok in key_tokens):
                        continue

                sc = 0.0
                if sl:
                    sc = max(sc, difflib.SequenceMatcher(None, slug_guess, sl.lower()).ratio())
                if nm:
                    sc = max(sc, difflib.SequenceMatcher(None, name_norm, nm.lower()).ratio())

                if sc > best_score:
                    best_score = sc
                    best_rec = rec

            if best_rec is not None and best_score >= 0.78:
                school_id = str(best_rec.get("id") or "")
                if not school_slug:
                    # Prefer the blob slug if present; otherwise use our guess.
                    school_slug = str(best_rec.get("slug") or best_rec.get("schoolSlug") or slug_guess)
                slug_for_url = school_slug or trace_slug

        # If still unresolved, output a traceable majors URL based on our best slug guess.
        if not school_id:
            if slug_for_url:
                return f"{COLLEGEVINE_BASE}/{slug_for_url}/majors", _cv_site_flag(0), 0, 0, []
            return "", _cv_site_flag(0), 0, 0, []

    # 3) Load static blob
    static_data = _load_collegevine_static_data(session, endpoints)
    if not static_data:
        return f"{COLLEGEVINE_BASE}/{slug_for_url}/majors", _cv_site_flag(0), 0, 0, []

    majors_lookup = _build_majors_lookup(static_data)
    if not majors_lookup:
        return f"{COLLEGEVINE_BASE}/{slug_for_url}/majors", _cv_site_flag(0), 0, 0, []

    # 4) Find school record in staticSchools and map majors
    school_rec = None
    for rec in (static_data.get("staticSchools") or []):
        if isinstance(rec, dict) and str(rec.get("id") or "") == str(school_id):
            school_rec = rec
            break

    if not school_rec or not isinstance(school_rec, dict):
        return f"{COLLEGEVINE_BASE}/{slug_for_url}/majors", _cv_site_flag(0), 0, 0, []

    major_codes = school_rec.get("majors") or []
    if not isinstance(major_codes, list) or not major_codes:
        return f"{COLLEGEVINE_BASE}/{slug_for_url}/majors", _cv_site_flag(0), 0, 0, []

    offered_names: List[str] = []
    for code in major_codes:
        nm = majors_lookup.get(str(code))
        if nm:
            offered_names.append(normalize_unicode_text(nm))

    if not offered_names:
        return f"{COLLEGEVINE_BASE}/{slug_for_url}/majors", _cv_site_flag(0), 0, 0, []

    # 5) Compute control hits and target program titles
    # Normalize + uniquify offered major names BEFORE control-hit counting and title extraction.
    # This prevents duplicates/variants from inflating control hits and producing redundant titles.
    deduped_offered: List[str] = []
    _seen_ci: Set[str] = set()
    for nm in offered_names:
        key = (normalize_unicode_text(nm) or "").strip().lower()
        if not key:
            continue
        if key in _seen_ci:
            continue
        _seen_ci.add(key)
        deduped_offered.append(normalize_unicode_text(nm))

    if not deduped_offered:
        return f"{COLLEGEVINE_BASE}/{slug_for_url}/majors", _cv_site_flag(0), 0, 0, []

    ctrl_hits = count_control_hits_in_text(" ".join(deduped_offered))

    raw_titles = [t for t in deduped_offered if TARGET_ANY_REGEX.search(t)]
    # These are already clean major names, but keep the cleaner for consistency.
    # Dedup again (case-insensitive) before cleaning to reduce noise.
    deduped_raw_titles: List[str] = []
    _seen_titles_ci: Set[str] = set()
    for t in raw_titles:
        k = (normalize_unicode_text(t) or "").strip().lower()
        if not k or k in _seen_titles_ci:
            continue
        _seen_titles_ci.add(k)
        deduped_raw_titles.append(normalize_unicode_text(t))

    cleaned_titles = clean_program_titles(deduped_raw_titles, progtitle_strictness=progtitle_strictness)

    cv_url = f"{COLLEGEVINE_BASE}/{slug_for_url}/majors"
    return cv_url, _cv_site_flag(1), int(ctrl_hits), int(len(cleaned_titles)), cleaned_titles
# ============================================================
# Batches
# ============================================================
def _batch_paths(final_output_path: Path, batch_idx: int) -> Tuple[Path, Path]:
    out_dir = final_output_path.parent
    stem = final_output_path.stem
    progress = out_dir / f"{stem}__batch_{batch_idx:03d}__progress.csv"
    batch_out = out_dir / f"{stem}__batch_{batch_idx:03d}.csv"
    return progress, batch_out


def _load_completed_unitids(progress_path: Path, unitid_col: str) -> Set[str]:
    """Load completed unitids from an existing progress CSV WITHOUT pandas (fast/low-mem)."""
    done: Set[str] = set()
    if not progress_path.exists():
        return done

    with progress_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return done
        if unitid_col not in header:
            return done
        uix = header.index(unitid_col)

        for row in reader:
            if not row:
                continue
            if uix >= len(row):
                continue
            u = (row[uix] or "").strip()
            if u:
                done.add(u)
    return done


def _append_rows_csv(path: Path, fieldnames: List[str], rows: List[dict]) -> None:
    if not rows:
        return
    is_new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if is_new:
            w.writeheader()
        for r in rows:
            w.writerow(r)


def _compact_progress_csv(progress_path: Path, unitid_col: str, fieldnames: List[str]) -> None:
    """Drop duplicate unitids keeping last; rewrite in stable column order."""
    if not progress_path.exists():
        return
    try:
        dfp = pd.read_csv(progress_path, dtype=str, keep_default_na=False)
    except Exception:
        return
    if dfp.empty:
        return
    if unitid_col in dfp.columns:
        dfp = dfp.drop_duplicates(subset=[unitid_col], keep="last")
    # enforce column order (keep any extras at end)
    cols = [c for c in fieldnames if c in dfp.columns] + [c for c in dfp.columns if c not in fieldnames]
    dfp = dfp[cols]
    dfp.to_csv(progress_path, index=False)


def _concat_batch_outputs(batch_outputs: List[Path], final_output_path: Path) -> None:
    """Concatenate batch CSVs into final output (streaming, low-memory)."""
    wrote_header = False
    with final_output_path.open("w", newline="", encoding="utf-8") as out_f:
        out_w = None

        for p in batch_outputs:
            if not p.exists():
                continue
            with p.open("r", newline="", encoding="utf-8") as in_f:
                r = csv.reader(in_f)
                try:
                    header = next(r)
                except StopIteration:
                    continue

                if not wrote_header:
                    out_w = csv.writer(out_f)
                    out_w.writerow(header)
                    wrote_header = True

                for row in r:
                    if row:
                        out_w.writerow(row)

# ============================================================
# Batch reset
# ============================================================
def _reset_batch_state() -> None:
    """Clear mutable globals between batches to mimic a fresh run and avoid growth."""
    try:
        reset_status_tally()
    except Exception:
        pass
    try:
        reset_cv_status_tally()
    except Exception:
        pass

    # Drop CV static blob cache between batches (fresh-ish restart behavior)
    global _COLLEGEVINE_STATIC_CACHE, _COLLEGEVINE_STATIC_CACHE_URL
    _COLLEGEVINE_STATIC_CACHE = None
    _COLLEGEVINE_STATIC_CACHE_URL = ""
# ============================================================
# Defaults (override via CLI)
# ============================================================
DEFAULT_INPUT_PATH = Path("ace_unitid_merge__ace_x_2013comp__inner_on_unitid_plusURL_plus2013name.csv")
DEFAULT_OUTPUT_PATH = Path("ace_unitid_merge__ace_x_2013comp__webscrape__v15simple.csv.csv")

UNITID_COL = "unitid"
NAME_COL = "name"
WEB_COL = "Web_address"

REQUEST_TIMEOUT_SEC = 25
SLEEP_BETWEEN_REQUESTS_SEC = 0.25
SLEEP_BETWEEN_INSTITUTIONS_SEC = 0.20

MAX_BYTES_PER_PAGE = 2_000_000
MAX_JSON_BLOB_CHARS = 200_000

CACHE_DIR = Path(".cache_v14_html")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_TTL_SECONDS = 14 * 24 * 3600

# Per-institution budgets
PHASE1_MAX_INVENTORY_CANDIDATES = 18
PHASE1_MAX_CATALOG_CANDIDATES = 10
PHASE1_MAX_HOMEPAGE_LINKS = 22
PHASE2_BESTFIRST_MAX_PAGES = 55
PHASE2_MAX_DEPTH = 3

TOP_K_ALTS = 6
ONE_HOP_MAX_LINKS_TOTAL = 40
ONE_HOP_MAX_FETCHES = 18

SITEMAP_MAX_FETCHES = 5
SITEMAP_MAX_URLS = 2500
SITEMAP_CANDIDATE_CAP = 240

MIN_VISIBLE_TEXT_LEN = 900
MIN_MAIN_TEXT_LEN = 650

ENABLE_PDF_FALLBACK = True
PDF_MAX_FETCHES_PER_INSTITUTION = 2

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; majors-scan/14.0; +https://nces.ed.gov/)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ============================================================
# URL templates
# ============================================================
INVENTORY_PATHS = [
    "/academics/programs", "/academics/programs/",
    "/programs", "/programs/",
    "/academics/majors-minors", "/academics/majors-minors/",
    "/majors-minors", "/majors-minors/",
    "/academics/majors", "/academics/majors/",
    "/majors", "/majors/",
    "/undergraduate/majors", "/undergraduate/majors/",
    "/departments-and-programs", "/departments-and-programs/",
    "/academics/departments-and-programs", "/academics/departments-and-programs/",
    "/departments", "/departments/",
    "/academics/departments", "/academics/departments/",
    "/areas-of-study", "/areas-of-study/",
    "/fields-of-study", "/fields-of-study/",
    "/academic-programs", "/academic-programs/",
    "/academics/academic-programs", "/academics/academic-programs/",
    "/find-your-program", "/en/find-your-program", "/en/find-your-program/",
]

CATALOG_PATHS = [
    "/college-catalog", "/college-catalog/",
    "/course-catalog", "/course-catalog/",
    "/catalog", "/catalog/",
    "/catalogue", "/catalogue/",
    "/bulletin", "/bulletin/",
    "/academics/catalog", "/academics/catalog/",
]

YEAR_TOKENS = [
    "2526", "20252026", "2025-2026", "2025–2026", "2025—2026", "2025_2026", "202526",
    "25-26", "25–26", "25_26",
]
YEAR_PATH_TEMPLATES = [
    "/catalog/{y}",
    "/catalogue/{y}",
    "/college-catalog/{y}",
    "/course-catalog/{y}",
    "/academic-catalog/{y}",
    "/bulletin/{y}",
    "/academics/catalog/{y}",
    "/academiclife/college-catalog/{y}",
]


# ============================================================
# Patterns / scoring signals
# ============================================================
YEAR_REGEX = re.compile(
    r"(2025\s*[-–—_/]?\s*2026)|(\b2526\b)|(\b202526\b)|(\b20252026\b)|(\b25\s*[-–—_/]\s*26\b)",
    flags=re.IGNORECASE,
)

GRAD_URL_PENALTY = re.compile(
    r"\b(graduate|grad-studies|gradstudies|master|masters|doctoral|phd|mba|mfa|ms|ma)\b",
    flags=re.IGNORECASE,
)
PROFILE_URL_PENALTY = re.compile(
    r"(/people/|/person/|/faculty/|/faculty-directory/|/staff/|/directory/|/profiles?/|/bio/|/biography/)",
    flags=re.IGNORECASE,
)
IRRELEVANT_URL_PENALTY = re.compile(
    r"(/news/|/events/|/event/|/calendar/|/press/|/alumni/|/giving/|/donate/|/commencement|/story/|/blog/|/announcement/)",
    flags=re.IGNORECASE,
)
ADMISSIONS_URL_PENALTY = re.compile(
    r"(/admissions/|/apply/|/visit/|/financial-aid/|/tuition/|/request-info/)",
    flags=re.IGNORECASE,
)
ARCHIVE_URL_PENALTY = re.compile(r"\barchive\b|/past-|/past/", flags=re.IGNORECASE)
AWARDS_URL_PENALTY = re.compile(r"\b(awards?|honors?)\b", flags=re.IGNORECASE)
COMPLIANCE_URL_PENALTY = re.compile(
    r"(consumer-information|disclosures?|cip\b|gainful-employment|institutional-research)",
    flags=re.IGNORECASE,
)

SOFT_404_TEXT = re.compile(
    r"\b(page\s+not\s+found|404|not\s+found|we\s+can'?t\s+find|does\s+not\s+exist)\b",
    flags=re.IGNORECASE,
)

UNDERGRAD_TITLE_BOOST = re.compile(
    r"\b(undergraduate|majors\s+and\s+minors|majors-minors|bachelor|B\.?A\.?|B\.?S\.?)\b",
    flags=re.IGNORECASE,
)

PDF_LINK_REGEX = re.compile(r"\.pdf(\?|$)", flags=re.IGNORECASE)

CONTROL_TERMS: Dict[str, str] = {
    "anthropology": r"\banthropolog[a-z]*\b",
    "math": r"\bmath[a-z]*\b",
    # "bio": r"\bbio[a-z]*\b", # not a good control due to "biography" and other simple matches
    "linguistics": r"\blinguist[a-z]*\b",
    "chem": r"\bchem(?:istry|ical)?\b",
    "architect": r"\barchitect[a-z]*\b",
    "economics": r"\beconomics?\b",
    "psychology": r"\bpsycholog[a-z]*\b",
    "sociology": r"\bsociolog[a-z]*\b",
    "history": r"\bhistory\b",
    "english": r"\benglish\b",
    "political_science": r"\bpolitical\s+science\b|\bpolisci\b",
    "philosophy": r"\bphilosoph[a-z]*\b",
    "computer_science": r"\bcomputer\s+science\b|\bcomp\s+sci\b|\bcs\b(?![a-z])",
    "engineering": r"\bengineering\b",
    "physics": r"\bphysics\b",
    "geology": r"\bgeolog[a-z]*\b",
    "statistics": r"\bstatistic[a-z]*\b",
    "neuroscience": r"\bneuroscien[a-z]*\b",
}

STRUCT_TERMS: Dict[str, str] = {
    "majors": r"\bmajors?\b",
    "minors": r"\bminors?\b",
    "degrees": r"\bdegrees?\b",
    "programs": r"\bprograms?\b",
    "bachelor": r"\bbachelor(?:'s)?\b",
    "BA": r"\bB\.?A\.?\b",
    "BS": r"\bB\.?S\.?\b",
}

#
# Expanded target token family (v12): includes Africa/African/Africana, Pan-African,
# African American, Afro-American, Africology, Race, plus existing afri/ethnic/black.
#
# NOTE: Keep "TARGET_ANY_REGEX" broad (union) so extraction triggers properly,
# but keep per-bucket regexes interpretable for output columns.
TARGET_TOKEN_REGEX = {
    # "afri" bucket (kept for output column stability) WITHOUT broad afri* wildcard.
    "afri": re.compile(
        r"\b(africa(?:n|na)?|pan[-\s]?african|african[-\s]?american|afro[-\s]?american|africology)\b",
        flags=re.IGNORECASE,
    ),
    # Include both "ethnic*" and "ethnicity" so the bucket reflects real page phrasing.
    "ethnic": re.compile(r"\b(ethnic[a-z\-]*|ethnicity)\b", flags=re.IGNORECASE),
    "black": re.compile(r"\bblack[a-z\-]*\b", flags=re.IGNORECASE),
    # New bucket: race / racial
    "race": re.compile(r"\b(race|racial)\b", flags=re.IGNORECASE),
}

# Trigger regex used to decide whether text is in-scope for extraction.
# Keep this broad (union of all target families) so we don't miss relevant programs.
TARGET_ANY_REGEX = re.compile(
    r"\b(africa(?:n|na)?|pan[-\s]?african|african[-\s]?american|afro[-\s]?american|africology|"
    r"ethnic[a-z\-]*|ethnicity|black[a-z\-]*|race|racial)\b",
    flags=re.IGNORECASE,
)

# Output column stability: keep these buckets as fixed output columns.
# If TARGET_TOKEN_REGEX changes, these columns will still be present (empty if missing).
OUTPUT_TOKEN_BUCKETS = ["afri", "ethnic", "black", "race"]

TITLE_NEGATIVE_CONTEXT = re.compile(
    r"\b(student\s+union|black\s+student|history\s+month|cultural\s+center|lives\s+matter|alumni|news|event|calendar|office)\b",
    flags=re.IGNORECASE,
)

COURSE_CODE = re.compile(r"^\s*[A-Z]{2,6}(?:/[A-Z]{2,6})*\s*[-]?\s*\d{1,3}[A-Za-z]?\b")
COURSE_WORDS = re.compile(r"\b(units?|credits?|semester|fall|spring|course(s)?|syllabus)\b", flags=re.IGNORECASE)
PERSON_NAME_ONLY = re.compile(r"^[A-Z][a-z]+(?:\s+[A-Z]\.)?(?:\s+[A-Z][a-z]+){1,2}$")
PROGRAM_KEYWORDS = re.compile(
    r"\b(studies|program|department|major|minor|concentration|track|certificate|center)\b",
    flags=re.IGNORECASE,
)

# --- Title junk / prose detection ---
URL_LIKE_TEXT = re.compile(r"(https?://|www\.|\b[a-z0-9\-]+\.(edu|com|org|net)(/|\b))", flags=re.IGNORECASE)
PROSE_VERBS = re.compile(
    r"\b(define|apply|enrich|explore|learn|prepare|prepares|focus(es)?|offers|provides|designed|helps|aims)\b",
    flags=re.IGNORECASE,
)

# --- Program-title cleaning / postprocessing (v14) ---
TITLE_PLATFORM_KEYWORDS = re.compile(
    r"\b(facebook|instagram|twitter|linkedin|youtube|blackboard|canvas|email|login)\b",
    flags=re.IGNORECASE,
)
TITLE_SLUG_LIKE = re.compile(r"^[a-z0-9\-]{10,}$")
TITLE_LEADING_SLASH = re.compile(r"^/\s*")

TITLE_STRIP_PREFIXES = [
    # salvage patterns (capture group is the likely program name)
    re.compile(r"^\s*welcome\s+to\s+(.+)$", re.I),
    re.compile(r"^\s*about\s+(?:the\s+)?(?:program|department)\s+(?:in|of)\s+(.+)$", re.I),
    re.compile(r"^\s*view\s+(.+)$", re.I),
    re.compile(r"^\s*connect\s+with\s+(.+)$", re.I),
    re.compile(r"^\s*prospective\s+students\s*:\s*(.+)$", re.I),
    re.compile(r"^\s*learn\s+more\s+about\s+(.+)$", re.I),
    re.compile(r"^\s*explore\s+(.+)$", re.I),
]

TITLE_HARD_DROPS = re.compile(
    r"\b(lecturer\s+pool|job|position|apply\b|hiring\b|employment\b)\b",
    flags=re.IGNORECASE,
)

TITLE_CONTENTY_WORDS = re.compile(
    r"\b(discusses|announces|becomes|celebrate|event|workshop|lecture|seminar|news|story|profile|alumni|upcoming)\b",
    flags=re.IGNORECASE,
)

TITLE_YEARISH = re.compile(r"\b(20\d{2}|\d{2}\s*[-–—/]\s*\d{2})\b")


def _has_program_intent(s: str) -> bool:
    s_low = (s or "").lower()
    # Strong program intent: explicit keywords OR common framing
    if PROGRAM_KEYWORDS.search(s) is not None:
        return True
    if s_low.endswith(" studies") or s_low.endswith(" studies program"):
        return True
    if s_low.startswith("department of ") or s_low.startswith("center for ") or s_low.startswith("centre for "):
        return True
    return False


def _split_title_candidates(s: str) -> List[str]:
    """Split breadcrumb-ish or concatenated titles into candidate chunks."""
    s = normalize_unicode_text(s)
    if not s:
        return []

    parts: List[str] = [s]
    for sep in ["|", "•", "·", "–", "—"]:
        if sep in s:
            tmp: List[str] = []
            for p in parts:
                tmp.extend([x.strip() for x in p.split(sep) if x.strip()])
            parts = tmp

    # Light split on " / " only (avoid breaking AAAS/WGS course codes)
    tmp2: List[str] = []
    for p in parts:
        if " / " in p:
            tmp2.extend([x.strip() for x in p.split(" / ") if x.strip()])
        else:
            tmp2.append(p)
    parts = tmp2

    return [normalize_unicode_text(p) for p in parts if p]


def _salvage_prefix(s: str) -> List[str]:
    s = normalize_unicode_text(s)
    if not s:
        return []
    out = [s]
    for rx in TITLE_STRIP_PREFIXES:
        m = rx.match(s)
        if m:
            g = normalize_unicode_text(m.group(1))
            if g and g != s:
                out.append(g)
    return out


def _normalize_degree_suffix(s: str) -> str:
    # Normalize common degree-label patterns that appear after colons or at end.
    s = normalize_unicode_text(s)
    s = re.sub(r"\s*:\s*(B\.?A\.?|B\.?S\.?|BA|BS)\b\s*", " ", s, flags=re.I)
    s = re.sub(r"\s+\b(B\.?A\.?|B\.?S\.?|BA|BS)\b\s*$", "", s, flags=re.I)
    return normalize_unicode_text(s)


# ============================================================
# Loose-normalized concordance helpers
# ============================================================

def _title_key_set_loose(titles: List[str]) -> Set[str]:
    """Loose normalized key set for concordance checks.

    Mirrors `_title_key_set` but uses `norm_title_key_loose()` so common formatting
    differences (e.g., '&' vs 'and', generic suffix words) don't block matches.

    IMPORTANT: Apply `apply_synonym_map()` before loose-normalization so known
    spelling/synonym fixes used in partial matching (e.g., carribbean->caribbean)
    also influence loose concordance.
    """
    out: Set[str] = set()
    for t in (titles or []):
        t_norm = normalize_unicode_text(t)
        if not t_norm:
            continue
        t_pre = apply_synonym_map(t_norm)
        k = norm_title_key_loose(t_pre)
        if k:
            out.add(k)
    return out


def _field_key_set_loose(field_val: str) -> Set[str]:
    """Loose normalized key set from a scalar program-name field.

    Mirrors `_field_key_set` but uses `norm_title_key_loose()`.

    IMPORTANT: Apply `apply_synonym_map()` before loose-normalization so known
    spelling/synonym fixes used in partial matching are reflected here too.
    """
    out: Set[str] = set()
    for x in _split_program_name_field(field_val):
        x_norm = normalize_unicode_text(x)
        if not x_norm:
            continue
        x_pre = apply_synonym_map(x_norm)
        k = norm_title_key_loose(x_pre)
        if k:
            out.add(k)
    return out


def _score_title_variant(s: str) -> int:
    """Higher is better."""
    s_norm = normalize_unicode_text(s)
    s_low = s_norm.lower()
    score = 0

    if _has_program_intent(s_norm):
        score += 40
    if re.search(r"\b(major|minor|concentration|certificate)\b", s_norm, re.I):
        score += 15
    if s_low.startswith("department of "):
        score += 8
    if s_low.endswith(" studies"):
        score += 10

    # Penalize content-y / announcement-y strings
    if TITLE_HARD_DROPS.search(s_norm):
        score -= 200
    if TITLE_YEARISH.search(s_norm):
        score -= 80
    if TITLE_CONTENTY_WORDS.search(s_norm) and not _has_program_intent(s_norm):
        score -= 120

    # Mild penalty for boilerplate verbs/prefixes that survived
    if re.match(r"^(about|welcome|view|connect|prospective\s+students)\b", s_low):
        score -= 40

    # Prefer reasonably short, name-like strings
    if 8 <= len(s_norm) <= 80:
        score += 5
    if len(s_norm) > 110:
        score -= 25

    return score


def clean_program_titles(raw_titles: List[str], progtitle_strictness: int) -> List[str]:
    """Clean, salvage, split, score, and dedupe extracted program-title candidates."""
    expanded: List[str] = []
    for t in raw_titles:
        for s in _salvage_prefix(t):
            expanded.extend(_split_title_candidates(s))

    keep: List[str] = []
    for t in expanded:
        t = _normalize_degree_suffix(t)
        if not t:
            continue

        # Hard drops
        if TITLE_PLATFORM_KEYWORDS.search(t):
            continue
        if TITLE_LEADING_SLASH.search(t):
            continue
        if TITLE_SLUG_LIKE.match(t):
            continue
        if URL_LIKE_TEXT.search(t):
            continue
        if TITLE_HARD_DROPS.search(t):
            continue

        # Course detection (must drop)
        if COURSE_CODE.match(t):
            continue
        if COURSE_WORDS.search(t):
            continue

        # Always require target token family (keeps extraction scoped)
        if TARGET_ANY_REGEX.search(t) is None:
            continue

        # Contenty strings are only acceptable if they ALSO show program intent
        if (TITLE_CONTENTY_WORDS.search(t) or PROSE_VERBS.search(t)) and not _has_program_intent(t):
            if progtitle_strictness >= 3:
                continue

        # Drop sentence-like cases
        if ". " in t and len(t) >= 40:
            continue
        if ": " in t and len(t) >= 60 and not _has_program_intent(t):
            continue

        keep.append(t)

    # Score + dedupe by normalized key
    best_by_key: Dict[str, Tuple[int, str]] = {}
    for t in keep:
        k = norm_title_key(t)
        if not k:
            continue
        sc = _score_title_variant(t)
        cur = best_by_key.get(k)
        if cur is None or sc > cur[0] or (sc == cur[0] and len(t) < len(cur[1])):
            best_by_key[k] = (sc, t)

    out = [v[1] for v in best_by_key.values()]
    return sorted(out)

INVENTORY_URL_HINTS = (
    "/programs", "/academics/programs",
    "/majors-minors", "/majors",
    "/departments-and-programs", "/departments",
    "/areas-of-study", "/fields-of-study",
    "/academic-programs", "find-your-program",
)
CATALOG_URL_HINTS = ("catalog", "catalogue", "college-catalog", "course-catalog", "bulletin")

# "Listing-like" URL patterns for anchors we count toward hubness
LISTING_LINK_PATTERNS = [
    re.compile(r"/programs/[^/]+/?$", re.I),
    re.compile(r"/majors-minors/[^/]+/?$", re.I),
    re.compile(r"/majors/[^/]+(\.html|\.shtml)?$", re.I),
    re.compile(r"/majors-programs/[^/]+", re.I),
    re.compile(r"/departments-and-programs/[^/]+/?$", re.I),
    re.compile(r"/academics/departments-and-programs/[^/]+/?$", re.I),
    re.compile(r"/departments/[^/]+/?$", re.I),
    re.compile(r"/academiclife/departments/[^/]+/?$", re.I),
]

# Broader detail-page detection (so single-major pages don’t beat hubs)
DETAIL_URL_PATTERNS = [
    re.compile(r"/programs/[^/]+/.+", re.I),
    re.compile(r"/majors-minors/[^/]+/.+", re.I),
    re.compile(r"/departments-and-programs/[^/]+/.+", re.I),
    re.compile(r"/departments/[^/]+/.+", re.I),
    re.compile(r"/majors/[^/]+(\.html|\.shtml)?$", re.I),
    re.compile(r"/majors-programs/[^/]+", re.I),
    re.compile(r"/academiclife/departments/[^/]+/?$", re.I),
    re.compile(r"/content/.*/departments/[^/]+\.html$", re.I),
]

SITEMAP_PROGRAM_URL_PATTERNS = [
    re.compile(r"/academics/majors-minors/[^/]+/index\.html?$", flags=re.IGNORECASE),
    re.compile(r"/academics/majors-minors/[^/]+/?$", flags=re.IGNORECASE),
    re.compile(r"/programs/[^/]+/?$", flags=re.IGNORECASE),
    re.compile(r"/departments-and-programs/[^/]+/?$", flags=re.IGNORECASE),
    re.compile(r"/academics/departments-and-programs/[^/]+/?$", flags=re.IGNORECASE),
    re.compile(r"/departments/[^/]+/?$", flags=re.IGNORECASE),
]


# ============================================================
# Row tagging heuristics
# ============================================================
HUB_PATH_HINTS = [
    "majors", "minors", "programs", "degrees", "department", "departments",
    "departments-and-programs", "academics", "academic", "catalog", "bulletin",
    "undergraduate", "undergraduate-programs", "courses", "areas-of-study"
]

NON_ACADEMIC_HINTS = [
    "news", "event", "calendar", "giving", "alumni", "advancement",
    "athletics", "sports", "tickets", "shop", "bookstore",
    "it", "its", "help", "support", "login", "privacy", "accessibility",
    "admissions", "apply", "visit", "tuition", "financial-aid",
    "student-life", "housing", "dining", "library",
    "about", "contact", "directory", "people", "faculty", "staff",
    "press", "media", "magazine", "blog", "policy"
]

SUBSITE_HINTS = [
    "center", "centre", "institute", "office", "community", "belonging",
    "extension", "continuing-ed", "continuing-education",
    "professional", "outreach",
    # dataset-specific common traps
    "nwo", "cge",
]

# High-precision traps: if these appear in host+path, treat page as an automatic loss.
HARD_SUBSITE_BLOCK_TOKENS = [
    "nwo",  # dataset-specific trap
    "cge",  # dataset-specific trap
]

# Separate from the broad detail penalty: this targets department/program/detail pages that can look
# academically relevant but are still not the intended "programs hub" landing page.
# Starting value calibrated from sweep snapshots: “Correct-ish but too specific” was rare (~2/25 rows, ~8%),
# and usually didn’t have an obvious canonical hub alternative in the top-K alts; use a moderate penalty
# (similar scale to other URL penalties) to nudge toward hubs without overcorrecting.
TOO_SPECIFIC_PENALTY = 240

# If a canonical hub is within this many points of best, prefer it.
CANONICAL_TIE_THRESHOLD = 60

TOO_SPECIFIC_PATTERNS = [
    r"/departments?/[a-z0-9_-]+",
    r"/departments-and-programs/[a-z0-9_-]+",
    r"/programs?/[a-z0-9_-]+",
    r"/academics/.+/(major|minors|programs)\b",
]


# ============================================================
# Unicode normalization
# ============================================================

# Helper: best-effort mojibake repair for common UTF-8/latin-1 issues
def _maybe_fix_mojibake(s: str) -> str:
    """Best-effort repair for common mojibake (UTF-8 bytes decoded as latin-1/cp1252).

    This mainly targets artifacts like "\u201a\u00c4\u00ae" / "\u00c3" / "\u00e2" sequences seen in some CSVs.
    We only apply the repair when it measurably reduces these suspicious markers.
    """
    if not s:
        return s

    # Heuristic trigger: only attempt if we see common mojibake markers.
    if re.search(r"(\u201a\u00c4|\u00c3|\u00e2|\u00c2)", s) is None:
        return s

    try:
        fixed = s.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
    except Exception:
        return s

    if not fixed:
        return s

    def _badness(x: str) -> int:
        return (
            x.count("\u201a\u00c4")
            + x.count("\u00c3")
            + x.count("\u00e2")
            + x.count("\u00c2")
        )

    return fixed if _badness(fixed) < _badness(s) else s

def normalize_unicode_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = _maybe_fix_mojibake(s)
    s = unicodedata.normalize("NFKC", s)
    for ch in ["\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2212", "\u00AD"]:
        s = s.replace(ch, "-")
    s = (s.replace("\u2018", "'")
           .replace("\u2019", "'")
           .replace("\u201C", '"')
           .replace("\u201D", '"'))
    s = s.replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def compile_patterns(term_map: Dict[str, str]) -> Dict[str, re.Pattern]:
    return {k: re.compile(v, flags=re.IGNORECASE) for k, v in term_map.items()}


CONTROL_PATS = compile_patterns(CONTROL_TERMS)
STRUCT_PATS = compile_patterns(STRUCT_TERMS)


# ============================================================
# Cache + fetch
# ============================================================
def _cache_key(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _cache_paths(url: str) -> Tuple[Path, Path]:
    key = _cache_key(url)
    return CACHE_DIR / f"{key}.bin", CACHE_DIR / f"{key}.meta"


def fetch_bytes_cached(url: str, session: requests.Session) -> bytes:
    bin_path, meta_path = _cache_paths(url)
    if bin_path.exists() and meta_path.exists():
        try:
            ts = int(meta_path.read_text().strip())
            if time.time() - ts < CACHE_TTL_SECONDS:
                return bin_path.read_bytes()
        except Exception:
            pass

    try:
        r = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SEC, allow_redirects=True, stream=True)
    except Exception:
        # Record request exceptions for row-level tagging (timeouts, connection errors, etc.)
        try:
            record_request_exception()
        except Exception:
            pass
        raise
    # Record fetch status for row-level tagging (403-aware)
    try:
        record_http_status(getattr(r, "status_code", 0) or 0)
    except Exception:
        pass
    r.raise_for_status()
    content = r.content
    if len(content) > MAX_BYTES_PER_PAGE:
        content = content[:MAX_BYTES_PER_PAGE]

    try:
        bin_path.write_bytes(content)
        meta_path.write_text(str(int(time.time())))
    except Exception:
        pass
    return content


def fetch_text_cached(url: str, session: requests.Session) -> str:
    b = fetch_bytes_cached(url, session=session)
    try:
        return b.decode("utf-8", errors="replace")
    except Exception:
        return b.decode("latin-1", errors="replace")


# ============================================================
# URL helpers
# ============================================================
def ensure_scheme(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not re.match(r"^https?://", url, flags=re.IGNORECASE):
        url = "https://" + url
    return url


def same_domain(url: str, base_netloc: str) -> bool:
    try:
        return urlparse(url).netloc.lower().endswith(base_netloc.lower())
    except Exception:
        return False


def root_domain(netloc: str) -> str:
    n = (netloc or "").lower().strip()
    for p in ("www.", "m.", "web."):
        if n.startswith(p):
            n = n[len(p):]
    return n


def rootish_domain(netloc: str) -> str:
    """
    Lightweight "registrable-ish" domain: last two labels if possible.
    This is imperfect for .edu subdomains but works well enough for drift detection.
    """
    n = root_domain(netloc)
    parts = [p for p in n.split(".") if p]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return n


ALLOWED_ACADEMIC_SUBDOMAINS = {"www", "majors", "programs", "catalog", "bulletin", "courses"}


def is_subsite_like(url: str, base_netloc: str) -> bool:
    """
    Heuristic: "subsite-like" means the URL is likely an office/center/outreach subsite,
    or lives on a non-standard subdomain that frequently hosts non-inventory pages.

    Rules:
    - Different rootish domain => subsite-like
    - Subdomain first-label not in allowlist => subsite-like
      (unless it's literally the base host itself)
    - Path contains subsite hint words => subsite-like
    """
    u = ensure_scheme(url)
    if not u:
        return True

    host = (urlparse(u).netloc or "").lower()
    base = (base_netloc or "").lower()

    # Offsite-ish drift
    if rootish_domain(host) != rootish_domain(base):
        return True

    # Subdomain gating: majors.<root>, catalog.<root>, etc allowed; others penalized
    if host != base:
        # get first label (e.g., "catalog" in "catalog.bu.edu")
        parts = [p for p in host.split(".") if p]
        sub = parts[0] if parts else ""
        if sub and sub not in ALLOWED_ACADEMIC_SUBDOMAINS:
            return True

    path = (urlparse(u).path or "").lower()
    full = host + path

    if any(x in full for x in SUBSITE_HINTS):
        return True

    return False

def is_hard_subsite_block(url: str, base_netloc: str) -> bool:
    """True if URL is a known high-precision subsite trap (auto-loss)."""
    u = ensure_scheme(url)
    if not u:
        return True

    host = (urlparse(u).netloc or "").lower()
    base = (base_netloc or "").lower()

    # Drift to different registrable-ish domain => hard-block
    if rootish_domain(host) != rootish_domain(base):
        return True

    path = (urlparse(u).path or "").lower()
    full = host + path

    return any(tok in full for tok in HARD_SUBSITE_BLOCK_TOKENS)

def is_too_specific_url(url: str) -> bool:
    """True if URL path matches TOO_SPECIFIC_PATTERNS."""
    path = (urlparse(url).path or "").lower()
    for pat in TOO_SPECIFIC_PATTERNS:
        if re.search(pat, path):
            return True
    return False

def is_inventoryish_url(url: str) -> bool:
    ul = url.lower()
    return any(h in ul for h in INVENTORY_URL_HINTS)


def is_catalogish_url(url: str) -> bool:
    ul = url.lower()
    return any(h in ul for h in CATALOG_URL_HINTS)


def is_canonical_hub_url(url: str) -> bool:
    p = (urlparse(url).path or "/").rstrip("/")
    p_low = p.lower()
    canonical = {
        "/programs",
        "/academics/programs",
        "/majors-minors",
        "/academics/majors-minors",
        "/majors",
        "/academics/majors",
        "/departments-and-programs",
        "/academics/departments-and-programs",
        "/departments",
        "/academics/departments",
        "/areas-of-study",
        "/fields-of-study",
        "/academic-programs",
        "/academics/academic-programs",
    }
    return p_low in canonical


def looks_like_detail_url(url: str) -> bool:
    path = (urlparse(url).path or "").lower()
    return any(p.search(path) for p in DETAIL_URL_PATTERNS)


# ============================================================
# Row tagging
# ============================================================
def _norm_host_from_web_address(web_address: str) -> str:
    u = ensure_scheme(web_address)
    if not u:
        return ""
    return (urlparse(u).netloc or "").lower()


def tag_row(web_address: str, best_url: str) -> str:
    """
    Returns one of:
      - 'Correct hub'
      - 'Correct-ish but too specific'
      - 'Wrong subsite'
      - 'Wrong non-academic'
    """
    base_host = _norm_host_from_web_address(web_address)
    u = ensure_scheme(best_url)
    if not u or best_url == "N/A":
        return "Wrong non-academic"

    host = (urlparse(u).netloc or "").lower()
    path = (urlparse(u).path or "").lower()
    full = host + path

    if any(x in full for x in NON_ACADEMIC_HINTS):
        return "Wrong non-academic"

    # off-root-domain or obvious subsite hints
    if base_host and rootish_domain(host) != rootish_domain(base_host):
        return "Wrong subsite"
    if any(x in full for x in SUBSITE_HINTS):
        return "Wrong subsite"

    # hub-ish path
    if any(x in path for x in HUB_PATH_HINTS):
        for pat in TOO_SPECIFIC_PATTERNS:
            if re.search(pat, path):
                return "Correct-ish but too specific"
        return "Correct hub"

    # weaker academic fallback
    if "academ" in path or "catalog" in path or "bulletin" in path:
        return "Correct-ish but too specific"

    return "Wrong subsite"


# ============================================================
# HTML parse + main-content extraction
# ============================================================
@dataclass
class ParsedPage:
    url: str
    visible_text: str
    main_text: str
    anchor_texts: List[str]
    anchor_attr_texts: List[str]
    links: List[Tuple[str, str]]  # (anchor_text, abs_url)
    title_like: List[str]
    html_title: str
    h1: str
    corpus_any: str
    corpus_structured: str
    json_blob: str
    js_hint: int


def parse_html_to_parsedpage(html: str, page_url: str) -> ParsedPage:
    soup = BeautifulSoup(html, "html.parser")

    ttag = soup.find("title")
    html_title = normalize_unicode_text(ttag.get_text(" ", strip=True)) if ttag else ""

    h1_tag = soup.find("h1")
    h1 = normalize_unicode_text(h1_tag.get_text(" ", strip=True)) if h1_tag else ""

    # --- Embedded JSON support (v12 B): keep machine-readable program lists ---
    # Many modern hubs embed program lists as JSON (ld+json, app/json, Next/Nuxt payloads).
    # We extract likely-JSON script contents into `json_blob` and then remove scripts so visible text stays clean.
    json_chunks: List[str] = []
    for sc in soup.find_all("script"):
        typ = (sc.get("type") or "").strip().lower()
        sid = (sc.get("id") or "").strip().lower()

        is_json_type = ("json" in typ)  # includes application/ld+json, application/json, etc.
        is_framework_payload = sid in ("__next_data__", "__nuxt__")

        if not (is_json_type or is_framework_payload):
            continue

        raw = sc.string if sc.string is not None else sc.get_text(" ", strip=True)
        raw = raw.strip() if raw else ""
        if not raw:
            continue

        raw_norm = normalize_unicode_text(raw)
        if not raw_norm:
            continue

        # Keep only content that looks plausibly JSON-like to avoid pulling in inline JS.
        lstripped = raw_norm.lstrip()
        if not (
            lstripped.startswith("{")
            or lstripped.startswith("[")
            or '"@context"' in raw_norm
            or '"@type"' in raw_norm
            or '"@graph"' in raw_norm
            or '"itemListElement"' in raw_norm
        ):
            continue

        json_chunks.append(raw_norm)

    json_blob = normalize_unicode_text(" ".join(json_chunks))
    if len(json_blob) > MAX_JSON_BLOB_CHARS:
        json_blob = json_blob[:MAX_JSON_BLOB_CHARS]

    # Heuristic: if page looks like a JS-rendered app shell, mark it so scoring/candidate expansion can adapt
    raw_lower = (html or "").lower()
    js_hint = 1 if (
        "__next_data__" in raw_lower
        or "window.__nuxt__" in raw_lower
        or "data-reactroot" in raw_lower
        or "id=\"root\"" in raw_lower
        or "id=\"app\"" in raw_lower
        or "ng-version" in raw_lower
    ) else 0

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    links: List[Tuple[str, str]] = []
    anchor_texts: List[str] = []
    anchor_attr_texts: List[str] = []

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        abs_url = urljoin(page_url, href)
        abs_url, _ = urldefrag(abs_url)

        a_text = normalize_unicode_text(a.get_text(" ", strip=True))
        aria = normalize_unicode_text(a.get("aria-label") or "")
        title_attr = normalize_unicode_text(a.get("title") or "")

        if a_text:
            anchor_texts.append(a_text)
        if aria:
            anchor_attr_texts.append(aria)
        if title_attr:
            anchor_attr_texts.append(title_attr)

        links.append((a_text, abs_url))

    visible_text = normalize_unicode_text(soup.get_text(separator=" ", strip=True))

    main_node = soup.find("main") or soup.find(id=re.compile(r"content|main", re.I)) or soup.find("article")
    if main_node:
        main_text = normalize_unicode_text(main_node.get_text(separator=" ", strip=True))
    else:
        soup2 = BeautifulSoup(str(soup), "html.parser")
        for tag in soup2(["header", "footer", "nav"]):
            tag.decompose()
        main_text = normalize_unicode_text(soup2.get_text(separator=" ", strip=True))

    # Title-like candidates should be pulled from "main-ish" content when possible.
    # This reduces nav/news/social noise before later postprocessing.
    title_like: List[str] = []
    title_scope = main_node
    if title_scope is None:
        soup_scope = BeautifulSoup(str(soup), "html.parser")
        for tag in soup_scope(["header", "footer", "nav", "aside"]):
            tag.decompose()
        title_scope = soup_scope

    for tag in title_scope.find_all(["h1", "h2", "h3", "li"]):
        t = normalize_unicode_text(tag.get_text(" ", strip=True))
        if t:
            title_like.append(t)

    corpus_any = normalize_unicode_text(
        " ".join(
            x
            for x in [visible_text, json_blob, " ".join(anchor_texts), " ".join(anchor_attr_texts)]
            if x
        )
    )
    corpus_structured = normalize_unicode_text(
        " ".join(
            x
            for x in [json_blob, " ".join(anchor_texts), " ".join(anchor_attr_texts), " ".join(title_like)]
            if x
        )
    )

    return ParsedPage(
        url=page_url,
        visible_text=visible_text,
        main_text=main_text,
        anchor_texts=anchor_texts,
        anchor_attr_texts=anchor_attr_texts,
        links=links,
        title_like=title_like,
        html_title=html_title,
        h1=h1,
        corpus_any=corpus_any,
        corpus_structured=corpus_structured,
        json_blob=json_blob,
        js_hint=js_hint,
    )


def is_soft_404(parsed: ParsedPage) -> bool:
    blob = normalize_unicode_text(" ".join([parsed.html_title, parsed.h1] + parsed.title_like[:5]))
    if SOFT_404_TEXT.search(blob):
        return True
    if len(parsed.main_text) < 600 and SOFT_404_TEXT.search(parsed.visible_text):
        return True
    if len(parsed.main_text) < 250 and len(parsed.anchor_texts) < 5:
        return True
    return False


def page_is_thin(parsed: ParsedPage) -> bool:
    return (len(parsed.visible_text) < MIN_VISIBLE_TEXT_LEN) and (len(parsed.main_text) < MIN_MAIN_TEXT_LEN)


# ============================================================
# Sitemap parsing
# ============================================================
def _xml_text(el: Optional[ET.Element]) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def fetch_sitemap_urls(base_url: str, session: requests.Session, base_netloc: str) -> List[str]:
    start = urljoin(base_url, "/sitemap.xml")
    to_fetch = [start]
    fetched = 0
    urls: List[str] = []
    seen_fetch: Set[str] = set()
    seen_urls: Set[str] = set()

    while to_fetch and fetched < SITEMAP_MAX_FETCHES and len(urls) < SITEMAP_MAX_URLS:
        sm_url = to_fetch.pop(0)
        if sm_url in seen_fetch:
            continue
        seen_fetch.add(sm_url)
        fetched += 1

        try:
            xml_txt = fetch_text_cached(sm_url, session=session)
            root = ET.fromstring(xml_txt.encode("utf-8", errors="ignore"))
        except Exception:
            continue

        root_tag = _strip_ns(root.tag).lower()

        if root_tag == "sitemapindex":
            for el in root.findall(".//"):
                if _strip_ns(el.tag).lower() == "loc":
                    loc = _xml_text(el)
                    if loc and same_domain(loc, base_netloc):
                        to_fetch.append(loc)
            continue

        if root_tag == "urlset":
            for el in root.findall(".//"):
                if _strip_ns(el.tag).lower() == "loc":
                    loc = _xml_text(el)
                    if not loc:
                        continue
                    if not same_domain(loc, base_netloc):
                        continue
                    loc, _ = urldefrag(loc)
                    if loc not in seen_urls:
                        seen_urls.add(loc)
                        urls.append(loc)
                        if len(urls) >= SITEMAP_MAX_URLS:
                            break

    return urls


def sitemap_candidate_urls(all_urls: List[str]) -> List[str]:
    cands: List[str] = []
    for u in all_urls:
        ul = u.lower()

        # hard skips
        if IRRELEVANT_URL_PENALTY.search(ul) or ADMISSIONS_URL_PENALTY.search(ul) or PROFILE_URL_PENALTY.search(ul):
            continue
        if ARCHIVE_URL_PENALTY.search(ul):
            continue
        if "graduate" in ul or "grad" in ul:
            continue

        if any(p.search(ul) for p in SITEMAP_PROGRAM_URL_PATTERNS):
            cands.append(u)

    out: List[str] = []
    seen: Set[str] = set()
    for u in cands:
        if u not in seen:
            seen.add(u)
            out.append(u)
        if len(out) >= SITEMAP_CANDIDATE_CAP:
            break

    return out


# ============================================================
# Hubness: program-listing-only link counts
# ============================================================
def is_listing_link(url: str) -> bool:
    path = (urlparse(url).path or "")
    return any(p.search(path) for p in LISTING_LINK_PATTERNS)


def hubness_signature(parsed: ParsedPage, base_netloc: str) -> Tuple[int, Dict[str, int]]:
    listing_links = 0
    slug_set: Set[str] = set()
    distinct_anchor: Set[str] = set()

    for a_text, u in parsed.links:
        if not same_domain(u, base_netloc):
            continue
        if not is_listing_link(u):
            continue

        listing_links += 1

        t = (a_text or "").strip()
        if len(t) >= 6:
            distinct_anchor.add(t.lower())

        path = (urlparse(u).path or "").strip("/").lower()
        segs = [s for s in path.split("/") if s]
        if segs:
            slug_set.add(segs[-1])

    # weak secondary signal
    li_like = sum(1 for t in parsed.title_like if len(t) >= 6)

    sig = 0
    if listing_links >= 12:
        sig += 1
    if listing_links >= 25:
        sig += 1
    if len(slug_set) >= 8:
        sig += 1
    if len(slug_set) >= 16:
        sig += 1
    if len(distinct_anchor) >= 18:
        sig += 1
    if li_like >= 40 and (listing_links >= 10):
        sig += 1

    counts = {
        "listing_links": listing_links,
        "slug_diversity": len(slug_set),
        "distinct_anchor": len(distinct_anchor),
        "li_like": li_like,
    }
    return sig, counts


# ============================================================
# Scoring
# ============================================================
@dataclass
class PageSignals:
    url: str
    tier: str
    soft404: int
    thin: int

    canonical_hub: int
    hub_sig: int
    hub_counts: Dict[str, int]

    struct_hits: List[str]
    control_unique_structured: int

    year_hit: int
    undergrad_title_boost: int

    grad_penalty: int
    profile_penalty: int
    irrelevant_penalty: int
    admissions_penalty: int
    archive_penalty: int
    awards_penalty: int
    compliance_penalty: int

    subsite_like: int  # configurable penalty via knob
    hard_subsite_block: int  # auto-loss for known traps
    too_specific: int  # explicit penalty separate from generic detail penalty
    js_hub_hint: int


def page_tier(url: str) -> str:
    u = url.lower()
    path = urlparse(url).path or "/"
    if path == "/" or path == "":
        return "homepage"
    if is_inventoryish_url(url):
        return "inventory"
    if "bulletin" in u:
        return "bulletin"
    if any(h in u for h in ("catalog", "catalogue", "college-catalog", "course-catalog")):
        return "catalog"
    return "other"


def compute_signals(url: str, parsed: ParsedPage, base_netloc: str) -> PageSignals:
    soft404 = 1 if is_soft_404(parsed) else 0
    thin = 1 if page_is_thin(parsed) else 0

    control_unique_structured = sum(1 for pat in CONTROL_PATS.values() if pat.search(parsed.corpus_structured))
    struct_hits = [k for k, pat in STRUCT_PATS.items() if pat.search(parsed.corpus_any)]

    title_blob = normalize_unicode_text(" ".join([parsed.html_title, parsed.h1]))
    year_hit = 1 if (YEAR_REGEX.search(url) or YEAR_REGEX.search(title_blob)) else 0
    undergrad_title_boost = 1 if UNDERGRAD_TITLE_BOOST.search(title_blob) else 0

    u_low = url.lower()
    grad_penalty = 1 if (GRAD_URL_PENALTY.search(u_low) or re.search(r"\bgraduate\b", title_blob, re.I)) else 0
    profile_penalty = 1 if (PROFILE_URL_PENALTY.search(u_low) or re.search(r"\bfaculty directory\b", title_blob, re.I)) else 0
    irrelevant_penalty = 1 if IRRELEVANT_URL_PENALTY.search(u_low) else 0
    admissions_penalty = 1 if ADMISSIONS_URL_PENALTY.search(u_low) else 0
    archive_penalty = 1 if ARCHIVE_URL_PENALTY.search(u_low) else 0
    awards_penalty = 1 if AWARDS_URL_PENALTY.search(u_low) else 0
    compliance_penalty = 1 if COMPLIANCE_URL_PENALTY.search(u_low) else 0

    hub_sig, hub_counts = hubness_signature(parsed, base_netloc=base_netloc)
    canonical_hub = 1 if is_canonical_hub_url(url) else 0

    subsite_like = 1 if is_subsite_like(url, base_netloc=base_netloc) else 0
    hard_subsite_block = 1 if is_hard_subsite_block(url, base_netloc=base_netloc) else 0
    too_specific = 1 if (is_too_specific_url(url) and not is_canonical_hub_url(url)) else 0

    js_hub_hint = 1 if (getattr(parsed, "js_hint", 0) and (len(getattr(parsed, "json_blob", "")) >= 200)) else 0
    return PageSignals(
        url=url,
        tier=page_tier(url),
        soft404=soft404,
        thin=thin,
        canonical_hub=canonical_hub,
        hub_sig=hub_sig,
        hub_counts=hub_counts,
        struct_hits=struct_hits,
        control_unique_structured=control_unique_structured,
        year_hit=year_hit,
        undergrad_title_boost=undergrad_title_boost,
        grad_penalty=grad_penalty,
        profile_penalty=profile_penalty,
        irrelevant_penalty=irrelevant_penalty,
        admissions_penalty=admissions_penalty,
        archive_penalty=archive_penalty,
        awards_penalty=awards_penalty,
        compliance_penalty=compliance_penalty,
        subsite_like=subsite_like,
        hard_subsite_block=hard_subsite_block,
        too_specific=too_specific,
        js_hub_hint=js_hub_hint,
    )


def canonical_bonus_allowed(sig: PageSignals) -> bool:
    if sig.soft404:
        return False
    if sig.thin and sig.hub_sig == 0 and len(sig.struct_hits) < 2:
        return False
    return True


def score_inventory(sig: PageSignals, subsite_penalty: int) -> int:
    if sig.soft404:
        return -10_000
    
    # hard subsite blocks are automatic losses
    if getattr(sig, "hard_subsite_block", 0):
        return -10_000    

    score = 0

    # tier base
    if sig.tier == "inventory":
        score += 240
    elif sig.tier == "catalog":
        score += 130
    elif sig.tier == "bulletin":
        score += 85
    else:
        score += 40

    # hubness + structured hits
    score += 85 * sig.hub_sig
    score += 10 * len(sig.struct_hits)
    score += 4 * sig.control_unique_structured

    # canonical hub bonus (gated)
    if sig.canonical_hub and canonical_bonus_allowed(sig):
        score += 160

    # undergrad title boost
    if sig.undergrad_title_boost:
        score += 35

    # year boost only for catalog/bulletin pages
    if sig.year_hit and sig.tier in ("catalog", "bulletin") and is_catalogish_url(sig.url):
        score += 70

    # penalties (URL/title/H1)
    if sig.profile_penalty:
        score -= 260
    if sig.grad_penalty:
        score -= 170
    if sig.irrelevant_penalty:
        score -= 260
    if sig.admissions_penalty:
        score -= 160
    if sig.archive_penalty:
        score -= 220
    if sig.awards_penalty:
        score -= 200
    if sig.compliance_penalty:
        score -= 150

    # configurable subsite penalty
    if sig.subsite_like:
        score -= int(subsite_penalty)

    # explicit penalty for "too specific" department/program pages
    if getattr(sig, "too_specific", 0):
        score -= int(TOO_SPECIFIC_PENALTY)    

    # broad detail penalty so hubs win
    if looks_like_detail_url(sig.url):
        if sig.hub_sig <= 1:
            score -= 220
        elif sig.hub_sig == 2:
            score -= 90
        else:
            score -= 25

    # thin penalty (C2: JS hubs can be thin app shells; barely penalize)
    if sig.thin and sig.hub_sig == 0:
        if getattr(sig, "js_hub_hint", 0) and is_inventoryish_url(sig.url):
            score -= 20
        else:
            score -= 180

    return score
# ============================================================
# Candidate generation
# ============================================================

def looks_like_js_hub(parsed: ParsedPage, url: str) -> bool:
    """Detect JS-rendered program hubs early so we can expand via sitemap sooner."""
    if not page_is_thin(parsed):
        return False

    u = (url or "").lower()
    host = (urlparse(url).netloc or "").lower()
    inventoryish = is_inventoryish_url(url) or (host.startswith("majors.") or host.startswith("programs."))
    if not inventoryish and "find-your-program" not in u:
        return False

    # Typical app shells have few usable anchors and often embed data in JSON
    if len(parsed.links) > 60:
        return False
    if getattr(parsed, "js_hint", 0):
        return True
    if len(getattr(parsed, "json_blob", "")) >= 200:
        return True
    return False



def url_prior(url: str, base_netloc: str, subsite_penalty: int) -> int:
    u = url.lower()
    s = 0

    if any(h in u for h in INVENTORY_URL_HINTS):
        s += 200
    if any(k in u for k in ("academics", "academic", "program", "major", "minor", "depart", "areas-of-study", "fields-of-study")):
        s += 90
    if any(k in u for k in ("catalog", "catalogue")):
        s += 35
    if "bulletin" in u:
        s -= 20

    if IRRELEVANT_URL_PENALTY.search(u):
        s -= 260
    if ADMISSIONS_URL_PENALTY.search(u):
        s -= 160
    if PROFILE_URL_PENALTY.search(u):
        s -= 320
    if ARCHIVE_URL_PENALTY.search(u):
        s -= 140
    if COMPLIANCE_URL_PENALTY.search(u):
        s -= 90
    if AWARDS_URL_PENALTY.search(u):
        s -= 160
    if GRAD_URL_PENALTY.search(u):
        s -= 180

    # hard subsite blocks should be avoided aggressively
    if is_hard_subsite_block(url, base_netloc=base_netloc):
        s -= 10_000
    if is_subsite_like(url, base_netloc=base_netloc):
        s -= int(subsite_penalty)

    if is_canonical_hub_url(url):
        s += 160

    return s


def prioritize_links(links: List[Tuple[str, str]], base_netloc: str, subsite_penalty: int) -> List[Tuple[str, str]]:
    def rank(item: Tuple[str, str]) -> int:
        a_text, u = item
        if not same_domain(u, base_netloc):
            return 10_000

        s = url_prior(u, base_netloc=base_netloc, subsite_penalty=subsite_penalty)

        at = (a_text or "").lower()
        if re.search(r"\b(majors|minors|programs|departments|areas|fields|undergraduate)\b", at):
            s += 45
        if re.search(r"\b(admissions|apply|visit)\b", at):
            s -= 90
        if re.search(r"\b(faculty|staff|directory)\b", at):
            s -= 140

        return -s

    return sorted(links, key=rank)


# ============================================================
# Candidate structure
# ============================================================
@dataclass
class CandidatePage:
    url: str
    score: int
    sig: PageSignals
    parsed: ParsedPage


def get_parsed_page(url: str, session: requests.Session) -> ParsedPage:
    html = fetch_text_cached(url, session=session)
    return parse_html_to_parsedpage(html, page_url=url)


def fetch_and_score(url: str, session: requests.Session, base_netloc: str, subsite_penalty: int) -> Optional[CandidatePage]:
    try:
        parsed = get_parsed_page(url, session=session)
        sig = compute_signals(url, parsed, base_netloc=base_netloc)
        sc = score_inventory(sig, subsite_penalty=subsite_penalty)
        return CandidatePage(url=url, score=sc, sig=sig, parsed=parsed)
    except Exception:
        return None
    finally:
        time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)


# ============================================================
# Parallel batch fetch+score helper
# ============================================================
def fetch_and_score_many(
    urls: List[str],
    base_netloc: str,
    subsite_penalty: int,
    workers: int,
) -> List[CandidatePage]:
    """Fetch+score a batch of URLs concurrently (one Session per thread)."""
    # de-dupe while preserving order
    seen: Set[str] = set()
    uniq: List[str] = []
    for u in urls:
        if u and u not in seen:
            seen.add(u)
            uniq.append(u)

    if workers <= 1:
        out: List[CandidatePage] = []
        s = requests.Session()
        try:
            for u in uniq:
                cand = fetch_and_score(u, session=s, base_netloc=base_netloc, subsite_penalty=subsite_penalty)
                if cand is not None:
                    out.append(cand)
        finally:
            try:
                s.close()
            except Exception:
                pass
        return out

    out: List[CandidatePage] = []

    def _task(u: str) -> Optional[CandidatePage]:
        # IMPORTANT: call get_thread_session() inside the worker thread
        return fetch_and_score(
            u,
            session=get_thread_session(),
            base_netloc=base_netloc,
            subsite_penalty=subsite_penalty,
        )

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_task, u) for u in uniq]
        for fut in as_completed(futs):
            cand = fut.result()
            if cand is not None:
                out.append(cand)
    return out


def push_topk(topk: List[Tuple[int, int, CandidatePage]], cand: CandidatePage, k: int) -> None:
    if len(topk) < k:
        heapq.heappush(topk, (cand.score, len(topk), cand))
        return
    if cand.score > topk[0][0]:
        heapq.heapreplace(topk, (cand.score, topk[0][1], cand))


def topk_sorted(topk: List[Tuple[int, int, CandidatePage]]) -> List[CandidatePage]:
    """Return candidates sorted by score (desc), with an optional canonical-hub tie-break."""
    cands = [t[2] for t in sorted(topk, key=lambda x: x[0], reverse=True)]

    # C: Prefer canonical hubs more aggressively when tie-ish.
    if cands:
        best_score = cands[0].score

        canonical_best: Optional[CandidatePage] = None
        for c in cands:
            if c.sig.canonical_hub and (best_score - c.score) <= CANONICAL_TIE_THRESHOLD:
                if canonical_best is None or c.score > canonical_best.score:
                    canonical_best = c

        if canonical_best is not None and canonical_best is not cands[0]:
            cands = [canonical_best] + [x for x in cands if x is not canonical_best]

    return cands


# ============================================================
# Candidate generation
# ============================================================
def build_year_candidates(base_url: str) -> List[str]:
    out: List[str] = []
    for y in YEAR_TOKENS:
        for tpl in YEAR_PATH_TEMPLATES:
            out.append(urljoin(base_url, tpl.format(y=y)))

    seen: Set[str] = set()
    uniq: List[str] = []
    for u in out:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def build_subdomain_roots(base_url: str) -> List[str]:
    netloc = urlparse(base_url).netloc
    rd = root_domain(netloc)
    if "." not in rd:
        return []
    return [
        f"https://majors.{rd}/",
        f"https://programs.{rd}/",
        f"https://catalog.{rd}/",
        f"https://bulletin.{rd}/",
    ]


# ============================================================
# Program title extraction
# ============================================================
def norm_title_key(s: str) -> str:
    s = normalize_unicode_text(s).lower()
    s = re.sub(r"[^\w\s&-]", "", s)
    s = s.replace("&", " and ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_title_key_loose(s: str) -> str:
    """Looser normalization for concordance (handles '&' vs 'and', generic suffix words, etc.)."""
    s = normalize_unicode_text(s).lower()
    s = s.replace("&", " and ")
    # Drop very common generic words that frequently cause false non-matches
    s = re.sub(r"\b(program|department|major|minor|concentration|certificate|track|option)\b", " ", s)
    s = re.sub(r"\b(the|of|in|for|and|to|a|an)\b", " ", s)
    s = re.sub(r"[^\w\s-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ============================================================
# Synonym mapping: used ONLY for partial concordance + change tracking
# ============================================================

# Keep this intentionally conservative: we want to merge obvious equivalences without
# over-normalizing distinct programs.
# NOTE: applied ONLY in partial concordance scoring / alignment.
SYNONYM_REPLACEMENTS: List[Tuple[re.Pattern, str]] = [
    # ampersand handled earlier too, but keep for safety
    (re.compile(r"&", re.I), " and "),

    # common short forms
    (re.compile(r"\bdept\.?\b", re.I), " department "),
    (re.compile(r"\bprog\.?\b", re.I), " program "),

    # african-american variants
    (re.compile(r"\bafrican\s*[-–—]\s*american\b", re.I), " african american "),
    (re.compile(r"\bafro\s*[-–—]?\s*american\b", re.I), " african american "),

    # black studies / africana common equivalences (very conservative)
    (re.compile(r"\bblack\s+studies\b", re.I), " africana studies "),

    # gender/women's apostrophe variants
    (re.compile(r"\bwomen['’]s\b", re.I), " women "),
    (re.compile(r"\bgender\s+and\s+sexuality\b", re.I), " gender sexuality "),

    # common misspelling seen in some sources
    (re.compile(r"\bcarribbean\b", re.I), " caribbean "),
]


def apply_synonym_map(s: str) -> str:
    """Apply conservative synonym replacements.

    This is ONLY used for partial concordance scoring / alignment and does not affect
    strict or loose exact concordance columns.
    """
    s = normalize_unicode_text(s)
    if not s:
        return ""
    out = s
    for rx, repl in SYNONYM_REPLACEMENTS:
        out = rx.sub(repl, out)
    out = normalize_unicode_text(out)
    return out


def _content_tokens(s: str) -> Set[str]:
    """Tokenize a title into content-bearing tokens for partial matching.

    IMPORTANT: synonym mapping is applied by the caller (partial-only). This function
    assumes it is receiving the already-preprocessed string.
    """
    s = norm_title_key_loose(s)
    if not s:
        return set()
    toks = [t for t in re.split(r"[\s_-]+", s) if t]
    toks = [t for t in toks if len(t) >= 3]  # remove tiny junk tokens
    return set(toks)


def _overlap_coeff(a: Set[str], b: Set[str]) -> float:
    """|A∩B| / min(|A|,|B|)"""
    if not a or not b:
        return 0.0
    inter = len(a.intersection(b))
    denom = float(min(len(a), len(b)))
    return inter / denom if denom > 0 else 0.0


def best_partial_title_match(a_titles: List[str], b_titles: List[str], use_synonyms: bool = False) -> Tuple[float, str, str]:
    """Return best partial match score and the (a_title, b_title) pair.

    Score is max(token overlap coefficient, SequenceMatcher ratio) on loose-normalized strings.

    If `use_synonyms` is True, apply `apply_synonym_map()` BEFORE loose normalization.
    This is used ONLY for partial concordance + change tracking.
    """
    best_score = 0.0
    best_a = ""
    best_b = ""

    for a in (a_titles or []):
        a_raw = normalize_unicode_text(a)
        if not a_raw:
            continue
        a_pre = apply_synonym_map(a_raw) if use_synonyms else a_raw
        a_loose = norm_title_key_loose(a_pre)
        a_tok = _content_tokens(a_pre)

        for b in (b_titles or []):
            b_raw = normalize_unicode_text(b)
            if not b_raw:
                continue
            b_pre = apply_synonym_map(b_raw) if use_synonyms else b_raw
            b_loose = norm_title_key_loose(b_pre)
            b_tok = _content_tokens(b_pre)

            tok_score = _overlap_coeff(a_tok, b_tok)
            seq_score = difflib.SequenceMatcher(None, a_loose, b_loose).ratio() if (a_loose and b_loose) else 0.0
            score = max(tok_score, seq_score)

            if score > best_score:
                best_score = score
                best_a = a_raw
                best_b = b_raw

    return best_score, best_a, best_b


def partial_concordance(
    a_titles: List[str],
    b_titles: List[str],
    threshold: float = 0.80,
    use_synonyms: bool = False,
) -> Tuple[int, str]:
    """Binary + detail string for partial concordance between two title lists.

    If `use_synonyms` is True, apply synonym mapping ONLY during partial scoring.
    """
    score, a_best, b_best = best_partial_title_match(a_titles, b_titles, use_synonyms=use_synonyms)
    ok = int(score >= float(threshold))
    detail = ""
    if ok and a_best and b_best:
        detail = f'inst="{a_best}" ~ other="{b_best}" score={score:.2f}'
    return ok, detail


# ============================================================
# Change tracking (E): align program lists + report adds/losses
# ============================================================

def _pair_score(a: str, b: str, use_synonyms: bool = True) -> float:
    """Compute a pairwise similarity score for alignment."""
    a_raw = normalize_unicode_text(a)
    b_raw = normalize_unicode_text(b)
    if not a_raw or not b_raw:
        return 0.0

    a_pre = apply_synonym_map(a_raw) if use_synonyms else a_raw
    b_pre = apply_synonym_map(b_raw) if use_synonyms else b_raw

    a_loose = norm_title_key_loose(a_pre)
    b_loose = norm_title_key_loose(b_pre)

    a_tok = _content_tokens(a_pre)
    b_tok = _content_tokens(b_pre)

    tok_score = _overlap_coeff(a_tok, b_tok)
    seq_score = difflib.SequenceMatcher(None, a_loose, b_loose).ratio() if (a_loose and b_loose) else 0.0
    return float(max(tok_score, seq_score))


def align_title_lists(
    a_titles: List[str],
    b_titles: List[str],
    threshold: float = 0.80,
    use_synonyms: bool = True,
) -> Tuple[List[Tuple[str, str, float]], List[str], List[str]]:
    """Greedy 1:1 alignment between two title lists.

    Returns:
      matches: list of (a_title, b_title, score) with score>=threshold
      a_only: titles only in A (unmatched)
      b_only: titles only in B (unmatched)

    IMPORTANT: synonym mapping is applied ONLY if `use_synonyms` is True.
    """
    a_list = [normalize_unicode_text(x) for x in (a_titles or []) if normalize_unicode_text(x)]
    b_list = [normalize_unicode_text(x) for x in (b_titles or []) if normalize_unicode_text(x)]

    if not a_list and not b_list:
        return [], [], []

    # score all pairs
    pairs: List[Tuple[float, int, int]] = []
    for i, a in enumerate(a_list):
        for j, b in enumerate(b_list):
            sc = _pair_score(a, b, use_synonyms=use_synonyms)
            if sc >= float(threshold):
                pairs.append((sc, i, j))

    pairs.sort(key=lambda x: x[0], reverse=True)

    used_a: Set[int] = set()
    used_b: Set[int] = set()
    matches: List[Tuple[str, str, float]] = []

    for sc, i, j in pairs:
        if i in used_a or j in used_b:
            continue
        used_a.add(i)
        used_b.add(j)
        matches.append((a_list[i], b_list[j], float(sc)))

    a_only = [a for i, a in enumerate(a_list) if i not in used_a]
    b_only = [b for j, b in enumerate(b_list) if j not in used_b]

    return matches, a_only, b_only


def format_alignment_pairs(matches: List[Tuple[str, str, float]]) -> str:
    if not matches:
        return ""
    parts = []
    for a, b, sc in matches:
        parts.append(f'a="{a}" ~ b="{b}" score={sc:.2f}')
    return "|".join(parts)

def _split_program_name_field(s: str) -> List[str]:
    """Split a program-name field into candidate names (handles pipes/newlines/semicolons)."""
    s = normalize_unicode_text(s or "")
    if not s:
        return []
    parts: List[str] = [s]
    for sep in ["|", ";", "\n", "\r"]:
        tmp: List[str] = []
        for p in parts:
            if sep in p:
                tmp.extend([x.strip() for x in p.split(sep) if x.strip()])
            else:
                tmp.append(p)
        parts = tmp

    # Only split on commas when it looks like a list (avoid breaking names like
    # "African, Black & Caribbean Studies")
    tmp2: List[str] = []
    for p in parts:
        if p.count(",") >= 2:
            tmp2.extend([x.strip() for x in p.split(",") if x.strip()])
        else:
            tmp2.append(p)
    return [normalize_unicode_text(x) for x in tmp2 if x]


def _title_key_set(titles: List[str]) -> Set[str]:
    """Normalized key set for exact-match concordance checks."""
    return {norm_title_key(t) for t in (titles or []) if norm_title_key(t)}


def _field_key_set(field_val: str) -> Set[str]:
    """Normalized key set from a scalar program-name field (e.g., 2013_program_name)."""
    return {norm_title_key(x) for x in _split_program_name_field(field_val) if norm_title_key(x)}


def _any_exact_concordance(a: Set[str], b: Set[str]) -> int:
    """1 if any exact normalized title matches exist between sets; else 0."""
    if not a or not b:
        return 0
    return int(bool(a.intersection(b)))

def looks_like_program_title(s: str, progtitle_strictness: int, context: str = "hub") -> bool:
    """
    Strictness guidance:
      1-2: permissive (still requires TARGET_ANY)
      3: current default behavior
      4-5: stricter (prefer explicit program-ish keywords; tighter filters)
    context: "hub" (default) or "follow" (detail)
    """
    s_norm = normalize_unicode_text(s)
    if len(s_norm) < 6 or len(s_norm) > 140:
        return False

    s_low = s_norm.lower()

    # D1: obvious junk rejects
    if URL_LIKE_TEXT.search(s_norm):
        return False
    if "toggle" in s_low:
        return False

    # always require the target token family (keeps extraction scoped)
    if TARGET_ANY_REGEX.search(s_norm) is None:
        return False

    if COURSE_CODE.match(s_norm):
        return False
    if COURSE_WORDS.search(s_norm):
        return False

    # D1: sentence-like / prose-ish rejects
    if ". " in s_norm and len(s_norm) >= 40:
        return False
    if ": " in s_norm and len(s_norm) >= 55:
        # allow short degree-style labels, reject long descriptive clauses
        if PROGRAM_KEYWORDS.search(s_norm) is None and not re.search(r"\b(B\.?A\.?|B\.?S\.?|BA|BS)\b", s_norm, re.I):
            return False
    if PROSE_VERBS.search(s_norm) and len(s_norm) >= 45:
        # follow/detail pages are stricter; hubs can still reject obvious prose
        if context == "follow" or progtitle_strictness >= 3:
            return False

    # D1: long multi-clause strings are usually prose or navigation
    if len(s_norm) > 90:
        comma_ct = s_norm.count(",")
        if comma_ct >= 2 and PROGRAM_KEYWORDS.search(s_norm) is None:
            return False

    # NOTE: negative-context words (news/events/etc.) are handled primarily in
    # clean_program_titles() where we can condition on program intent.
    # Keep only a very strict early reject.
    if TITLE_NEGATIVE_CONTEXT.search(s_norm) and progtitle_strictness >= 5:
        return False

    # D2: follow/detail pages must show stronger "program" intent
    if context == "follow":
        if PROGRAM_KEYWORDS.search(s_norm) is None and "studies" not in s_low:
            return False

    if PERSON_NAME_ONLY.match(s_norm):
        if progtitle_strictness >= 3 and not PROGRAM_KEYWORDS.search(s_norm):
            return False
        if progtitle_strictness >= 5:
            return False

    if progtitle_strictness >= 4:
        if PROGRAM_KEYWORDS.search(s_norm) is None and "studies" not in s_low:
            return False

    return True
def is_program_detailish_url(url: str) -> bool:
    """Tight allowlist for one-hop follow: only follow likely program/major detail pages."""
    path = (urlparse(url).path or "")
    if any(p.search(path) for p in LISTING_LINK_PATTERNS):
        return True
    # also allow common majors-programs hubs/details
    if re.search(r"/majors-programs/[^/]+", path, re.I):
        return True
    return False


def token_matches_from_text(txt: str) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for key, pat in TARGET_TOKEN_REGEX.items():
        out[key] = sorted(set(m.group(0) for m in pat.finditer(txt)))
    return out


# ============================================================
# PDF fallback (best-effort)
# ============================================================
def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    try:
        from io import BytesIO
        from pdfminer.high_level import extract_text  # type: ignore
        return normalize_unicode_text(extract_text(BytesIO(pdf_bytes)))
    except Exception:
        return ""


def find_yearish_major_pdf_links(parsed: ParsedPage) -> List[str]:
    pdfs: List[str] = []
    for a_text, u in parsed.links:
        ul = (u or "").lower()
        at = (a_text or "").lower()
        if not ul:
            continue
        if not PDF_LINK_REGEX.search(ul):
            continue
        if YEAR_REGEX.search(ul) or YEAR_REGEX.search(at) or re.search(r"\b(major|minor|program|undergrad)\b", at):
            pdfs.append(u)

    out: List[str] = []
    seen: Set[str] = set()
    for u in pdfs:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# ============================================================
# Aggregate extraction from inventory + alts (+ limited 1-hop)
# ============================================================
def aggregate_outputs(
    pages: List[CandidatePage],
    base_url: str,
    session: requests.Session,
    progtitle_strictness: int,
    workers: int,
) -> Tuple[
    int,
    Set[str],
    Dict[str, int],
    Dict[str, List[str]],
    List[str],
    List[str],
]:
    base_netloc = urlparse(base_url).netloc

    struct_hits_union: Set[str] = set()
    control_hit_cols = {k: 0 for k in CONTROL_TERMS}
    any_control_found = 0

    tokens_agg: Dict[str, Set[str]] = {k: set() for k in TARGET_TOKEN_REGEX.keys()}
    titles: Set[str] = set()

    pdf_hits: List[str] = []

    # from chosen pages
    for cand in pages:
        any_control_found = max(any_control_found, 1 if cand.sig.control_unique_structured > 0 else 0)
        struct_hits_union.update(cand.sig.struct_hits)

        for ck, pat in CONTROL_PATS.items():
            if pat.search(cand.parsed.corpus_structured):
                control_hit_cols[ck] = 1

        tm = token_matches_from_text(cand.parsed.corpus_any)
        for k, vals in tm.items():
            tokens_agg[k].update(vals)

        for txt in (cand.parsed.anchor_texts + cand.parsed.anchor_attr_texts + cand.parsed.title_like):
            if txt and looks_like_program_title(txt, progtitle_strictness=progtitle_strictness, context="hub"):
                titles.add(txt)

    # 1-hop follow
    follow_urls: List[str] = []
    for cand in pages:
        for a_text, u in cand.parsed.links:
            if not u or not same_domain(u, base_netloc):
                continue
            ul = u.lower()
            if IRRELEVANT_URL_PENALTY.search(ul) or ADMISSIONS_URL_PENALTY.search(ul) or PROFILE_URL_PENALTY.search(ul):
                continue
            if "graduate" in ul or "grad" in ul:
                continue
            # D3: follow only if anchor text looks like a title, OR URL looks like a program detail page
            if looks_like_program_title(a_text or "", progtitle_strictness=progtitle_strictness, context="hub") or is_program_detailish_url(u):
                follow_urls.append(u)

    # sitemap assist if best looks thin
    if pages and page_is_thin(pages[0].parsed):
        all_urls = fetch_sitemap_urls(base_url, session=session, base_netloc=base_netloc)
        follow_urls.extend(sitemap_candidate_urls(all_urls))

    seen_u: Set[str] = set()
    uniq_follow: List[str] = []
    for u in follow_urls:
        if u not in seen_u:
            seen_u.add(u)
            uniq_follow.append(u)
        if len(uniq_follow) >= ONE_HOP_MAX_LINKS_TOTAL:
            break

    # Fetch a limited number of follow URLs (optionally in parallel)
    follow_batch = uniq_follow[:ONE_HOP_MAX_FETCHES]

    def _safe_fetch_parse(u: str) -> Optional[ParsedPage]:
        try:
            # Use per-thread sessions when parallel
            s = get_thread_session() if workers and workers > 1 else session
            return get_parsed_page(u, session=s)
        except Exception:
            return None
        finally:
            # Keep some politeness throttling even when parallel
            time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

    if workers and workers > 1 and len(follow_batch) > 1:
        max_w = min(int(workers), 8, len(follow_batch))
        with ThreadPoolExecutor(max_workers=max_w) as ex:
            futs = [ex.submit(_safe_fetch_parse, u) for u in follow_batch]
            for fut in as_completed(futs):
                parsed = fut.result()
                if not parsed:
                    continue

                tm = token_matches_from_text(parsed.corpus_any)
                for k, vals in tm.items():
                    tokens_agg[k].update(vals)

                for txt in (parsed.anchor_texts + parsed.anchor_attr_texts + parsed.title_like):
                    if txt and looks_like_program_title(txt, progtitle_strictness=progtitle_strictness, context="follow"):
                        titles.add(txt)
    else:
        for u in follow_batch:
            parsed = _safe_fetch_parse(u)
            if not parsed:
                continue

            tm = token_matches_from_text(parsed.corpus_any)
            for k, vals in tm.items():
                tokens_agg[k].update(vals)

            for txt in (parsed.anchor_texts + parsed.anchor_attr_texts + parsed.title_like):
                if txt and looks_like_program_title(txt, progtitle_strictness=progtitle_strictness, context="follow"):
                    titles.add(txt)

    # PDF fallback (rare)
    if ENABLE_PDF_FALLBACK:
        pdf_fetches = 0
        for cand in pages:
            if pdf_fetches >= PDF_MAX_FETCHES_PER_INSTITUTION:
                break
            for pdf_url in find_yearish_major_pdf_links(cand.parsed):
                if pdf_fetches >= PDF_MAX_FETCHES_PER_INSTITUTION:
                    break
                try:
                    pdf_bytes = fetch_bytes_cached(pdf_url, session=session)
                    txt = extract_text_from_pdf_bytes(pdf_bytes)
                    if not txt:
                        continue
                    if re.search(r"\b(major|minor|program|undergraduate)\b", txt, re.I):
                        tm = token_matches_from_text(txt)
                        for k, vals in tm.items():
                            tokens_agg[k].update(vals)
                        pdf_hits.append(pdf_url)
                        pdf_fetches += 1
                except Exception:
                    continue

    # Postprocess titles: split breadcrumbs, salvage boilerplate, drop course/news/social noise,
    # and select the best variant per normalized key.
    titles_out = clean_program_titles(list(titles), progtitle_strictness=progtitle_strictness)
    tokens_out = {k: sorted(v) for k, v in tokens_agg.items()}

    return any_control_found, struct_hits_union, control_hit_cols, tokens_out, titles_out, pdf_hits


#
# ============================================================
# Discovery: find candidate pages
# ============================================================
def find_candidates_for_institution(
    base_url: str,
    session: requests.Session,
    subsite_penalty: int,
    workers: int,
) -> List[CandidatePage]:
    base_netloc = urlparse(base_url).netloc
    tried: Set[str] = set()
    topk: List[Tuple[int, int, CandidatePage]] = []

    sitemap_cache: Optional[List[str]] = None

    def load_sitemap_candidates() -> List[str]:
        nonlocal sitemap_cache
        if sitemap_cache is not None:
            return sitemap_cache
        all_urls = fetch_sitemap_urls(base_url, session=session, base_netloc=base_netloc)
        sitemap_cache = sitemap_candidate_urls(all_urls)
        return sitemap_cache

    def consider(u: str) -> Optional[CandidatePage]:
        if not u or u in tried:
            return None
        tried.add(u)
        cand = fetch_and_score(u, session=session, base_netloc=base_netloc, subsite_penalty=subsite_penalty)
        if cand is None:
            return None
        push_topk(topk, cand, k=TOP_K_ALTS)
        return cand

    # Phase 0
    consider(base_url)

    # ------------------------------------------------------------
    # Phase 1 (parallelizable): explicit paths + year/catalog + home links
    # ------------------------------------------------------------
    batch_urls: List[str] = []

    for p in INVENTORY_PATHS[:PHASE1_MAX_INVENTORY_CANDIDATES]:
        batch_urls.append(urljoin(base_url, p))

    batch_urls.extend(build_year_candidates(base_url)[:PHASE1_MAX_CATALOG_CANDIDATES])

    for p in CATALOG_PATHS[:PHASE1_MAX_CATALOG_CANDIDATES]:
        batch_urls.append(urljoin(base_url, p))

    # homepage links require parsing homepage once (sequential), but the fetched URLs can be scored in batch
    try:
        home = get_parsed_page(base_url, session=session)
        
        # If the homepage/programs page is a JS hub (thin app shell), expand candidates via sitemap immediately
        if looks_like_js_hub(home, base_url):
            try:
                batch_urls.extend(load_sitemap_candidates()[:80])
            except Exception:
                pass
        home_links = [(t, u) for (t, u) in home.links if same_domain(u, base_netloc)]
        home_links = prioritize_links(home_links, base_netloc, subsite_penalty=subsite_penalty)
        for _, u in home_links[:PHASE1_MAX_HOMEPAGE_LINKS]:
            batch_urls.append(u)
    except Exception:
        pass

    # score the batch in parallel (if workers>1)
    batch_cands = fetch_and_score_many(
        batch_urls,
        base_netloc=base_netloc,
        subsite_penalty=subsite_penalty,
        workers=workers,
    )
    for cand in batch_cands:
        tried.add(cand.url)
        push_topk(topk, cand, k=TOP_K_ALTS)

    # If ANY early candidate looks like a JS-rendered hub, expand via sitemap NOW
    # (even if the thin hub didn't win the scoring yet).
    try:
        if sitemap_cache is None:
            js_hub_seen = any(looks_like_js_hub(c.parsed, c.url) for c in batch_cands)
            if js_hub_seen:
                sm_urls = load_sitemap_candidates()[:120]
                sm_cands = fetch_and_score_many(
                    sm_urls,
                    base_netloc=base_netloc,
                    subsite_penalty=subsite_penalty,
                    workers=workers,
                )
                for cand in sm_cands:
                    tried.add(cand.url)
                    push_topk(topk, cand, k=TOP_K_ALTS)
    except Exception:
        pass

    # Optional: if best so far looks weak, pull some sitemap candidates (also batchable)
    if topk:
        best_so_far = max(topk, key=lambda x: x[0])[2]
        if (best_so_far.sig.thin or best_so_far.sig.hub_sig == 0) and is_inventoryish_url(best_so_far.url):
            try:
                sm_urls = load_sitemap_candidates()[:50]
                sm_cands = fetch_and_score_many(
                    sm_urls,
                    base_netloc=base_netloc,
                    subsite_penalty=subsite_penalty,
                    workers=workers,
                )
                for cand in sm_cands:
                    tried.add(cand.url)
                    push_topk(topk, cand, k=TOP_K_ALTS)
            except Exception:
                pass

    # Best-first crawl
    pq: List[Tuple[int, int, str]] = []
    seen: Set[str] = set()

    def pq_push(u: str, depth: int) -> None:
        if not u or u in seen:
            return
        if not same_domain(u, base_netloc):
            return
        seen.add(u)
        heapq.heappush(pq, (-url_prior(u, base_netloc=base_netloc, subsite_penalty=subsite_penalty), depth, u))

    pq_push(base_url, 0)
    fetched = 0
    while pq and fetched < PHASE2_BESTFIRST_MAX_PAGES:
        _, depth, u = heapq.heappop(pq)
        cand = consider(u)
        fetched += 1
        if depth >= PHASE2_MAX_DEPTH:
            continue
        try:
            parsed = cand.parsed if cand is not None else get_parsed_page(u, session=session)
            links = [(t, lu) for (t, lu) in parsed.links if same_domain(lu, base_netloc)]
            links = prioritize_links(links, base_netloc, subsite_penalty=subsite_penalty)
            for _, lu in links[:70]:
                pq_push(lu, depth + 1)
        except Exception:
            pass

    # Subdomain roots
    for root in build_subdomain_roots(base_url):
        c_root = consider(root)
        if not c_root:
            continue
        root_is_good = (c_root.sig.hub_sig >= 1) or (len(c_root.sig.struct_hits) >= 2) or (not c_root.sig.thin)
        if root_is_good and not c_root.sig.soft404:
            continue
        for p in ("/programs", "/majors", "/majors-minors", "/academics/programs", "/departments-and-programs"):
            consider(urljoin(root, p))

    return topk_sorted(topk)


# ============================================================
# Reason string
# ============================================================
def inventory_reason(c: CandidatePage) -> str:
    s = c.sig
    flags = []
    if s.grad_penalty:
        flags.append("grad")
    if s.profile_penalty:
        flags.append("profile")
    if s.irrelevant_penalty:
        flags.append("irrelevant")
    if s.admissions_penalty:
        flags.append("admissions")
    if s.archive_penalty:
        flags.append("archive")
    if s.awards_penalty:
        flags.append("awards")
    if s.compliance_penalty:
        flags.append("compliance")
    if s.year_hit:
        flags.append("yearHit")
    if s.thin:
        flags.append("thin")
    if s.soft404:
        flags.append("soft404")
    if s.subsite_like:
        flags.append("subsiteLike")
    if getattr(s, "hard_subsite_block", 0):
        flags.append("hardSubsiteBlock")
    if getattr(s, "too_specific", 0):
        flags.append("tooSpecific")

    return ";".join([
        f"score={c.score}",
        f"tier={s.tier}",
        f"hubSig={s.hub_sig}",
        f"listingLinks={s.hub_counts.get('listing_links', 0)}",
        f"slugDiv={s.hub_counts.get('slug_diversity', 0)}",
        f"structHits={len(s.struct_hits)}",
        f"ctrlStructured={s.control_unique_structured}",
        f"canonicalHub={s.canonical_hub}",
        ("flags=" + ",".join(flags)) if flags else "flags=",
    ])


# ============================================================
# CLI
# ============================================================
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--input", type=str, default=str(DEFAULT_INPUT_PATH))
    ap.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT_PATH))

    # If omitted: CLI defaults to full run. In notebooks (ipykernel), default to TEST_HEAD_N.
    # Use --head 0 explicitly for full run.
    ap.add_argument(
        "--head",
        type=int,
        default=None,
        help="Limit rows for testing. Omit for full run on CLI; in notebooks defaults to TEST_HEAD_N. Use 0 for full run.",
    )
    # Batching / checkpointing controls
    ap.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help="Institutions per batch (default: BATCH_SIZE). Ignored if --batches is set.",
    )
    ap.add_argument(
        "--batches",
        type=int,
        default=None,
        help="Force the run to use exactly N batches (overrides --batch-size).",
    )
    ap.add_argument(
        "--checkpoint-every",
        type=int,
        default=CHECKPOINT_EVERY_N,
        help="Flush buffered rows to the progress file every N institutions.",
    )
    ap.add_argument(
        "--compact-every",
        type=int,
        default=COMPACT_EVERY_N,
        help="Compact (dedupe) the progress file every N institutions (0 disables).",
    )

    ap.add_argument("--out-suffix", type=str, default="", help="Append to output filename stem")
    ap.add_argument("--subsite-penalty", type=int, default=80)
    ap.add_argument("--progtitle-strictness", type=int, default=3)

    # Parallelism (keep small to be polite)
    ap.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Parallel fetch workers for candidate scoring (1 = off). Try 4.",
    )

    ap.add_argument(
        "--debug-jsonlen",
        action="store_true",
        help="Debug: print json_blob length (chars) for the best candidate per school",
    )

    # IMPORTANT: ignore Jupyter/ipykernel injected args like --f=...
    args, _unknown = ap.parse_known_args(argv)

    # Resolve default head behavior
    if args.head is None:
        try:
            import sys as _sys
            in_notebook = ("ipykernel" in _sys.modules)
        except Exception:
            in_notebook = False
        args.head = TEST_HEAD_N if in_notebook else 0

    # sanity clamp
    if args.workers is None or args.workers < 1:
        args.workers = 1
    if args.workers > MAX_WORKERS:
        args.workers = MAX_WORKERS

    # sanity clamp for batching knobs
    if args.batch_size is None or int(args.batch_size) < 1:
        args.batch_size = int(BATCH_SIZE)
    if args.batches is not None and int(args.batches) < 1:
        args.batches = None
    if args.checkpoint_every is None or int(args.checkpoint_every) < 1:
        args.checkpoint_every = int(CHECKPOINT_EVERY_N)
    if args.compact_every is None or int(args.compact_every) < 0:
        args.compact_every = int(COMPACT_EVERY_N)

    return args


def apply_out_suffix(out_path: Path, suffix: str) -> Path:
    if not suffix:
        return out_path
    return out_path.with_name(out_path.stem + suffix + out_path.suffix)

def main() -> None:
    args = parse_args()

    input_path = Path(args.input) if args.input else DEFAULT_INPUT_PATH
    output_path = Path(args.output) if args.output else DEFAULT_OUTPUT_PATH

    # Preserve existing out-suffix behavior (if your script has it)
    if getattr(args, "out_suffix", ""):
        output_path = output_path.with_name(output_path.stem + str(args.out_suffix) + output_path.suffix)

    # Load data (preserve your existing dtype/NA choices)
    df = pd.read_csv(input_path, dtype=str, keep_default_na=False)

    # Prefer uniqid for resume/skip if present; fallback to unitid.
    UNIQID_COL = "uniqid"
    RESUME_KEY_COL = UNIQID_COL if UNIQID_COL in df.columns else UNITID_COL

    # Apply --head (already resolved in parse_args). 0 means full run.
    head_n = int(getattr(args, "head", 0) or 0)
    if head_n > 0:
        df = df.head(head_n).copy()

    # ---------------------------------------------
    # Batch + checkpoint/resume
    # ---------------------------------------------
    import csv
    import math

    final_output_path = Path(output_path)
    out_dir = final_output_path.parent
    out_stem = final_output_path.stem

    def batch_paths(batch_idx: int) -> Tuple[Path, Path]:
        progress = out_dir / f"{out_stem}__batch_{batch_idx:03d}__progress.csv"
        batch_out = out_dir / f"{out_stem}__batch_{batch_idx:03d}.csv"
        return progress, batch_out

    def load_completed_unitids(progress_path: Path) -> Set[str]:
        done: Set[str] = set()
        if not progress_path.exists():
            return done
        try:
            with progress_path.open("r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader)
                # Resume key: prefer uniqid if present in the progress file; else unitid.
                key_col = RESUME_KEY_COL if RESUME_KEY_COL in header else (UNITID_COL if UNITID_COL in header else "")
                if not key_col:
                    return done
                uix = header.index(key_col)
                for row in reader:
                    if not row or uix >= len(row):
                        continue
                    u = (row[uix] or "").strip()
                    if u:
                        done.add(u)
        except Exception:
            return done
        return done

    def append_rows(progress_path: Path, fieldnames: List[str], rows: List[dict]) -> None:
        if not rows:
            return
        is_new = not progress_path.exists()
        with progress_path.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            if is_new:
                w.writeheader()
            for r in rows:
                w.writerow(r)

    def compact_progress(progress_path: Path, fieldnames: List[str]) -> None:
        if not progress_path.exists():
            return
        try:
            dfp = pd.read_csv(progress_path, dtype=str, keep_default_na=False)
        except Exception:
            return
        if dfp.empty:
            return
        key_col = RESUME_KEY_COL if RESUME_KEY_COL in dfp.columns else (UNITID_COL if UNITID_COL in dfp.columns else "")
        if key_col:
            dfp = dfp.drop_duplicates(subset=[key_col], keep="last")
        cols = [c for c in fieldnames if c in dfp.columns] + [c for c in dfp.columns if c not in fieldnames]
        dfp = dfp[cols]
        dfp.to_csv(progress_path, index=False)

    def reset_batch_state() -> None:
        # reset per-institution tallies
        try:
            reset_status_tally()
        except Exception:
            pass
        try:
            reset_cv_status_tally()
        except Exception:
            pass
        # drop large CV blob cache between batches (prevents long-run drift)
        try:
            global _COLLEGEVINE_STATIC_CACHE, _COLLEGEVINE_STATIC_CACHE_URL
            _COLLEGEVINE_STATIC_CACHE = None
            _COLLEGEVINE_STATIC_CACHE_URL = ""
        except Exception:
            pass

    def concat_batches(batch_files: List[Path], out_path: Path) -> None:
        wrote_header = False
        with out_path.open("w", newline="", encoding="utf-8") as out_f:
            out_w = None
            for p in batch_files:
                if not p.exists():
                    continue
                with p.open("r", newline="", encoding="utf-8") as in_f:
                    r = csv.reader(in_f)
                    try:
                        header = next(r)
                    except StopIteration:
                        continue
                    if not wrote_header:
                        out_w = csv.writer(out_f)
                        out_w.writerow(header)
                        wrote_header = True
                    for row in r:
                        if row:
                            out_w.writerow(row)

    # START_FRESH_RUN cleanup
    if START_FRESH_RUN:
        try:
            if final_output_path.exists():
                final_output_path.unlink()
        except Exception:
            pass
        for p in out_dir.glob(f"{out_stem}__batch_*.csv"):
            try:
                p.unlink()
            except Exception:
                pass
        for p in out_dir.glob(f"{out_stem}__batch_*__progress.csv"):
            try:
                p.unlink()
            except Exception:
                pass

    total_rows = len(df)
    if total_rows == 0:
        print("[INFO] No rows to process.")
        return

    # Resolve workers early so we can include it in the batch config print
    workers = int(getattr(args, "workers", DEFAULT_WORKERS) or DEFAULT_WORKERS)
    workers = max(1, min(workers, MAX_WORKERS))

    # Resolve batching configuration (CLI overrides globals)
    if getattr(args, "batches", None):
        num_batches = int(args.batches)
        batch_size = int(math.ceil(total_rows / float(num_batches)))
    else:
        batch_size = int(getattr(args, "batch_size", BATCH_SIZE) or BATCH_SIZE)
        num_batches = int(math.ceil(total_rows / float(batch_size)))

    checkpoint_every = int(getattr(args, "checkpoint_every", CHECKPOINT_EVERY_N) or CHECKPOINT_EVERY_N)
    compact_every = int(getattr(args, "compact_every", COMPACT_EVERY_N) or COMPACT_EVERY_N)

    print(
        f"[INFO] total_rows={total_rows:,} | head={head_n} | batch_size={batch_size} | batches={num_batches} | "
        f"workers={workers} | checkpoint_every={checkpoint_every} | compact_every={compact_every}"
    )

    # -------------------------------------------------
    # Output schema
    # -------------------------------------------------
    # Prefer explicit OUTPUT_COLUMNS if provided; otherwise build a stable schema:
    #   input columns + expected computed columns.
    fieldnames = list(globals().get("OUTPUT_COLUMNS") or [])

    # Expected computed columns from this v15simple crawl
    EXPECTED_EXTRA_COLUMNS: List[str] = [
        "best_guess_inventory_url",
        "best_guess_inventory_reason",
        "url_tag",
        "alt_candidate_urls",
        "any_control_found",
        "struct_hits_union",
        ">0_struct_hits_found",
        "program_title_count",
        "program_titles_found",
        "pdf_hits",
        "total_controls_found",
        "controls_sufficiency",
        "status",
        "error_detail",
        # CollegeVine outputs
        "college_vine_site",
        "college_vine_url",
        "college_vine_ctrl_status",
        "college_vine_program_title_count",
        "college_vine_program_titles_found",
    ]

    # Token-match columns (derived from TARGET_TOKEN_REGEX)
    try:
        for _k in list(TARGET_TOKEN_REGEX.keys()):
            EXPECTED_EXTRA_COLUMNS.append(f"{_k}_matches")
    except Exception:
        pass

    # Control hit columns (derived from CONTROL_TERMS)
    try:
        for _ck in list(CONTROL_TERMS.keys()):
            EXPECTED_EXTRA_COLUMNS.append(str(_ck))
    except Exception:
        pass

    def _dedup_keep_order(seq: List[str]) -> List[str]:
        seen: Set[str] = set()
        out: List[str] = []
        for x in seq:
            if x in seen:
                continue
            seen.add(x)
            out.append(x)
        return out

    if not fieldnames:
        # Build schema from input columns + expected extras.
        # df is already loaded above.
        fieldnames = _dedup_keep_order(list(df.columns) + EXPECTED_EXTRA_COLUMNS)
    else:
        # Ensure OUTPUT_COLUMNS includes at least the computed extras we populate
        fieldnames = _dedup_keep_order(list(fieldnames) + EXPECTED_EXTRA_COLUMNS)

    batch_outputs: List[Path] = []

    # ---- batch loop ----
    for batch_idx in range(1, num_batches + 1):
        start = (batch_idx - 1) * batch_size
        end = min(batch_idx * batch_size, total_rows)

        progress_path, batch_out_path = batch_paths(batch_idx)
        print(f"[BATCH {batch_idx}/{num_batches}] rows {start}:{end} -> {progress_path.name}")

        reset_batch_state()

        completed: Set[str] = set()
        if not START_FRESH_RUN:
            completed = load_completed_unitids(progress_path)

        buf_rows: List[dict] = []
        completed_in_batch = 0

        df_batch = df.iloc[start:end]

        # Insert process_one_row helper exactly here
        def process_one_row(
            row: dict,
            *,
            workers: int,
            subsite_penalty: int,
            progtitle_strictness: int,
            debug_jsonlen: bool,
        ) -> dict:
            """Run the original per-row crawl/extraction and return a single output record dict.

            This function is deliberately self-contained so `main()` can always checkpoint a dict
            even when crawl/extraction fails.
            """
            # Start with all original columns so downstream columns aren't lost.
            row_out: dict = dict(row)
            unitid = str(row.get(UNITID_COL, "") or "").strip()
            name = str(row.get(NAME_COL, "") or "").strip()

            # Preserve the raw Web_address exactly as provided in the input/output.
            # Use a schemed variant only for crawling.
            raw_web_address = str(row.get(WEB_COL, "") or "").strip()
            base_url = ensure_scheme(raw_web_address)

            # Ensure minimum identity fields exist
            row_out[UNITID_COL] = unitid
            row_out[NAME_COL] = name
            row_out[WEB_COL] = raw_web_address
            if RESUME_KEY_COL not in row_out and RESUME_KEY_COL in row:
                row_out[RESUME_KEY_COL] = row.get(RESUME_KEY_COL, "")

            # Defaults for common output fields (safe even if OUTPUT_COLUMNS is larger)
            row_out.setdefault("best_guess_inventory_url", "")
            row_out.setdefault("best_guess_inventory_reason", "")
            row_out.setdefault("url_tag", "")
            row_out.setdefault("alt_candidate_urls", "")
            row_out.setdefault("any_control_found", 0)
            row_out.setdefault("struct_hits_union", "")
            row_out.setdefault(">0_struct_hits_found", 0)
            row_out.setdefault("program_title_count", 0)
            row_out.setdefault("program_titles_found", "")
            row_out.setdefault("pdf_hits", "")
            row_out.setdefault("total_controls_found", 0)
            row_out.setdefault("controls_sufficiency", "")
            row_out.setdefault("status", "")

            # Ensure all expected output keys exist so DictWriter doesn't drop columns.
            row_out.setdefault("error_detail", "")
            # CollegeVine outputs (defaults)
            row_out.setdefault("college_vine_site", "")
            row_out.setdefault("college_vine_url", "")
            row_out.setdefault("college_vine_ctrl_status", "")
            row_out.setdefault("college_vine_program_title_count", 0)
            row_out.setdefault("college_vine_program_titles_found", "")
            try:
                for _k in list(TARGET_TOKEN_REGEX.keys()):
                    row_out.setdefault(f"{_k}_matches", "")
            except Exception:
                pass
            try:
                for _ck in list(CONTROL_TERMS.keys()):
                    row_out.setdefault(str(_ck), 0)
            except Exception:
                pass

            if not base_url:
                row_out["status"] = "error_missing_web_address"
                return row_out

            # Reset per-institution status tally for fetch-status tagging (so url_tag reflects only institution-site fetches)
            try:
                reset_status_tally()
            except Exception:
                pass

            # Perform the crawl
            session = None
            try:
                session = requests.Session()

                # Candidate discovery + scoring
                pages = find_candidates_for_institution(
                    base_url,
                    session=session,
                    subsite_penalty=int(subsite_penalty),
                    workers=int(workers),
                )

                if not pages:
                    row_out["status"] = "no_candidates"
                    return row_out

                # --- candidate URL selection and url_tag wiring ---
                best = pages[0]
                row_out["best_guess_inventory_url"] = best.url
                row_out["best_guess_inventory_reason"] = inventory_reason(best)

                # Freeze institution-site fetch-status tag BEFORE any CollegeVine calls
                # (so CV requests cannot pollute the institution-site status tally).
                try:
                    row_out["url_tag"] = fetch_status_tag(best.url)
                except Exception:
                    # Fall back to heuristic tagging if status-tagging isn't available
                    row_out["url_tag"] = tag_row(str(row.get(WEB_COL, "") or ""), best.url)

                # Reset status tally so any later 3rd-party fetches won't affect institution url_tag
                try:
                    reset_status_tally()
                except Exception:
                    pass

                # Alt URLs for debugging (cap to TOP_K_ALTS)
                try:
                    row_out["alt_candidate_urls"] = "|".join([c.url for c in pages[1:TOP_K_ALTS]])
                except Exception:
                    row_out["alt_candidate_urls"] = ""

                if debug_jsonlen:
                    try:
                        print(
                            f"[DEBUG] unitid={unitid} best_json_blob_chars={len(getattr(best.parsed, 'json_blob', '') or '')}"
                        )
                    except Exception:
                        pass

                # Aggregate extraction from best + alts (+ limited follow)
                any_control_found, struct_hits_union, control_hit_cols, tokens_out, titles_out, pdf_hits = aggregate_outputs(
                    pages[:TOP_K_ALTS],
                    base_url=base_url,
                    session=session,
                    progtitle_strictness=int(progtitle_strictness),
                    workers=int(workers),
                )

                row_out["any_control_found"] = int(any_control_found)
                row_out["struct_hits_union"] = "|".join(sorted(struct_hits_union))
                row_out[">0_struct_hits_found"] = int(len(struct_hits_union) > 0)
                row_out["program_title_count"] = int(len(titles_out or []))
                row_out["program_titles_found"] = "|".join(titles_out or [])
                row_out["pdf_hits"] = "|".join(pdf_hits or [])

                # Also expose token matches if output columns exist for them
                for k, vals in (tokens_out or {}).items():
                    # store as a pipe-separated list
                    row_out[f"{k}_matches"] = "|".join(vals or [])

                # expose the per-control hit columns if desired by downstream schema
                for ck, v in (control_hit_cols or {}).items():
                    row_out[str(ck)] = int(v)

                # total controls found (proxy for "how many majors we saw")
                try:
                    total_controls_found = int(sum(int(v) for v in (control_hit_cols or {}).values()))
                except Exception:
                    total_controls_found = 0
                row_out["total_controls_found"] = int(total_controls_found)

                # sufficiency tag based on MIN_CONTROL_HITS_FOR_CONFIDENT_HUB (default 10)
                try:
                    min_hits = int(globals().get("MIN_CONTROL_HITS_FOR_CONFIDENT_HUB", 10) or 10)
                except Exception:
                    min_hits = 10

                if int(total_controls_found) < int(min_hits):
                    row_out["controls_sufficiency"] = f"warning_{int(min_hits)}_MajorsFound"
                else:
                    row_out["controls_sufficiency"] = "sufficient majors"

                # -------------------------
                # CollegeVine (optional)
                # -------------------------
                # NOTE: fetch_collegevine_majors_page signature is:
                #   (school_name: str, session: requests.Session, progtitle_strictness: int)
                # and it returns a tuple:
                #   (college_vine_url, college_vine_site, college_vine_control_hits,
                #    college_vine_program_title_count, college_vine_program_titles)
                try:
                    cv_fn = globals().get("fetch_collegevine_majors_page")
                    if callable(cv_fn):
                        cv_session = None
                        try:
                            cv_session = requests.Session()

                            # Defaults (robust against partial failures)
                            cv_url: str = ""
                            cv_site: int = 0
                            cv_control_hits: int = 0
                            cv_title_count: int = 0
                            cv_titles: List[str] = []

                            # Correct call + correct tuple unpacking
                            cv_url, cv_site, cv_control_hits, cv_title_count, cv_titles = cv_fn(
                                school_name=name,
                                session=cv_session,
                                progtitle_strictness=int(progtitle_strictness),
                            )

                            # Type safety / normalization
                            cv_url = normalize_unicode_text(cv_url or "")
                            try:
                                cv_site = int(cv_site or 0)
                            except Exception:
                                cv_site = 0
                            try:
                                cv_control_hits = int(cv_control_hits or 0)
                            except Exception:
                                cv_control_hits = 0
                            try:
                                cv_title_count = int(cv_title_count or 0)
                            except Exception:
                                cv_title_count = 0
                            if not isinstance(cv_titles, list):
                                cv_titles = []

                            cv_titles_clean = [
                                normalize_unicode_text(str(x))
                                for x in (cv_titles or [])
                                if normalize_unicode_text(str(x))
                            ]

                            # Row outputs
                            row_out["college_vine_site"] = str(cv_site)
                            row_out["college_vine_url"] = cv_url

                            # Status label: prefer explicit block signal; otherwise threshold outcome
                            ctrl_status = "cv_ok"
                            try:
                                if callable(globals().get("cv_block_or_ratelimit_seen")) and globals()["cv_block_or_ratelimit_seen"]():
                                    ctrl_status = "cv_blocked"
                            except Exception:
                                pass

                            if ctrl_status != "cv_blocked":
                                try:
                                    min_hits = int(globals().get("MIN_CONTROL_HITS_FOR_CONFIDENT_HUB", 0) or 0)
                                except Exception:
                                    min_hits = 0
                                if int(cv_site):
                                    ctrl_status = "passed_ctrl_thresh" if int(cv_control_hits) >= int(min_hits) else "failed_ctrl_thresh"
                                else:
                                    ctrl_status = "cv_unresolved"

                            row_out["college_vine_ctrl_status"] = ctrl_status

                            # Store titles (and count)
                            row_out["college_vine_program_title_count"] = int(len(cv_titles_clean))
                            row_out["college_vine_program_titles_found"] = "|".join(cv_titles_clean)
                        finally:
                            try:
                                if cv_session is not None:
                                    cv_session.close()
                            except Exception:
                                pass
                    else:
                        # CV code not present in this variant
                        row_out["college_vine_ctrl_status"] = "cv_unresolved"

                except Exception as e:
                    # Do not fail the row; preserve institution crawl outputs.
                    row_out["college_vine_ctrl_status"] = "cv_error"
                    prev = str(row_out.get("error_detail") or "")
                    msg = f"CV:{type(e).__name__}:{e}"
                    row_out["error_detail"] = (prev + (" | " if prev else "") + msg)

                row_out["status"] = "ok"
                return row_out

            except requests.HTTPError as e:
                row_out["status"] = "error_http"
                row_out["error_detail"] = f"HTTPError: {e}"
                return row_out
            except Exception as e:
                row_out["status"] = "error_exception"
                row_out["error_detail"] = f"{type(e).__name__}: {e}"
                return row_out
            finally:
                try:
                    if session is not None:
                        session.close()
                except Exception:
                    pass

        for _, row in df_batch.iterrows():
            key_val = str(row.get(RESUME_KEY_COL, "") or "").strip()
            if key_val and key_val in completed:
                continue

            # --- Actual per-row work: always produce a dict for checkpointing ---
            row_out = process_one_row(
                dict(row),
                workers=workers,
                subsite_penalty=int(getattr(args, "subsite_penalty", 80) or 80),
                progtitle_strictness=int(getattr(args, "progtitle_strictness", 3) or 3),
                debug_jsonlen=bool(getattr(args, "debug_jsonlen", False)),
            )
            produced = row_out

            # Ensure produced row has every column in the schema.
            for _c in fieldnames:
                if _c not in produced:
                    produced[_c] = ""

            buf_rows.append(produced)
            completed_in_batch += 1

            if len(buf_rows) >= checkpoint_every:
                append_rows(progress_path, fieldnames, buf_rows)
                buf_rows.clear()

            if compact_every and (completed_in_batch % int(compact_every) == 0):
                compact_progress(progress_path, fieldnames)

        if buf_rows:
            append_rows(progress_path, fieldnames, buf_rows)
            buf_rows.clear()

        compact_progress(progress_path, fieldnames)

        # batch output is the compacted progress snapshot
        try:
            dfp = pd.read_csv(progress_path, dtype=str, keep_default_na=False)
            dfp.to_csv(batch_out_path, index=False)
        except Exception:
            # Fallback: if progress CSV can't be read, still attempt to copy bytes so concat can proceed.
            try:
                batch_out_path.write_bytes(progress_path.read_bytes())
            except Exception:
                pass
        if batch_out_path.exists() and batch_out_path.stat().st_size > 0:
            batch_outputs.append(batch_out_path)

        print(f"[BATCH {batch_idx}] done -> {batch_out_path.name}")
        # Optional: brief pause between batches (separate knob from per-institution sleep if desired)
        try:
            time.sleep(float(globals().get("SLEEP_BETWEEN_BATCHES_SEC", 0) or 0))
        except Exception:
            pass

    print(f"[FINAL] Concatenating {len(batch_outputs)} batch outputs -> {final_output_path}")
    concat_batches(batch_outputs, final_output_path)
    print(f"[FINAL] Wrote {final_output_path} ({final_output_path.stat().st_size:,} bytes)")

if __name__ == "__main__":
    main()