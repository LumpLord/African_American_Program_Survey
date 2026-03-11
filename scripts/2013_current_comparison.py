#!/usr/bin/env python3
"""2013 vs Current Program/Signal Matching

DATA DICTIONARY (output columns)
===============================
This script compares each institution's single 2013 program name against *current* discovered titles
from multiple sources (crawl + CollegeVine), within the same `unitid`.

Input expectations (wide CSV)
-----------------------------
Required columns:
  - unitid
  - name
  - 2013_program_name
  - program_titles_found                       (pipe/semicolon/newline separated list; mostly crawl)
  - college_vine_program_titles_found          (pipe/semicolon/newline separated list; CV)

Optional columns (used if present):
  - real_nonprogram_signals                    (pipe/semicolon/newline separated; may be prefixed with `crawl:` / `cv:`)

Output columns
--------------
1)  match_2013__best_title
    - The matched candidate title selected by the match ladder. Empty if no match.

2)  match_2013__best_source
    - Where the best_title came from: crawl | cv | signal | both | unknown.
      `both` occurs when the same title appears in both crawl and cv candidate pools.

3)  match_2013__best_kind
    - Candidate kind: program | signal. (Signals come from `real_nonprogram_signals` or titles that look like
      departments/centers/etc. and are treated as signals when matching.)

4)  match_2013__match_level
    - One of: strict_raw | strict_canonical | fuzzy_raw | fuzzy_canonical | fuzzy_syn | related_credential | family_rescue | related_domain_backstop | NO_MATCH

5)  match_2013__match_score
    - Score for fuzzy matches (0-1). For strict matches, 1.0. For NO_MATCH, 0.

6)  match_2013__detail
    - Human-readable detail about the selected match (best pair + score).

7)  match_2013__is_signal_marker_in_2013
    - 1 if 2013_program_name contains an explicit signal marker (department/center/institute/committee/etc.), else 0.

8)  discovered__program_titles__crawl
    - Canonicalized list of crawl candidates (deduped, pipe-joined).

9)  discovered__program_titles__cv
    - Canonicalized list of CV candidates (deduped, pipe-joined).

10) discovered__signal_titles
    - Canonicalized list of signal candidates (deduped, pipe-joined).

11) discovered__all_titles
    - All deduped titles (raw display, pipe-joined), across crawl/cv/signal.

12) discovered__new_titles_unmatched
    - Titles that were discovered (across crawl/cv/signal) but did not match 2013_program_name
      under ANY match mode (strict/raw, strict/canon, fuzzy/raw, fuzzy/canon, fuzzy/syn).

13) discovered__new_program_titles_when_best_signal
    - If best match is a signal-kind (e.g., Department), this lists program-ish titles also discovered
      (crawl/cv) that did not become the best match. Empty otherwise.

- Matching is *within unitid* and uses a ladder:
    (1) strict_raw   (normalized raw equality)
    (2) strict_canonical
    (3) fuzzy_raw
    (4) fuzzy_canonical
    (5) fuzzy_syn     (fuzzy with conservative synonym map)
    (6) related_credential (credentialed program items within domain family; stricter gating)
    (7) family_rescue (gated: direct family overlap + anchor keywords; low lexical threshold)
    (8) related_domain_backstop (best-available related match; see below)
- Default behavior prefers program-kind candidates UNLESS the 2013 string itself contains explicit signal markers.
  If 2013 looks signal-like, we prefer signal candidates when they match.
- "Approximate matching" uses max(token overlap coefficient, SequenceMatcher ratio).
- `related_domain_backstop` is used only when no other match mode succeeds; it uses domain-keyword gating plus a lower threshold to pick a best-available related match.

Usage
-----
  python 2013_current_comparison.py \
    --input ace_unitid_merge__ace_x_2013comp__webscrape__v15simple__bucketed_programs.csv

Outputs a new CSV alongside the input with suffix:
  __2013_current_matches.csv

"""

from __future__ import annotations

import argparse
import csv
import difflib
import os
import re
import sys
import unicodedata
import unittest
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd


# ============================================================
# Normalization helpers
# ============================================================

_WS_RE = re.compile(r"\s+")


def normalize_unicode_text(s: str) -> str:
    """Normalize unicode + whitespace and strip."""
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u00a0", " ")
    s = _WS_RE.sub(" ", s).strip()
    return s


_PUNCT_TO_SPACE = re.compile(r"[\t\n\r\f\v\u2010\u2011\u2012\u2013\u2014\u2212_/]+")


def _basic_clean(s: str) -> str:
    s = normalize_unicode_text(s)
    s = s.replace("&", " and ")
    s = _PUNCT_TO_SPACE.sub(" ", s)
    s = re.sub(r"[\"'’]", "", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def norm_title_key(s: str) -> str:
    """Strict-ish key: normalize unicode, lower, collapse whitespace, drop most punctuation."""
    s = _basic_clean(s)
    s = s.lower()
    return s


def norm_title_key_loose(s: str) -> str:
    """Loose key: strict key plus removal of some stop-words + degree noise."""
    s = norm_title_key(s)
    if not s:
        return ""

    # remove very common degree/label noise
    s = re.sub(r"\b(b\.?a\.?|b\.?s\.?|ba|bs|m\.?a\.?|m\.?s\.?|ma|ms|ph\.?d\.?|phd)\b", " ", s)
    s = re.sub(r"\b(bachelor|master|masters|doctorate|doctoral)\b", " ", s)
    s = re.sub(r"\b(major|minor|certificate|concentration|track|option|emphasis)\b", " ", s)
    s = re.sub(r"\b(in|of|and|for|the|a|an)\b", " ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


_PAREN_CONTENT = re.compile(r"\s*\([^)]*\)\s*")


def canonicalize_program_title(s: str, drop_parenthetical: bool = False) -> str:
    """Conservative canonicalization.

    - Keeps the full title by default.
    - If drop_parenthetical=True, removes parenthetical segments (useful for matching).
    """
    s = normalize_unicode_text(s)
    if not s:
        return ""

    # normalize hyphens/quotes and spacing
    s = s.replace("&", " and ")
    s = _WS_RE.sub(" ", s).strip()

    if drop_parenthetical:
        s = _PAREN_CONTENT.sub(" ", s)
        s = _WS_RE.sub(" ", s).strip()

    # remove common leading degree phrases (do NOT remove mid-title degree tokens)
    s = re.sub(r"^(ba|bs|b\.a\.|b\.s\.|ma|ms|m\.a\.|m\.s\.|master of arts|master of science|bachelor of arts|bachelor of science)\s+in\s+",
               "", s, flags=re.I)

    # strip repeated whitespace
    s = _WS_RE.sub(" ", s).strip()
    return s


# ============================================================
# Candidate splitting / parsing
# ============================================================


def _split_program_name_field(
    s: str,
    debug_added: Optional[List[str]] = None,
    split_pipes: bool = True,
    atomic_pipe_taxonomy: bool = False,
) -> List[str]:
    """Split a pipe/semicolon/newline-delimited field into items.

    Only split on commas when it looks like an actual list (>=2 commas), to avoid
    breaking names like "African, Black and Caribbean Studies".

    Recombination policy:
      - If the raw field contains `|` and yields >=3 non-empty segments, ALWAYS add:
          (a) full recombined title (segments joined by spaces)
          (b) recombined title with trailing junk segments removed (Other, Overview, etc.)

    Patch 5: atomic_pipe_taxonomy disables pipe splitting for taxonomy/category labels.
    """
    s = normalize_unicode_text(s or "")
    if not s:
        return []

    # Patch A (upgrade of Patch 5): atomic taxonomy heuristic — detect taxonomy/category labels with pipe-encoding.
    # IMPORTANT: Do NOT replace pipes with commas; commas may later be treated as list separators.
    # Instead, replace with a non-list visual delimiter and suppress comma-based splitting below.
    s_for_split = s
    _atomic_applied = False
    _suppress_comma_split = False

    # Patch A2: CV taxonomy labels often arrive comma-encoded like:
    #   "Ethnic, Cultural Minority, and Gender Studies, Other"
    # When atomic_pipe_taxonomy is enabled (CV pool), treat these as atomic and do NOT split on commas.
    if atomic_pipe_taxonomy and ("|" not in s) and (s.count(",") >= 2):
        segs_comma = [x.strip() for x in s.split(",") if x.strip()]
        if len(segs_comma) >= 3:
            last_seg = segs_comma[-1].lower()
            has_studies = ("studies" in s.lower())
            if last_seg in {"other", "overview", "general", "misc"} and has_studies:
                _suppress_comma_split = True
    if atomic_pipe_taxonomy and "|" in s:
        segs = [x.strip() for x in s.split("|") if x.strip()]
        if len(segs) >= 3:
            last_seg = segs[-1].lower()
            has_studies = any("studies" in seg.lower() for seg in segs)
            if last_seg in {"other", "overview", "general", "misc"} or has_studies:
                # treat as atomic; do not split on pipes
                s_for_split = " / ".join(segs)
                split_pipes = False
                _atomic_applied = True
                _suppress_comma_split = True

    # --- unconditional recombination when pipe fragments are present ---
    recombined_variants: List[str] = []
    if "|" in s:
        segs = [x.strip() for x in s.split("|") if x.strip()]
        if len(segs) >= 3:
            full = normalize_unicode_text(" ".join(segs))
            if full:
                recombined_variants.append(full)

            # Remove trailing junk segments like "Other", "Overview", etc.
            junk = {
                "other",
                "overview",
                "general",
                "misc",
                "programs",
                "program",
                "more",
                "learn more",
                "details",
                "information",
            }
            segs2 = list(segs)
            while segs2 and segs2[-1].strip().lower() in junk:
                segs2 = segs2[:-1]
            if segs2 and segs2 != segs:
                trimmed = normalize_unicode_text(" ".join(segs2))
                if trimmed:
                    recombined_variants.append(trimmed)

            if debug_added is not None and recombined_variants:
                # Keep debug compact: show only the created variants.
                debug_added.append(
                    f"recombined_from_pipe: {normalize_unicode_text(s)} => {', '.join(recombined_variants)}"
                )

    # Patch 5: build separator list dynamically
    seps = [";", "\n", "\r"]
    if split_pipes:
        seps = ["|"] + seps
    parts: List[str] = [s_for_split]
    for sep in seps:
        tmp: List[str] = []
        for p in parts:
            if sep in p:
                tmp.extend([x.strip() for x in p.split(sep) if x.strip()])
            else:
                tmp.append(p)
        parts = tmp

    tmp2: List[str] = []
    for p in parts:
        # Patch A/A2: if we treated a taxonomy label as atomic (pipe-encoded OR comma-encoded), NEVER split on commas.
        if (not _suppress_comma_split) and p.count(",") >= 2:
            tmp2.extend([x.strip() for x in p.split(",") if x.strip()])
        else:
            tmp2.append(p)

    out = [normalize_unicode_text(x) for x in tmp2 if normalize_unicode_text(x)]

    # Ensure recombined variants are included (and placed first) even if splitting already happened upstream.
    if recombined_variants:
        existing = {norm_title_key(x) for x in out}
        for rc in reversed(recombined_variants):
            k = norm_title_key(rc)
            if k and k not in existing:
                out.insert(0, rc)
                existing.add(k)

    return out



SIGNAL_MARKERS = re.compile(
    r"\b(department|dept\.?|school|college|division|faculty|center|centre|institute|program\b\s+in|committee|council|office|administration|administrative|unit)\b",
    re.I,
)


def has_signal_marker(s: str) -> bool:
    return bool(SIGNAL_MARKERS.search(normalize_unicode_text(s)))

# Structured signal encodings sometimes appear as prefix strings like:
#   department_unit:africana:crawl:Africana Studies
#   center_institute:race:crawl:Center for Race, Inequality...
# We want the human title portion to be matchable.
_STRUCTURED_SIGNAL_PREFIX = re.compile(
    r"^(department_unit|center_institute|committee_admin|other_nonprogram|junk|course|event_news|maybe_program|program)\s*:\s*[^:]*\s*:\s*(crawl|cv)\s*:\s*(.+)$",
    re.I,
)


def strip_structured_prefix(s: str) -> str:
    """Strip structured prefixes like `department_unit:bucket:src:Title` to just `Title`."""
    s0 = normalize_unicode_text(s)
    if not s0:
        return ""

    m = _STRUCTURED_SIGNAL_PREFIX.match(s0)
    if m:
        return normalize_unicode_text(m.group(3))

    # Fallback: if it looks like a 3+ segment colon prefix, take the last segment.
    # Only do this when the first segment is one of our known structured kinds.
    head = s0.split(":", 1)[0].strip().lower()
    if head in {
        "department_unit",
        "center_institute",
        "committee_admin",
        "other_nonprogram",
        "junk",
        "course",
        "event_news",
        "maybe_program",
        "program",
    } and s0.count(":") >= 3:
        return normalize_unicode_text(s0.split(":")[-1])

    return s0


def is_fragment_candidate(title: str) -> bool:
    t = normalize_unicode_text(title)
    if not t:
        return True

    # Patch B: clause fragments created by over-splitting often begin with conjunctions.
    # Examples: "and Gender Studies", "or Related Fields".
    if re.match(r"^(and|or|&)\b", t, flags=re.I):
        return True

    # Very short strings are almost always fragments from overly-split fields.
    if len(t) < 10:
        return True

    toks = _content_tokens(t)

    # One-token candidates (e.g., "Ethnic", "Gender") are usually fragment artifacts.
    if len(toks) < 2:
        return True

    return False


def signal_intent_bonus(ref: str, cand: str) -> float:
    """Boost when both ref and candidate look like signals; penalize mismatched intent."""
    r_sig = has_signal_marker(ref)
    c_sig = has_signal_marker(cand)
    if r_sig and c_sig:
        return 0.08
    if r_sig and not c_sig:
        return -0.05
    return 0.0


@dataclass(frozen=True)
class Candidate:
    title: str
    source: str  # crawl | cv | signal | unknown
    kind: str    # program | signal

    def title_key(self) -> str:
        return norm_title_key(self.title)


def _dedupe_preserve_order(items: Sequence[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for x in items:
        k = norm_title_key(x)
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def parse_candidates_from_row(row: pd.Series) -> Tuple[str, List[Candidate], str]:
    """Return 2013 title and candidate list for a row, plus debug string."""
    t2013 = normalize_unicode_text(row.get("2013_program_name", ""))

    debug_added: List[str] = []

    # Crawl candidates
    crawl_raw = _split_program_name_field(row.get("program_titles_found", ""), debug_added=debug_added)

    # CV candidates (Patch 5: atomic_pipe_taxonomy protection)
    cv_raw = _split_program_name_field(
        row.get("college_vine_program_titles_found", ""),
        debug_added=debug_added,
        atomic_pipe_taxonomy=True
    )

    # Signals (optional)
    signal_raw: List[str] = []
    if "real_nonprogram_signals" in row.index:
        signal_raw = _split_program_name_field(row.get("real_nonprogram_signals", ""), debug_added=debug_added)

    # Second-pass recombine: if any candidate strings still contain '|', re-split and
    # inject recombined variants. This covers cases where upstream re-joined lists.
    def _second_pass_pipe_fix(items: List[str], atomic_pipe_taxonomy: bool = False) -> List[str]:
        fixed: List[str] = []
        for it in items:
            if "|" in normalize_unicode_text(it):
                fixed.extend(_split_program_name_field(
                    it,
                    debug_added=debug_added,
                    atomic_pipe_taxonomy=atomic_pipe_taxonomy,
                ))
            else:
                fixed.append(it)
        return fixed

    crawl_raw = _second_pass_pipe_fix(crawl_raw)
    cv_raw = _second_pass_pipe_fix(cv_raw, atomic_pipe_taxonomy=True)
    signal_raw = _second_pass_pipe_fix(signal_raw)

    candidates: List[Candidate] = []

    for t in crawl_raw:
        candidates.append(Candidate(title=t, source="crawl", kind="program"))

    for t in cv_raw:
        candidates.append(Candidate(title=t, source="cv", kind="program"))

    for t in signal_raw:
        t0 = strip_structured_prefix(t)
        src = "signal"
        # allow prefixed encodings like `crawl:Title` or `cv:Title`
        m = re.match(r"^(crawl|cv)\s*:\s*(.+)$", t0, flags=re.I)
        if m:
            src = "signal"
            t0 = normalize_unicode_text(m.group(2))
        t0 = strip_structured_prefix(t0)
        candidates.append(Candidate(title=t0, source=src, kind="signal"))

    # If the program pools contain explicit signal-marker titles, ALSO include them as signal-kind.
    # This helps match "X Department" even if it arrived via a noisy program_titles_found.
    for t in (crawl_raw + cv_raw):
        if has_signal_marker(t):
            candidates.append(Candidate(title=t, source="signal", kind="signal"))

    # Deduplicate by title key while preserving a preference for program-kind entries.
    # If both program and signal exist for the same title, keep both (for src=both accounting).
    # We'll handle src aggregation at match-time.
    debug_str = "||".join(_dedupe_preserve_order(debug_added))
    return t2013, candidates, debug_str


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

    # africology variants (seen in some catalogs)
    (re.compile(r"\bafricology\b", re.I), " africana studies "),

    # pan-african hyphenation / spacing variants
    (re.compile(r"\bpan\s*[-–—]?\s*african\b", re.I), " pan african "),
    (re.compile(r"\bpan\s+african\s+studies\b", re.I), " africana studies "),

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

    # Drop common generic tail tokens that appear in fragmented pipe fields.
    drop = {"other", "misc", "general"}
    toks = [t for t in toks if t not in drop]

    return set(toks)


def _overlap_coeff(a: Set[str], b: Set[str]) -> float:
    """Token overlap similarity.

    Base is |A∩B| / min(|A|,|B|), but we guard against single-token candidates
    producing a perfect score against much longer references.
    """
    if not a or not b:
        return 0.0
    inter = len(a.intersection(b))
    la = len(a)
    lb = len(b)
    if inter == 0:
        return 0.0

    denom_min = float(min(la, lb))
    base = inter / denom_min if denom_min > 0 else 0.0

    # Guard: if one side is a single token but the other is meaningfully longer,
    # require broader coverage by down-weighting to an inter/max formulation.
    if min(la, lb) == 1 and max(la, lb) >= 3:
        denom_max = float(max(la, lb))
        return inter / denom_max if denom_max > 0 else 0.0

    return base


# ============================================================
# Domain-aware similarity helpers (used to boost best-available matches)
# ============================================================

# --- must-carry constraint for strong refs ---
# Accept hyphens/en-dashes/em-dashes between words (African-American, African–American, etc.)
_DASHSEP = r"[\s\-\u2010\u2011\u2012\u2013\u2014\u2212]+"
_AFRICAN_AMERICAN_RX = rf"\bafrican(?:{_DASHSEP})american\b"
_AFRO_AMERICAN_RX = rf"\bafro(?:{_DASHSEP})american\b"

_STRONG_REF_RX = re.compile(rf"\b(black|africana|{_AFRICAN_AMERICAN_RX}|{_AFRO_AMERICAN_RX})\b", re.I)
_STRONG_CAND_RX = re.compile(rf"\b(black|africana|{_AFRICAN_AMERICAN_RX}|{_AFRO_AMERICAN_RX})\b", re.I)

def is_strong_ref(ref: str) -> bool:
    """True if the reference expresses a strong Black/Africana/African-American intent.

    When True, candidates must "carry" at least one of {black, africana, african american}
    to be eligible for fuzzy/backstop matching (see `candidate_eligible_under_strong_ref`).
    """
    r = normalize_unicode_text(ref)
    return bool(_STRONG_REF_RX.search(r))

def candidate_has_strong_token(cand: str) -> bool:
    c = normalize_unicode_text(cand)
    return bool(_STRONG_CAND_RX.search(c))

def candidate_eligible_under_strong_ref(ref: str, cand: str, *, allow_black_credential_exception: bool) -> bool:
    """Eligibility gating to prevent african-only candidates from matching africana/black refs.

    Rules:
      - If ref is strong (black/africana/african-american), candidate must contain at least one
        of {black, africana, african american} to be eligible for ANY fuzzy match OR backstop.
      - Exception (only when `allow_black_credential_exception=True`): allow candidates that
        are credentialed AND contain black (e.g., "Black American Studies Minor").
      - If ref is not strong (pure african, ethnic, race), no extra gating is applied.
    """
    if not is_strong_ref(ref):
        return True

    # Normal path: must carry a strong token.
    if candidate_has_strong_token(cand):
        return True

    if allow_black_credential_exception:
        c = normalize_unicode_text(cand)
        if _CREDENTIAL_MARKERS.search(c) and re.search(r"\bblack\b", c, flags=re.I):
            return True

    return False

DOMAIN_FAMILY_PATTERNS: Dict[str, List[re.Pattern]] = {
    # NOTE: keep conservative; these are only used as a small bonus and for backstop gating
    "black": [
        re.compile(r"\bblack\b", re.I),
        re.compile(_AFRICAN_AMERICAN_RX, re.I),
        re.compile(_AFRO_AMERICAN_RX, re.I),
        re.compile(r"\bblack\s+diaspor", re.I),
        re.compile(r"\bpan[-\s]?african\b", re.I),
    ],
    "africana": [
        re.compile(r"\bafricana\b", re.I),
        re.compile(r"\bpan[-\s]?african\b", re.I),
        re.compile(r"\bafrican\s+diaspor", re.I),
    ],
    "african": [
        re.compile(r"\bafrican\b", re.I),
        re.compile(r"\bafrica\b", re.I),
    ],
    "ethnic": [
        re.compile(r"\bethnic\b", re.I),
        re.compile(r"\bethnicity\b", re.I),
        re.compile(r"\bmulticultural(?:ism)?\b", re.I),
        re.compile(r"\bmulti[-\s]?cultural(?:ism)?\b", re.I),
        re.compile(r"\bmulti[-\s]?ethnic\b", re.I),
        re.compile(r"\binter[-\s]?cultural\b", re.I),
        re.compile(r"\bcultural\s+minority\b", re.I),
        re.compile(r"\bmigration\b", re.I),
        re.compile(r"\bchicanx\b", re.I),
        re.compile(r"\blatinx\b", re.I),
        re.compile(r"\basian\s+american\b", re.I),
        re.compile(r"\bnative\s+american\b", re.I),
    ],
    "race": [
        re.compile(r"\brace\b", re.I),
        re.compile(r"\bracial\b", re.I),
        re.compile(r"\bjustice\b", re.I),
        re.compile(r"\bequity\b", re.I),
    ],
}

# Related-family mapping for best-available fallback matches
RELATED_DOMAIN_FAMILIES: Dict[str, Set[str]] = {
    "black": {"black", "africana", "african"},
    "africana": {"africana", "black", "african"},
    "african": {"african", "africana", "black"},
    "ethnic": {"ethnic", "race"},
    "race": {"race", "ethnic"},
}

# ============================================================
# CV taxonomy label heuristic (optional rejection)
# ============================================================

# Very common CollegeVine category labels that are not full program entities (normalized keys).
_KNOWN_CV_TAXONOMY_LABELS: Set[str] = {
    norm_title_key(x)
    for x in [
        "Ethnic Studies",
        "Race and Ethnicity",
        "African American Studies",
        "Africana Studies",
        "African Studies",
        "Black Studies",
        "Gender Studies",
        "Women's Studies",
        "Women and Gender Studies",
        "Latinx Studies",
        "Chicanx Studies",
        "Asian American Studies",
        "Native American Studies",
        "Indigenous Studies",
        "Queer Studies",
        "LGBTQ Studies",
    ]
}

def is_cv_taxonomy_label(title: str) -> bool:
    """Heuristic: True if a CV title looks like a taxonomy/category label rather than a program entity.
    Apply only when the candidate source is CollegeVine (source == 'cv').
    """
    t = normalize_unicode_text(title)
    if not t:
        return False

    # If it's explicitly credentialed or signal-like, treat as a real entity.
    if has_credential_marker(t) or has_signal_marker(t):
        return False

    k = norm_title_key(t)
    if k in _KNOWN_CV_TAXONOMY_LABELS:
        return True

    # Generic short labels (<=3 tokens) ending in "studies" are often taxonomy-like.
    toks = [x for x in norm_title_key(t).split() if x]
    if len(toks) <= 3 and toks and toks[-1] == "studies":
        return True

    return False

# ============================================================
# Winner-selection (Patch 1): prefer best on-site primary academic titles
# ============================================================

_PRIMARY_ACAD_RX = re.compile(
    r"\b(program|major|minor|department|dept\.?|studies)\b",
    re.I,
)

_SECONDARY_ORG_RX = re.compile(
    r"\b(center|centre|institute|committee|council|association)\b",
    re.I,
)

# ============================================================
# Patch 4: Rename-family rescue (Africana / African-American / Black Studies)
# ============================================================

# Trigger when the 2013 reference indicates a strong Africana/Black/African-American intent.
_RENAME_FAMILY_REF_RX = re.compile(
    rf"\b(africana|black|pan{_DASHSEP}?african|{_AFRICAN_AMERICAN_RX}|{_AFRO_AMERICAN_RX})\b|\bafrican\s+and\s+african\s+american\b",
    re.I,
)


def is_rename_family_ref(ref: str) -> bool:
    r = normalize_unicode_text(ref)
    return bool(r and _RENAME_FAMILY_REF_RX.search(r))


def rename_family_rescue_match(
    ref: str,
    candidates: List[Candidate],
    kind_pref: str,
    threshold: float = 0.55,
    *,
    allow_category_mapping: bool = False,
) -> Tuple[bool, str, float, str]:
    """Rename-family rescue tier.

    Purpose: rescue common institutional retitles/renames among:
      Africana ↔ African-American ↔ Black Studies ↔ Pan-African

    Gating (low risk):
      - ref must be in rename-family intent (`is_rename_family_ref`)
      - candidate must be primary-academic (Patch 1 classifier), OR (taxonomy only when no on-site primary exists)
      - candidate must be in the related rename family (black/africana/african)
      - apply standard nav/year/mega/nonprogram filters

    Special case: allow African Studies (african-only) ONLY as a last resort when no
    stronger black/africana candidates exist for the row.
    """
    ref0 = normalize_unicode_text(ref)
    if not ref0:
        return False, "", 0.0, ""

    if not is_rename_family_ref(ref0):
        return False, "", 0.0, ""

    # Candidate pool by kind
    pool_cands: List[Candidate] = []
    seen: Set[str] = set()
    for c in candidates:
        if c.kind != kind_pref:
            continue
        k = norm_title_key(c.title)
        if not k or k in seen:
            continue
        seen.add(k)
        pool_cands.append(c)

    # Standard filters
    pool_cands = [c for c in pool_cands if not is_nav_prefix_title(c.title)]
    pool_cands = [c for c in pool_cands if not is_mega_string_candidate(c.title)]

    if not has_year_token(ref0):
        pool_cands = [c for c in pool_cands if not has_year_token(c.title)]

    if kind_pref == "program":
        pool_cands = [c for c in pool_cands if not is_nonprogram_title(c.title)]
    else:
        if not has_signal_marker(ref0):
            pool_cands = [c for c in pool_cands if not is_nonprogram_title(c.title)]
        else:
            pool_cands = [c for c in pool_cands if (not is_nonprogram_title(c.title)) or has_signal_marker(c.title)]

    if not pool_cands:
        return False, "", 0.0, ""

    # Pre-compute whether any strong (black/africana/african-american) candidate exists.
    def _sources_set_for_title(title: str) -> Set[str]:
        k = norm_title_key(title)
        return {c.source for c in candidates if norm_title_key(c.title) == k}

    strong_exists = False
    onsite_primary_exists = False
    for c in pool_cands:
        t = normalize_unicode_text(c.title)
        if not t:
            continue
        srcs = _sources_set_for_title(t)
        cls = candidate_class(t, srcs)
        fams = domain_families_present(t)
        if cls == "primary_academic" and srcs != {"cv"}:
            onsite_primary_exists = True
        if ("black" in fams) or ("africana" in fams) or candidate_has_strong_token(t):
            strong_exists = True

    best_score = 0.0
    best_title = ""
    best_detail = ""

    f_ref = domain_families_present(ref0)

    for c in pool_cands:
        cand0 = normalize_unicode_text(c.title)
        if not cand0:
            continue

        # Must be non-fragment
        cand_tok = _content_tokens(cand0)
        if len(cand_tok) < 2 or is_fragment_candidate(cand0):
            continue

        srcs = _sources_set_for_title(cand0)
        cls = candidate_class(cand0, srcs)

        # Require primary academic; allow taxonomy only when explicitly allowed OR
        # when we have no on-site primary academic survivors.
        if cls == "taxonomy":
            if (not allow_category_mapping) and onsite_primary_exists:
                continue
        elif cls != "primary_academic":
            continue

        f_cand = domain_families_present(cand0)
        if not f_cand:
            continue

        # Must be in the rename family neighborhood.
        if not (f_cand.intersection({"black", "africana", "african"})):
            continue

        # Special case: African-only candidates are allowed only as a last resort.
        african_only = ("african" in f_cand) and ("black" not in f_cand) and ("africana" not in f_cand) and (not candidate_has_strong_token(cand0))
        if african_only and strong_exists:
            continue

        # Scoring (synonyms + parenthetical drop) using shared scorer.
        sc, a_best, b_best = best_partial_title_match(
            [ref0],
            [cand0],
            use_synonyms=True,
            drop_parenthetical=True,
        )

        # Apply additional domain + intent adjustments to the score so it behaves like other tiers.
        # (best_partial_title_match already includes domain_bonus/signal_intent/entity_type_penalty)
        sc = float(sc)

        if sc > best_score:
            best_score = sc
            best_title = b_best or cand0
            best_detail = (
                f'rename_family_rescue: ref="{ref0}" ~ cand="{best_title}" score={best_score:.2f} '
                f'fam_ref={sorted(f_ref)} fam_cand={sorted(f_cand)} cls={cls} african_only={int(african_only)}'
            )

    if best_title and best_score >= float(threshold):
        return True, best_title, float(best_score), best_detail

    return False, "", float(best_score), ""

# ============================================================
# Patch 2: Entity-type penalty for center/institute drift
# ============================================================

# Apply a soft penalty when the reference is a primary academic unit (dept/program/studies)
# but the candidate is an organizational entity (center/institute/committee/etc.).
# This improves specificity while retaining sensitivity (the candidate can still win
# if no better academic unit exists).
_ENTITY_TYPE_PENALTY = 0.15


def _looks_primary_academic_ref(ref: str) -> bool:
    r = normalize_unicode_text(ref)
    if not r:
        return False
    if has_credential_marker(r):
        return True
    if _PRIMARY_ACAD_RX.search(r):
        return True
    # short noun phrase ending in Studies
    toks = [x for x in norm_title_key(r).split() if x]
    if toks and toks[-1] == "studies":
        return True
    return False


def _looks_secondary_org_candidate(cand: str) -> bool:
    c = normalize_unicode_text(cand)
    if not c:
        return False
    return bool(_SECONDARY_ORG_RX.search(c))


def entity_type_penalty(ref: str, cand: str) -> float:
    """Return a negative penalty when a secondary org (center/institute/etc.) is matching
    a primary academic reference (dept/program/studies).

    Penalty is soft (subtracts ~0.15) and is applied in similarity scoring.
    """
    if _looks_primary_academic_ref(ref) and _looks_secondary_org_candidate(cand):
        return -_ENTITY_TYPE_PENALTY
    return 0.0


def candidate_class(cand_title: str, cand_sources: Set[str]) -> str:
    """Classify a candidate title for winner-selection.

    Returns one of: primary_academic | secondary_org | taxonomy | other

    - taxonomy is reserved for CV taxonomy/category labels when the title exists ONLY via cv.
    - primary_academic includes program-ish entities (Program/Major/Minor/Department/Studies).
    """
    t = normalize_unicode_text(cand_title)
    if not t:
        return "other"

    # Taxonomy: only when the title is *only* from CV and looks like a CV category label.
    only_cv = (cand_sources == {"cv"})
    if only_cv and is_cv_taxonomy_label(t):
        return "taxonomy"

    if _SECONDARY_ORG_RX.search(t):
        return "secondary_org"

    if _PRIMARY_ACAD_RX.search(t) or has_credential_marker(t):
        return "primary_academic"

    # Short noun-phrase program names (e.g., "Africana Studies")
    toks = [x for x in norm_title_key(t).split() if x]
    if 1 < len(toks) <= 3 and toks[-1] == "studies":
        return "primary_academic"

    return "other"


def _score_pair_for_winner(ref: str, cand: str) -> Tuple[float, float, bool]:
    """Score a single (ref, cand) pair for winner-selection.

    Returns: (score_0_to_1, ref_coverage_0_to_1, ref_subset_of_cand_tokens)

    Uses the same scoring primitives as fuzzy matching: max(token overlap, ref coverage, seq ratio)
    plus domain + intent bonuses, and applies nonprogram penalties.
    """
    r0 = normalize_unicode_text(ref)
    c0 = normalize_unicode_text(cand)
    if not r0 or not c0:
        return 0.0, 0.0, False

    # Reject/penalize obvious non-program navigation/marketing titles.
    reject_np, np_pen = nonprogram_penalty(r0, c0)
    if reject_np:
        return 0.0, 0.0, False

    r_can = canonicalize_program_title(r0, drop_parenthetical=True)
    c_can = canonicalize_program_title(c0, drop_parenthetical=True)

    # Use conservative synonym mapping for winner-selection consistency
    r_pre = apply_synonym_map(r_can)
    c_pre = apply_synonym_map(c_can)

    r_loose = norm_title_key_loose(r_pre)
    c_loose = norm_title_key_loose(c_pre)

    r_tok = _content_tokens(r_pre)
    c_tok = _content_tokens(c_pre)

    tok_score = _overlap_coeff(r_tok, c_tok)
    coverage = (len(r_tok.intersection(c_tok)) / float(len(r_tok))) if r_tok else 0.0
    seq_score = difflib.SequenceMatcher(None, r_loose, c_loose).ratio() if (r_loose and c_loose) else 0.0

    base = max(tok_score, coverage, seq_score)

    if np_pen > 0:
        base = max(0.0, base * (1.0 - np_pen))
    # Patch 6: soft demotion for boilerplate titles that still contain academic keywords.
    bp_pen = boilerplate_penalty_fraction(c0)
    if bp_pen > 0:
        base = max(0.0, base * (1.0 - bp_pen))
    # Fragment / short-candidate penalty (same spirit as best_partial_title_match)
    short_penalty = 0.0
    if len(c_tok) < 2:
        short_penalty += 0.25
    if len(c0) < 14:
        short_penalty += 0.10
    if len(r_tok) >= 3 and len(c_tok) == 1:
        short_penalty += 0.20
    if short_penalty > 0:
        base = max(0.0, base * (1.0 - min(0.60, short_penalty)))

    bonus = domain_bonus(r0, c0, r_tok, c_tok) + signal_intent_bonus(r0, c0) + entity_type_penalty(r0, c0)
    score = min(1.0, float(base + bonus))

    subset = bool(r_tok) and r_tok.issubset(c_tok)
    return score, float(coverage), subset


def select_best_title_patch1(
    ref: str,
    candidates: List[Candidate],
    *,
    primary_thresh: float = 0.80,
) -> Tuple[str, float, str, str]:
    """Select the best title using Patch 1 precedence.

    Returns (best_title, best_score, best_source, winner_reason).

    Precedence:
      1) primary_academic with score >= primary_thresh (prefer on-site if available)
      2) secondary_org (prefer on-site if available)
      3) taxonomy backstop

    Tie-breakers:
      - Higher score
      - Prefer ref token subset of candidate tokens
      - Higher ref coverage
      - Shorter/cleaner strings when scores are close
    """
    ref0 = normalize_unicode_text(ref)
    if not ref0:
        return "", 0.0, "", ""

    # Build unique titles -> sources + kind presence
    title_to_sources: Dict[str, Set[str]] = {}
    for c in candidates:
        t = normalize_unicode_text(c.title)
        if not t:
            continue
        k = norm_title_key(t)
        if not k:
            continue
        title_to_sources.setdefault(k, set()).add(c.source)

    # Candidate pool: apply the same high-level rejects used in matching.
    unique_titles: List[str] = []
    seen_k: Set[str] = set()
    for c in candidates:
        t = normalize_unicode_text(c.title)
        if not t:
            continue
        k = norm_title_key(t)
        if not k or k in seen_k:
            continue
        seen_k.add(k)

        # Apply absolute rejects
        if is_nav_prefix_title(t):
            continue
        if is_mega_string_candidate(t):
            continue
        if (not has_year_token(ref0)) and has_year_token(t):
            continue

        # Must-carry constraint: never allow african-only candidates to match strong refs.
        if not candidate_eligible_under_strong_ref(ref0, t, allow_black_credential_exception=False):
            continue

        # Patch B2: hard fragment filter for Patch1 winner selection.
        # Prevent titles like "and Gender Studies" from winning under taxonomy_backstop.
        if is_fragment_candidate(t):
            continue

        unique_titles.append(t)

    if not unique_titles:
        return "", 0.0, "", ""

    # Score all candidates
    scored: List[Tuple[str, float, float, bool, str]] = []
    # tuple: (title, score, coverage, subset, class)
    for t in unique_titles:
        sc, cov, subset = _score_pair_for_winner(ref0, t)
        srcs = title_to_sources.get(norm_title_key(t), set())
        cls = candidate_class(t, srcs)
        scored.append((t, float(sc), float(cov), bool(subset), cls))

    # Helper: prefer on-site sources when available
    def _is_onsite(srcs: Set[str]) -> bool:
        return bool(srcs.intersection({"crawl", "signal", "both"}))

    def _pick_best(pool: List[Tuple[str, float, float, bool, str]], reason: str, prefer_onsite: bool) -> Tuple[str, float, str, str]:
        if not pool:
            return "", 0.0, "", ""

        # If requested, restrict to on-site if any exist.
        if prefer_onsite:
            onsite_pool = []
            for t, sc, cov, subset, cls in pool:
                srcs = title_to_sources.get(norm_title_key(t), set())
                if _is_onsite(srcs):
                    onsite_pool.append((t, sc, cov, subset, cls))
            if onsite_pool:
                pool = onsite_pool

        # Sort by: score desc, subset desc, coverage desc, length asc
        pool_sorted = sorted(
            pool,
            key=lambda x: (-x[1], -int(x[3]), -x[2], len(normalize_unicode_text(x[0]))),
        )

        # When scores are very close, prefer shorter/cleaner strings
        best = pool_sorted[0]
        best_title, best_score, best_cov, best_subset, best_cls = best

        best_src = _sources_for_title(candidates, best_title)
        return best_title, float(best_score), best_src, reason

    # 1) primary academic above threshold
    prim = [(t, sc, cov, subset, cls) for (t, sc, cov, subset, cls) in scored if cls == "primary_academic" and sc >= float(primary_thresh)]
    bt, bs, bsrc, breason = _pick_best(prim, "patch1:primary_academic", prefer_onsite=True)
    if bt:
        return bt, bs, bsrc, breason

    # 2) secondary org (no threshold beyond >0)
    sec = [(t, sc, cov, subset, cls) for (t, sc, cov, subset, cls) in scored if cls == "secondary_org" and sc > 0.0]
    bt, bs, bsrc, breason = _pick_best(sec, "patch1:secondary_org", prefer_onsite=True)
    if bt:
        return bt, bs, bsrc, breason

    # 3) taxonomy backstop
    tax = [(t, sc, cov, subset, cls) for (t, sc, cov, subset, cls) in scored if cls == "taxonomy" and sc > 0.0]
    bt, bs, bsrc, breason = _pick_best(tax, "patch1:taxonomy_backstop", prefer_onsite=False)
    if bt:
        return bt, bs, bsrc, breason

    # If nothing classified, fall back to best-scoring non-fragment title
    other = [(t, sc, cov, subset, cls) for (t, sc, cov, subset, cls) in scored if sc > 0.0 and not is_fragment_candidate(t)]
    bt, bs, bsrc, breason = _pick_best(other, "patch1:best_available", prefer_onsite=True)
    return bt, bs, bsrc, breason

# ============================================================
# Family-only rescue stage (gated, low-lexical-overlap)
# ============================================================

# Anchor keywords required for the rescue gate. This prevents overly-broad family matches.
_FAMILY_RESCUE_ANCHOR_RX = re.compile(
    r"\b("
    r"race|racial|ethnic|ethnicity|diaspora|"
    r"african|africana|black|"
    r"latinx|indigenous|sovereignty|"
    r"queer|gender|women"
    r")\b",
    re.I,
)

# Families that are considered strong enough to trigger rescue when present in the ref.
_FAMILY_RESCUE_STRONG_FAMS: Set[str] = {"ethnic", "race", "black", "africana"}


def _family_rescue_anchor_hit(title: str) -> str:
    """Return the matching anchor keyword (lowercased) if present, else empty."""
    t = normalize_unicode_text(title)
    m = _FAMILY_RESCUE_ANCHOR_RX.search(t) if t else None
    return (m.group(1).lower() if m else "")

def family_only_rescue_match(
    ref: str,
    candidates: List[Candidate],
    kind_pref: str,
    threshold: float = 0.52,
    *,
    allow_category_mapping: bool = False,
) -> Tuple[bool, str, float, str]:
    """Family-only rescue stage.

    Purpose: allow a lower similarity threshold when *direct* domain-family overlap is strong,
    but lexical overlap is otherwise weak.

    Gating:
      - Ref must contain at least one "strong" family (ethnic/race/black/africana)
      - Candidate must share a DIRECT overlapping family with the ref (not merely related)
      - Candidate must contain at least one rescue anchor keyword
      - Candidate must be non-fragment (>=2 content tokens)
      - Candidate must pass standard nonprogram/nav/year/mega-string filters

    Additional guards:
      - Structural mismatch: reject program ↔ credentialed item (minor/certificate/etc.) unless both sides agree.
      - CV taxonomy/category labels are rejected by default (unless allow_category_mapping=True).
    """
    ref0 = normalize_unicode_text(ref)
    if not ref0:
        return False, "", 0.0, ""

    f_ref = domain_families_present(ref0)
    if not f_ref or not (f_ref.intersection(_FAMILY_RESCUE_STRONG_FAMS)):
        return False, "", 0.0, ""

    ref_has_cred = has_credential_marker(ref0)

    # Work with Candidate objects so we can apply CV taxonomy heuristics using source.
    pool_cands: List[Candidate] = []
    seen: Set[str] = set()
    for c in candidates:
        if c.kind != kind_pref:
            continue
        k = norm_title_key(c.title)
        if not k or k in seen:
            continue
        seen.add(k)
        pool_cands.append(c)

    # Standard filters (mirror fuzzy/related tiers)
    pool_cands = [c for c in pool_cands if not is_nav_prefix_title(c.title)]
    pool_cands = [c for c in pool_cands if not is_mega_string_candidate(c.title)]

    if not has_year_token(ref0):
        pool_cands = [c for c in pool_cands if not has_year_token(c.title)]

    # Anti-nonprogram eligibility filtering (same policy as other tiers)
    if kind_pref == "program":
        pool_cands = [c for c in pool_cands if not is_nonprogram_title(c.title)]
    else:
        if not has_signal_marker(ref0):
            pool_cands = [c for c in pool_cands if not is_nonprogram_title(c.title)]
        else:
            pool_cands = [c for c in pool_cands if (not is_nonprogram_title(c.title)) or has_signal_marker(c.title)]

    if not pool_cands:
        return False, "", 0.0, ""

    best_score = 0.0
    best_title = ""
    best_detail = ""

    for c in pool_cands:
        cand0 = normalize_unicode_text(c.title)
        if not cand0:
            continue

        # Structural mismatch guard: program ↔ credentialed item (minor/cert/etc.)
        cand_has_cred = has_credential_marker(cand0)
        if ref_has_cred != cand_has_cred:
            continue

        # Default: reject CV taxonomy/category labels unless explicitly allowed.
        if (not allow_category_mapping) and c.source == "cv" and is_cv_taxonomy_label(cand0):
            continue

        # Must-carry constraint: protect strong black/africana refs
        if not candidate_eligible_under_strong_ref(ref0, cand0, allow_black_credential_exception=False):
            continue

        # Fragment suppression
        cand_tok = _content_tokens(cand0)
        if len(cand_tok) < 2 or is_fragment_candidate(cand0):
            continue

        # Direct family overlap only (not just related)
        f_cand = domain_families_present(cand0)
        if not f_cand:
            continue
        fam_overlap = f_ref.intersection(f_cand)
        if not fam_overlap:
            continue

        # Anchor keyword requirement
        anchor = _family_rescue_anchor_hit(cand0)
        if not anchor:
            continue

        sc, a_best, b_best = best_partial_title_match(
            [ref0],
            [cand0],
            use_synonyms=True,
            drop_parenthetical=True,
        )

        if sc > best_score:
            best_score = float(sc)
            best_title = b_best or cand0
            best_detail = (
                f'family_rescue: ref="{ref0}" ~ cand="{best_title}" score={best_score:.2f} '
                f'anchor={anchor} fam_ref={sorted(f_ref)} fam_cand={sorted(f_cand)} fam_overlap={sorted(fam_overlap)}'
            )

    if best_title and best_score >= float(threshold):
        return True, best_title, best_score, best_detail

    return False, "", best_score, ""


# Normalize dash variants for family extraction (domain_families_present operates on a
# lightly normalized string, not the loose key). This ensures patterns like
# `multi[-\s]?ethnic` match “Multi‑Ethnic” / “Multi–Ethnic” / etc.
_DASH_VARIANTS = re.compile(r"[\u2010\u2011\u2012\u2013\u2014\u2015\u2212]")


def _preprocess_for_family_extraction(s: str) -> str:
    s0 = normalize_unicode_text(s)
    if not s0:
        return ""
    # Standardize unicode dash variants to ASCII hyphen so family regexes can match reliably.
    s0 = _DASH_VARIANTS.sub("-", s0)
    s0 = _WS_RE.sub(" ", s0).strip()
    return s0


def domain_families_present(s: str) -> Set[str]:
    s0 = _preprocess_for_family_extraction(s)
    if not s0:
        return set()
    fams: Set[str] = set()
    for fam, pats in DOMAIN_FAMILY_PATTERNS.items():
        for rx in pats:
            if rx.search(s0):
                fams.add(fam)
                break
    return fams


def domain_related(f1: Set[str], f2: Set[str]) -> bool:
    if not f1 or not f2:
        return False
    for a in f1:
        rel = RELATED_DOMAIN_FAMILIES.get(a, {a})
        if rel.intersection(f2):
            return True
    return False


def domain_bonus(ref: str, cand: str, ref_tok: Set[str], cand_tok: Set[str]) -> float:
    """Small bonus for strong domain-keyword alignment.

    Applies only when both sides have at least 2 content tokens to avoid rewarding
    fragment candidates (e.g., "Ethnic").
    """
    if len(ref_tok) < 2 or len(cand_tok) < 2:
        return 0.0

    f_ref = domain_families_present(ref)
    f_cand = domain_families_present(cand)
    if not f_ref or not f_cand:
        return 0.0

    # Asymmetry penalty: ref is black/africana but candidate is african-only.
    # This prevents "African Studies" from scoring like a near-match to "Africana/Black" programs.
    if ("black" in f_ref or "africana" in f_ref) and ("african" in f_cand) and ("black" not in f_cand) and ("africana" not in f_cand):
        return -0.10

    # Strong bonus when families overlap directly
    if f_ref.intersection(f_cand):
        return 0.12

    # Smaller bonus for related families (e.g., africana ~ african)
    if domain_related(f_ref, f_cand):
        return 0.06

    return 0.0


def best_partial_title_match(
    a_titles: List[str],
    b_titles: List[str],
    use_synonyms: bool = False,
    drop_parenthetical: bool = False,
) -> Tuple[float, str, str]:
    """Return best partial match score and the (a_title, b_title) pair.

    Score is max(token overlap coefficient, SequenceMatcher ratio) on loose-normalized strings.

    If `use_synonyms` is True, apply `apply_synonym_map()` BEFORE loose normalization.
    If `drop_parenthetical` is True, drop parenthetical segments before scoring.

    This is used ONLY for partial concordance + change tracking.
    """
    best_score = 0.0
    best_a = ""
    best_b = ""

    for a in (a_titles or []):
        a_raw = normalize_unicode_text(a)
        if not a_raw:
            continue
        a_can = canonicalize_program_title(a_raw, drop_parenthetical=drop_parenthetical)
        a_pre = apply_synonym_map(a_can) if use_synonyms else a_can
        a_loose = norm_title_key_loose(a_pre)
        a_tok = _content_tokens(a_pre)

        for b in (b_titles or []):
            b_raw = normalize_unicode_text(b)
            if not b_raw:
                continue
            # Reject/penalize obvious non-program navigation/marketing titles.
            reject_np, np_pen = nonprogram_penalty(a_raw, b_raw)
            if reject_np:
                continue
            b_can = canonicalize_program_title(b_raw, drop_parenthetical=drop_parenthetical)
            b_pre = apply_synonym_map(b_can) if use_synonyms else b_can
            b_loose = norm_title_key_loose(b_pre)
            b_tok = _content_tokens(b_pre)

            tok_score = _overlap_coeff(a_tok, b_tok)

            # Ref-coverage: how much of the reference token set is explained by the candidate
            coverage = (len(a_tok.intersection(b_tok)) / float(len(a_tok))) if a_tok else 0.0

            seq_score = (
                difflib.SequenceMatcher(None, a_loose, b_loose).ratio() if (a_loose and b_loose) else 0.0
            )

            base = max(tok_score, coverage, seq_score)

            # Apply soft non-program penalties (multiplicative)
            if np_pen > 0:
                base = max(0.0, base * (1.0 - np_pen))

            # Patch 6: soft demotion for boilerplate titles that still contain academic keywords.
            bp_pen = boilerplate_penalty_fraction(b_raw)
            if bp_pen > 0:
                base = max(0.0, base * (1.0 - bp_pen))

            # (1) Fragment / short-candidate penalty: discourage single-word or very short candidates
            # from winning as best matches (common artifact from over-splitting).
            short_penalty = 0.0
            if len(b_tok) < 2:
                short_penalty += 0.25
            if len(b_raw) < 14:
                short_penalty += 0.10
            if len(a_tok) >= 3 and len(b_tok) == 1:
                short_penalty += 0.20

            # Apply penalties multiplicatively to preserve ordering among strong candidates.
            if short_penalty > 0:
                base = max(0.0, base * (1.0 - min(0.60, short_penalty)))

            # Domain + intent bonuses (small, capped) + Patch 2 entity-type penalty
            bonus = domain_bonus(a_raw, b_raw, a_tok, b_tok)
            bonus += signal_intent_bonus(a_raw, b_raw)
            bonus += entity_type_penalty(a_raw, b_raw)

            score = min(1.0, float(base + bonus))

            if score > best_score:
                best_score = score
                best_a = a_raw
                best_b = b_raw

    return best_score, best_a, best_b

# ============================================================
# Patch 6: Boilerplate / navigational-title demotion
# ============================================================

# Phrases commonly emitted by hub pages that are not real program titles.
_BOILERPLATE_RX = re.compile(
    r"\b("
    r"check\s+out|prospective\s+students\s*:|welcome\s+to|"
    r"read\s+more|learn\s+more|please\s+visit|click\s+here|"
    r"find\s+out\s+more|explore\s+the"
    r")\b",
    re.I,
)

_BOILERPLATE_SAFE_KEYWORDS = re.compile(
    r"\b(studies|department|dept\.?|program|major|minor|concentration|certificate)\b",
    re.I,
)


def boilerplate_drop(title: str) -> bool:
    """Return True if `title` looks like boilerplate AND lacks strong academic keywords.

    Patch 6 rule: drop boilerplate outright unless it contains a strong keyword like 'Studies',
    'Department', 'Program', etc. If it contains a strong keyword, keep it but apply a penalty
    in scoring (see `boilerplate_penalty_fraction`).
    """
    t = normalize_unicode_text(title)
    if not t:
        return True
    if not _BOILERPLATE_RX.search(t):
        return False
    # Drop if it does not contain any strong academic keyword.
    return not bool(_BOILERPLATE_SAFE_KEYWORDS.search(t))


def boilerplate_penalty_fraction(title: str) -> float:
    """Return a soft penalty fraction for boilerplate titles that we still keep.

    Patch 6 rule: if boilerplate AND it *does* contain a strong academic keyword, demote it.
    """
    t = normalize_unicode_text(title)
    if not t:
        return 1.0
    if _BOILERPLATE_RX.search(t) and _BOILERPLATE_SAFE_KEYWORDS.search(t):
        return 0.25
    return 0.0

#
# Heuristic: titles that are likely marketing/navigation pages rather than program entities.
# For PROGRAM-kind pools, these should be rejected outright.
# For SIGNAL-kind pools, they are only allowed when BOTH ref and candidate are signal-like.
_NONPROGRAM_PREFIX = re.compile(
    r"^(why|about|welcome|visit|check\s+out|read\s+more|learn\s+more|prospective\s+students)\b",
    re.I,
)
_NONPROGRAM_CONTAINS = re.compile(
    r"\b("
    r"news|event|events|donate|faculty|staff|"
    r"calendar|meeting|committee\s+meeting|committee|"
    r"updated|please\s+visit|note\s*:|"
    r"curriculum\s+review"
    r")\b",
    re.I,
)

def is_nav_prefix_title(title: str) -> bool:
    """True if title starts with navigation/marketing prefixes (absolute reject in all pools)."""
    t = normalize_unicode_text(title)
    return bool(t and _NONPROGRAM_PREFIX.search(t))



def is_nonprogram_title(title: str) -> bool:
    """Return True if `title` looks like navigation/marketing/non-program content."""
    t = normalize_unicode_text(title)
    if not t:
        return True
    if is_nav_prefix_title(t):
        return True
    if _NONPROGRAM_CONTAINS.search(t):
        return True
    if t.endswith("?"):
        return True
    return False

# Year-like strings (e.g., 2025-26, 2025–2026, 2025/26, 2025)
_YEARISH = re.compile(r"\b(19\d{2}|20\d{2})(?:\s*[-/\u2010-\u2015]\s*\d{2,4})?\b")

def has_year_token(s: str) -> bool:
    """True if the string contains a year or year-range like 2025-26."""
    t = normalize_unicode_text(s)
    return bool(t and _YEARISH.search(t))


# ============================================================
# Mega-string reject heuristic
# ============================================================

# Candidates that are "everything on the page" (mega-strings) can score ~1.0 by accident.
# We reject them early to protect specificity.
_MEGA_NAV_PHRASES = re.compile(
    r"\b(welcome\s+to|prospective\s+students|check\s+out|read\s+more|learn\s+more|visit)\b",
    re.I,
)

# Count credential markers (allow repeats). This is intentionally broader than _CREDENTIAL_MARKERS.
_MEGA_CREDENTIAL_MARKERS = re.compile(
    r"\b(ba|b\.?a\.?|bs|b\.?s\.?|ma|m\.?a\.?|ms|m\.?s\.?|phd|ph\.?d\.?|bachelor|master|doctoral|major|minor|certificate|concentration|degree\s+type)\b",
    re.I,
)


def is_mega_string_candidate(title: str, *, token_threshold: int = 22) -> bool:
    """Return True if `title` looks like an over-concatenated mega-string.

    Heuristic:
      - Many tokens (>= token_threshold)
      - AND (multiple credential markers OR obvious nav phrases)
      - OR extremely long token counts (>= 30) regardless
    """
    t = normalize_unicode_text(title)
    if not t:
        return True

    toks = [x for x in norm_title_key(t).split() if x]
    n_tok = len(toks)

    if n_tok >= 30:
        return True

    if n_tok < token_threshold:
        return False

    cred_count = len(_MEGA_CREDENTIAL_MARKERS.findall(t))
    has_nav = bool(_MEGA_NAV_PHRASES.search(t))

    # Require at least some additional evidence beyond just being long.
    if has_nav:
        return True
    if cred_count >= 2:
        return True

    return False


def nonprogram_penalty(ref: str, cand: str) -> Tuple[bool, float]:
    """Return (reject, penalty_fraction).

    This function is used inside similarity scoring loops.
    """
    r_sig = has_signal_marker(ref)
    c_sig = has_signal_marker(cand)

    t = normalize_unicode_text(cand)
    if not t:
        return True, 1.0

    # Absolute reject for nav/marketing prefix titles in ALL pools.
    if is_nav_prefix_title(t):
        return True, 1.0

    # For other nonprogram (news/events/donate/faculty/etc.), only allow when BOTH ref and cand are signal-like.
    if is_nonprogram_title(t):
        if r_sig and c_sig:
            return False, 0.0
        return True, 1.0

    # Patch 6: demote (or drop) boilerplate titles.
    # Drop is handled by upstream filters where possible; here we apply a soft penalty for
    # boilerplate strings that still contain strong academic keywords.
    bp_pen = boilerplate_penalty_fraction(t)
    if bp_pen > 0:
        return False, bp_pen

    return False, 0.0

# ============================================================
# Match ladder
# ============================================================


def _sources_for_title(cands: List[Candidate], title: str) -> str:
    """Return crawl/cv/signal/both/unknown summary for a title."""
    k = norm_title_key(title)
    srcs = sorted({c.source for c in cands if norm_title_key(c.title) == k})

    # normalize crawl/cv present alongside signal
    has_crawl = "crawl" in srcs
    has_cv = "cv" in srcs

    if has_crawl and has_cv:
        return "both"
    if has_crawl:
        return "crawl"
    if has_cv:
        return "cv"
    if "signal" in srcs:
        return "signal"
    if srcs:
        return srcs[0]
    return "unknown"


def _kinds_for_title(cands: List[Candidate], title: str) -> Set[str]:
    k = norm_title_key(title)
    return {c.kind for c in cands if norm_title_key(c.title) == k}


def _pick_best_of_kind(
    ref: str,
    candidates: List[Candidate],
    kind_pref: str,
    level: str,
    detail_label: str,
    threshold: float,
    use_synonyms: bool,
    canonical: bool,
    allow_category_mapping: bool = False,
) -> Tuple[bool, str, float, str]:
    """Try to match within candidates restricted by kind.

    Returns (ok, best_title, score, detail). Always returns diagnostic `detail` even when no match is returned.

    Fuzzy scoring uses max(token overlap, ref-coverage, SequenceMatcher) plus a small domain-keyword bonus (capped at 1.0).
    """
    ref = normalize_unicode_text(ref)
    if not ref:
        return False, "", 0.0, ""

    def _diag_score_pair(r: str, c: str) -> float:
        """Diagnostic-only similarity score used to explain filtered/suppressed candidates.

        IMPORTANT: This MUST NOT affect match outcomes. It intentionally ignores
        nonprogram/mega/year rejection logic and fragment suppression.
        """
        r0 = normalize_unicode_text(r)
        c0 = normalize_unicode_text(c)
        if not r0 or not c0:
            return 0.0

        drop_paren = True if canonical else False

        r_can = canonicalize_program_title(r0, drop_parenthetical=drop_paren)
        c_can = canonicalize_program_title(c0, drop_parenthetical=drop_paren)

        r_pre = apply_synonym_map(r_can) if use_synonyms else r_can
        c_pre = apply_synonym_map(c_can) if use_synonyms else c_can

        r_loose = norm_title_key_loose(r_pre)
        c_loose = norm_title_key_loose(c_pre)

        r_tok = _content_tokens(r_pre)
        c_tok = _content_tokens(c_pre)

        tok_score = _overlap_coeff(r_tok, c_tok)
        coverage = (len(r_tok.intersection(c_tok)) / float(len(r_tok))) if r_tok else 0.0
        seq_score = difflib.SequenceMatcher(None, r_loose, c_loose).ratio() if (r_loose and c_loose) else 0.0

        base = max(tok_score, coverage, seq_score)
        bonus = domain_bonus(r0, c0, r_tok, c_tok) + signal_intent_bonus(r0, c0) + entity_type_penalty(r0, c0)
        return float(min(1.0, max(0.0, base + bonus)))

    raw_pool = [c.title for c in candidates if c.kind == kind_pref]
    raw_pool = _dedupe_preserve_order(raw_pool)

    def _title_sources(title: str) -> Set[str]:
        k = norm_title_key(title)
        return {c.source for c in candidates if norm_title_key(c.title) == k}

    removed: Dict[str, List[str]] = {}

    def _apply_filter(name: str, items: List[str], keep_fn) -> List[str]:
        kept: List[str] = []
        dropped: List[str] = []
        for it in items:
            if keep_fn(it):
                kept.append(it)
            else:
                dropped.append(it)
        if dropped:
            removed.setdefault(name, []).extend(dropped)
        return kept

    pool = list(raw_pool)

    # Absolute rejects (always removed)
    pool = _apply_filter("nav_prefix", pool, lambda t: not is_nav_prefix_title(t))
    pool = _apply_filter("mega_string", pool, lambda t: not is_mega_string_candidate(t))
    pool = _apply_filter("boilerplate_drop", pool, lambda t: not boilerplate_drop(t))
    # Reject year-tagged titles unless the reference itself includes a year.
    if not has_year_token(ref):
        pool = _apply_filter("year_token", pool, lambda t: not has_year_token(t))

    # Nonprogram filtering policy:
    # - Program-kind pools: reject any nonprogram title outright.
    # - Signal-kind pools: allow nonprogram ONLY when BOTH ref and candidate are signal-like.
    if kind_pref == "program":
        pool = _apply_filter("nonprogram", pool, lambda t: not is_nonprogram_title(t))
    else:
        if not has_signal_marker(ref):
            pool = _apply_filter("nonprogram", pool, lambda t: not is_nonprogram_title(t))
        else:
            pool = _apply_filter("nonprogram", pool, lambda t: (not is_nonprogram_title(t)) or has_signal_marker(t))

    # Patch 3: CV taxonomy labels are backstop-only unless explicitly allowed.
    # If any on-site candidates (crawl/signal) survive filtering, drop CV taxonomy labels.
    if (not allow_category_mapping) and kind_pref == "program":
        try:
            has_onsite_survivor = any(
                ("cv" not in _title_sources(t)) or (len(_title_sources(t)) > 1)
                for t in pool
            )
        except Exception:
            has_onsite_survivor = False

        if has_onsite_survivor:
            pool = _apply_filter(
                "cv_taxonomy_backstop",
                pool,
                lambda t: not ("cv" in _title_sources(t) and _title_sources(t) == {"cv"} and is_cv_taxonomy_label(t)),
            )

    # Must-carry constraint: never allow african-only candidates to match strong refs (black/africana/african-american)
    # for ANY fuzzy mode.
    if level not in {"strict_raw", "strict_canonical"}:
        pool = _apply_filter(
            "strong_ref_gate",
            pool,
            lambda t: candidate_eligible_under_strong_ref(ref, t, allow_black_credential_exception=False),
        )

    if not pool:
        # Diagnostic-only: report the best-looking excluded candidate and why it was excluded.
        best_reason = ""
        best_cand = ""
        best_sc = 0.0
        for reason, items in removed.items():
            for it in items:
                sc = _diag_score_pair(ref, it)
                if sc > best_sc:
                    best_sc = sc
                    best_cand = it
                    best_reason = reason

        if best_cand:
            return (
                False,
                "",
                float(best_sc),
                f'{detail_label}: rejected by {best_reason} filter cand="{best_cand}" score={best_sc:.2f}',
            )

        return False, "", 0.0, ""

    # STRICT levels
    if level == "strict_raw":
        ref_k = norm_title_key(ref)
        keys = {norm_title_key(t): t for t in pool}
        if ref_k in keys:
            t = keys[ref_k]
            return True, t, 1.0, f'{detail_label}: ref="{ref}" == cand="{t}"'
        return False, "", 0.0, ""

    if level == "strict_canonical":
        ref_can = canonicalize_program_title(ref, drop_parenthetical=True)
        ref_k = norm_title_key(ref_can)
        keys = {norm_title_key(canonicalize_program_title(t, drop_parenthetical=True)): t for t in pool}
        if ref_k in keys:
            t = keys[ref_k]
            return True, t, 1.0, f'{detail_label}: ref="{ref}" == cand="{t}"'
        return False, "", 0.0, ""

    # FUZZY levels
    drop_paren = True if canonical else False
    # Hard gate: if the candidate pool contains only fragment-like titles, we still score,
    # but we will not allow a fragment to be returned as the best match.
    fragment_keys = {norm_title_key(t) for t in pool if is_fragment_candidate(t)}

    score, a_best, b_best = best_partial_title_match(
        [ref],
        pool,
        use_synonyms=use_synonyms,
        drop_parenthetical=drop_paren,
    )

    if b_best:
        # Do not allow fragments to win as best matches.
        if norm_title_key(b_best) in fragment_keys:
            return (
                False,
                "",
                float(score),
                f'{detail_label}: suppressed fragment candidate cand="{b_best}" score={score:.2f}',
            )

        if score >= float(threshold):
            detail = f'{detail_label}: ref="{a_best}" ~ cand="{b_best}" score={score:.2f}'
            return True, b_best, float(score), detail

        # Diagnostic-only: below threshold but still report best pair.
        return (
            False,
            "",
            float(score),
            f'{detail_label}: below_threshold ref="{a_best}" ~ cand="{b_best}" score={score:.2f}',
        )

    return False, "", float(score), ""




def related_domain_backstop_match(
    ref: str,
    candidates: List[Candidate],
    kind_pref: str,
    threshold: float = 0.62,
    *,
    allow_category_mapping: bool = False,
) -> Tuple[bool, str, float, str]:
    """Best-available related match used only when all other modes fail.

    Gated by domain-family presence and requires non-fragment candidates.
    CV taxonomy/category labels are rejected by default unless allow_category_mapping=True, and credential mismatch is blocked.
    """
    ref0 = normalize_unicode_text(ref)
    if not ref0:
        return False, "", 0.0, ""

    f_ref = domain_families_present(ref0)
    if not f_ref:
        return False, "", 0.0, ""

    ref_has_cred = has_credential_marker(ref0)

    # Candidate pool by kind preference (preserve Candidate objects so we can apply CV taxonomy heuristics)
    pool_cands: List[Candidate] = []
    seen: Set[str] = set()
    for c in candidates:
        if c.kind != kind_pref:
            continue
        k = norm_title_key(c.title)
        if not k or k in seen:
            continue
        seen.add(k)
        pool_cands.append(c)

    # Standard filters
    pool_cands = [c for c in pool_cands if not is_nav_prefix_title(c.title)]
    pool_cands = [c for c in pool_cands if not is_mega_string_candidate(c.title)]

    # Reject year-tagged titles unless the reference itself includes a year.
    if not has_year_token(ref0):
        pool_cands = [c for c in pool_cands if not has_year_token(c.title)]

    # Anti-nonprogram eligibility filtering (same policy as fuzzy pools)
    if kind_pref == "program":
        pool_cands = [c for c in pool_cands if not is_nonprogram_title(c.title)]
    else:
        if not has_signal_marker(ref0):
            pool_cands = [c for c in pool_cands if not is_nonprogram_title(c.title)]
        else:
            pool_cands = [c for c in pool_cands if (not is_nonprogram_title(c.title)) or has_signal_marker(c.title)]

    if not pool_cands:
        return False, "", 0.0, ""

    best_score = 0.0
    best_title = ""
    best_detail = ""

    # Use the same partial matcher scoring (now includes coverage + domain bonus)
    for c in pool_cands:
        cand0 = normalize_unicode_text(c.title)
        if not cand0:
            continue
        # Structural mismatch guard: block program ↔ credentialed item (minor/cert/etc.) unless both sides agree.
        cand_has_cred = has_credential_marker(cand0)
        if ref_has_cred != cand_has_cred:
            continue

        # Default: reject CV taxonomy/category labels unless explicitly allowed.
        if (not allow_category_mapping) and c.source == "cv" and is_cv_taxonomy_label(cand0):
            continue

        # Must-carry constraint: strong refs cannot backstop-match to african-only candidates.
        if not candidate_eligible_under_strong_ref(ref0, cand0, allow_black_credential_exception=False):
            continue

        # Fragment suppression: require >=2 content tokens on candidate
        ref_tok = _content_tokens(ref0)
        cand_tok = _content_tokens(cand0)
        if len(cand_tok) < 2:
            continue

        # If the 2013 ref is signal-like, require the candidate to also look signal-like
        # for the signal-kind backstop. This prevents program titles from stealing signal intent.
        if has_signal_marker(ref0) and kind_pref == "signal" and not has_signal_marker(cand0):
            continue

        f_cand = domain_families_present(cand0)
        if not f_cand:
            continue

        # Must be same-family or related-family
        if not (f_ref.intersection(f_cand) or domain_related(f_ref, f_cand)):
            continue

        sc, a_best, b_best = best_partial_title_match(
            [ref0],
            [cand0],
            use_synonyms=True,
            drop_parenthetical=True,
        )

        if sc > best_score:
            best_score = float(sc)
            best_title = b_best or cand0
            best_detail = (
                f'related_domain_backstop: ref="{ref0}" ~ cand="{best_title}" score={best_score:.2f} '
                f'fam_ref={sorted(f_ref)} fam_cand={sorted(f_cand)}'
            )

    if best_title and best_score >= float(threshold):
        return True, best_title, best_score, best_detail

    return False, "", best_score, ""

# --- related_credential tier ---
_CREDENTIAL_MARKERS = re.compile(r"\b(minor|major|certificate|concentration)\b", re.I)

def has_credential_marker(s: str) -> bool:
    """True if the title explicitly encodes a credential type (minor/major/certificate/concentration)."""
    t = normalize_unicode_text(s)
    return bool(t and _CREDENTIAL_MARKERS.search(t))

def related_credential_tier_match(
    ref: str,
    candidates: List[Candidate],
    kind_pref: str,
    threshold: float,
) -> Tuple[bool, str, float, str]:
    """Related-but-not-same credential tier.

    Fires only when:
      - ref is in a domain family (black/africana/african/ethnic/race)
      - candidate contains an in-family keyword (domain family present)
      - candidate contains a credential marker (minor/major/certificate/concentration)
      - candidate is not a fragment (>=2 content tokens)
      - score clears a slightly-lower-than-fuzzy threshold
    """
    ref0 = normalize_unicode_text(ref)
    if not ref0:
        return False, "", 0.0, ""

    f_ref = domain_families_present(ref0)
    if not f_ref:
        return False, "", 0.0, ""

    pool = _dedupe_preserve_order([c.title for c in candidates if c.kind == kind_pref])
    pool = [t for t in pool if not is_nav_prefix_title(t)]
    pool = [t for t in pool if not is_mega_string_candidate(t)]

    # Reject year-tagged titles unless the reference itself includes a year.
    if not has_year_token(ref0):
        pool = [t for t in pool if not has_year_token(t)]
    # Anti-nonprogram eligibility filtering (related_credential is still a program-tier)
    if kind_pref == "program":
        pool = [t for t in pool if not is_nonprogram_title(t)]
    else:
        if not has_signal_marker(ref0):
            pool = [t for t in pool if not is_nonprogram_title(t)]
        else:
            pool = [t for t in pool if (not is_nonprogram_title(t)) or has_signal_marker(t)]
    if not pool:
        return False, "", 0.0, ""

    best_score = 0.0
    best_title = ""
    best_detail = ""

    ref_tok = _content_tokens(ref0)
    if len(ref_tok) < 2:
        return False, "", 0.0, ""

    for cand in pool:
        cand0 = normalize_unicode_text(cand)
        if not cand0:
            continue
        # Must-carry constraint with a narrow exception: allow only credentialed candidates that contain "black"
        # (e.g., "Black American Studies Minor"), but never plain "African Studies".
        if not candidate_eligible_under_strong_ref(ref0, cand0, allow_black_credential_exception=True):
            continue

        # Must look like a credentialed program item
        if not _CREDENTIAL_MARKERS.search(cand0):
            continue

        cand_tok = _content_tokens(cand0)
        if len(cand_tok) < 2:
            continue

        f_cand = domain_families_present(cand0)
        if not f_cand:
            continue

        # Require same-family or related-family alignment
        if not (f_ref.intersection(f_cand) or domain_related(f_ref, f_cand)):
            continue

        sc, a_best, b_best = best_partial_title_match(
            [ref0],
            [cand0],
            use_synonyms=True,
            drop_parenthetical=True,
        )

        if sc > best_score:
            best_score = float(sc)
            best_title = b_best or cand0
            best_detail = (
                f'related_credential: ref="{ref0}" ~ cand="{best_title}" score={best_score:.2f} '
                f'fam_ref={sorted(f_ref)} fam_cand={sorted(f_cand)}'
            )

    if best_title and best_score >= float(threshold):
        return True, best_title, best_score, best_detail

    return False, "", best_score, ""


def match_2013_to_candidates(
    t2013: str,
    candidates: List[Candidate],
    fuzzy_threshold: float = 0.80,
    *,
    allow_category_mapping: bool = False,
) -> Tuple[str, str, str, str, float, str]:
    """Return best match tuple:

      (best_title, best_source, best_kind, match_level, match_score, detail)
    """
    t2013 = normalize_unicode_text(t2013)
    if not t2013:
        return "", "", "", "NO_MATCH", 0.0, ""

    is_sig_2013 = has_signal_marker(t2013)

    # primary kind preference based on the 2013 string
    # If 2013 is signal-like, we heavily prefer signal candidates.
    primary_kind = "signal" if is_sig_2013 else "program"
    secondary_kind = "program" if primary_kind == "signal" else "signal"

    def _resolve_best_kind(chosen_kind_pref: str, kinds: Set[str]) -> str:
        """Resolve output kind for a matched title.

        - If the chosen ladder step was signal-preferred, and the title exists as a signal candidate, report signal.
        - Else, if the title exists only as signal, report signal.
        - Otherwise report program.
        """
        if chosen_kind_pref == "signal" and "signal" in kinds:
            return "signal"
        if "signal" in kinds and "program" not in kinds:
            return "signal"
        return "program"

    ladder = [
        ("strict_raw", primary_kind, 1.0, False, False),
        ("strict_raw", secondary_kind, 1.0, False, False),
        ("strict_canonical", primary_kind, 1.0, False, True),
        ("strict_canonical", secondary_kind, 1.0, False, True),
        ("fuzzy_raw", primary_kind, fuzzy_threshold, False, False),
        ("fuzzy_raw", secondary_kind, fuzzy_threshold, False, False),
        ("fuzzy_canonical", primary_kind, fuzzy_threshold, False, True),
        ("fuzzy_canonical", secondary_kind, fuzzy_threshold, False, True),
        ("fuzzy_syn", primary_kind, fuzzy_threshold, True, True),
        ("fuzzy_syn", secondary_kind, fuzzy_threshold, True, True),
    ]

    best_diag_detail = ""
    best_diag_score = 0.0

    chosen_best: Optional[Tuple[str, str, str, str, float, str]] = None
    for level, kind, thr, use_syn, use_can in ladder:
        ok, best_title, score, detail = _pick_best_of_kind(
            t2013,
            candidates,
            kind_pref=kind,
            level="strict_raw" if level == "strict_raw" else ("strict_canonical" if level == "strict_canonical" else "fuzzy"),
            detail_label=level,
            threshold=thr,
            use_synonyms=use_syn,
            canonical=use_can,
            allow_category_mapping=allow_category_mapping,
        )

        if (not ok) and detail and float(score) >= float(best_diag_score):
            best_diag_score = float(score)
            best_diag_detail = detail

        # The helper uses "fuzzy" for fuzzy modes; map to ladder label for output.
        if ok and best_title:
            best_source = _sources_for_title(candidates, best_title)
            kinds = _kinds_for_title(candidates, best_title)
            best_kind = _resolve_best_kind(kind, kinds)

            out_level = level
            out_score = 1.0 if level.startswith("strict") else float(score)
            out_detail = detail if detail else f"{level}: matched"
            chosen_best = (best_title, best_source, best_kind, out_level, out_score, out_detail)
            break

    # Tier: Patch 4 rename-family rescue (Africana / African-American / Black Studies)
    # Only fires when ref indicates rename-family intent, and only considers primary-academic candidates.
    ok, best_title, score, detail = rename_family_rescue_match(
        t2013,
        candidates,
        kind_pref=primary_kind,
        threshold=0.55,
        allow_category_mapping=allow_category_mapping,
    )
    if not ok:
        ok, best_title, score, detail = rename_family_rescue_match(
            t2013,
            candidates,
            kind_pref=secondary_kind,
            threshold=0.55,
            allow_category_mapping=allow_category_mapping,
        )

    if ok and best_title:
        best_source = _sources_for_title(candidates, best_title)
        kinds = _kinds_for_title(candidates, best_title)
        best_kind = _resolve_best_kind(primary_kind, kinds)
        chosen_best = (best_title, best_source, best_kind, "rename_family_rescue", float(score), detail)

    # Tier: related-but-not-same credential match (only after all strict/fuzzy modes fail)
    f_ref = domain_families_present(t2013)
    if ("ethnic" in f_ref) or ("race" in f_ref):
        related_credential_threshold = max(0.66, min(0.76, float(fuzzy_threshold) - 0.08))
    else:
        related_credential_threshold = max(0.70, min(0.78, float(fuzzy_threshold) - 0.06))

    ok, best_title, score, detail = related_credential_tier_match(
        t2013,
        candidates,
        kind_pref=primary_kind,
        threshold=related_credential_threshold,
    )
    if not ok:
        ok, best_title, score, detail = related_credential_tier_match(
            t2013,
            candidates,
            kind_pref=secondary_kind,
            threshold=related_credential_threshold,
        )

    if ok and best_title:
        best_source = _sources_for_title(candidates, best_title)
        kinds = _kinds_for_title(candidates, best_title)
        best_kind = _resolve_best_kind(primary_kind, kinds)
        chosen_best = (best_title, best_source, best_kind, "related_credential", float(score), detail)

    # Tier: family-only rescue (gated) — sits between related_credential and related_domain_backstop
    # Lower threshold is allowed only when direct family overlap + anchor constraints pass.
    if ("ethnic" in f_ref) or ("race" in f_ref):
        rescue_threshold = 0.50
    else:
        rescue_threshold = 0.52

    ok, best_title, score, detail = family_only_rescue_match(
        t2013,
        candidates,
        kind_pref=primary_kind,
        threshold=rescue_threshold,
        allow_category_mapping=allow_category_mapping,
    )
    if not ok:
        ok, best_title, score, detail = family_only_rescue_match(
            t2013,
            candidates,
            kind_pref=secondary_kind,
            threshold=rescue_threshold,
            allow_category_mapping=allow_category_mapping,
        )

    if ok and best_title:
        best_source = _sources_for_title(candidates, best_title)
        kinds = _kinds_for_title(candidates, best_title)
        best_kind = _resolve_best_kind(primary_kind, kinds)
        chosen_best = (best_title, best_source, best_kind, "family_rescue", float(score), detail)

    # Backstop: best-available related-domain match (only when nothing else matched)
    if ("ethnic" in f_ref) or ("race" in f_ref):
        related_threshold = max(0.50, min(0.58, float(fuzzy_threshold) - 0.22))
    else:
        related_threshold = max(0.50, min(0.62, float(fuzzy_threshold) - 0.15))

    # Prefer the same kind ordering used above
    ok, best_title, score, detail = related_domain_backstop_match(
        t2013,
        candidates,
        kind_pref=primary_kind,
        threshold=related_threshold,
        allow_category_mapping=allow_category_mapping,
    )
    if not ok:
        ok, best_title, score, detail = related_domain_backstop_match(
            t2013,
            candidates,
            kind_pref=secondary_kind,
            threshold=related_threshold,
            allow_category_mapping=allow_category_mapping,
        )

    if ok and best_title:
        best_source = _sources_for_title(candidates, best_title)
        kinds = _kinds_for_title(candidates, best_title)
        best_kind = _resolve_best_kind(primary_kind, kinds)
        chosen_best = (best_title, best_source, best_kind, "related_domain_backstop", float(score), detail)

    # ------------------------------------------------------------
    # Patch 1: final winner-selection override
    # ------------------------------------------------------------
    # If we already have a selected match, we may override it to prefer the best
    # on-site primary academic title over secondary org titles and CV taxonomy labels.
    patch1_title, patch1_score, patch1_source, patch1_reason = select_best_title_patch1(
        t2013,
        candidates,
        primary_thresh=float(fuzzy_threshold),
    )

    if patch1_title:
        # If we had no match at all, adopt Patch 1 pick.
        if chosen_best is None:
            kinds = _kinds_for_title(candidates, patch1_title)
            best_kind = "signal" if ("signal" in kinds and "program" not in kinds) else "program"
            return patch1_title, patch1_source, best_kind, "winner_override", float(patch1_score), (
                f"winner_override: {patch1_reason} | ref=\"{normalize_unicode_text(t2013)}\" ~ cand=\"{patch1_title}\" score={patch1_score:.2f}"
            )

        # Otherwise, override only when Patch 1 yields a different, higher-precedence candidate
        # and is not meaningfully worse in score.
        cur_title, cur_source, cur_kind, cur_level, cur_score, cur_detail = chosen_best

        if norm_title_key(patch1_title) != norm_title_key(cur_title):
            # Allow a small score delta when upgrading precedence (e.g., department beats center).
            if float(patch1_score) >= float(cur_score) - 0.05:
                kinds = _kinds_for_title(candidates, patch1_title)
                best_kind = "signal" if ("signal" in kinds and "program" not in kinds) else "program"
                return patch1_title, patch1_source, best_kind, "winner_override", float(patch1_score), (
                    f"winner_override: replaced=\"{cur_title}\" ({cur_level}) -> \"{patch1_title}\"; {patch1_reason} | score={patch1_score:.2f}"
                )

        # If Patch 1 agrees with the chosen title, return the chosen result.
        return chosen_best

    # No Patch 1 candidate; return chosen_best if present.
    if chosen_best is not None:
        return chosen_best

    if chosen_best is not None:
        return chosen_best
    return "", "", "", "NO_MATCH", 0.0, (best_diag_detail or "")


def any_match_under_any_mode(
    t2013: str,
    candidates: List[Candidate],
    fuzzy_threshold: float = 0.80,
) -> Set[str]:
    """Return set of title keys that match 2013 under any mode.

    Used to compute discovered__new_titles_unmatched.
    """
    t2013 = normalize_unicode_text(t2013)
    if not t2013:
        return set()

    matched: Set[str] = set()
    pools = {
        "program": _dedupe_preserve_order([c.title for c in candidates if c.kind == "program"]),
        "signal": _dedupe_preserve_order([c.title for c in candidates if c.kind == "signal"]),
    }
    pools["program"] = [t for t in pools["program"] if not is_mega_string_candidate(t)]
    pools["signal"] = [t for t in pools["signal"] if not is_mega_string_candidate(t)]

    # Reject year-tagged titles unless the reference itself includes a year.
    if not has_year_token(t2013):
        pools["program"] = [t for t in pools["program"] if not has_year_token(t)]
        pools["signal"] = [t for t in pools["signal"] if not has_year_token(t)]

    # Apply the same anti-nonprogram eligibility filtering used by the match ladder.
    pools["program"] = [t for t in pools["program"] if not is_nonprogram_title(t)]

    if not has_signal_marker(t2013):
        pools["signal"] = [t for t in pools["signal"] if not is_nonprogram_title(t)]
    else:
        pools["signal"] = [
            t
            for t in pools["signal"]
            if (not is_nonprogram_title(t)) or has_signal_marker(t)
        ]

    # strict raw
    ref_k = norm_title_key(t2013)
    for kind, pool in pools.items():
        for t in pool:
            if kind == "signal" and is_nonprogram_title(t):
                if not (has_signal_marker(t2013) and has_signal_marker(t)):
                    continue
            if norm_title_key(t) == ref_k:
                matched.add(norm_title_key(t))

    # strict canonical
    ref_can = canonicalize_program_title(t2013, drop_parenthetical=True)
    ref_ck = norm_title_key(ref_can)
    for kind, pool in pools.items():
        for t in pool:
            if kind == "signal" and is_nonprogram_title(t):
                if not (has_signal_marker(t2013) and has_signal_marker(t)):
                    continue
            if norm_title_key(canonicalize_program_title(t, drop_parenthetical=True)) == ref_ck:
                matched.add(norm_title_key(t))

    # fuzzy raw
    for kind, pool in pools.items():
        sc, a_best, b_best = best_partial_title_match([t2013], pool, use_synonyms=False, drop_parenthetical=False)
        if sc >= float(fuzzy_threshold) and b_best:
            matched.add(norm_title_key(b_best))

    # fuzzy canonical
    for kind, pool in pools.items():
        sc, a_best, b_best = best_partial_title_match([t2013], pool, use_synonyms=False, drop_parenthetical=True)
        if sc >= float(fuzzy_threshold) and b_best:
            matched.add(norm_title_key(b_best))

    # fuzzy synonym
    for kind, pool in pools.items():
        sc, a_best, b_best = best_partial_title_match([t2013], pool, use_synonyms=True, drop_parenthetical=True)
        if sc >= float(fuzzy_threshold) and b_best:
            matched.add(norm_title_key(b_best))

    return matched


def _pipe_join(items: Sequence[str]) -> str:
    items = [normalize_unicode_text(x) for x in items if normalize_unicode_text(x)]
    return "|".join(items)


def build_output_for_row(
    row: pd.Series,
    fuzzy_threshold: float,
    allow_category_mapping: bool = False,
) -> Dict[str, object]:
    t2013, candidates, debug_recombined = parse_candidates_from_row(row)

    best_title, best_source, best_kind, match_level, match_score, detail = match_2013_to_candidates(
        t2013,
        candidates,
        fuzzy_threshold=fuzzy_threshold,
        allow_category_mapping=bool(allow_category_mapping),
    )

    is_sig_2013 = int(has_signal_marker(t2013))

    # Candidate summaries
    crawl_titles = _dedupe_preserve_order([c.title for c in candidates if c.source == "crawl" and c.kind == "program"])
    cv_titles = _dedupe_preserve_order([c.title for c in candidates if c.source == "cv" and c.kind == "program"])
    signal_titles = _dedupe_preserve_order([c.title for c in candidates if c.kind == "signal"])

    # display lists in canonical display form (keeping parentheticals)
    crawl_disp = [canonicalize_program_title(t) for t in crawl_titles]
    cv_disp = [canonicalize_program_title(t) for t in cv_titles]
    sig_disp = [canonicalize_program_title(t) for t in signal_titles]

    all_titles = _dedupe_preserve_order(crawl_titles + cv_titles + signal_titles)

    matched_keys_any = any_match_under_any_mode(t2013, candidates, fuzzy_threshold=fuzzy_threshold)
    new_unmatched = [t for t in all_titles if norm_title_key(t) not in matched_keys_any]

    new_program_when_best_signal: List[str] = []
    if best_kind == "signal":
        # show program-ish discovered titles besides the best match
        prog_all = _dedupe_preserve_order(crawl_titles + cv_titles)
        new_program_when_best_signal = [t for t in prog_all if norm_title_key(t) != norm_title_key(best_title)]

    return {
        "match_2013__best_title": best_title,
        "match_2013__best_source": best_source,
        "match_2013__best_kind": best_kind,
        "match_2013__match_level": match_level,
        "match_2013__match_score": float(match_score),
        "match_2013__detail": detail,
        "match_2013__is_signal_marker_in_2013": is_sig_2013,
        "debug__recombined_candidates_added": debug_recombined,
        "discovered__program_titles__crawl": _pipe_join(crawl_disp),
        "discovered__program_titles__cv": _pipe_join(cv_disp),
        "discovered__signal_titles": _pipe_join(sig_disp),
        "discovered__all_titles": _pipe_join(all_titles),
        "discovered__new_titles_unmatched": _pipe_join(new_unmatched),
        "discovered__new_program_titles_when_best_signal": _pipe_join(new_program_when_best_signal),
    }


#
# ============================================================
# Unit tests (family extraction ordering / dash handling)
# ============================================================

class TestDomainFamilyExtraction(unittest.TestCase):
    def test_ethnic_family_multicultural_gender_program(self):
        s = "Multicultural and Gender Studies Program"
        fams = domain_families_present(s)
        self.assertIn("ethnic", fams)

    def test_ethnic_family_multi_ethnic_program(self):
        # Use a dash that is common in scraped text (unicode hyphen or en dash)
        # to ensure our family extraction preprocessing is robust.
        for s in [
            "Multi-Ethnic Studies Program",          # ASCII hyphen
            "Multi‑Ethnic Studies Program",      # non-breaking hyphen
            "Multi–Ethnic Studies Program",      # en dash
            "Multi−Ethnic Studies Program",      # minus sign
        ]:
            fams = domain_families_present(s)
            self.assertIn("ethnic", fams, msg=f"failed for: {s!r}")


def _run_tests() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestDomainFamilyExtraction)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


def derive_output_path(input_path: str) -> str:
    base, ext = os.path.splitext(input_path)
    if not ext:
        ext = ".csv"
    return f"{base}__2013_current_matches{ext}"


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Match 2013 program name to current discovered titles within unitid",
        allow_abbrev=False,
    )
    ap.add_argument(
        "--input",
        default="ace_unitid_merge__ace_x_2013comp__webscrape__v15simple__bucketed_programs.csv",
        help="Input CSV (wide table)",
    )
    ap.add_argument(
        "--output",
        default="",
        help="Output CSV (default: input with __2013_current_matches suffix)",
    )
    ap.add_argument(
        "--fuzzy-threshold",
        type=float,
        default=0.80,
        help="Threshold for fuzzy matching (default: 0.80)",
    )
    ap.add_argument(
        "--allow-category-mapping",
        action="store_true",
        help="Allow CV taxonomy/category labels (e.g., 'Ethnic Studies') to be eligible in rescue/backstop tiers.",
    )
    ap.add_argument(
        "--run-tests",
        action="store_true",
        help="Run unit tests for family extraction / normalization and exit",
    )

    # In notebooks (ipykernel), sys.argv often includes `-f <kernel.json>`.
    # With `allow_abbrev=True` (argparse default), `-f` can be mis-read as an
    # abbreviation for `--fuzzy-threshold`. We disable abbreviation and ignore
    # unknown args when argv is None (i.e., using sys.argv).
    if argv is None:
        args, unknown = ap.parse_known_args()
        # If you run this as a script and pass unknown flags, surface them.
        # In IPython/Jupyter, `-f` is expected and should be ignored.
        if unknown:
            # ipykernel adds: ['-f', '/path/to/kernel.json']
            if not (len(unknown) == 2 and unknown[0] == "-f" and unknown[1].endswith(".json")):
                print(f"WARNING: ignoring unknown args: {unknown}", file=sys.stderr)
    else:
        args = ap.parse_args(argv)

    if getattr(args, "run_tests", False):
        return _run_tests()

    in_path = args.input
    out_path = args.output or derive_output_path(in_path)

    if not os.path.exists(in_path):
        print(f"ERROR: input not found: {in_path}", file=sys.stderr)
        return 2

    df = pd.read_csv(in_path, dtype=str, keep_default_na=False)

    # basic required columns
    required = ["unitid", "2013_program_name", "program_titles_found", "college_vine_program_titles_found"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"ERROR: missing required columns: {missing}", file=sys.stderr)
        return 2

    out_rows: List[Dict[str, object]] = []
    for _, row in df.iterrows():
        out_rows.append(
            build_output_for_row(
                row,
                fuzzy_threshold=float(args.fuzzy_threshold),
                allow_category_mapping=bool(getattr(args, "allow_category_mapping", False)),
            )
        )

    out_df = df.copy()
    for k in out_rows[0].keys() if out_rows else []:
        out_df[k] = [r.get(k, "") for r in out_rows]

    out_df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    #raise SystemExit(main())
    main()