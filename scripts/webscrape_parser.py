# --- Program Title Bucketing (Crawl + CollegeVine) ---
# Notebook-friendly: define inputs/outputs here, run top-to-bottom.

import re
import html
import unicodedata
import pandas as pd
from pathlib import Path
import argparse

# =========================
# Inputs / Outputs (CLI)
# =========================

DEFAULT_CRAWL_TITLES_COL = "program_titles_found"
DEFAULT_CV_TITLES_COL    = "college_vine_program_titles_found"
DEFAULT_REF_PROGRAM_NAME_COL = "2013_program_name"


def _default_outputs_for_input(input_path: Path, outdir: Path) -> dict:
    """Derive default output filenames from the Stage A input filename."""
    stem = input_path.name
    if "__webscrape__v15simple" in stem:
        base = stem.replace("__webscrape__v15simple", "__webscrape__v15simple__bucketed_programs").replace(".csv", "")
    else:
        base = stem.replace(".csv", "") + "__bucketed_programs"

    return {
        "wide": outdir / f"{base}.csv",
        "long": outdir / f"{base}__long.csv",
        "long_bucket": outdir / f"{base}__long_bucket_summary.csv",
        "long_programs": outdir / f"{base}__long_programs.csv",
        "long_programs_agg": outdir / f"{base}__long_programs_agg.csv",
        "long_signals": outdir / f"{base}__long_signals.csv",
    }


def parse_args(argv=None):
    ap = argparse.ArgumentParser(
        prog="webscrape_parser.py",
        description="Stage B: Parse Stage A outputs, bucket titles, and emit wide + long-form tables.",
    )

    ap.add_argument("--input", required=True, help="Stage A CSV to parse (e.g. *__webscrape__v15simple.csv)")
    ap.add_argument("--outdir", default=None, help="Directory for outputs (default: same directory as --input)")

    ap.add_argument("--output-wide", default=None)
    ap.add_argument("--output-long", default=None)
    ap.add_argument("--output-long-bucket", default=None)
    ap.add_argument("--output-long-programs", default=None)
    ap.add_argument("--output-long-programs-agg", default=None)
    ap.add_argument("--output-long-signals", default=None)

    ap.add_argument("--crawl-titles-col", default=DEFAULT_CRAWL_TITLES_COL)
    ap.add_argument("--cv-titles-col", default=DEFAULT_CV_TITLES_COL)
    ap.add_argument("--ref-program-name-col", default=DEFAULT_REF_PROGRAM_NAME_COL)

    return ap.parse_args(argv)

# =========================
# Bucket Definitions
# =========================
# Precedence matters: first match wins (one title -> one bucket).
BUCKET_ORDER = [
    "black",      # African American / Black Studies (highest priority)
    "africana",
    "mena",       # Middle East / North Africa / Near East / Central Asia
    "african",    # generic African Studies (NOT African American)
    "minority",
    "ethnic",
    "race",
    "other",
]

# Real but non-program signals we want to track (centers/committees etc.)
REAL_NONPROGRAM_KINDS = ["center_institute", "committee_admin", "department_unit", "program_signal"]

# MENA trigger terms for exclusion and detection
MENA_TERMS = [
    "middle east", "middle eastern", "north africa", "north african",
    "mena", "near east", "near eastern", "central asian", "central asia",
]

# Regex patterns (precedence is controlled by BUCKET_ORDER)
BUCKET_PATTERNS = {
    "black": [
        r"\bafrican[-\s]?american\b",
        r"\bafro[-\s]?american\b",
        r"\bblack\s+studies\b",
        r"\bblack\s+(american|diaspora|diasporic)\b",
        r"\bafrican\s*,?\s*black\b",  # broad signal (keep)
        r"\bafrican\s*,?\s*black\s+and\s+caribbean\s+studies\b(?:\s+(?:program|department|major|minor|certificate|center|centre))?",  # e.g., "African, Black and Caribbean Studies"
        r"\bblack\s+and\s+.*\bstudies\b",  # e.g., "Black and Latino Studies"
        r"\bcritical\s+black\s+studies\b",
        r"\bafrican\s+and\s+african[-\s]+american\s+studies\b",  # robust to hyphen/space
        r"\bafrican\s+and\s+african\s+american\s+studies\b",  # legacy exact form
        r"\bblack\s+visual\s+cultures\b",
    ],
    "africana": [
        r"\bafricana\b",
        r"\bpan[-\s]?african\b",
        r"\bafricolog\w*\b",
    ],
    "mena": [
        r"\bmiddle\s+east\b",
        r"\bmiddle\s+eastern\b",
        r"\bnorth\s+africa\b",
        r"\bnorth\s+african\b",
        r"\bnear\s+east\b",
        r"\bnear\s+eastern\b",
        r"\bmena\b",
        r"\bmiddle\s+eastern\s+and\s+north\s+african\s+studies\b",
        r"\bmiddle\s+eastern\s+and\s+north\s+africa\s+studies\b",
        r"\bmiddle\s+east\s+and\s+north\s+africa\s+studies\b",
        r"\bmiddle\s+east\s+and\s+north\s+african\s+studies\b",
        r"\bmiddle\s+eastern\s*,\s*central\s+asian\s*,\s*and\s+north\s+african\s+studies\b",
    ],
    "african": [ #we exclude these from "African-American" bucket later
        r"\bafrican\s+studies\b",
        r"\bafrican\s+and\s+african\s+diaspora\s+studies\b",
        r"\bafrican\s+and\s+african\s+diasporic\s+studies\b",
        r"\bafrican\s+diaspora\s+studies\b",
        r"\bafrican\s+languages?\b",
        r"\bafrican\s+literatures?\b",
        r"\bafrican\s+language\s+program\b",
        r"\bafrica\b", 
    ],
    "minority": [
        r"\bminority\b",
        r"\bcultural\s+minority\b",
    ],
    "race": [
        r"\brace\b",
        r"\bracial\b",
        r"\bcritical\s+race\b",
        r"\bracial\s+justice\b",
        r"\brace\s*,?\s*power\b",
    ],
    "ethnic": [
        r"\brace\s*,?\s*ethnicity\b",
        r"\brace\s+and\s+ethnicity\b",
        r"\bethnic\b",
        r"\bethnicity\b",
    ],
}

# Strong indicators a string is an academic program title (vs. navigation/news/admin text)
# NOTE: intentionally does NOT include the word "studies" because it's too broad.
PROGRAM_TOKENS = [
    "major", "minor", "department", "program", "concentration",
    "certificate",
    "b.a", "ba", "b.s", "bs", "m.a", "ma", "m.s", "ms", "phd", "doctorate", "degree",
]

# Phrases that commonly indicate navigation/page chrome/admin/news (not a program title)
JUNK_PHRASES = [
    "welcome", "about", "view", "check out", "read more", "learn more", "connect with", "visit",
    "calendar", "faculty", "staff", "donate", "newsletter", "timeline",
    "annual report", "events", "news", "image", "photo", "video",
    "lecture series", "launchpad", "symposium", "workshop",
    "financial aid", "scholarship", "fellowship", "career map",
]
# Descriptor/support pages that are not themselves programs
DESCRIPTOR_NONPROGRAM_RE = re.compile(r"\b(financial\s+aid|scholarship|fellowship|career\s+map)\b", re.IGNORECASE)

# Common black-related false positives
BLACK_FALSE_POSITIVES = [
    "black belt", "blackstone", "blackboard", "black history month", "black student union",
]

# Heuristic verbs that often indicate trips/stories rather than programs
TRIP_STORY_VERBS = [
    "experience", "travel", "study abroad", "healing", "healthcare", "reads into", "honoring",
]

# Countries/regions commonly appearing in non-program strings (study abroad, course topics, stories)
COUNTRY_TERMS = [
    "ghana", "south africa", "nigeria", "kenya", "ethiopia", "uganda", "tanzania", "rwanda",
]

YEAR_PATTERN = re.compile(r"\b(19|20)\d{2}([\-/]\d{2})?\b")
CLASS_YEAR_PATTERN = re.compile(r"\b['\u2019]\d{2}\b")

# Course-like detection (run BEFORE bucket/program inference)
COURSE_PREFIX_RE = re.compile(
    r"^(introduction|intro|seminar|topics|special\s+topics|survey|advanced|capstone|independent\s+study|practicum|critical\s+debates|research|research\s+in)\b",
    flags=re.IGNORECASE,
)
COURSE_PHRASES = [
    "introduction to", "intro to", "critical debates", "seminar", "special topics", "research in",
    "4-year plan", "transfer plan", "curriculum", "requirements", "electives",
    "prerequisite", "prerequisites", "prereq",
]

# Major/minor shorthand evidence like "Africana (M, m)"
MM_SHORTHAND_RE = re.compile(r"\(\s*M\s*,\s*m\s*\)", re.IGNORECASE)

# Confidence helpers
STRONG_PROGRAM_TOKEN_RE = re.compile(r"\b(major|minor|department|program|certificate|concentration|degree)\b", re.IGNORECASE)
DEGREE_TOKEN_RE = re.compile(r"\b(b\.?a\.?|b\.?s\.?|m\.?a\.?|m\.?s\.?|ph\.?d\.?)\b", re.IGNORECASE)
DEGREE_PHRASE_RE = re.compile(
    r"\b(ba|bs|b\.a\.|b\.s\.|ma|ms|m\.a\.|m\.s\.|phd|ph\.d\.)\s+in\b|\bbachelor\s+of\s+(arts|science)\s+in\b|\bmaster\s+of\s+(arts|science)\s+in\b",
    re.IGNORECASE,
)
ADMIN_MARKERS_RE = re.compile(
    r"\b("
    r"curriculum|requirements|electives|4-year\s+plan|transfer\s+plan|catalog|degree\s+type|research\s+guide|guide"
    r"|prereq(?:uisite)?s?"
    r"|open\s+to\s+students"
    r"|have\s+declared"
    r"|program\s+of\s+study"
    r"|senior\s+priority"
    r")\b",
    re.IGNORECASE,
)
STORY_MARKERS_RE = re.compile(r"\b(reads\s+into|named|talking\s+with|studying|honoring|why\b)\b|\?", re.IGNORECASE)
PROFILE_DASH_RE = re.compile(r"\b[A-Z][a-z]+\s+[A-Z][a-z]+\b\s*[-–—]\s*", re.UNICODE)
# Patch B: Allow dash-degree suffixes like "X - Minor" through profile dash penalty
DASH_DEGREE_SUFFIX_RE = re.compile(r"[-–—]\s*(major|minor|certificate|concentration)\b", re.IGNORECASE)

BARE_STUDIES_PROGRAM_RE = re.compile(r"\bstudies\b$", re.IGNORECASE)

# =========================
# Helpers
# =========================
def _try_mojibake_repair(s: str) -> str:
    """
    Attempt to repair common UTF-8 bytes mis-decoded as cp1252/latin1.
    This is a simpler/general alternative to huge find+replace lists.
    """
    if s is None:
        return ""
    s = str(s)

    # quick heuristic: only attempt if it looks like mojibake
    if not any(x in s for x in ["‚Ä", "Ã", "Â", "�"]):
        return s

    # try cp1252 -> utf-8
    for enc in ("cp1252", "latin1"):
        try:
            repaired = s.encode(enc, errors="ignore").decode("utf-8", errors="ignore")
            if repaired and (repaired != s) and ("�" not in repaired):
                return repaired
        except Exception:
            pass
    return s

def normalize_text(s: str) -> str:
    """General cleaning: mojibake repair + unicode normalization + whitespace + HTML unescape."""
    if s is None:
        return ""
    s = str(s)

    s = html.unescape(s)
    s = _try_mojibake_repair(s)
    s = unicodedata.normalize("NFKC", s)

    # normalize common dash variants
    s = s.replace("\u2010", "-").replace("\u2011", "-").replace("\u2012", "-").replace("\u2013", "-").replace("\u2014", "-")
    s = s.replace("&", " and ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _normalize_title(t: str) -> str:
    return normalize_text(t)

def _split_titles(cell) -> list[str]:
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return []
    s = str(cell).strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split("|")]
    parts = [_normalize_title(p) for p in parts if p.strip()]
    # de-dupe while preserving order
    seen = set()
    out = []
    for p in parts:
        key = p.lower()
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out

def _word_count(s: str) -> int:
    return len([w for w in re.split(r"\s+", s.strip()) if w])

def looks_like_junk_or_page_text(title: str) -> bool:
    t = _normalize_title(title)
    t_low = t.lower()

    if YEAR_PATTERN.search(t_low) or CLASS_YEAR_PATTERN.search(t_low):
        return True

    for p in JUNK_PHRASES:
        if p in t_low:
            return True

    # long sentence-like strings without strong program tokens
    if _word_count(t_low) > 15:
        if (not any(tok in t_low for tok in PROGRAM_TOKENS)) and ("studies" not in t_low):
            return True

    return False

def canonicalize_program_title(title: str) -> str:
    """
    Canonicalize to a 'program label' where possible, collapsing variants:
      - Department of X Studies -> X Studies
      - X Studies Major -> X Studies
      - X Studies Minor -> X Studies
      - Drop trailing parenthetical like (ABD) or (M, m)
      - Preserve trailing parenthetical tracks like (Pan African Studies)
    """
    t = _normalize_title(title)

    # Strip leading navigation/page prefixes
    t = re.sub(
        r"^\s*(academic\s+departments\s+and\s+programs|majors\s+and\s+minors|majors|minors|programs)\b[:\s-]*",
        "",
        t,
        flags=re.IGNORECASE,
    ).strip()
    t = re.sub(r"^\s*(check\s+out|view|welcome\s+to|about)\b[:\s-]*", "", t, flags=re.IGNORECASE).strip()

    # --- Parentheticals ---
    # Keep (M, m) evidence for program confidence (handled upstream), but remove it from the canonical label.
    t = re.sub(r"\s*\(\s*M\s*,\s*m\s*\)\s*$", "", t, flags=re.IGNORECASE).strip()

    # Preserve trailing parenthetical "tracks" (e.g., "Ethnic Studies (Pan African Studies)")
    # but still drop short acronym parentheticals like (ABD).
    m_track = re.search(r"\s*\(([^)]{2,120})\)\s*$", t)
    if m_track:
        track_txt = m_track.group(1).strip()
        track_low = track_txt.lower()

        # Drop common acronym-only tags like (ABD), (AFST), etc.
        if re.fullmatch(r"[A-Z]{2,6}", track_txt):
            t = re.sub(r"\s*\((?:[A-Z]{2,6})\)\s*$", "", t).strip()
        else:
            # Keep parenthetical if it looks like an academic track/specialization that affects bucketing.
            # (We keep this permissive: any in-scope bucket signal OR contains 'studies'.)
            track_has_bucket_signal = False
            for b in ["black", "africana", "mena", "african", "minority", "ethnic", "race"]:
                for pat in BUCKET_PATTERNS.get(b, []):
                    if re.search(pat, track_low, flags=re.IGNORECASE):
                        track_has_bucket_signal = True
                        break
                if track_has_bucket_signal:
                    break

            if ("studies" in track_low) or track_has_bucket_signal:
                # Keep the parenthetical track (no change)
                pass
            else:
                # Otherwise, drop the trailing parenthetical as a last resort cleanup.
                t = re.sub(r"\s*\((?:[^)]*)\)\s*$", "", t).strip()
    else:
        # If no parenthetical, no action needed
        pass

    # drop obvious nav suffixes
    t = re.sub(r"\s+(home|overview)\b", "", t, flags=re.IGNORECASE).strip()

    # normalize common prefixes
    t = re.sub(r"^\s*department\s+of\s+", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"^\s*the\s+department\s+of\s+", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"^\s*the\s+", "", t, flags=re.IGNORECASE).strip()

    # Strip leading degree phrases when they are just wrappers around a program title
    # (preserves any trailing parenthetical track, e.g., "Ethnic Studies (Pan African Studies)")
    t = re.sub(r"^\s*(ba|bs|b\.a\.|b\.s\.|ma|ms|m\.a\.|m\.s\.|phd|ph\.d\.)\s+in\s+", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"^\s*bachelor\s+of\s+(arts|science)\s+in\s+", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"^\s*master\s+of\s+(arts|science)\s+in\s+", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"^\s*master\s+of\s+arts\s+in\s+", "", t, flags=re.IGNORECASE).strip()

    # Normalize some very common standalone variants
    # (We keep this small and conservative: only normalize when the intent is extremely clear.)
    t_low = t.lower().strip()
    if t_low == "africana":
        t = "Africana Studies"
        t_low = t.lower()

    # Remove trailing degree-structure tokens
    t = re.sub(r"\s+\bmajor\s+and\s+minor(s)?\b$", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"\s+\b(major|minor|program)\b$", "", t, flags=re.IGNORECASE).strip()

    # If this is a combined title with multiple study areas, prefer the in-scope anchor program label
    # NOTE: do NOT collapse comma-joined labels like "African, Black and Caribbean Studies".
    t_low = t.lower()
    if " and " in t_low:
        # Patch C: prevent MENA titles like "Middle Eastern and North African Studies" from
        # accidentally collapsing to "African Studies" due to the substring "African Studies".
        if any(term in t_low for term in MENA_TERMS) and ("studies" in t_low):
            # Canonicalize to a consistent MENA label
            if "middle eastern" in t_low:
                t = "Middle Eastern and North African Studies"
            elif "middle east" in t_low:
                t = "Middle East and North Africa Studies"
            elif "north african" in t_low:
                # fall back to the more common combined label
                t = "Middle Eastern and North African Studies"
        # Patch E: keep "Race and Ethnic Studies" as a distinct program label (do NOT collapse to "Ethnic Studies")
        elif re.search(r"\brace\s+and\s+ethnic\s+studies\b", t_low, flags=re.IGNORECASE):
            t = "Race and Ethnic Studies"
        elif "ethnic studies" in t_low:
            t = "Ethnic Studies"
        elif "black studies" in t_low:
            t = "Black Studies"
        elif "africana studies" in t_low:
            t = "Africana Studies"
        elif "african and african american studies" in t_low:
            t = "African and African American Studies"
        elif "african studies" in t_low and ("african american" not in t_low) and ("african-american" not in t_low):
            t = "African Studies"

    # Explicit anchor for "African, Black and Caribbean Studies" style labels
    if re.search(r"\bafrican\s*,\s*black\s+and\s+caribbean\s+studies\b", t.lower(), flags=re.IGNORECASE):
        t = "African, Black and Caribbean Studies"

    # collapse duplicated phrase like "African and African Diaspora Studies African and African Diaspora Studies"
    words = t.split()
    if len(words) >= 6:
        mid = len(words) // 2
        if " ".join(words[:mid]).lower() == " ".join(words[mid:]).lower():
            t = " ".join(words[:mid]).strip()

    # Trim trailing punctuation artifacts (commas, colons, etc.)
    t = re.sub(r"[\s,:;]+$", "", t).strip()

    return t

def bucket_title(title: str) -> str:
    t_norm = _normalize_title(title)
    t_low = t_norm.lower()

    for fp in BLACK_FALSE_POSITIVES:
        if fp in t_low:
            return "other"

    if not t_low:
        return "other"

    # Explicit overrides for common joint/compound labels
    if re.search(r"\bafrican\s*,?\s*black\s+and\s+caribbean\s+studies\b", t_low, flags=re.IGNORECASE):
        return "black"
    if re.search(r"\bafrican\s+and\s+african[-\s]?american\s+studies\b", t_low, flags=re.IGNORECASE):
        return "black"
    # Patch E: "Race and Ethnic Studies" should be treated as a race-bucket program (race > ethnic for this phrase)
    if re.search(r"\brace\s+and\s+ethnic\s+studies\b", t_low, flags=re.IGNORECASE):
        return "race"

    for bucket in BUCKET_ORDER:
        if bucket == "other":
            continue
        for pat in BUCKET_PATTERNS.get(bucket, []):
            if re.search(pat, t_low, flags=re.IGNORECASE):
                if bucket == "african":
                    if re.search(r"\bafrican[-\s]?american\b", t_low) or re.search(r"\bafricana\b", t_low):
                        continue
                    if any(term in t_low for term in MENA_TERMS):
                        continue
                return bucket

    return "other"

def program_confidence(title: str, source: str = "") -> tuple[int, str]:
    """Return (0-100 confidence, reason_flags)."""
    t = _normalize_title(title)
    t_low = t.lower()
    flags = []
    score = 0
    wc = _word_count(t_low)

    # Patch D (revised): CV taxonomy label should be treated as a target program.
    # This label often lacks degree/program tokens but should be included as a program in the minority bucket.
    if source == "cv" and re.search(
        r"^ethnic\s*,\s*cultural\s+minority\s*,\s*and\s+gender\s+studies\s*,\s*other\b",
        t_low,
        flags=re.IGNORECASE,
    ):
        score += 70
        flags.append("cv_taxonomy_target")

    # Positive evidence
    if STRONG_PROGRAM_TOKEN_RE.search(t_low):
        score += 60
        flags.append("strong_token")

    if DEGREE_TOKEN_RE.search(t_low):
        score += 40
        flags.append("degree_token")

    if DEGREE_PHRASE_RE.search(t_low):
        score += 60
        flags.append("degree_phrase")

    if MM_SHORTHAND_RE.search(t):
        score += 40
        flags.append("major_minor_shorthand")

    # Bare 'X Studies' program name (only boost when it appears in-scope)
    if BARE_STUDIES_PROGRAM_RE.search(t_low) and wc <= 6:
        bkt = bucket_title(t)
        if bkt != "other":
            score += 60
            flags.append("bare_studies_name")

    # Anchor program labels even when embedded (helps joint titles like "Ethnic Studies and ...")
    if any(x in t_low for x in [
        "ethnic studies",
        "black studies",
        "africana studies",
        "african studies",
        "african and african american studies",
    ]):
        score += 20
        flags.append("anchor_phrase")
    # Compound titles that clearly contain an anchor program label (common on catalog pages)
    # e.g. "Ethnic Studies and Women's, Gender, and Sexuality Studies"
    if (" and " in t_low) and any(x in t_low for x in [
        "ethnic studies",
        "black studies",
        "africana studies",
        "african studies",
        "african and african american studies",
    ]):
        # Only boost if it doesn't look like admin/course/story
        if (not ADMIN_MARKERS_RE.search(t_low)) and (not STORY_MARKERS_RE.search(t_low)) and (not DESCRIPTOR_NONPROGRAM_RE.search(t_low)):
            score += 40
            flags.append("compound_anchor")
            
    # Bucket match is weak evidence
    # IMPORTANT: bucket_match should reflect the FINAL bucket decision (bucket_title),
    # not the first regex pattern hit. This keeps the reason string consistent with
    # overrides like Patch E (e.g., "Race and Ethnic Studies" -> race).
    bkt = bucket_title(t)
    if bkt != "other":
        score += 10
        flags.append(f"bucket_match:{bkt}")

    # CV source bonus: taxonomy labels often omit tokens
    if source == "cv" and ("/" in t or ("studies" in t_low and wc <= 8)):
        score += 20
        flags.append("cv_bonus")

    # Penalties
    if ADMIN_MARKERS_RE.search(t_low):
        score -= 60
        flags.append("admin_marker")

    if STORY_MARKERS_RE.search(t_low):
        score -= 60
        flags.append("story_marker")

    # Profile-style pages often look like "Firstname Lastname - Africana Studies".
    # But allow real program markers like "X - Minor" / "X - Major" to pass through.
    if PROFILE_DASH_RE.search(t) and (not DASH_DEGREE_SUFFIX_RE.search(t)):
        score -= 60
        flags.append("profile_dash")

    # Descriptor/support pages that are not themselves programs
    if DESCRIPTOR_NONPROGRAM_RE.search(t_low):
        score -= 80
        flags.append("descriptor_nonprogram")

    # Course-like, unless it has strong program tokens or (M,m) shorthand
    if (COURSE_PREFIX_RE.search(t_low) or any(p in t_low for p in COURSE_PHRASES)):
        if (not STRONG_PROGRAM_TOKEN_RE.search(t_low)) and (not MM_SHORTHAND_RE.search(t)):
            score -= 80
            flags.append("course_like")

    if wc > 12 and not STRONG_PROGRAM_TOKEN_RE.search(t_low) and not DEGREE_TOKEN_RE.search(t_low):
        score -= 50
        flags.append("sentence_like")

    score = max(0, min(100, score))
    return score, "|".join(flags)

def classify_title_kind(title: str, source: str = "") -> str:
    """
    Classify extracted strings; we will only print those that are 'program' in wide outputs.
    We still store other kinds in long outputs.
    """
    t = _normalize_title(title)
    t_low = t.lower()

    if not t_low:
        return "junk"

    # Patch D (revised): Treat the specific CV taxonomy label as a target program.
    # Ensures it is included as a program even though it may omit program/degree tokens.
    if source == "cv" and re.search(
        r"^ethnic\s*,\s*cultural\s+minority\s*,\s*and\s+gender\s+studies\s*,\s*other\b",
        t_low,
        flags=re.IGNORECASE,
    ):
        return "program"

    # Hard junk/page text
    if looks_like_junk_or_page_text(t_low):
        return "junk"

    # Guard against black-related false positives
    if any(fp in t_low for fp in BLACK_FALSE_POSITIVES):
        # unless explicitly Black Studies / African-American studies
        if ("black studies" not in t_low) and ("african-american" not in t_low) and ("african american" not in t_low):
            return "junk"

    # Descriptor/support pages that are not themselves programs
    if DESCRIPTOR_NONPROGRAM_RE.search(t_low):
        return "program_signal"

    # Department/unit pages (real signal but not a degree program by themselves)
    if re.search(r"\bdepartment\b", t_low) or re.search(r"^department\s+of\b", t_low):
        # If it also clearly specifies a degree (e.g., BA/BS/MA/PhD), keep it eligible as a program
        if DEGREE_TOKEN_RE.search(t_low) or DEGREE_PHRASE_RE.search(t_low):
            pass
        else:
            return "department_unit"

    # Course number
    if re.search(r"\b\d{3,4}\b", t_low):
        return "course"

    # Centers/institutes
    if any(x in t_low for x in ["center", "centre", "institute", "initiative", "lab", "laboratory", "research center", "research centre"]):
        return "center_institute"

    # Committees/councils/admin
    if any(x in t_low for x in ["committee", "council", "office", "division", "board"]):
        return "committee_admin"

    # Event/news-like
    if any(v in t_low for v in TRIP_STORY_VERBS):
        return "event_news"
    if any(c in t_low for c in COUNTRY_TERMS) and (not any(tok in t_low for tok in PROGRAM_TOKENS)) and (not MM_SHORTHAND_RE.search(t)):
        return "event_news"

    # Course-like prefixes/phrases without strong program tokens and without (M, m) shorthand => course
    if (COURSE_PREFIX_RE.search(t_low) or any(p in t_low for p in COURSE_PHRASES)):
        if (not any(tok in t_low for tok in PROGRAM_TOKENS)) and (not MM_SHORTHAND_RE.search(t)):
            return "course"

    # Confidence-based decision
    conf, _ = program_confidence(t, source=source)

    if conf >= 70:
        return "program"
    if conf >= 40:
        return "maybe_program"
    return "other_nonprogram"

def format_bucket_cell(items: list[str], source: str) -> str:
    if not items:
        return ""
    return f"{source}:" + "|".join(items)

def merge_sources_cell(crawl_items: list[str], cv_items: list[str]) -> str:
    parts = []
    if crawl_items:
        parts.append(format_bucket_cell(crawl_items, "crawl"))
    if cv_items:
        parts.append(format_bucket_cell(cv_items, "cv"))
    return " || ".join(parts)

def main(argv=None) -> int:
    args = parse_args(argv)

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"[ERROR] input not found: {input_path}")
        return 2

    outdir = Path(args.outdir) if args.outdir else input_path.parent
    outdir.mkdir(parents=True, exist_ok=True)

    defaults = _default_outputs_for_input(input_path, outdir)
    output_wide = Path(args.output_wide) if args.output_wide else defaults["wide"]
    output_long = Path(args.output_long) if args.output_long else defaults["long"]
    output_long_bucket = Path(args.output_long_bucket) if args.output_long_bucket else defaults["long_bucket"]
    output_long_programs = Path(args.output_long_programs) if args.output_long_programs else defaults["long_programs"]
    output_long_programs_agg = Path(args.output_long_programs_agg) if args.output_long_programs_agg else defaults["long_programs_agg"]
    output_long_signals = Path(args.output_long_signals) if args.output_long_signals else defaults["long_signals"]

    # Override column globals (rest of script uses these)
    global CRAWL_TITLES_COL, CV_TITLES_COL, REF_PROGRAM_NAME_COL
    CRAWL_TITLES_COL = args.crawl_titles_col
    CV_TITLES_COL = args.cv_titles_col
    REF_PROGRAM_NAME_COL = args.ref_program_name_col

    print("[INFO] Stage B (webscrape_parser)")
    print(f"  input : {input_path}")
    print(f"  outdir: {outdir}")
    print("  outputs:")
    print(f"    wide             : {output_wide}")
    print(f"    long             : {output_long}")
    print(f"    long_bucket      : {output_long_bucket}")
    print(f"    long_programs    : {output_long_programs}")
    print(f"    long_programs_agg: {output_long_programs_agg}")
    print(f"    long_signals     : {output_long_signals}")

    df = pd.read_csv(input_path, dtype=str)
    # Ensure row_index is always a simple 0..N-1 integer index (prevents tuple/MultiIndex issues)
    df = df.reset_index(drop=True)

    for col in [CRAWL_TITLES_COL, CV_TITLES_COL]:
        if col not in df.columns:
            df[col] = ""

    # normalize mojibake in reference program name col (and extracted title cols)
    if REF_PROGRAM_NAME_COL in df.columns:
        df[REF_PROGRAM_NAME_COL] = df[REF_PROGRAM_NAME_COL].fillna("").map(normalize_text)

    for c in [CRAWL_TITLES_COL, CV_TITLES_COL]:
        df[c] = df[c].fillna("").map(normalize_text)

    # LONG evidence table
    records = []
    for idx, row in df.iterrows():
        unitid = row.get("unitid", "")
        crawl_titles = _split_titles(row.get(CRAWL_TITLES_COL, ""))
        cv_titles    = _split_titles(row.get(CV_TITLES_COL, ""))

        for t in crawl_titles:
            conf, conf_reason = program_confidence(t, source="crawl")
            kind = classify_title_kind(t, source="crawl")
            is_prog = 1 if kind == "program" else 0
            # For maybe_program, still compute bucket/canonical so we can diagnose and promote later
            # Always compute bucket/canonical if it matches a bucket OR is a real signal/program candidate.
            bucket_guess = bucket_title(t)
            if (kind in ("program", "maybe_program")) or (kind in REAL_NONPROGRAM_KINDS) or (bucket_guess != "other"):
                bucket = bucket_guess
                canonical = canonicalize_program_title(t)
            else:
                bucket = ""
                canonical = ""
            records.append({
                "row_index": int(idx),
                "unitid": unitid,
                "source": "crawl",
                "raw_title": t,
                "title_kind": kind,
                "is_program_title": is_prog,
                "bucket": bucket,
                "canonical_title": canonical,
                "program_conf": conf,
                "program_conf_reason": conf_reason,
            })

        for t in cv_titles:
            conf, conf_reason = program_confidence(t, source="cv")
            kind = classify_title_kind(t, source="cv")
            is_prog = 1 if kind == "program" else 0
            # For maybe_program, still compute bucket/canonical so we can diagnose and promote later
            # Always compute bucket/canonical if it matches a bucket OR is a real signal/program candidate.
            bucket_guess = bucket_title(t)
            if (kind in ("program", "maybe_program")) or (kind in REAL_NONPROGRAM_KINDS) or (bucket_guess != "other"):
                bucket = bucket_guess
                canonical = canonicalize_program_title(t)
            else:
                bucket = ""
                canonical = ""
            records.append({
                "row_index": int(idx),
                "unitid": unitid,
                "source": "cv",
                "raw_title": t,
                "title_kind": kind,
                "is_program_title": is_prog,
                "bucket": bucket,
                "canonical_title": canonical,
                "program_conf": conf,
                "program_conf_reason": conf_reason,
            })

    long_df = pd.DataFrame.from_records(records)
    if not long_df.empty and "row_index" in long_df.columns:
        # Hardening: keep row_index a plain integer column
        long_df["row_index"] = pd.to_numeric(long_df["row_index"], errors="coerce").fillna(-1).astype(int)

    # Ensure REAL_NONPROGRAM_KINDS (especially departments) always get bucket/canonical filled
    # even if upstream logic left them blank for any reason.
    if not long_df.empty:
        _np_mask = long_df["title_kind"].isin(REAL_NONPROGRAM_KINDS)
        if _np_mask.any():
            # Fill bucket when missing/blank
            _bucket_blank = long_df.loc[_np_mask, "bucket"].astype(str).str.strip().eq("")
            if _bucket_blank.any():
                long_df.loc[_np_mask & _bucket_blank, "bucket"] = long_df.loc[_np_mask & _bucket_blank, "raw_title"].map(bucket_title)

            # Fill canonical_title when missing/blank
            _canon_blank = long_df.loc[_np_mask, "canonical_title"].astype(str).str.strip().eq("")
            if _canon_blank.any():
                long_df.loc[_np_mask & _canon_blank, "canonical_title"] = long_df.loc[_np_mask & _canon_blank, "raw_title"].map(canonicalize_program_title)

            # As a final fallback, normalize any remaining empty buckets to "other"
            long_df.loc[_np_mask & long_df["bucket"].astype(str).str.strip().eq(""), "bucket"] = "other"

    # Signals-only (real but not programs) for downstream
    long_signals_df = long_df[long_df["title_kind"].isin(REAL_NONPROGRAM_KINDS)].copy() if not long_df.empty else pd.DataFrame()

    # Hardening: ensure real non-program signals ALWAYS have a bucket + canonical_title
    # (prevents blanks from leaking into long outputs and keeps wide signals consistent)
    if not long_signals_df.empty:
        # bucket fallback
        long_signals_df["bucket"] = long_signals_df["bucket"].astype(str).str.strip()
        _b_blank = long_signals_df["bucket"].eq("") | long_signals_df["bucket"].isna()
        if _b_blank.any():
            long_signals_df.loc[_b_blank, "bucket"] = long_signals_df.loc[_b_blank, "raw_title"].map(bucket_title)
        long_signals_df.loc[long_signals_df["bucket"].astype(str).str.strip().eq(""), "bucket"] = "other"

        # canonical fallback
        long_signals_df["canonical_title"] = long_signals_df["canonical_title"].astype(str).str.strip()
        _c_blank = long_signals_df["canonical_title"].eq("") | long_signals_df["canonical_title"].isna()
        if _c_blank.any():
            long_signals_df.loc[_c_blank, "canonical_title"] = long_signals_df.loc[_c_blank, "raw_title"].map(canonicalize_program_title)

    # Program-only view for inventory and pivot
    long_prog = long_df[(long_df["is_program_title"] == 1) & (long_df["bucket"].astype(str).str.len() > 0)].copy()

    # =========================
    # Program bucket provenance (for long summary output)
    # =========================
    if not long_prog.empty:
        bucket_src = (
            long_prog
            .groupby(["row_index", "unitid", "bucket"], dropna=False)["source"]
            .apply(lambda s: "|".join(sorted(set([str(x) for x in s if pd.notna(x) and str(x) != ""]))))
            .reset_index()
            .rename(columns={"source": "sources_in_key"})
        )
        bucket_src["has_crawl_in_key"] = bucket_src["sources_in_key"].str.contains(r"\bcrawl\b", regex=True).astype(int)
        bucket_src["has_cv_in_key"]    = bucket_src["sources_in_key"].str.contains(r"\bcv\b", regex=True).astype(int)
        bucket_src["has_both_in_key"]  = ((bucket_src["has_crawl_in_key"] == 1) & (bucket_src["has_cv_in_key"] == 1)).astype(int)
        bucket_src["key_type"] = "bucket"
        bucket_src = bucket_src.rename(columns={"bucket": "key"})

        # merge provenance back to program rows
        long_prog = long_prog.merge(
            bucket_src[["row_index", "unitid", "key", "sources_in_key", "has_crawl_in_key", "has_cv_in_key", "has_both_in_key"]],
            left_on=["row_index", "unitid", "bucket"],
            right_on=["row_index", "unitid", "key"],
            how="left",
        ).drop(columns=["key"], errors="ignore")
    else:
        bucket_src = pd.DataFrame(columns=[
            "row_index", "unitid", "key", "sources_in_key",
            "has_crawl_in_key", "has_cv_in_key", "has_both_in_key", "key_type"
        ])

    # Non-program provenance (kept only in long summary output)
    long_nonprog = long_df[long_df["is_program_title"] == 0].copy()
    if not long_nonprog.empty:
        # Ensure bucket is never blank for the real non-program kinds we track
        _np_mask2 = long_nonprog["title_kind"].isin(REAL_NONPROGRAM_KINDS)
        if _np_mask2.any():
            long_nonprog["bucket"] = long_nonprog["bucket"].astype(str).str.strip()
            _b_blank2 = _np_mask2 & (long_nonprog["bucket"].eq("") | long_nonprog["bucket"].isna())
            if _b_blank2.any():
                long_nonprog.loc[_b_blank2, "bucket"] = long_nonprog.loc[_b_blank2, "raw_title"].map(bucket_title)
            long_nonprog.loc[_np_mask2 & long_nonprog["bucket"].astype(str).str.strip().eq(""), "bucket"] = "other"

        signal_src = (
            long_nonprog[long_nonprog["title_kind"].isin(REAL_NONPROGRAM_KINDS)]
            .groupby(["row_index", "unitid", "title_kind", "bucket"], dropna=False)["source"]
            .apply(lambda s: "|".join(sorted(set([str(x) for x in s if pd.notna(x) and str(x) != ""]))))
            .reset_index()
            .rename(columns={"title_kind": "key", "source": "sources_in_key"})
        )
        signal_src["has_crawl_in_key"] = signal_src["sources_in_key"].str.contains(r"\bcrawl\b", regex=True).astype(int)
        signal_src["has_cv_in_key"]    = signal_src["sources_in_key"].str.contains(r"\bcv\b", regex=True).astype(int)
        signal_src["has_both_in_key"]  = ((signal_src["has_crawl_in_key"] == 1) & (signal_src["has_cv_in_key"] == 1)).astype(int)
        signal_src["key_type"] = "title_kind"
    else:
        signal_src = pd.DataFrame(columns=[
            "row_index", "unitid", "bucket", "key", "sources_in_key",
            "has_crawl_in_key", "has_cv_in_key", "has_both_in_key", "key_type"
        ])

    long_bucket_df = pd.concat([bucket_src, signal_src], ignore_index=True)

    # =========================
    # Build program inventory tables (clean)
    # =========================
    if not long_prog.empty:
        # --- Inventory-specific shaping ---
        # 1) If a unit has a clear anchor program label (e.g., "Black Studies"), fold course/admin-like
        #    spillover titles into that anchor as supporting evidence instead of separate canonical programs.
        # 2) Expand sentences like "X Studies is offered as a major, minor, and concentration." into
        #    three inventory rows (major/minor/concentration) for program accounting.

        def _is_demotable_as_evidence(raw: str) -> bool:
            t = _normalize_title(raw)
            t_low = t.lower()
            if not t_low:
                return True
            if DESCRIPTOR_NONPROGRAM_RE.search(t_low):
                return True
            if ADMIN_MARKERS_RE.search(t_low):
                return True
            if STORY_MARKERS_RE.search(t_low):
                return True
            if PROFILE_DASH_RE.search(t):
                return True
            if (COURSE_PREFIX_RE.search(t_low) or any(p in t_low for p in COURSE_PHRASES)):
                return True
            return False

        ANCHOR_CANONICALS = {
            "Black Studies",
            "Africana Studies",
            "African Studies",
            "Ethnic Studies",
            "African and African American Studies",
            "African-American/Black Studies",
        }

        # Work on a copy so we don't affect downstream wide pivot behavior
        inv_prog = long_prog.copy()

        # Anchor folding within (unitid, bucket, source)
        inv_prog["_is_anchor"] = inv_prog["canonical_title"].isin(ANCHOR_CANONICALS)
        inv_prog["_demote"] = inv_prog["raw_title"].map(_is_demotable_as_evidence)

        def _fold_to_anchor(g: pd.DataFrame) -> pd.DataFrame:
            anchors = g[g["_is_anchor"]]
            if anchors.empty:
                return g
            # Prefer the most common anchor title; if tie, the first
            anchor_title = anchors["canonical_title"].value_counts().index[0]
            g.loc[g["_demote"], "canonical_title"] = anchor_title
            return g

        inv_prog = (
            inv_prog
            .groupby(["unitid", "bucket", "source"], dropna=False, group_keys=False)
            .apply(_fold_to_anchor)
        )

        # Sentence expansion for inventory only
        # Example: "Ethnic Studies is offered as a major, minor, and concentration." => 3 inventory rows
        def _expand_offered_as(g: pd.DataFrame) -> pd.DataFrame:
            out_rows = []
            for _, r in g.iterrows():
                raw = str(r.get("raw_title", ""))
                raw_low = raw.lower()
                if "is offered as" in raw_low and "major" in raw_low and "minor" in raw_low and "concentration" in raw_low:
                    base = str(r.get("canonical_title", "")).strip() or str(r.get("raw_title", "")).strip()
                    # Use inventory-specific titles that preserve the three-program signal
                    for suffix in ("Major", "Minor", "Concentration"):
                        r2 = r.copy()
                        r2["canonical_title"] = f"{base} ({suffix})"
                        out_rows.append(r2)
                else:
                    out_rows.append(r)
            return pd.DataFrame(out_rows)

        inv_prog = (
            inv_prog
            .groupby(["unitid", "bucket", "source"], dropna=False, group_keys=False)
            .apply(_expand_offered_as)
        )

        # Build inventory tables
        long_programs_df = (
            inv_prog
            .groupby(["unitid", "source", "bucket", "canonical_title"], dropna=False)
            .agg(
                supporting_raw_titles=("raw_title", lambda xs: "|".join(sorted(set([str(x) for x in xs if str(x).strip()])))),
                program_conf_max=("program_conf", "max"),
                program_conf_reasons=("program_conf_reason", lambda xs: "|".join(sorted(set([str(x) for x in xs if str(x).strip()])))),
            )
            .reset_index()
        )

        long_programs_agg = (
            long_programs_df
            .groupby(["unitid", "bucket", "canonical_title"], dropna=False)
            .agg(
                sources=("source", lambda xs: "|".join(sorted(set(xs)))),
                program_conf_max=("program_conf_max", "max"),
            )
            .reset_index()
        )
        long_programs_agg["has_both_sources"] = long_programs_agg["sources"].str.contains("crawl") & long_programs_agg["sources"].str.contains("cv")
        long_programs_agg["program_conf_final"] = (long_programs_agg["program_conf_max"] + long_programs_agg["has_both_sources"].astype(int) * 10).clip(0, 100)
    else:
        long_programs_df = pd.DataFrame(columns=["unitid", "source", "bucket", "canonical_title", "supporting_raw_titles", "program_conf_max", "program_conf_reasons"])
        long_programs_agg = pd.DataFrame(columns=["unitid", "bucket", "canonical_title", "sources", "program_conf_max", "has_both_sources", "program_conf_final"])

    # =========================
    # Pivot back to WIDE: one column per bucket with provenance
    # =========================
    for b in BUCKET_ORDER:
        df[f"program_bucket__{b}"] = ""
        df[f"program_bucket__{b}__crawl"] = ""
        df[f"program_bucket__{b}__cv"] = ""

    # NEW: wide column for real non-program signals
    df["real_nonprogram_signals"] = ""

    if not long_prog.empty:
        grouped = (
            long_prog
            .groupby(["row_index", "bucket", "source"])["canonical_title"]
            .apply(list)
            .reset_index()
        )

        for (row_index, bucket), sub in grouped.groupby(["row_index", "bucket"]):
            crawl_items = []
            cv_items = []
            for _, r in sub.iterrows():
                if r["source"] == "crawl":
                    crawl_items = r["canonical_title"]
                elif r["source"] == "cv":
                    cv_items = r["canonical_title"]

            # de-dupe in-source
            def _dedupe(xs):
                seen = set()
                out = []
                for x in xs:
                    k = str(x).lower()
                    if k not in seen and str(x).strip():
                        seen.add(k)
                        out.append(x)
                return out

            crawl_items = _dedupe(crawl_items)
            cv_items = _dedupe(cv_items)

            df.at[row_index, f"program_bucket__{bucket}__crawl"] = "|".join(crawl_items) if crawl_items else ""
            df.at[row_index, f"program_bucket__{bucket}__cv"]    = "|".join(cv_items) if cv_items else ""
            df.at[row_index, f"program_bucket__{bucket}"]        = merge_sources_cell(crawl_items, cv_items)

    # Fill wide real_nonprogram_signals with provenance (centers/committees)
    if not long_signals_df.empty:
        sig_grouped = (
            long_signals_df
            .groupby(["row_index", "title_kind", "bucket", "source"])["canonical_title"]
            .apply(list)
            .reset_index()
        )

        for row_index, sub in sig_grouped.groupby("row_index"):
            parts = []
            for _, r in sub.iterrows():
                kind = r["title_kind"]
                bkt = r["bucket"] if str(r.get("bucket", "")).strip() else "other"
                src = r["source"]
                titles = r["canonical_title"] if isinstance(r["canonical_title"], list) else [r["canonical_title"]]

                # de-dupe
                seen = set()
                titles2 = []
                for x in titles:
                    x = str(x).strip()
                    if not x:
                        continue
                    k = x.lower()
                    if k not in seen:
                        seen.add(k)
                        titles2.append(x)

                parts.append(f"{kind}:{bkt}:{src}:" + "|".join(titles2))
            df.at[row_index, "real_nonprogram_signals"] = " || ".join(parts)

    # Bucket hit summary
    def _bucket_hit_list(row) -> str:
        hits = []
        for b in BUCKET_ORDER:
            if row.get(f"program_bucket__{b}", ""):
                hits.append(b)
        return "|".join(hits)

    df["program_buckets_hit"] = df.apply(_bucket_hit_list, axis=1)

    # =========================
    # Save
    # =========================
    df.to_csv(output_wide, index=False)
    long_df.to_csv(output_long, index=False)
    long_bucket_df.to_csv(output_long_bucket, index=False)
    long_programs_df.to_csv(output_long_programs, index=False)
    long_programs_agg.to_csv(output_long_programs_agg, index=False)
    long_signals_df.to_csv(output_long_signals, index=False)

    print("Wrote:")
    print(" -", output_wide)
    print(" -", output_long)
    print(" -", output_long_bucket)
    print(" -", output_long_programs)
    print(" -", output_long_programs_agg)
    print(" -", output_long_signals)

if __name__ == "__main__":
    raise SystemExit(main())