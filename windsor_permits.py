#!/usr/bin/env python3
"""
Windsor Construction Permit Scraper
===================================
Pulls permit-level data from the City of Windsor's public
"Building Information Reports" page (Construction Activity Reports /
Major Construction Reports, published as PDFs):

    https://www.citywindsor.ca/residents/building/building-information/building-information-reports

What it does:
  1. Discovers all report PDFs linked on the page (handles new reports
     appearing over time -- the "pagination" of this source).
  2. Downloads only PDFs it hasn't processed before (rate limited).
  3. Parses permit rows: permit number, issue date, address, description,
     construction value, construction type.
  4. Filters IN:  ADU / additional dwelling unit / secondary suite /
                  water service / water meter / meter pit / meter installation
     Filters OUT: remodels, renovations, fences, pools.
  5. Incremental: keeps state in state.json so weekly re-runs only add
     permits it hasn't seen before.
  6. Outputs:
       output/permits_raw.json  - full raw dump of every matched permit
       output/permits.csv       - sorted by issue date, newest first,
                                  with repeat-builder flag columns

IMPORTANT LIMITATION (verified July 2026):
  Windsor's public reports do NOT include the contractor/builder of record,
  and Enwin Utilities does not publish permit records at all. The
  `contractor` field is therefore blank unless you enrich records via
  another source (see enrich_contractor() stub + README). The duplicate-
  builder flag logic is fully implemented and activates automatically
  once contractor values are present.

Usage:
    python windsor_permits.py            # normal incremental run
    python windsor_permits.py --full     # ignore state, reprocess everything
    python windsor_permits.py --dry-run  # discover + parse, write nothing
"""

import argparse
import csv
import hashlib
import io
import json
import logging
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

try:
    import pdfplumber
except ImportError:
    sys.exit("pdfplumber is required: pip install pdfplumber")

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

REPORTS_PAGE = (
    "https://www.citywindsor.ca/residents/building/"
    "building-information/building-information-reports"
)

USER_AGENT = (
    "WindsorPermitResearch/1.0 (personal research; contact: you@example.com)"
)

# Seconds to wait between HTTP requests (be polite -- these are big PDFs
# on a municipal server).
RATE_LIMIT_SECONDS = 5.0

# Retry behaviour
MAX_RETRIES = 3
RETRY_BACKOFF = 10  # seconds, doubled each retry

# Permit descriptions must match at least one pattern (case-insensitive).
# Patterns are grouped by category; the first matching group labels the
# permit's `category` column. \b word boundaries so "ADU" doesn't match
# "GRADUATE" and "CONSTRUCT" doesn't match "RECONSTRUCT".
CATEGORY_PATTERNS = {
    "ADU": [
        r"\bADU\b",
        r"\bADUS\b",
        r"ADDITIONAL DWELLING UNIT",
        r"SECONDARY\s*/?\s*ADDITIONAL",
        r"SECONDARY SUITE",
    ],
    "Water/Meter": [
        r"\bWATER SERVICE\b",
        r"\bWATER METER\b",
        r"\bMETER PIT\b",
        r"\bMETER INSTALLATION\b",
    ],
    "New Construction": [
        # residential new builds -- CONSTRUCT paired with a building noun
        # to avoid matching interior work like "construct partition wall"
        r"\bCONSTRUCT\b.*\b(DWELLING|TOWNHOUSE|TOWNHOME|DUPLEX|TRIPLEX|"
        r"APARTMENT|MULTIPLE DWELLING)\b",
        r"\bNEW\b.*\bDWELLING\b",
        # commercial / industrial / institutional new builds
        r"\bERECT\b",
        r"\bCONSTRUCT\b.*\b(BUILDING|WAREHOUSE|STORE|PLAZA|CENTRE|CENTER|"
        r"FACILITY|OFFICE|RESTAURANT|HOTEL|SCHOOL|CHURCH|CLINIC)\b",
    ],
}
INCLUDE_PATTERNS = [p for pats in CATEGORY_PATTERNS.values() for p in pats]

# If a description matches any of these, skip it even if an include
# pattern also matched (e.g. "renovate pool house near water meter").
EXCLUDE_PATTERNS = [
    r"\bREMODEL",
    r"\bRENOVAT",       # renovate / renovation
    r"\bFENCE\b",
    r"\bFENCING\b",
    r"\bPOOL\b",
    r"\bHOT TUB\b",
]

INCLUDE_RE = [re.compile(p, re.IGNORECASE) for p in INCLUDE_PATTERNS]
EXCLUDE_RE = [re.compile(p, re.IGNORECASE) for p in EXCLUDE_PATTERNS]
CATEGORY_RE = {cat: [re.compile(p, re.IGNORECASE) for p in pats]
               for cat, pats in CATEGORY_PATTERNS.items()}

# Paths (relative to this script)
BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "state.json"
OUTPUT_DIR = BASE_DIR / "output"
RAW_JSON = OUTPUT_DIR / "permits_raw.json"
CSV_FILE = OUTPUT_DIR / "permits.csv"
PDF_CACHE = BASE_DIR / "pdf_cache"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("windsor-permits")

# --------------------------------------------------------------------------
# Data model
# --------------------------------------------------------------------------

@dataclass
class Permit:
    permit_number: str
    issue_date: str          # ISO YYYY-MM-DD
    permit_type: str         # Residential / Commercial / Industrial / Institutional
    project_address: str
    description: str
    valuation: float
    category: str = ""       # ADU / Water\/Meter / New Construction
    contractor: str = ""     # not published by Windsor -- see README
    source_report: str = ""  # which PDF this came from
    scraped_at: str = ""


# --------------------------------------------------------------------------
# HTTP with rate limiting
# --------------------------------------------------------------------------

class PoliteSession:
    """requests.Session wrapper that enforces a minimum delay between calls."""

    def __init__(self, delay: float = RATE_LIMIT_SECONDS):
        self.delay = delay
        self._last_request = 0.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def get(self, url: str, **kwargs) -> requests.Response:
        wait = self.delay - (time.monotonic() - self._last_request)
        if wait > 0:
            time.sleep(wait)

        backoff = RETRY_BACKOFF
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._last_request = time.monotonic()
                resp = self.session.get(url, timeout=60, **kwargs)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {resp.status_code}")
                resp.raise_for_status()
                return resp
            except requests.RequestException as exc:
                if attempt == MAX_RETRIES:
                    raise
                log.warning("Request failed (%s), retry %d/%d in %ds",
                            exc, attempt, MAX_RETRIES, backoff)
                time.sleep(backoff)
                backoff *= 2
        raise RuntimeError("unreachable")


# --------------------------------------------------------------------------
# State (for incremental weekly runs)
# --------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {
        "seen_permit_numbers": [],
        "processed_reports": {},   # url -> {sha256, parsed_rows, last_fetched}
        "last_run": None,
    }


def save_state(state: dict) -> None:
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    STATE_FILE.write_text(json.dumps(state, indent=2))


# --------------------------------------------------------------------------
# Step 1: discover report PDFs ("pagination")
# --------------------------------------------------------------------------

def discover_report_pdfs(http: PoliteSession) -> list[str]:
    """
    Return absolute URLs of every PDF linked from the Building Information
    Reports page. New months/years show up as new links, so re-running
    weekly automatically picks up new 'pages' of data.
    """
    log.info("Fetching report index: %s", REPORTS_PAGE)
    resp = http.get(REPORTS_PAGE)
    soup = BeautifulSoup(resp.text, "html.parser")

    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" in href.lower() and "building-information" in href.lower():
            urls.append(urljoin(REPORTS_PAGE, href))

    # de-dup, keep order
    seen, unique = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)
    log.info("Found %d report PDF(s) on index page", len(unique))
    return unique


# --------------------------------------------------------------------------
# Step 2/3: download + parse PDFs
# --------------------------------------------------------------------------

# Permit rows in Windsor's reports look like (text-extraction order can
# interleave, so we match tolerantly across newlines):
#   2025 048569 CPBC 942 CAMPBELL AVE UNIT 3 2026-03-02
#   *** ADU UNIT 3 *** ALTERATIONS TO CONSTRUCT ADDITIONAL DWELLING UNIT
#   $382,479.00 Residential
ROW_RE = re.compile(
    r"(?P<permit>20\d{2}\s?\d{6})\s+CPBC\s+"       # permit number
    r"(?P<middle>.*?)"                             # address + date + description (interleaved)
    r"\$(?P<value>[\d,]+\.\d{2})\s+"               # construction value
    r"(?P<ptype>Residential|Commercial|Industrial|Institutional)",
    re.DOTALL,
)
DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
NOISE_RE = re.compile(
    r"(Permit number\s+Municipal address\s+Issued Date.*?Type of Construction"
    r"|Page \d+ of \d+"
    r"|Value of\s*\n?\s*construction)",
    re.DOTALL,
)


def clean_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def parse_report_pdf(pdf_bytes: bytes, source_url: str) -> list[Permit]:
    """Extract permit rows from a report PDF. Returns [] if the PDF
    doesn't contain permit-level rows (e.g. summary dashboards)."""
    text_parts = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text_parts.append(page.extract_text() or "")
    text = NOISE_RE.sub(" ", "\n".join(text_parts))

    permits = []
    now = datetime.now(timezone.utc).isoformat()
    for m in ROW_RE.finditer(text):
        middle = m.group("middle")
        dm = DATE_RE.search(middle)
        if not dm:
            continue
        issue_date = dm.group(1)
        address = clean_ws(middle[: dm.start()])
        description = clean_ws(middle[dm.end():])
        permits.append(
            Permit(
                permit_number=clean_ws(m.group("permit")),
                issue_date=issue_date,
                permit_type=m.group("ptype"),
                project_address=address,
                description=description,
                valuation=float(m.group("value").replace(",", "")),
                source_report=source_url,
                scraped_at=now,
            )
        )
    return permits


# --------------------------------------------------------------------------
# Step 4: filtering
# --------------------------------------------------------------------------

def wanted(p: Permit) -> bool:
    haystack = f"{p.description} {p.project_address}"
    if any(rx.search(haystack) for rx in EXCLUDE_RE):
        return False
    return any(rx.search(haystack) for rx in INCLUDE_RE)


def categorize(p: Permit) -> str:
    haystack = f"{p.description} {p.project_address}"
    for cat, regexes in CATEGORY_RE.items():
        if any(rx.search(haystack) for rx in regexes):
            return cat
    return "Other" 


# --------------------------------------------------------------------------
# Optional contractor enrichment (stub)
# --------------------------------------------------------------------------

def enrich_contractor(p: Permit) -> Permit:
    """
    Windsor does not publish the contractor of record in its public
    reports, and Enwin publishes no permit data. If you obtain a source
    (e.g. periodic FOI/MFIPPA export, a purchased permit-data feed, or a
    per-address lookup you are authorized to use), map it here:

        p.contractor = lookup_by_permit_number(p.permit_number) or ""

    The duplicate-builder flagging below activates automatically once
    contractor values are non-empty.
    """
    return p


# --------------------------------------------------------------------------
# Step 5/6: outputs
# --------------------------------------------------------------------------

def write_outputs(all_permits: list[Permit]) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Raw JSON dump (master list, all runs)
    RAW_JSON.write_text(
        json.dumps([asdict(p) for p in all_permits], indent=2)
    )

    # Duplicate-builder flagging (only meaningful when contractor is known)
    counts = Counter(p.contractor for p in all_permits if p.contractor)

    # CSV sorted newest first; permit_number as tiebreaker for stable output
    rows = sorted(all_permits, key=lambda p: (p.issue_date, p.permit_number),
                  reverse=True)
    with CSV_FILE.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "permit_number", "issue_date", "permit_type", "category",
            "project_address", "description", "valuation", "contractor",
            "builder_permit_count", "repeat_builder", "source_report",
        ])
        for p in rows:
            n = counts.get(p.contractor, 0) if p.contractor else 0
            writer.writerow([
                p.permit_number, p.issue_date, p.permit_type, p.category,
                p.project_address, p.description, f"{p.valuation:.2f}",
                p.contractor, n if n else "",
                "YES" if n > 1 else "", p.source_report,
            ])
    log.info("Wrote %d permits -> %s and %s",
             len(all_permits), RAW_JSON.name, CSV_FILE.name)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--full", action="store_true",
                    help="ignore saved state and reprocess every report")
    ap.add_argument("--dry-run", action="store_true",
                    help="parse and report, but write no files")
    args = ap.parse_args()

    state = load_state() if not args.full else {
        "seen_permit_numbers": [], "processed_reports": {}, "last_run": None,
    }
    seen: set[str] = set(state["seen_permit_numbers"])

    http = PoliteSession()
    PDF_CACHE.mkdir(exist_ok=True)

    # Load existing master list so weekly runs append rather than replace
    existing: list[Permit] = []
    if RAW_JSON.exists() and not args.full:
        existing = [Permit(**d) for d in json.loads(RAW_JSON.read_text())]

    new_permits: list[Permit] = []
    for url in discover_report_pdfs(http):
        already = state["processed_reports"].get(url)
        if already and not args.full:
            log.info("Skipping already-processed report: %s", url.split('/')[-1])
            continue

        log.info("Downloading %s", url.split("/")[-1])
        try:
            pdf_bytes = http.get(url).content
        except requests.RequestException as exc:
            log.error("Failed to download %s: %s", url, exc)
            continue

        sha = hashlib.sha256(pdf_bytes).hexdigest()
        (PDF_CACHE / f"{sha[:16]}.pdf").write_bytes(pdf_bytes)

        rows = parse_report_pdf(pdf_bytes, url)
        matched = [enrich_contractor(p) for p in rows if wanted(p)]
        for p in matched:
            p.category = categorize(p)
        fresh = [p for p in matched if p.permit_number not in seen]
        seen.update(p.permit_number for p in fresh)
        new_permits.extend(fresh)

        state["processed_reports"][url] = {
            "sha256": sha,
            "parsed_rows": len(rows),
            "matched_rows": len(matched),
            "last_fetched": datetime.now(timezone.utc).isoformat(),
        }
        log.info("  parsed %d rows, %d matched filters, %d new",
                 len(rows), len(matched), len(fresh))
        if not rows:
            log.warning("  no permit rows found -- likely a summary "
                        "dashboard PDF, recorded so it won't be refetched")

    all_permits = existing + new_permits
    log.info("Run summary: %d new permit(s), %d total on file",
             len(new_permits), len(all_permits))

    if args.dry_run:
        for p in new_permits:
            print(f"{p.issue_date}  {p.permit_number}  "
                  f"${p.valuation:,.0f}  {p.project_address}")
        return 0

    write_outputs(all_permits)
    state["seen_permit_numbers"] = sorted(seen)
    save_state(state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
