#!/usr/bin/env python3
"""
Twitter/X handle enrichment for B2B contacts.

Coverage-first: every contact that produces a plausible profile gets a handle.
Accuracy stays recoverable because each row carries a confidence tier, so you can
filter to `very_high`/`high` whenever you need precision over reach.

Search backend: Zyte's Search API (POST https://api.zyte.com/v1/search).
Each query becomes one Google search fetched through Zyte, which returns
structured organic results (title/url/snippet) directly - no HTML parsing
needed.

Throughput and durability come from:
  * concurrency                - many searches in flight at once (asyncio + a
                                 semaphore, since Zyte has no batch endpoint)
  * two-stage querying         - cheap high-yield queries run for everyone, the
                                 long-tail fallbacks only for contacts that have
                                 not already produced a confident match
  * an on-disk JSONL cache     - flushed per response, so re-runs replay finished
                                 work for free and a crash costs nothing
  * chunked checkpointing      - the output CSV is rewritten every N contacts, so
                                 a long run always has partial results on disk

Usage
-----
    python3 enrich_segment5_twitter.py                     # full run
    python3 enrich_segment5_twitter.py --limit 200         # quick sample
    python3 enrich_segment5_twitter.py --no-stage2         # stage 1 only, ~3x faster

The Zyte key is read from $ZYTE_API_KEY, falling back to the key supplied for
this project. Prefer the environment variable for anything shared or scheduled.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import random
import re
import sys
import time
import unicodedata
from collections import Counter
from pathlib import Path

import httpx
import pandas as pd

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

def load_local_env(path: Path) -> None:
    """Load KEY=value lines from a local .env when the shell has not set them."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_local_env(Path(__file__).resolve().parent / ".env")
ZYTE_API_KEY = os.getenv("ZYTE_API_KEY", "")
ZYTE_URL = "https://api.zyte.com/v1/search"

if not ZYTE_API_KEY:
    sys.exit(
        "ERROR: ZYTE_API_KEY is not set.\n"
        "Set it in your environment before running, e.g.:\n"
        "  export ZYTE_API_KEY=your_key_here      (macOS/Linux)\n"
        "  set ZYTE_API_KEY=your_key_here          (Windows cmd)\n"
        "  $env:ZYTE_API_KEY=\"your_key_here\"       (PowerShell)"
    )

# Zyte's Search API handles one query per request - there is no batch endpoint
# the way serper.dev had one. Throughput instead comes from how many searches
# run concurrently at once (bounded by --workers via an asyncio semaphore).
MAX_WORKERS = 30          # concurrent Zyte requests in flight
RESULTS_PER_QUERY = 10    # organic results to request per search
MAX_RETRIES = 10          # per-query retries on throttling/network errors
REQUEST_TIMEOUT = 50.0

# Zyte returns these on transient blocks/overload - worth a retry with backoff.
RETRYABLE_STATUS = {429, 500, 502, 503, 504, 520}

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = PROJECT_DIR / "data" / "datahub_100k.csv"
DEFAULT_OUTPUT = PROJECT_DIR / "outputs" / "Segment5_twitter_enriched.xlsx"
CACHE_PATH = PROJECT_DIR / "outputs" / "zyte_cache.jsonl"

# URL paths on x.com that are never a person's profile
RESERVED_HANDLES = {
    "home", "explore", "search", "i", "intent", "share", "hashtag", "settings",
    "notifications", "messages", "compose", "login", "signup", "about", "tos",
    "privacy", "help", "status", "download", "signin", "logout", "account",
    "widgets", "welcome", "oauth", "jobs", "press", "developer", "en", "es",
}

PROFILE_RE = re.compile(r"https?://(?:www\.)?(?:x|twitter)\.com/([A-Za-z0-9_]{1,15})\b", re.I)
LINKEDIN_SLUG_RE = re.compile(r"linkedin\.com/in/([A-Za-z0-9\-_]+)")

CONFIDENCE_RANK = {"very_high": 4, "high": 3, "medium": 2, "low": 1}

# Columns appended to the source sheet
OUTPUT_COLUMNS = [
    "twitter_username",
    "twitter_handle",
    "twitter_profile_url",
    "twitter_confidence",
    "twitter_username_high_conf",
    "twitter_matched_query",
    "twitter_match_evidence",
]


# --------------------------------------------------------------------------
# Text helpers
# --------------------------------------------------------------------------

def clean(value) -> str:
    """Spreadsheet cell -> trimmed string, with blank-ish values collapsed to ''."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    return "" if text.lower() in ("nan", "none", "null", "n/a", "na", "-") else text


def norm(value) -> str:
    """Lowercase, strip accents, reduce to space-separated alphanumeric tokens."""
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def compact(value) -> str:
    """Normalized text with all spaces removed, for substring tests against handles."""
    return norm(value).replace(" ", "")


def linkedin_slug(url) -> str:
    """
    Vanity slug from a LinkedIn URL, e.g. .../in/simon-peel-123 -> simon-peel-123.

    LinkedIn's opaque 'ACwAA...' ids carry no name signal, so they are dropped.
    """
    match = LINKEDIN_SLUG_RE.search(str(url or ""))
    if not match:
        return ""
    slug = match.group(1)
    return "" if slug.startswith("ACwAA") else slug


# --------------------------------------------------------------------------
# Query construction
# --------------------------------------------------------------------------

def _contact_fields(row) -> dict:
    first = clean(row.get("ic_fname")).title()
    last = clean(row.get("ic_lname")).title()
    return {
        "first": first,
        "last": last,
        "full": f"{first} {last}".strip(),
        "company": clean(row.get("ic_company")),
        "title": clean(row.get("ic_jtitle")),
        "city": clean(row.get("Location")).split(",")[0].strip(),
        "slug": linkedin_slug(row.get("ic_link")),
    }


def single_query(row) -> list[tuple[str, str]]:
    """
    Exactly ONE query per contact - the single highest-confidence variant
    available for that row. Cheapest possible mode: 1 Zyte query per
    contact instead of stage 1 + stage 2's average of ~6.

    Priority: company match > city match > name-only. Company is preferred
    because "name" + "company" together is the most specific, lowest-
    false-positive combination - it is also the top single variant in the
    stage 1 measurement referenced in stage1_queries() above.

    Trade-off: coverage (the % of contacts that get ANY handle) drops,
    because there is no second attempt if the first query's phrasing
    happens to miss. Contacts stage 1+2 would have found via a fallback
    variant (title, bare name, LinkedIn slug, etc.) are simply not found
    here. Accuracy on the handles that ARE found is unaffected - every
    match still goes through the same confidence scoring and the
    verify_twitter_handles.py check afterwards.
    """
    f = _contact_fields(row)
    if not f["full"]:
        return []
    if f["company"]:
        return [("v1_company", f'(site:twitter.com OR site:x.com) "{f["full"]}" "{f["company"]}"')]
    if f["city"]:
        return [("v2_location", f'(site:twitter.com OR site:x.com) "{f["full"]}" {f["city"]}')]
    return [("v4_nameonly", f'(site:twitter.com OR site:x.com) "{f["full"]}"')]


def stage1_queries(row) -> list[tuple[str, str]]:
    """
    The two highest-marginal-yield query shapes, run for every contact.

    Measured on a 60-contact sample of Segment 5, these two alone reach roughly
    18% coverage - about two thirds of everything the full seven-variant set
    finds, for under a third of the API spend.
    """
    f = _contact_fields(row)
    if not f["full"]:
        return []

    queries = []
    if f["company"]:
        queries.append(("v1_company", f'(site:twitter.com OR site:x.com) "{f["full"]}" "{f["company"]}"'))
    if f["city"]:
        queries.append(("v2_location", f'(site:twitter.com OR site:x.com) "{f["full"]}" {f["city"]}'))
    if not queries:
        queries.append(("v4_nameonly", f'(site:twitter.com OR site:x.com) "{f["full"]}"'))
    return queries


def stage2_queries(row) -> list[tuple[str, str]]:
    """Long-tail fallbacks, only spent on contacts stage 1 could not resolve."""
    f = _contact_fields(row)
    if not f["full"]:
        return []

    queries = []
    if f["title"]:
        queries.append(("v5_title", f'"{f["full"]}" {f["title"]} (twitter.com OR x.com)'))
    queries.append(("v6_bare", f'"{f["full"]}" twitter'))
    queries.append(("v4_nameonly", f'(site:twitter.com OR site:x.com) "{f["full"]}"'))
    if f["slug"]:
        queries.append(("v7_slug", f'(site:twitter.com OR site:x.com) {f["slug"]}'))
    if f["company"]:
        queries.append(("v3_open", f'"{f["full"]}" "{f["company"]}" (twitter.com OR x.com)'))
    return queries


def capped_queries(row, max_queries: int) -> list[tuple[str, str]]:
    """Run the strongest query variants up front, capped per contact."""
    seen = set()
    queries = []
    for variant, query in stage1_queries(row) + stage2_queries(row):
        if query in seen:
            continue
        seen.add(query)
        queries.append((variant, query))
        if len(queries) >= max_queries:
            break
    return queries


# --------------------------------------------------------------------------
# Candidate scoring
# --------------------------------------------------------------------------

def score_candidate(row, handle: str, title: str, snippet: str) -> str:
    """
    Grade how well a search hit supports 'this handle belongs to this contact'.

    Two independent families of evidence carry the decision:
      name_in_text   - the contact's name appears in the result title/snippet
      name_in_handle - the handle itself is built from the contact's name

    Company and location act as corroboration. A handle that spells out the
    company but not the person is a brand account, not the contact, and is
    rejected outright - that check alone removed several false positives
    (@SynclyTech for Luke Smith, @zaymodotcom for Brice Douglas) during tuning.
    """
    first, last = norm(row.get("ic_fname")), norm(row.get("ic_lname"))
    company, location = norm(row.get("ic_company")), norm(row.get("Location"))
    text = norm(f"{title} {snippet}")

    handle_lower = handle.lower()
    # split camelCase so "MartinHuntJr" exposes the tokens martin / hunt
    handle_tokens = norm(re.sub(r"(?<=[a-z])(?=[A-Z])", " ", handle))

    last_compact = last.replace(" ", "")
    first_compact = first.replace(" ", "")

    name_in_text = bool(first and last) and first in text and last in text
    name_in_handle = bool(last) and (
        last_compact in handle_lower
        or last in handle_tokens
        or (first and (first_compact[:1] + last_compact) in handle_lower)
        or (first and first_compact in handle_lower and last_compact in handle_lower)
    )

    company_hit = bool(company) and any(t in text for t in company.split() if len(t) > 3)
    location_hit = bool(location) and any(t in text for t in location.split() if len(t) > 3)

    company_compact = compact(company)
    is_brand_account = (
        len(company_compact) > 3
        and company_compact in handle_lower
        and not name_in_handle
    )
    if is_brand_account:
        return "low"

    if name_in_text and name_in_handle and (company_hit or location_hit):
        return "very_high"
    if name_in_text and (name_in_handle or company_hit):
        return "high"
    if name_in_handle and (company_hit or location_hit):
        return "high"
    if name_in_text or name_in_handle:
        return "medium"
    return "low"


def best_from_payload(row, payload: dict, variant: str) -> dict | None:
    """Pick the strongest profile candidate out of one query's organic results."""
    if not isinstance(payload, dict):
        return None

    best = None
    for item in payload.get("organic", []):
        link = item.get("link", "")
        if "/status/" in link or "/i/" in link:
            continue
        match = PROFILE_RE.search(link)
        if not match:
            continue
        handle = match.group(1)
        if handle.lower() in RESERVED_HANDLES:
            continue

        title, snippet = item.get("title", ""), item.get("snippet", "")
        confidence = score_candidate(row, handle, title, snippet)
        candidate = {
            "username": handle,
            "url": f"https://x.com/{handle}",
            "confidence": confidence,
            "variant": variant,
            "evidence": f"{title} | {snippet}"[:300],
        }
        if best is None or CONFIDENCE_RANK[confidence] > CONFIDENCE_RANK[best["confidence"]]:
            best = candidate
        if confidence == "very_high":
            break
    return best


# --------------------------------------------------------------------------
# Search client: Zyte API (Google SERP automatic extraction), cached
# --------------------------------------------------------------------------

class SearchClient:
    """
    Zyte-backed search client with a persistent on-disk response cache.

    Each query becomes one Google search, fetched through Zyte's dedicated
    Search API (POST https://api.zyte.com/v1/search, {"domain": ..., "query":
    ..., "include": ["organic"]}), which returns structured organic results
    (title/url/snippet) directly - no HTML parsing needed. Zyte has no batch
    endpoint the way serper.dev did, so concurrency comes from many requests
    in flight at once (asyncio + a semaphore) rather than grouping queries
    into batches.

    Every response is normalized to the same `{"organic": [...]}` shape the
    rest of this script already expects, so scoring, caching, and the rest of
    the pipeline needed no changes when switching backends.
    """

    def __init__(self, api_key: str, cache_path: Path, workers: int = MAX_WORKERS):
        self.api_key = api_key
        self.workers = workers
        self.cache_path = cache_path
        self.cache: dict[str, dict] = {}
        self._cache_file = None
        self.stats = Counter()
        self._load_cache()

    def _load_cache(self):
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        if self.cache_path.exists():
            with self.cache_path.open(encoding="utf-8") as fh:
                for line in fh:
                    try:
                        entry = json.loads(line)
                        self.cache[entry["q"]] = entry["r"]
                    except (json.JSONDecodeError, KeyError):
                        continue
            print(f"  cache: {len(self.cache):,} queries already on disk")
        self._cache_file = self.cache_path.open("a", encoding="utf-8")

    def _remember(self, query: str, payload: dict):
        self.cache[query] = payload
        self._cache_file.write(json.dumps({"q": query, "r": payload}, ensure_ascii=False) + "\n")
        # Flush every line: this file is the resume log, and buffered lines
        # are exactly what a hard kill or a power cut would throw away.
        self._cache_file.flush()

    @staticmethod
    def _normalize(payload: dict | None) -> dict:
        """Zyte Search API's organicResults -> the {"organic": [...]} shape used everywhere else."""
        organic = []
        for item in (payload or {}).get("organicResults") or []:
            organic.append({
                "link": item.get("url") or "",
                "title": item.get("title") or "",
                "snippet": item.get("snippet") or "",
            })
        return {"organic": organic}

    async def _fetch_one(self, client: httpx.AsyncClient, query: str) -> dict | None:
        """One query, retried with jittered exponential backoff on throttling/network errors."""
        for attempt in range(MAX_RETRIES):
            try:
                resp = await client.post(
                    ZYTE_URL,
                    auth=(self.api_key, ""),
                    json={
                        "domain": "google.com",
                        "query": query,
                        "include": ["organic"],
                        "maxResults": RESULTS_PER_QUERY,
                    },
                )
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                self.stats["network_error"] += 1
                if attempt == MAX_RETRIES - 1:
                    print(f"\n  zyte network failure: {exc}", file=sys.stderr)
                    break
                await asyncio.sleep(min(2 ** attempt, 20) * (0.5 + random.random()))
                continue

            if resp.status_code == 200:
                try:
                    payload = resp.json()
                except (ValueError, json.JSONDecodeError) as exc:
                    self.stats["invalid_response"] += 1
                    if attempt == MAX_RETRIES - 1:
                        print(f"\n  zyte invalid response: {exc}", file=sys.stderr)
                        break
                    await asyncio.sleep(min(2 ** attempt, 20) * (0.5 + random.random()))
                    continue
                self.stats["live_queries"] += 1
                return self._normalize(payload)

            if resp.status_code in RETRYABLE_STATUS:
                self.stats["throttled"] += 1
                if attempt == MAX_RETRIES - 1:
                    break
                await asyncio.sleep(min(2 ** attempt, 20) * (0.5 + random.random()))
                continue

            # Not retryable - e.g. 400 "Not enough credits", 401 bad key.
            # Surface it once and stop instead of burning retries on a request
            # that will never succeed.
            self.stats["http_error"] += 1
            print(f"\n  zyte HTTP {resp.status_code}: {resp.text[:200]}", file=sys.stderr)
            return None

        self.stats["dropped_queries"] += 1
        return None

    async def _run_async(self, pending: list[str]) -> None:
        semaphore = asyncio.Semaphore(self.workers)
        done = 0
        started = time.time()

        async def worker(query: str, client: httpx.AsyncClient):
            nonlocal done
            try:
                async with semaphore:
                    payload = await self._fetch_one(client, query)
                if payload is not None:
                    self._remember(query, payload)
            except Exception as exc:  # one bad response must not cancel the batch
                self.stats["worker_error"] += 1
                self.stats["dropped_queries"] += 1
                print(f"\n  zyte worker failure: {type(exc).__name__}: {exc}", file=sys.stderr)
            finally:
                done += 1
                elapsed = time.time() - started
                if elapsed > 0:
                    print(f"\r    {done:,}/{len(pending):,} queries  "
                          f"{done / elapsed:.0f} q/s", end="", flush=True)

        limits = httpx.Limits(
            max_connections=max(1, self.workers),
            max_keepalive_connections=max(1, min(self.workers, 8)),
            keepalive_expiry=5.0,
        )
        timeout = httpx.Timeout(REQUEST_TIMEOUT, connect=20.0)
        async with httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            http2=False,
            follow_redirects=False,
        ) as client:
            await asyncio.gather(*(worker(q, client) for q in pending))
        print()

    def run(self, queries: list[str], label: str = "") -> dict[str, dict]:
        """Resolve every query to its payload, hitting the network only for cache misses."""
        unique = list(dict.fromkeys(queries))
        pending = [q for q in unique if q not in self.cache]
        self.stats["cache_hits"] += len(unique) - len(pending)

        if not pending:
            print(f"  {label}: all {len(unique):,} queries served from cache")
            return {q: self.cache[q] for q in unique}

        print(f"  {label}: {len(pending):,} live queries "
              f"({len(unique) - len(pending):,} cached)")
        asyncio.run(self._run_async(pending))
        return {q: self.cache.get(q, {}) for q in unique}

    def close(self):
        if self._cache_file:
            self._cache_file.close()


# --------------------------------------------------------------------------
# Enrichment stages
# --------------------------------------------------------------------------

def run_stage(df: pd.DataFrame, indices: list[int], query_fn, client: SearchClient,
              best: dict, label: str):
    """Build queries for `indices`, execute them, and fold results into `best`."""
    tasks = [(idx, variant, query)
             for idx in indices
             for variant, query in query_fn(df.loc[idx])]
    if not tasks:
        return

    payloads = client.run([q for _, _, q in tasks], label=label)

    for idx, variant, query in tasks:
        candidate = best_from_payload(df.loc[idx], payloads.get(query, {}), variant)
        if candidate is None:
            continue
        current = best.get(idx)
        if current is None or CONFIDENCE_RANK[candidate["confidence"]] > CONFIDENCE_RANK[current["confidence"]]:
            best[idx] = candidate


def demote_overclaimed_handles(best: dict, threshold: int = 3):
    """
    One handle matched to many different people is a ranking artifact, not a
    person - a popular account that surfaces for lots of unrelated queries.
    Such handles get knocked down to `low` so they cannot masquerade as solid hits.
    """
    counts = Counter(c["username"].lower() for c in best.values())
    demoted = 0
    for candidate in best.values():
        if counts[candidate["username"].lower()] >= threshold and candidate["confidence"] != "very_high":
            candidate["confidence"] = "low"
            candidate["evidence"] = "[demoted: handle claimed by multiple contacts] " + candidate["evidence"]
            demoted += 1
    if demoted:
        print(f"  demoted {demoted:,} rows whose handle was claimed by {threshold}+ contacts")


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

def coverage_report(df: pd.DataFrame) -> pd.DataFrame:
    total = len(df)
    counts = df["twitter_confidence"].value_counts().to_dict()
    found = int((df["twitter_username"].astype(str).str.strip() != "").sum())
    high = int(df["twitter_confidence"].isin(["very_high", "high"]).sum())
    medium_plus = high + int((df["twitter_confidence"] == "medium").sum())

    rows = [
        ("total_contacts", total, 100.0),
        ("handle_found_any_tier", found, 100 * found / total if total else 0),
        ("very_high", counts.get("very_high", 0), 100 * counts.get("very_high", 0) / total if total else 0),
        ("high", counts.get("high", 0), 100 * counts.get("high", 0) / total if total else 0),
        ("medium", counts.get("medium", 0), 100 * counts.get("medium", 0) / total if total else 0),
        ("low", counts.get("low", 0), 100 * counts.get("low", 0) / total if total else 0),
        ("not_found", counts.get("not_found", 0), 100 * counts.get("not_found", 0) / total if total else 0),
        ("high_plus (recommended accuracy filter)", high, 100 * high / total if total else 0),
        ("medium_plus", medium_plus, 100 * medium_plus / total if total else 0),
    ]
    return pd.DataFrame(rows, columns=["metric", "count", "percent_of_total"]).round(2)


def apply_results(df: pd.DataFrame, best: dict[int, dict]) -> None:
    """
    Fold the winning candidates into the frame, in place.

    Called after every chunk so each checkpoint CSV reflects everything found so
    far, and once more at the end after the overclaimed-handle demotion.
    Contacts not yet reached simply stay `not_found`.
    """
    for column in OUTPUT_COLUMNS:
        if column not in df.columns:
            df[column] = ""
    df["twitter_confidence"] = "not_found"

    for idx, candidate in best.items():
        df.at[idx, "twitter_username"] = candidate["username"]
        df.at[idx, "twitter_handle"] = "@" + candidate["username"]
        df.at[idx, "twitter_profile_url"] = candidate["url"]
        df.at[idx, "twitter_confidence"] = candidate["confidence"]
        df.at[idx, "twitter_matched_query"] = candidate["variant"]
        df.at[idx, "twitter_match_evidence"] = candidate["evidence"]
        df.at[idx, "twitter_username_high_conf"] = (
            candidate["username"] if candidate["confidence"] in ("very_high", "high") else "")


def read_table(path: Path, sheet: str | None = None) -> pd.DataFrame:
    """
    Load .csv or .xlsx by extension, so a 100k-row CSV export and a Segment
    workbook are interchangeable inputs. `sheet` is ignored for CSV.
    """
    if path.suffix.lower() in (".csv", ".txt"):
        return pd.read_csv(path, low_memory=False)
    return pd.read_excel(path, sheet_name=sheet) if sheet else pd.read_excel(path)


def write_table(frames: dict[str, pd.DataFrame], output_path: Path,
                excel: bool = True) -> list[Path]:
    """
    Write one frame per named sheet.

    Excel encoding is the bottleneck at scale - a 100k x 32 frame takes ~90s
    per sheet versus ~2s for the same data as CSV, and the deliverable writes
    several full-size sheets. Past roughly 50k rows the workbook stops being
    worth the wait, so `excel=False` emits CSV siblings instead and every
    caller exposes it as --no-excel.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    primary = next(iter(frames))
    csv_path = output_path.with_suffix(".csv")
    frames[primary].to_csv(csv_path, index=False, encoding="utf-8-sig")
    written.append(csv_path)

    if not excel:
        for name, frame in list(frames.items())[1:]:
            slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
            side = output_path.with_name(f"{output_path.stem}__{slug}.csv")
            frame.to_csv(side, index=False, encoding="utf-8-sig")
            written.append(side)
        return written

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for name, frame in frames.items():
            # Excel caps sheet names at 31 characters
            frame.to_excel(writer, sheet_name=name[:31], index=False)
    written.insert(0, output_path)
    return written


def write_output(df: pd.DataFrame, report: pd.DataFrame, output_path: Path,
                 excel: bool = True):
    frames = {
        "Segment 5 Enriched": df,
        "Coverage Report": report,
        "High Confidence Only": df[df["twitter_confidence"].isin(["very_high", "high"])],
    }
    return write_table(frames, output_path, excel=excel)


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--sheet", default="Segment 5")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, help="only process the first N contacts")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    parser.add_argument("--queries-per-contact", type=int, choices=[1, 2, 5],
                        help="cap enrichment search queries per contact. 1 uses "
                             "the single best query, 2 uses stage 1 only, 5 runs "
                             "the strongest five variants without the full fallback set.")
    parser.add_argument("--no-stage2", action="store_true",
                        help="skip long-tail fallback queries (cheapest, lower coverage)")
    parser.add_argument("--single-query", action="store_true",
                        help="cheapest mode: exactly 1 Zyte query per contact "
                             "(no stage 2, only the single best variant per row). "
                             "Lowest coverage, lowest cost.")
    parser.add_argument("--no-excel", action="store_true",
                        help="write CSV only - much faster above ~50k rows")
    parser.add_argument("--checkpoint-every", type=int, default=1000,
                        metavar="N",
                        help="rewrite the output CSV every N contacts so a long "
                             "run always has partial results on disk (0 = only at the end)")
    parser.add_argument("--shard", type=int, default=0, metavar="N",
                        help="which slice this process handles (1-based), e.g. --shard 1")
    parser.add_argument("--of", type=int, default=0, metavar="M",
                        help="how many slices in total, e.g. --of 2. Run one "
                             "process per shard, then merge with merge_shards.py")
    parser.add_argument("--cache", type=Path, default=CACHE_PATH)
    args = parser.parse_args()

    if args.queries_per_contact == 1:
        args.single_query = True
    elif args.queries_per_contact == 2:
        args.no_stage2 = True

    started = time.time()

    print(f"Reading {args.input.name}" + (f" [{args.sheet}]" if args.input.suffix.lower() != ".csv" else ""))
    df = read_table(args.input, args.sheet)
    if args.limit:
        df = df.head(args.limit).copy()

    # Sharding: split the file into contiguous blocks so several processes can
    # work different slices at once. Each shard gets its own output and cache
    # file, so the processes never write over one another; merge_shards.py
    # stitches the pieces back together afterwards.
    if args.of > 1:
        if not 1 <= args.shard <= args.of:
            sys.exit(f"--shard must be between 1 and {args.of}, got {args.shard}")
        block = math.ceil(len(df) / args.of)
        lo, hi = (args.shard - 1) * block, min(args.shard * block, len(df))
        df = df.iloc[lo:hi].copy()
        suffix = f"_shard{args.shard}of{args.of}"
        args.output = args.output.with_name(
            f"{args.output.stem}{suffix}{args.output.suffix}")
        args.cache = args.cache.with_name(f"{args.cache.stem}{suffix}{args.cache.suffix}")
        print(f"  shard {args.shard} of {args.of}: rows {lo:,}-{hi - 1:,}")

    df = df.reset_index(drop=True)
    print(f"  {len(df):,} contacts")

    client = SearchClient(ZYTE_API_KEY, args.cache, workers=args.workers)
    print(f"  backend: zyte.com Google SERP ({args.workers} concurrent workers)")

    best: dict[int, dict] = {}

    # Contacts are processed in chunks and the CSV is rewritten after each one,
    # so a long run always has a usable output file on disk rather than
    # producing everything or nothing. An 18-hour 100k run that dies at hour 17
    # would otherwise leave nothing behind.
    chunk_size = args.checkpoint_every if args.checkpoint_every > 0 else len(df)
    chunks = [list(df.index[i:i + chunk_size]) for i in range(0, len(df), chunk_size)]

    try:
        for number, chunk in enumerate(chunks, start=1):
            tag = f"chunk {number}/{len(chunks)}" if len(chunks) > 1 else ""
            if args.queries_per_contact and args.queries_per_contact > 2:
                stage1_fn = lambda row, n=args.queries_per_contact: capped_queries(row, n)
                stage1_label = f"capped query set ({args.queries_per_contact}/contact)"
                skip_stage2 = True
            else:
                stage1_fn = single_query if args.single_query else stage1_queries
                stage1_label = "single query (1/contact)" if args.single_query else "company and location queries"
                skip_stage2 = args.no_stage2 or args.single_query
            print(f"\nStage 1 - {stage1_label}  {tag}")
            run_stage(df, chunk, stage1_fn, client, best, f"stage 1 {tag}".strip())
            resolved = sum(1 for c in best.values() if CONFIDENCE_RANK[c["confidence"]] >= 3)
            print(f"  {len(best):,} contacts with a candidate, {resolved:,} at high+")

            if not skip_stage2:
                unresolved = [i for i in chunk
                              if i not in best or CONFIDENCE_RANK[best[i]["confidence"]] < 4]
                print(f"Stage 2 - fallback queries for {len(unresolved):,} unresolved  {tag}")
                run_stage(df, unresolved, stage2_queries, client, best, f"stage 2 {tag}".strip())

            if len(chunks) > 1:
                apply_results(df, dict(best))
                partial = args.output.with_suffix(".csv")
                partial.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(partial, index=False, encoding="utf-8-sig")
                seen = min(number * chunk_size, len(df))
                print(f"  checkpoint: {seen:,}/{len(df):,} contacts written to {partial.name}")
    finally:
        client.close()

    print("\nPost-processing")
    demote_overclaimed_handles(best)
    apply_results(df, best)

    report = coverage_report(df)
    written = write_output(df, report, args.output, excel=not args.no_excel)
    elapsed = time.time() - started

    print("\n" + "=" * 62)
    print("ENRICHMENT COMPLETE")
    print("=" * 62)
    print(report.to_string(index=False))
    print(f"\nlive queries : {client.stats['live_queries']:,}")
    print(f"cache hits   : {client.stats['cache_hits']:,}")
    print(f"throttled    : {client.stats['throttled']:,} "
          f"(breaker trips {client.stats['breaker_trips']:,})")
    dropped = client.stats["dropped_queries"]
    if dropped:
        print(f"DROPPED      : {dropped:,} queries never returned - "
              f"re-run to retry them for free from cache")
    else:
        print("dropped      : 0 (no coverage lost to throttling)")
    print(f"elapsed      : {elapsed / 60:.1f} min ({elapsed:.0f}s)")
    if elapsed > 0 and client.stats["live_queries"]:
        rate = client.stats["live_queries"] / elapsed
        print(f"throughput   : {rate:.0f} queries/sec  "
              f"-> ~{10000 * 6 / rate / 60:.0f} min for 10k contacts")
    print("\nwritten:")
    for path in written:
        print(f"  {path}")


if __name__ == "__main__":
    main()
