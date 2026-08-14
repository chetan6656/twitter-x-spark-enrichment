#!/usr/bin/env python3
"""
Verification pass over the handles produced by enrich_segment5_twitter.py.

The enrichment stage infers a match from Google result snippets, which is
recall-friendly but lets confident-looking false positives through - during
tuning it matched @ProfKhanapuri to Trevorlyn Menezes and @LynnThorpe to Mark
Fennessy purely on ranking noise.

This stage adds an independent check. Querying

    "x.com/<handle>" OR "twitter.com/<handle>"

returns the profile's own card, whose title carries the account's real display
name:

    Simon Peel (@SimonPeel) / Posts / X

Comparing that display name against the contact's name either confirms the
match or exposes it as somebody else entirely. One query per handle, so the
whole pass costs about a quarter as much as the enrichment run it validates.

Verdicts written to `twitter_verified`:
    confirmed   - profile display name matches the contact's name
    rejected    - profile belongs to a visibly different person
    unverified  - profile card not indexed; original confidence left alone

Usage:
    python3 verify_twitter_handles.py
    python3 verify_twitter_handles.py --input outputs/Segment5_twitter_enriched.xlsx
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

from enrich_segment5_twitter import (
    CONFIDENCE_RANK,
    SearchClient,
    ZYTE_API_KEY,
    clean,
    coverage_report,
    norm,
    read_table,
    write_table,
)

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = PROJECT_DIR / "outputs" / "Segment5_twitter_enriched.csv"
DEFAULT_OUTPUT = PROJECT_DIR / "outputs" / "Segment5_twitter_verified.xlsx"
CACHE_PATH = PROJECT_DIR / "outputs" / "zyte_verify_cache.jsonl"

# Google phrasing: "Simon Peel (@SimonPeel) / Posts / X"
TITLE_RE = re.compile(r"^(.+?)\s*[\(\[]@([A-Za-z0-9_]{1,15})[\)\]]")
# Fallback phrasing seen on some tweet results: 'Rob Imbeault on X: "..."'
NAME_ON_X_RE = re.compile(r"^(.+?)\s+on X:")
FOLLOWERS_RE = re.compile(r"([\d][\d,\.]*\s*[KMkm]?)\s*Followers")


def verify_query(handle: str) -> str:
    """Google honours this quoted OR form for the profile lookup."""
    return f'"x.com/{handle}" OR "twitter.com/{handle}"'


def parse_profile_card(payload: dict, handle: str) -> dict | None:
    """Pull display name / location / follower count out of the profile's own result."""
    if not isinstance(payload, dict):
        return None

    wanted = handle.lower()
    fallback = None

    for item in payload.get("organic", []):
        link = (item.get("link") or "").lower()
        # the URL must belong to this exact handle, not merely contain it
        if not re.search(rf"(?:x|twitter)\.com/{re.escape(wanted)}(?:[/?#]|$)", link):
            continue

        title = item.get("title") or ""
        snippet = item.get("snippet") or ""
        followers = FOLLOWERS_RE.search(snippet)

        match = TITLE_RE.match(title)
        if match and match.group(2).lower() == wanted:
            return {
                "display_name": match.group(1).strip(),
                "followers": followers.group(1).strip() if followers else "",
                "snippet": snippet[:200],
            }

        # Some results title a tweet '<Display Name> on X: "..."' instead of a
        # profile card. Hold the first one as a fallback and keep looking for
        # a proper profile card.
        on_x = NAME_ON_X_RE.match(title)
        if on_x and fallback is None:
            fallback = {
                "display_name": on_x.group(1).strip(),
                "followers": followers.group(1).strip() if followers else "",
                "snippet": snippet[:200],
            }

    return fallback


def judge(row, display_name: str) -> str:
    """
    Decide whether a profile's display name belongs to this contact.

    Requiring only the surname would wave through common-name collisions, and
    requiring an exact full-string match would reject legitimate profiles that
    carry a middle name, a suffix, or an emoji. Both name parts must appear.
    """
    first, last = norm(row.get("ic_fname")), norm(row.get("ic_lname"))
    shown = norm(display_name)
    if not shown:
        return "unverified"
    if not (first or last):
        return "unverified"

    shown_tokens = set(shown.split())
    first_hit = bool(first) and any(t.startswith(first) or first.startswith(t)
                                    for t in shown_tokens)
    last_hit = bool(last) and any(t.startswith(last) or last.startswith(t)
                                  for t in shown_tokens)

    if first_hit and last_hit:
        return "confirmed"
    # a real profile card whose owner shares neither name part is a different person
    return "rejected"


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--sheet", default="Segment 5 Enriched")
    parser.add_argument("--workers", type=int, default=30)
    parser.add_argument("--no-excel", action="store_true",
                        help="write CSV only - much faster above ~50k rows")
    parser.add_argument("--cache", type=Path, default=CACHE_PATH)
    args = parser.parse_args()

    df = read_table(args.input, args.sheet)
    df["twitter_username"] = df["twitter_username"].fillna("").astype(str).str.strip()
    # An all-empty column round-trips through CSV as float64 NaN, which then
    # rejects string assignments below - force it back to string regardless.
    df["twitter_username_high_conf"] = df["twitter_username_high_conf"].fillna("").astype(str).str.strip()

    targets = df.index[df["twitter_username"] != ""].tolist()
    print(f"{len(df):,} contacts, {len(targets):,} handles to verify")

    payloads = {}
    if targets:
        client = SearchClient(ZYTE_API_KEY, args.cache, workers=args.workers)
        print("  backend: zyte.com Google SERP")
        try:
            queries = [verify_query(df.at[i, "twitter_username"]) for i in targets]
            payloads = client.run(queries, label="verify")
        finally:
            client.close()
    else:
        print("  nothing to verify - writing the file through unchanged so the "
              "next step still has something to read")

    for column in ["twitter_display_name", "twitter_followers", "twitter_verified"]:
        df[column] = ""
    df["twitter_verified"] = ""

    counts = {"confirmed": 0, "rejected": 0, "unverified": 0}
    for idx in targets:
        handle = df.at[idx, "twitter_username"]
        card = parse_profile_card(payloads.get(verify_query(handle), {}), handle)

        if card is None:
            verdict = "unverified"
        else:
            df.at[idx, "twitter_display_name"] = card["display_name"]
            df.at[idx, "twitter_followers"] = card["followers"]
            verdict = judge(df.loc[idx], card["display_name"])

        df.at[idx, "twitter_verified"] = verdict
        counts[verdict] += 1

        if verdict == "confirmed":
            df.at[idx, "twitter_confidence"] = "very_high"
            df.at[idx, "twitter_username_high_conf"] = handle
        elif verdict == "rejected":
            df.at[idx, "twitter_confidence"] = "rejected"
            df.at[idx, "twitter_username_high_conf"] = ""

    verified_df = df[df["twitter_verified"] == "confirmed"]
    report = coverage_report(df)
    extra = pd.DataFrame([
        ("verified_confirmed", counts["confirmed"], 100 * counts["confirmed"] / len(df)),
        ("verified_rejected", counts["rejected"], 100 * counts["rejected"] / len(df)),
        ("verified_unverified", counts["unverified"], 100 * counts["unverified"] / len(df)),
    ], columns=["metric", "count", "percent_of_total"]).round(2)
    report = pd.concat([report, extra], ignore_index=True)

    written = write_table({
        "Segment 5 Verified": df,
        "Coverage Report": report,
        "Confirmed Only": verified_df,
    }, args.output, excel=not args.no_excel)

    print("\n" + "=" * 62)
    print("VERIFICATION COMPLETE")
    print("=" * 62)
    print(report.to_string(index=False))
    checked = counts["confirmed"] + counts["rejected"]
    if checked:
        print(f"\nprecision on checkable handles: {100 * counts['confirmed'] / checked:.1f}% "
              f"({counts['confirmed']:,} confirmed / {checked:,} with a readable profile card)")
    print("\nwritten:")
    for path in written:
        print(f"  {path}")


if __name__ == "__main__":
    main()
