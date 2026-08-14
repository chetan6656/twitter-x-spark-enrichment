#!/usr/bin/env python3
"""
Apply the accuracy filter that actually works, and emit the deliverable.

The 99 handles checked against their real X profile card gave the first hard
precision numbers of this project, and they showed something useful: precision
barely depends on which search query found a handle (50-78% across all seven
variants), and depends almost entirely on whether the handle contains the
person's name.

    first + last in handle    95.8%   (46/48)
    first name only          100.0%   ( 8/8 )
    last name only            76.5%   (13/17)
    no name in handle         20.6%   ( 7/34)   <- discard

That last bucket is where nearly every false positive lives: @kimc2830 for
Edward Barraclough, @SamiraDaruki for Nicole Hao, @PayWithExtend for Guillaume
Bouvard. Those are real, live accounts - existence checks cannot catch them,
because the account is fine, it just belongs to somebody else.

Dropping that one bucket lifts measured precision to roughly 92% on the
remaining handles, clearing the 80% accuracy bar. It costs about a third of the
raw handle count, which is the coverage/accuracy trade made explicit.

Usage:
    python3 build_final_deliverable.py
    python3 build_final_deliverable.py --strict     # first+last and first-only
"""

from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path

import pandas as pd

from enrich_segment5_twitter import read_table, write_table

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = PROJECT_DIR / "outputs" / "Segment5_twitter_verified.csv"
DEFAULT_OUTPUT = PROJECT_DIR / "outputs" / "Segment5_Twitter_DELIVERABLE.xlsx"

# Measured precision per class, carried into the sheet so the numbers travel
# with the data instead of living only in a chat log.
CLASS_PRECISION = {
    "first_and_last_in_handle": 95.8,
    "first_name_only": 100.0,
    "last_name_only": 76.5,
    "no_name_in_handle": 20.6,
}
RECOMMENDED_CLASSES = ["first_and_last_in_handle", "first_name_only", "last_name_only"]
STRICT_CLASSES = ["first_and_last_in_handle", "first_name_only"]


def squash(value) -> str:
    """Lowercase, strip accents, drop everything that is not a letter or digit."""
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def classify(row) -> str:
    handle = squash(row.get("twitter_username"))
    if not handle:
        return ""
    first, last = squash(row.get("ic_fname")), squash(row.get("ic_lname"))

    if first and last and first in handle and last in handle:
        return "first_and_last_in_handle"
    if last and last in handle:
        return "last_name_only"
    if first and first in handle:
        return "first_name_only"
    return "no_name_in_handle"


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--sheet", default="Segment 5 Purged")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--strict", action="store_true",
                        help="keep only the two ~96%%+ classes")
    parser.add_argument("--no-excel", action="store_true",
                        help="write CSV only - much faster above ~50k rows")
    args = parser.parse_args()

    keep = STRICT_CLASSES if args.strict else RECOMMENDED_CLASSES

    df = read_table(args.input, args.sheet)
    df["twitter_username"] = df["twitter_username"].fillna("").astype(str).str.strip()

    df["twitter_name_match_class"] = df.apply(classify, axis=1)
    df["twitter_class_precision_pct"] = df["twitter_name_match_class"].map(CLASS_PRECISION).fillna("")
    df["twitter_recommended"] = df["twitter_name_match_class"].isin(keep) & (df["twitter_username"] != "")

    # The delivered handle: blank unless the row clears the accuracy bar.
    df["twitter_username_final"] = ""
    df["twitter_handle_final"] = ""
    df["twitter_profile_url_final"] = ""
    sel = df["twitter_recommended"]
    df.loc[sel, "twitter_username_final"] = df.loc[sel, "twitter_username"]
    df.loc[sel, "twitter_handle_final"] = "@" + df.loc[sel, "twitter_username"]
    df.loc[sel, "twitter_profile_url_final"] = "https://x.com/" + df.loc[sel, "twitter_username"]

    total = len(df)
    raw = int((df["twitter_username"] != "").sum())
    delivered = int(sel.sum())

    breakdown = (df[df["twitter_username"] != ""]
                 .groupby("twitter_name_match_class")
                 .size()
                 .reset_index(name="handles"))
    breakdown["measured_precision_pct"] = breakdown["twitter_name_match_class"].map(CLASS_PRECISION)
    breakdown["in_deliverable"] = breakdown["twitter_name_match_class"].isin(keep)

    # Coverage-weighted expectation across the classes actually shipped.
    shipped = breakdown[breakdown["in_deliverable"]]
    expected = ((shipped["handles"] * shipped["measured_precision_pct"]).sum()
                / shipped["handles"].sum()) if len(shipped) else 0.0

    report = pd.DataFrame([
        ("total_contacts", total, 100.0),
        ("raw_handles_found", raw, round(100 * raw / total, 2)),
        ("delivered_handles", delivered, round(100 * delivered / total, 2)),
        ("discarded_low_precision", raw - delivered, round(100 * (raw - delivered) / total, 2)),
        ("expected_precision_pct", round(expected, 1), ""),
    ], columns=["metric", "value", "percent_of_total"])

    written = write_table({
        "DELIVERABLE 80pct accuracy": df[df["twitter_recommended"]],
        "All Contacts": df,
        "Summary": report,
        "Precision By Class": breakdown,
    }, args.output, excel=not args.no_excel)

    print("=" * 62)
    print("DELIVERABLE BUILT" + ("  [strict]" if args.strict else ""))
    print("=" * 62)
    print(breakdown.to_string(index=False))
    print()
    print(report.to_string(index=False))
    print(f"\nexpected precision on delivered handles: {expected:.1f}%")
    print(f"coverage delivered: {delivered:,}/{total:,} = {100 * delivered / total:.2f}%")
    print("\nwritten:")
    for path in written:
        print(f"  {path}")


if __name__ == "__main__":
    main()
