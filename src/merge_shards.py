#!/usr/bin/env python3
"""
Stitch the shard CSVs back into one file.

When enrichment is split across several terminals, each process writes its own
slice:

    outputs/Segment5_twitter_enriched_shard1of2.csv
    outputs/Segment5_twitter_enriched_shard2of2.csv

This concatenates them, in shard order, into the single file the verification
step expects:

    outputs/Segment5_twitter_enriched.csv

Run it once every shard has finished. It reads one shard at a time rather than
loading them all at once, so peak memory stays close to the size of a single
shard - which matters on a 16GB machine already running two enrichment
processes.

Usage:
    python3 merge_shards.py
    python3 merge_shards.py --pattern "Segment5_twitter_enriched_shard*of*.csv"
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_DIR = PROJECT_DIR / "outputs"
DEFAULT_PATTERN = "Segment5_twitter_enriched_shard*of*.csv"
DEFAULT_OUTPUT = DEFAULT_DIR / "Segment5_twitter_enriched.csv"

SHARD_RE = re.compile(r"_shard(\d+)of(\d+)\.csv$", re.I)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dir", type=Path, default=DEFAULT_DIR)
    parser.add_argument("--pattern", default=DEFAULT_PATTERN)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    # The glob alone also catches the sidecar reports each run writes
    # (..._shard1of2__coverage_report.csv), which are a different shape entirely
    # and would corrupt the merge. SHARD_RE anchors on the real suffix, so only
    # true shard files survive.
    shards = [p for p in sorted(args.dir.glob(args.pattern)) if SHARD_RE.search(p.name)]
    if not shards:
        sys.exit(f"no shard files matching {args.pattern} in {args.dir}\n"
                 f"Did the enrichment runs finish? Check that directory.")

    # order by shard number, not filename, so shard10 lands after shard9
    def order(path):
        m = SHARD_RE.search(path.name)
        return int(m.group(1)) if m else 0

    shards.sort(key=order)

    expected = None
    for path in shards:
        m = SHARD_RE.search(path.name)
        if m:
            expected = int(m.group(2))
            break
    if expected and len(shards) != expected:
        found = ", ".join(str(order(p)) for p in shards)
        print(f"WARNING: the filenames say there should be {expected} shards, "
              f"but only {len(shards)} are present (found shard {found}).")
        print("         Merging anyway - contacts from the missing shard will be absent.")

    print(f"merging {len(shards)} shards into {args.output.name}")

    total, header_written = 0, False
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8-sig", newline="") as out:
        for path in shards:
            frame = pd.read_csv(path, low_memory=False)
            frame.to_csv(out, index=False, header=not header_written)
            header_written = True
            total += len(frame)
            found = int((frame.get("twitter_username", pd.Series(dtype=str))
                         .fillna("").astype(str).str.strip() != "").sum())
            print(f"   {path.name:<48} {len(frame):>7,} rows, {found:>6,} handles")
            del frame

    print(f"\n{total:,} rows -> {args.output}")
    print("\nNext step:")
    print("   python verify_twitter_handles.py --no-excel")


if __name__ == "__main__":
    main()
