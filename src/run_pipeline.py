#!/usr/bin/env python3
"""
Run the whole Twitter/X enrichment pipeline, start to finish.

Runs the three steps back to back in a single process:

    1. enrich_segment5_twitter.py   - find candidate handles
    2. verify_twitter_handles.py    - confirm each handle against the real profile
    3. build_final_deliverable.py   - apply the accuracy filter, write the final CSV

Two ways to run it
-------------------

A) ONE terminal, whole file:

    python run_pipeline.py --limit 10000 --no-excel

B) TWO terminals in parallel, each doing half the rows (use this when you
   want to split a 2,000-row file into two 1,000-row halves and run both
   terminals at once):

    Terminal 1:
        python run_pipeline.py --shard 1 --of 2 --no-excel
    Terminal 2:
        python run_pipeline.py --shard 2 --of 2 --no-excel

   Terminal 1 handles rows 1-1000 and writes outputs\\first_output.csv
   Terminal 2 handles rows 1001-2000 and writes outputs\\second_output.csv
   Every intermediate file (cache, enriched, verified) is also kept
   separate per shard, so the two terminals never touch the same file and
   can safely run at the exact same time.

Query budget
------------
By default each contact costs up to ~6-7 search queries (2 cheap ones +
several long-tail fallbacks). To cut credit usage, use:

    --queries-per-contact 2   # (recommended) up to 2 queries/contact, no fallback stage
    --queries-per-contact 1   # cheapest: exactly 1 query/contact

If a step fails partway through, just run the exact same command again -
every result is cached on disk and the enrichment step's checkpointing
means nothing already found is lost; re-running resumes rather than
starting over.

Usage
-----
    python run_pipeline.py --limit 10000 --no-excel --queries-per-contact 2
    python run_pipeline.py --shard 1 --of 2 --no-excel --queries-per-contact 2
    python run_pipeline.py --shard 2 --of 2 --no-excel --queries-per-contact 2
    python run_pipeline.py --input data\\my_contacts.csv --no-excel
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
OUTPUTS = PROJECT_DIR / "outputs"


def shard_label(shard: int, of: int) -> str | None:
    """Human-friendly name for a shard's final file: first_output, second_output, ..."""
    if of <= 1:
        return None
    names = {1: "first_output", 2: "second_output", 3: "third_output", 4: "fourth_output"}
    return names.get(shard, f"output_{shard}")


def safe_suffix(tag: str) -> str:
    """Filename-safe suffix for keeping separate batch outputs apart."""
    tag = re.sub(r"[^A-Za-z0-9_.-]+", "_", tag.strip()).strip("_")
    return f"_{tag}" if tag else ""


def banner(text: str) -> None:
    print("\n" + "=" * 70)
    print(text)
    print("=" * 70)


def run_step(step_num: int, total: int, description: str, cmd: list[str]) -> None:
    banner(f"STEP {step_num}/{total} - {description}")
    print("  running:", " ".join(str(c) for c in cmd))
    started = time.time()
    result = subprocess.run(cmd, cwd=PROJECT_DIR)
    elapsed = time.time() - started
    if result.returncode != 0:
        sys.exit(
            f"\nStep {step_num} ({description}) failed with exit code "
            f"{result.returncode}.\n"
            f"Fix the error above, then just run this same run_pipeline.py "
            f"command again - finished work is cached and will not be redone."
        )
    print(f"\n  step {step_num} done in {elapsed / 60:.1f} min")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--input", type=Path,
                        help="contact list to enrich (default: data\\datahub_100k.csv)")
    parser.add_argument("--sheet", default="Segment 5",
                        help="sheet name, only used if --input is an .xlsx")
    parser.add_argument("--limit", type=int,
                        help="only process the first N contacts, e.g. --limit 10000")
    parser.add_argument("--workers", type=int,
                        help="concurrent request workers (default: script's own default)")
    parser.add_argument("--queries-per-contact", type=int, choices=[1, 2, 5], default=2,
                        help="max search queries spent per contact: 2 (default, "
                             "recommended - stage 1 only, no long-tail fallback) "
                             "1 (cheapest - single best query per contact), "
                             "or 5 (larger capped query set)")
    parser.add_argument("--no-stage2", action="store_true",
                        help="(advanced) same as --queries-per-contact 2 - kept for "
                             "backwards compatibility")
    parser.add_argument("--single-query", action="store_true",
                        help="(advanced) same as --queries-per-contact 1 - kept for "
                             "backwards compatibility")
    parser.add_argument("--strict", action="store_true",
                        help="final deliverable: keep only the two highest-precision classes")
    parser.add_argument("--no-excel", action="store_true", default=True,
                        help="write CSV only (default: on - recommended at this size)")
    parser.add_argument("--with-excel", dest="no_excel", action="store_false",
                        help="also write .xlsx workbooks (slower)")
    parser.add_argument("--tag", default="",
                        help="label to keep this run's output/cache files separate, "
                             "e.g. --tag first2k")
    parser.add_argument("--shard", type=int, default=0, metavar="N",
                        help="which slice THIS terminal handles (1-based), e.g. --shard 1")
    parser.add_argument("--of", type=int, default=0, metavar="M",
                        help="how many terminals total, e.g. --of 2. Run one command "
                             "per terminal with a different --shard, at the same time.")
    args = parser.parse_args()

    # --queries-per-contact is the friendly control; fold it into the same
    # flags the underlying scripts already understand.
    if args.queries_per_contact == 1:
        args.single_query = True
    if args.queries_per_contact == 2 and not args.single_query:
        args.no_stage2 = True

    started_all = time.time()

    label = shard_label(args.shard, args.of)
    if args.of > 1 and not (1 <= args.shard <= args.of):
        sys.exit(f"--shard must be between 1 and {args.of}, got {args.shard}")

    # Every file this run touches - cache, enriched, verified, and the final
    # deliverable - gets a shard-specific name so two terminals running at
    # once never read or write the same file.
    tag_suffix = safe_suffix(args.tag)
    enrich_output_arg = OUTPUTS / f"Segment5_twitter_enriched{tag_suffix}.csv"
    enrich_cache_arg = OUTPUTS / f"zyte_cache{tag_suffix}.jsonl"

    if label:
        suffix = f"_shard{args.shard}of{args.of}"
        enrich_csv = OUTPUTS / f"Segment5_twitter_enriched{tag_suffix}{suffix}.csv"
        enrich_cache = OUTPUTS / f"zyte_cache{tag_suffix}{suffix}.jsonl"
        verify_cache = OUTPUTS / f"zyte_verify_cache{tag_suffix}{suffix}.jsonl"
        verified_csv = OUTPUTS / f"Segment5_twitter_verified{tag_suffix}{suffix}.csv"
        deliverable_csv = OUTPUTS / f"{label}{tag_suffix}.csv"
    else:
        enrich_csv = OUTPUTS / f"Segment5_twitter_enriched{tag_suffix}.csv"
        enrich_cache = OUTPUTS / f"zyte_cache{tag_suffix}.jsonl"
        verify_cache = OUTPUTS / f"zyte_verify_cache{tag_suffix}.jsonl"
        verified_csv = OUTPUTS / f"Segment5_twitter_verified{tag_suffix}.csv"
        deliverable_csv = OUTPUTS / f"Segment5_Twitter_DELIVERABLE{tag_suffix}.csv"

    banner("TWITTER/X ENRICHMENT" + (f" - shard {args.shard}/{args.of}" if label else " - single-terminal run"))
    print(f"  project folder     : {PROJECT_DIR}")
    if args.input:
        print(f"  input file         : {args.input}")
    if args.limit:
        print(f"  contact limit      : {args.limit:,}")
    if args.tag:
        print(f"  output tag         : {args.tag}")
    print(f"  queries/contact    : {args.queries_per_contact}")
    if label:
        print(f"  this terminal      : shard {args.shard} of {args.of} -> {deliverable_csv.name}")
        print("  Run the other shard(s) in their own terminal window(s) at the same time.")
    else:
        print("  This one command runs all 3 steps in order, in this window.")

    py = sys.executable

    # ---- Step 1: enrich --------------------------------------------------
    enrich_cmd = [py, "enrich_segment5_twitter.py", "--output", str(enrich_output_arg),
                  "--cache", str(enrich_cache_arg)]
    if args.input:
        enrich_cmd += ["--input", str(args.input)]
        if args.input.suffix.lower() not in (".csv", ".txt"):
            enrich_cmd += ["--sheet", args.sheet]
    if args.limit:
        enrich_cmd += ["--limit", str(args.limit)]
    if args.workers:
        enrich_cmd += ["--workers", str(args.workers)]
    if args.no_stage2:
        enrich_cmd += ["--no-stage2"]
    if args.single_query:
        enrich_cmd += ["--single-query"]
    if args.queries_per_contact == 5:
        enrich_cmd += ["--queries-per-contact", "5"]
    if args.no_excel:
        enrich_cmd += ["--no-excel"]
    if label:
        # enrich_segment5_twitter.py's own --shard/--of slices the input rows
        # AND appends "_shard{N}of{M}" to its output/cache paths itself, which
        # is exactly enrich_csv above.
        enrich_cmd += ["--shard", str(args.shard), "--of", str(args.of)]
    run_step(1, 3, "finding candidate handles (enrich_segment5_twitter.py)", enrich_cmd)

    # ---- Step 2: verify ----------------------------------------------------
    verify_cmd = [py, "verify_twitter_handles.py", "--input", str(enrich_csv),
                  "--sheet", "Segment 5 Enriched",
                  "--output", str(verified_csv.with_suffix(".xlsx")),
                  "--cache", str(verify_cache)]
    if args.workers:
        verify_cmd += ["--workers", str(args.workers)]
    if args.no_excel:
        verify_cmd += ["--no-excel"]
    run_step(2, 3, "verifying handles against real profiles (verify_twitter_handles.py)", verify_cmd)

    # ---- Step 3: build final deliverable -----------------------------------
    build_cmd = [py, "build_final_deliverable.py", "--input", str(verified_csv),
                 "--sheet", "Segment 5 Verified",
                 "--output", str(deliverable_csv.with_suffix(".xlsx"))]
    if args.strict:
        build_cmd += ["--strict"]
    if args.no_excel:
        build_cmd += ["--no-excel"]
    run_step(3, 3, "applying accuracy filter (build_final_deliverable.py)", build_cmd)

    elapsed = time.time() - started_all
    banner("PIPELINE COMPLETE")
    print(f"  total time: {elapsed / 60:.1f} min")
    print(f"\n  final file: {deliverable_csv}")
    print("  Use the twitter_username_final / twitter_handle_final / "
          "twitter_profile_url_final columns.")


if __name__ == "__main__":
    main()
