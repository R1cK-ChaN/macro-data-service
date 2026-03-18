#!/usr/bin/env python3
"""Quick status check for shadow mode — shows latest digest and recent errors."""

from __future__ import annotations

import json
from pathlib import Path

LOG_DIR = Path(__file__).parent / ".macro-data" / "logs"
DIGEST_FILE = LOG_DIR / "daily_digest.jsonl"
LOG_FILE = LOG_DIR / "shadow.log"


def show_status() -> None:
    # Latest digest
    if DIGEST_FILE.exists():
        lines = DIGEST_FILE.read_text().strip().split("\n")
        if lines:
            latest = json.loads(lines[-1])
            print("=== LATEST DIGEST ===")
            print(f"  Time:     {latest['timestamp']}")
            print(f"  Cycle:    {latest.get('cycle', '?')}")
            print(f"  Coverage: {latest['concepts_covered']}/{latest['concepts_total']} ({latest['coverage_pct']}%)")
            print(f"  Total:    {latest['total_obs']} observations")
            print(f"  Duration: {latest.get('cycle_duration_s', '?')}s")
            print()

            print("  Source breakdown:")
            for src, info in sorted(latest.get("sources", {}).items()):
                print(f"    {src:<20s} {info['series']:>3d} series  {info['obs']:>5d} obs")

            errors = latest.get("error_sources", [])
            if errors:
                print(f"\n  ERRORS ({len(errors)}):")
                for src in errors:
                    err = latest.get("source_results", {}).get(src, {}).get("error", "")
                    print(f"    {src}: {err[:80]}")

            # Show trend if multiple digests
            if len(lines) >= 2:
                prev = json.loads(lines[-2])
                delta_obs = latest["total_obs"] - prev["total_obs"]
                print(f"\n  Delta from previous cycle: {'+' if delta_obs >= 0 else ''}{delta_obs} obs")
    else:
        print("No digest file found. Shadow mode may not have run yet.")
        print(f"  Expected: {DIGEST_FILE}")

    # Recent errors from log
    if LOG_FILE.exists():
        print("\n=== RECENT ERRORS (last 20) ===")
        lines = LOG_FILE.read_text().strip().split("\n")
        errors = [l for l in lines if " ERROR" in l]
        for line in errors[-20:]:
            print(f"  {line}")
        if not errors:
            print("  (none)")
    print()


if __name__ == "__main__":
    show_status()
