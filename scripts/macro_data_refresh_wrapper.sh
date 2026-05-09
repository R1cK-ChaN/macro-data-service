#!/usr/bin/env bash
# Wrapper for the umbrella `macro-data-service refresh` invoked from
# systemd --user timer (issue #106).
#
# Calls `refresh_all_sources` — the catch-all daily refresh for every
# source registered in the orchestrator. The release-aware watcher
# (`macro-data-release-watch.timer`, #130) handles high-priority
# concepts on a tighter loop; this umbrella covers everything else
# and acts as a safety-net so no source goes more than 24h without
# a refresh attempt.
#
# Per-family timers (US / EU / JP / CN / global) are a future
# enhancement once each family's refresh budget and cadence are
# tuned independently.
#
# Responsibilities:
#   * flock guard — overlapping fires cannot race the engine DB.
#   * cd into the repo so .macro-data/, .env, and scripts/ resolve.
#   * exec the python module so the systemd unit's MainPID tracks
#     the CLI entry-point directly (clean exit-code reporting).
#
# Override REPO_ROOT to point at a different checkout. The default is
# the directory containing this script's parent.

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
LOCK_FILE="${REPO_ROOT}/.macro-data/macro_data_refresh.lock"

mkdir -p "$(dirname "$LOCK_FILE")"

cd "$REPO_ROOT"
# PYTHONPATH=src makes the CLI runnable even on hosts without an
# editable install (`pip install -e .`).
exec flock --nonblock "$LOCK_FILE" \
    env PYTHONPATH="${REPO_ROOT}/src" \
    python3 -m macro_data.cli refresh "$@"
