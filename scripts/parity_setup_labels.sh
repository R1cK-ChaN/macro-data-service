#!/usr/bin/env bash
# Idempotent label bootstrap for the parity tripwire (issue #22 P5).
#
# `gh issue create --label foo` errors out if the label does not exist
# in the repo; the parity filer relies on `parity-drift` plus one
# `agency:<id>` label per agency in `agency_registry.AGENCIES`. Running
# this script once per repo (or after adding a new agency) creates any
# missing labels with a stable colour mapping.
#
# Safe to re-run: existing labels are detected and skipped (no
# overwrite, no `gh label edit`).

set -euo pipefail

cd "$(cd "$(dirname "$0")/.." && pwd)"

ensure_label() {
    local name="$1"
    local color="${2:-0e8a16}"
    local description="${3:-}"
    if gh label list --search "$name" --limit 50 \
        | awk -F'\t' '{print $1}' | grep -Fxq "$name"; then
        echo "exists: $name"
        return
    fi
    echo "creating: $name"
    if [[ -n "$description" ]]; then
        gh label create "$name" --color "$color" --description "$description"
    else
        gh label create "$name" --color "$color"
    fi
}

ensure_label "parity-drift" "d93f0b" \
    "Daily TE parity tripwire — TE actual disagrees with our official-source first-release (issue #22)."
ensure_label "agency:infra" "5319e7" \
    "Parity job infrastructure failure (2-strike self-monitor)."

AGENCY_IDS="$(PYTHONPATH=src python3 -c '
from ingestion.calendar.agency_registry import AGENCIES
for a in AGENCIES:
    print(a.agency_id)
')"

while IFS= read -r agency_id; do
    [[ -z "$agency_id" ]] && continue
    ensure_label "agency:${agency_id}" "0e8a16" \
        "Owns calendar releases tracked by the parity tripwire."
done <<< "$AGENCY_IDS"

echo "done."
