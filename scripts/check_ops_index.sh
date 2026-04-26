#!/bin/sh
# Non-fatal drift detector for ``docs/ops_index.md``.
#
# Re-runs ``tools/gen_ops_index.py`` and warns if the generated index
# differs from what is checked in. Surfaces stale auto-doc early so the
# next commit picks up the refresh.
#
# Exit code is always 0 — this is advisory, not gating. Wire it up as a
# pre-commit hook, CI step, or local audit:
#
#   ./scripts/check_ops_index.sh
#
set -u

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GENERATOR="$REPO_ROOT/tools/gen_ops_index.py"
INDEX="$REPO_ROOT/docs/ops_index.md"

if [ ! -f "$GENERATOR" ]; then
    printf 'WARN  generator missing: %s\n' "$GENERATOR" 1>&2
    exit 0
fi

# Treat a missing index as drift the user must regenerate explicitly —
# do not run the generator, so an accidental deletion is surfaced
# instead of being silently masked by this hook.
if [ ! -f "$INDEX" ]; then
    printf 'WARN  docs/ops_index.md is missing — regenerate with:\n' 1>&2
    printf '      python3 tools/gen_ops_index.py\n' 1>&2
    exit 0
fi

# Snapshot the existing index, regenerate, diff, then restore — so a
# stale checkin is not silently rewritten as a side effect of running
# this hook. The committer is responsible for the actual refresh.
backup="$(mktemp)"
cp "$INDEX" "$backup"

python3 "$GENERATOR" >/dev/null

if ! diff -q "$backup" "$INDEX" >/dev/null 2>&1; then
    printf 'WARN  docs/ops_index.md is stale — regenerate with:\n' 1>&2
    printf '      python3 tools/gen_ops_index.py\n' 1>&2
    cp "$backup" "$INDEX"
fi

rm -f "$backup"
exit 0
