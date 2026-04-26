#!/bin/sh
# Non-fatal warning emitter for `*.py` files over the readability cap.
#
# Threshold = 2500 LOC. Surfaces drift early so reviewers can decide
# whether the file genuinely warrants its size or wants splitting (see
# issue #58 for the in-tree modularisation plan).
#
# Exit code is always 0 — this is advisory, not gating. Wire it up as a
# pre-commit hook, CI step, or local audit:
#
#   ./scripts/check_file_size.sh                # scan src/ + tests/ + scripts/
#   ./scripts/check_file_size.sh path/to/dir    # scan a specific tree
#
set -u

THRESHOLD=2500
SEARCH_DIRS="${*:-src tests scripts}"

count=0
warned=0
while IFS= read -r file; do
    count=$((count + 1))
    loc=$(wc -l < "$file")
    if [ "$loc" -gt "$THRESHOLD" ]; then
        printf 'WARN  %s — %d LOC (> %d)\n' "$file" "$loc" "$THRESHOLD" 1>&2
        warned=$((warned + 1))
    fi
done <<EOF
$(find $SEARCH_DIRS -type f -name '*.py' 2>/dev/null | sort)
EOF

if [ "$warned" -gt 0 ]; then
    printf '\n%d/%d Python file(s) exceed the %d LOC readability cap.\n' \
        "$warned" "$count" "$THRESHOLD" 1>&2
    printf 'Non-fatal — see issue #58 for the splitting plan.\n' 1>&2
fi

exit 0
