#/usr/bin/env bash
# Wait until the dependent processes ($1) finish and exectue
# the command with its arguments ($@)
set -euo pipefail

DEPS=$1
shift

SUCCESS=0  # track whether any of the dependencies failed
for dep in $DEPS; do
    wait $dep || SUCCESS=1
done

if [[ $SUCCESS -eq 0 ]]; then
    echo "Dependencies finished successfully." >&2
    echo "Executing: $@" >&2
    bash $@
else
    echo "One of the dependencies exited with non-zero status." >&2
    echo "Exiting without executing..." >&2
    exit 1
fi
