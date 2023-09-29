#!/usr/bin/env bash
set -euo pipefail

PIPELINE=$1
RAW_DIR=$2
OUTPUT_DIR=$3

prefix=$(basename $PIPELINE)
prefix=${prefix/%.filters.json/}

echo "[opuscleaner] Processing $PIPELINE..." >&2
opuscleaner-clean $PIPELINE --parallel 8 -b $RAW_DIR > \
    >(
        tee \
            >(cut -f1 | pigz > $OUTPUT_DIR/${prefix}.en.gz) \
            >(cut -f2 | pigz > $OUTPUT_DIR/${prefix}.vi.gz) \
            >/dev/null
    ) \
    # TODO: Can opuscleaner crash with non-zero exit code?
