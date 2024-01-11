#!/usr/bin/env bash
set -euo pipefail

OPUSCLEANER=$1
SRC=$2
TGT=$3

INPUT_DIR=$4
OUTPUT_DIR=$5
FILTER_FILE=$6
N_CPUS=${7:-"8"}

dataset=$(basename $FILTER_FILE)
dataset=${dataset/.filters.json/}

if [[ ! -z $TGT ]]; then
    # Bilingual data
    $OPUSCLEANER \
        $FILTER_FILE \
        --parallel $N_CPUS \
        -b $INPUT_DIR \
        > >( \
            tee \
                >(cut -f1 | pigz -c > $OUTPUT_DIR/$dataset.$SRC.gz) \
                >(cut -f2 | pigz -c > $OUTPUT_DIR/$dataset.$TGT.gz) \
                > /dev/null \
        )

        # Sanity check
        src_lines=$(zcat $OUTPUT_DIR/$dataset.$SRC.gz | wc -l)
        tgt_lines=$(zcat $OUTPUT_DIR/$dataset.$TGT.gz | wc -l)
        [[ $src_lines -ne $tgt_lines ]] \
            && echo "Lines in the output files do not match ($src_lines != $tgt_lines)" >&2 \
            && exit 1
else
    # Monolingual data
    $OPUSCLEANER \
        $FILTER_FILE \
        --parallel $N_CPUS \
        -b $INPUT_DIR \
        > >( \
            tee \
                >(cut -f1 | pigz -c > $OUTPUT_DIR/$dataset.$SRC.gz) \
                > /dev/null \
        )
fi
exit 0
