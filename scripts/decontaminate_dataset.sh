#!/usr/bin/env bash
set -euo pipefail

DECONTAMINATE=$1
SRC=$2
TGT=$3

INPUT_DIR=$4
OUTPUT_DIR=$5
LOG_DIR=$6
VALID_DIRS=$7

DATASET=$8
MIN_LENGTH=${9:-"25"}

if [[ ! -z $TGT ]]; then
    # Bilingual data
    valid_dsets=""
    for valid_dir in $VALID_DIRS; do
        for dset in $valid_dir/*$SRC; do
            path_prefix=${dset%%.$SRC}
            [[ -e $path_prefix.$SRC-$TGT ]] \
                || paste $path_prefix.$SRC $path_prefix.$TGT \
                    | tr -d $'\r' \
                    > $path_prefix.$SRC-$TGT
            valid_dsets="$path_prefix.$SRC-$TGT $valid_dsets"
        done
    done

    paste \
        <(pigz -dc $INPUT_DIR/$DATASET.$SRC.gz) \
        <(pigz -dc $INPUT_DIR/$DATASET.$TGT.gz) \
    | python $DECONTAMINATE \
        --min-length $MIN_LENGTH \
        ${valid_dsets%% } \
    > >( \
        tee \
            >(cut -f1 | pigz -c > $OUTPUT_DIR/$DATASET.$SRC.gz) \
            >(cut -f2 | pigz -c > $OUTPUT_DIR/$DATASET.$TGT.gz) \
            > /dev/null
    ) \
    2> >(tee $LOG_DIR/decontaminate.$DATASET.log >&2)

    # Sanity check
    src_lines=$(zcat $OUTPUT_DIR/$DATASET.$SRC.gz | wc -l)
    tgt_lines=$(zcat $OUTPUT_DIR/$DATASET.$TGT.gz | wc -l)
    [[ $src_lines -ne $tgt_lines ]] \
        && echo "Lines in the output files do not match ($src_lines != $tgt_lines)" >&2 \
        && exit 1
else
    # Monolingual data
    valid_dsets=""
    for valid_dir in $VALID_DIRS; do
        valid_dsets="$valid_dir/*$SRC $valid_dsets"
    done

    pigz -dc $INPUT_DIR/$DATASET.$SRC.gz \
    | python $DECONTAMINATE \
        --min-length $MIN_LENGTH \
        ${valid_dsets%% } \
    > >( \
        tee \
            >(cut -f1 | pigz -c > $OUTPUT_DIR/$DATASET.$SRC.gz) \
            > /dev/null
    ) \
    2> >(tee $LOG_DIR/decontaminate.$DATASET.log >&2)
fi
exit 0
