#!/usr/bin/env bash
#SBATCH --job-name=gather_train
#SBATCH --nodes=1
#SBATCH --ntasks=6
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --partition=small
# Gather the individual datasets into training groups based
# on the opuscleaner categories
# TODO: logging
set -euo pipefail
. config/pipeline.config.sh
. config/pipeline.functions.sh

categories_json="$RAW_PARA_DATA_DIR/categories.json"
categories=$(python -c "import json, sys; print(' '.join([x['name'] for x in json.load(open('$categories_json', 'r'))['categories']]))")
for category in $categories; do
    for l in $SRC $TGT; do
        datasets=$(python -c "import json, sys; print(' '.join(json.load(open('$categories_json', 'r'))['mapping']['$category']))")
        f_out="$TRAIN_DIR/$category.para.$l.gz"
        for dset in $datasets; do
            ds_file=$CLEAN_PARA_DATA_DIR/$dset.decontaminated.$l.gz
            [[ ! -e "$ds_file" ]] \
                && echo "Missing $ds_file..." >&2 \
                && rm -r $f_out \
                && exit 1
            echo "Adding $ds_file" >&2
            cat $ds_file
        done > $f_out &
    done
    # TODO: test for identical num. of src/tgt lines
done
