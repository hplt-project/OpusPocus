#!/usr/bin/env bash
#SBATCH --job-name=opuscleaner-clean
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=20G
#SBATCH --partition=small
#SBATCH -o logs/clean_para.%j.log
#SBATCH -e logs/clean_para.%j.log
# Preprocess and clean the data (mainly using opuscleaner)
# TODO: replace non-parallel gzip with pigz
set -euo pipefail
filter_file=$1

export SRC=${2:-en}
export TGT=${3:-he}
export EXP_NAME=${4:-debug}

. config/pipeline.config.sh
. config/pipeline.functions.sh

dataset=$(basename $filter_file)
dataset=${dataset/.filters.json/}

logdir="$CLEAN_PARA_DATA_DIR/logs"
mkdir -p $logdir

# TODO: do we want to remove the output files every time
for l in $SRC $TGT; do
    [[ -e $CLEAN_PARA_DATA_DIR/$dataset.$l.gz ]] \
        && rm $CLEAN_PARA_DATA_DIR/$dataset.$l.gz
    [[ -e $CLEAN_PARA_DATA_DIR/$dataset.decontaminated.$l.gz ]] \
        && rm $CLEAN_PARA_DATA_DIR/$dataset.decontaminated.$l.gz
done

## Run OpusCleaner ##
echo "Cleaning $dataset..." >&2
opuscleaner-clean \
    $filter_file \
    --parallel $SLURM_CPUS_PER_TASK \
    -b $RAW_PARA_DATA_DIR \
    > >(
        tee \
            >(cut -f1 | gzip -c > $CLEAN_PARA_DATA_DIR/${dataset}.$SRC.gz) \
            >(cut -f2 | gzip -c > $CLEAN_PARA_DATA_DIR/${dataset}.$TGT.gz) \
        > /dev/null
    ) \
    2> >(tee $logdir/clean.$dataset.log >&2)

# Validate Output
src_lines=$(zcat $CLEAN_PARA_DATA_DIR/$dataset.$SRC.gz | wc -l)
tgt_lines=$(zcat $CLEAN_PARA_DATA_DIR/$dataset.$TGT.gz | wc -l)
[[ $src_lines -ne $tgt_lines ]] \
    && echo "Error [opuscleaner]: Lines in the output files do not match ($src_lines != $tgt_lines)" >&2 \
    && exit 1

## Decontaminate ##
echo "Decontaminating $dataset..." >&2
for dset in $VALID_DIR/*$SRC $TEST_DIR/*$SRC; do
    path_prefix=${dset/.$SRC/}
    [[ -e $path_prefix.$SRC-$TGT ]] \
        || paste $path_prefix.$SRC $path_prefix.$TGT > $path_prefix.$SRC-$TGT
done
paste \
    <(zcat $CLEAN_PARA_DATA_DIR/$dataset.$SRC.gz) <(zcat $CLEAN_PARA_DATA_DIR/$dataset.$TGT.gz) \
| python $SCRIPTS/decontaminate.py \
    --min-length 25 \
    $VALID_DIR/*$SRC-$TGT \
    $TEST_DIR/*$SRC-$TGT \
    > >(
        tee \
            >(cut -f1 | gzip -c > $CLEAN_PARA_DATA_DIR/$dataset.decontaminated.$SRC.gz) \
            >(cut -f2 | gzip -c > $CLEAN_PARA_DATA_DIR/$dataset.decontaminated.$TGT.gz) \
        > /dev/null
    )
    2> >(tee $logdir/decontaminate.$dataset.log >&2)

# Validate Output
src_lines=$(zcat $CLEAN_PARA_DATA_DIR/$dataset.decontaminated.$SRC.gz | wc -l)
tgt_lines=$(zcat $CLEAN_PARA_DATA_DIR/$dataset.decontaminated.$TGT.gz | wc -l)
[[ $src_lines -ne $tgt_lines ]] \
    && echo "Error [decontaminate]: Lines in the output files do not match ($src_lines != $tgt_lines)" >&2 \
    && exit 1
