#!/usr/bin/env bash
#SBATCH --job-name=opuscleaner-clean
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=20G
#SBATCH --partition=small
# Preprocess and clean the data (mainly using opuscleaner)
# TODO: logging, proper printing of the error messages
set -euo pipefail
filter_file=$1

# debugging only
#export SRC=en
#export TGT=he
#export EXP_NAME=debug
. config/pipeline.config.sh
. config/pipeline.functions.sh

dataset=$(basename $filter_file)
dataset=${dataset/.filters.json/}


## Run OpusCleaner ##
echo "Cleaning $dataset..." >&2
#srun bash $SCRIPTS/cliean_para.sh $filter_file $RAW_PARA_DATA_DIR $CLEAN_PARA_DATA_DIR
srun ( \
    opuscleaner-clean \
        $filter_file \
        --parallel $SLURM_CPUS_PER_TASK \
        -b $RAW_PARA_DATA_DIR \
    | tee \
        >(cut -f1 | pigz > $CLEAN_PARA_DATA_DIR/${prefix}.$SRC.gz) \
        >(cut -f2 | pigz > $CLEAN_PARA_DATA_DIR/${prefix}.$TGT.gz) \
        >/dev/null
)

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
srun ( \
    paste \
        <(pigz -dc $CLEAN_PARA_DATA_DIR/$dataset.$SRC.gz) <(pigz -dc $CLEAN_PARA_DATA_DIR/$dataset.$TGT.gz) \
    | python decontaminate.py \
        --min-length 25 \
        $VALID_DIR/*$SRC-$TGT \
        $TEST_DIR/*$SRC-$TGT \
    | tee \
        >(cut -f1 | pigz -c > $CLEAN_PARA_DATA_DIR/$dataset.decontaminated.$SRC.gz) \
        >(cut -f2 | pigz -c > $CLEAN_PARA_DATA_DIR/$dataset.decontaminated.$TGT.gz) \
        > /dev/null
)

# Validate Output
src_lines=$(zcat $CLEAN_PARA_DATA_DIR/$dataset.decontaminated.$SRC.gz | wc -l)
tgt_lines=$(zcat $CLEAN_PARA_DATA_DIR/$dataset.decontaminated.$TGT.gz | wc -l)
[[ $src_lines -ne $tgt_lines ]] \
    && echo "Error [decontaminate]: Lines in the output files do not match ($src_lines != $tgt_lines)" >&2 \
    && exit 1`
