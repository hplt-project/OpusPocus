#!/usr/bin/env bash
# Main pipeline taking care of scheduling individual steps
set -euo pipefail

export SRC=$1
export TGT=$2
export EXP_NAME=$3

# Global Variables
. config/pipeline.config.sh
. config/pipeline.functions.sh

slurm_deps=""
next_slurm_deps=""

## 00 Download the data ##
#jid=$(sbatch \
#    --parsable \
#    --account=project_$PROJECT_NUM \
#    --partition=small \
#    $SLURM_SCRIPTS/get_data.sh
#)
#slurm_deps="afterok:${jid}"


## 01 Clean and Deduplicate the data ##
for filter in $RAW_PARA_DATA_DIR/*filters.json; do
    # TODO: submit only files that do not have output already
    jid=$(sbatch \
        --parsable \
        --dependency=$slurm_deps \
        --account=project_$PROJECT_NUM \
        --partition=small \
        $SLURM_SCRIPTS/clean_para.sh $filter \
    )
    next_slurm_deps="${next_slurm_deps}afterok:${jid},"
done
slurm_deps=${next_slurm_deps%%,}
next_slurm_deps=""


## 02 Create training datasets ##
jid=$(sbatch \
    --parsable \
    --dependency=$slurm_deps \
    --account=project_$PROJECT_NUM \
    --partition=small \
    $SLURM_SCRIPTS/gather_train.sh \
)
slurm_deps="afterok:${jid}"


## 03 Generate subword vocabulary ##
jid=$(sbatch \
    --parsable \
    --dependency=$slurm_deps \
    --account=project_$PROJECT_NUM \
    --partition=small \
    $SLURM_SCRIPTS/generate_vocab.sh \
)
slurm_deps="afterok:${jid}"
next_slurm_deps=""

## 04 Start Training ##
## TODO: a loop submitting sbatch for each iter_bt iteration
for i in `seq 0 1 $NUM_ITERATIONS`; do
    # 04.1 Train
    # Forward model
    jid=$(sbatch \
        --parsable \
        --dependency=$slurm_deps \
        --account=$project_$PROJECT_NUM \
        --partition=small-g \
        $SLURM_SCRIPTS/train_model.sh $i $SRC $TGT \
    )
    next_slurm_deps="afterok:${jid}"

    # Backward model
    jid=$(sbatch \
        --parsable \
        --dependency=$slurm_deps \
        --account=$project_$PROJECT_NUM \
        --partition=small-g \
        $SLURM_SCRIPTS/train_model.sh $i $TGT $SRC \
    )
    next_slurm_deps="$next_slurm_deps,afterok:${jid}"

    slurm_deps=$next_slurm_deps
    next_slurm_deps=""

    # - Validate

    # 04.2 Split-mono + Backtranslation + Merge

    # 04.3 Clean Backtranlation

done

## (TODO) 05 Train students ##

