#!/usr/bin/env bash
#SBATCH --job-name=train_model
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --gpus-per-node=8
#SBATCH --mem=20G
#SBATCH -o logs/train_model.%j.log
#SBATCH -e logs/train_model.%J.log
# TODO: training recovery
# TODO: iteration N training initialization by iteration N-1 model
# TODO: replace the hardwired flores-200 
set -euo pipefail

LANG_SRC=$1
LANG_TGT=$2
ITER=$3

export SRC=${4:-en}
export TGT=${5:-he}
export EXP_NAME=${6:-debug}

. config/pipeline.config.sh
. config/pipeline.functions.sh

OPUSTRAINER_CONFIG_FILE="config/opustrainer.teacher.$MODEL.yml"
MARIAN_CONFIG_FILE="config/marian.train.teacher.$MODEL.yml"

cp $OPUSTRAINER_CONFIG_FILE $EXP_DIR/config.opustrainer.teacher.$MODEL.yml
cp $MARIAN_CONFIG_FILE $EXP_DIR/config.marian.train.teacher.$MODEL.yml

MODEL_FILE="$MODEL_DIR/$LANG_SRC-$LANG_TGT.$MODEL/model-$ITER.npz"
VOCAB_FILE="$MODEL_DIR/model.$LANG_SRC-$LANG_TGT.spm"
TEMP_DIR="$TMPDIR/$LANG_SRC-$LANG_TGT.$ITER.$SLURM_JOBID"
VALID_OUT_FILE="$MODEL_DIR/$LANG_SRC-$LANG_TGT.$MODEL/model-$ITER.valid.out"
TRAIN_LOG_FILE="$MODEL_DIR/$LANG_SRC-$LANG_TGT.$MODEL/model-$ITER.train.log"
VALID_LOG_FILE="$MODEL_DIR/$LANG_SRC-$LANG_TGT.$MODEL/model-$ITER.valid.log"

TRAIN_PREFIX="$TRAIN_DIR/clean.para"
VALID_PREFIX="$VALID_DIR/flores-200.dev"

RESUBMIT_TIME_LEFT=600

mkdir -p \
    $TEMP_DIR \
    $MODEL_DIR/$LANG_SRC-$LANG_TGT.$MODEL

compute_opt="--cpu-threads 1"
[[ $SLURM_GPUS_PER_NODE -gt 0 ]] \
    && compute_opt="--devices $(seq 0 1 $(expr $SLURM_GPUS_PER_NODE - 1))"

# do we just init with previous model ITER or do we continue
# training where previous ITER finished?
init_opt=""
[[ $ITER -gt 0 ]] \
    && init_opt="--pretrained-model $MODEL_DIR/$LANG_SRC-$LANG_TGT.$MODEL/model-$(expr $ITER - 1).npz"

# TODO: Use OpusTrainer instead
$MARIAN_DIR/bin/marian \
    -c $MARIAN_CONFIG_FILE \
    --seed $SEED \
    --data-threads $SLURM_CPUS_PER_TASK \
    --model $MODEL_FILE \
    --vocabs $VOCAB_FILE $VOCAB_FILE \
    --dim-vocabs $VOCAB_SIZE \
    --tempdir $TEMP_DIR \
    --train-sets $TRAIN_PREFIX.{$LANG_SRC,$LANG_TGT}.gz \
    --valid-sets $VALID_PREFIX.{$LANG_SRC,$LANG_TGT} \
    --valid-translation-output $VALID_OUT_FILE \
    --log-level info \
    --log $TRAIN_LOG_FILE \
    --valid-log $VALID_LOG_FILE \
    $init_opt \
    $compute_opt &
pid=$!

# Wait for the time limit to run out
while [[ $(python $SCRIPTS/slurm_time_to_seconds.py $(squeue -h -j $SLURM_JOBID -o %L)) -gt $RESUBMIT_TIME_LEFT ]]; do
    sleep 60s
    # Exit if Marian finished
    ps -p $pid > /dev/null || exit 0
done

echo "Training termination due to SLURM time limit. Submitting a continuation job..." >&2

# Terminate the training and resubmit
kill -15 $pid
sbatch \
    --parsable \
    --dependency="afterany:$SLURM_JOBID" \
    --account=$SLURM_JOB_ACCOUNT \
    --partition=$SLURM_JOB_PARTITION \
    --time=$(squeue -h -j $SLURM_JOBID -o %l) \
    $SLURM_SCRIPTS/train_model.sh $LANG_SRC $LANG_TGT $ITER


#opustrainer-train \
#    --config $EXP_DIR/config.opustrainer.teacher.$MODEL.yml \
#    --temporary-directory $TEMP_DIR \
#    $MARIAN_DIR/bin/marian \
#        -c $EXP_DIR/config.marian.train.teacher.$MODEL.yml \
#        --seed $SEED \
#        --data-threads $SLURM_CPUS_PER_TASK \
#        --model $MODEL_FILE \
#        --vocabs $VOCAB_FILE $VOCAB_FILE \
#        --dim-vocabs $VOCAB_SIZE \
#        --tempdir $TEMP_DIR \
#        --valid-sets $VALID_PREFIX.{$LANG_SRC,$LANG_TGT} \
#        --valid-translation-output $VALID_OUT_FILE \
#        --log-level debug \
#        --log $TRAIN_LOG_FILE \
#        --valid-log $VALID_LOG_FILE \
#        $compute_opt
       
