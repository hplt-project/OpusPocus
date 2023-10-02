#!/usr/bin/env bash
#SBATCH --job-name=generate_vocab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --gpus-per-node=8
#SBATCH --mem=16G
# TODO: logging (logfiles - maybe in the ``pipeline'' script?)
# TODO: tmpdir setup, cleanup
# TODO: training recovery
# TODO: iteration N training initialization by iteration N-1 model
# TODO: replace the hardwired flores-200 
set -euo pipefail
. config/pipeline.config.sh
. config/pipeline.functions.sh

LANG_SRC=$1
LANG_TGT=$2
ITER=$3

CONFIG_FILE="config/marian_train.$MODEL.yml"
MODEL_FILE="$MODEL_DIR/$LANG_SRC-$LANG_TGT/$MODEL/model-$ITER.npz"
VOCAB_FILE="$MODEL_DIR/model.$LANG_SRC-$LANG_TGT.vocab"
TEMP_DIR="$TMPDIR/$LANG_SRC-$LANG_TGT.$ITER.$SLURM_JOB_ID"
VALID_OUT_FILE="$MODEL_DIR/$LANG_SRC-$LANG_TGT/$MODEL/model-$ITER.valid.out"
TRAIN_LOG_FILE="$MODEL_DIR/$LANG_SRC-$LANG_TGT/$MODEL/model-$ITER.train.log"
VALID_LOG_FILE="$MODEL_DIR/$LANG_SRC-$LANG_TGT/$MODEL/model-$ITER.valid.log"

TRAIN_PREFIX="$TRAIN_DIR/clean.para"
VALID_PREFIX="$VALID_DIR/flores-200.dev"

compute_opt="--cpu-threads 1"
[[ -n "$CUDA_VISIBLE_DEVICES" ]] \
    && compute_opt="--devices ${CUDA_VISIBLE_DEVICES//,/ }"

# TODO: Use OpusTrainer
srun $MARIAN_DIR/bin/marian \
    -c $CONFIG_FILE \
    --seed $SEED \
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
    $compute_opt
wait
