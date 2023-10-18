#!/usr/bin/env bash
#SBATCH --job-name=generate_vocab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=20G
#SBATCH --partition=small
#SBATCH -o logs/generate_vocab.%j.log
#SBATCH -e logs/generate_vocab.%j.log
# TODO: logging (logfiles - maybe in the ``pipeline'' script?)
set -euo pipefail

export SRC=${1:-en}
export TGT=${2:-he}
export EXP_NAME=${3:-debug}

. config/pipeline.config.sh
. config/pipeline.functions.sh

$MARIAN_DIR/bin/spm_train \
    --random_seed=$SEED \
    --bos_id=-1 \
    --eos_id=0 \
    --unk_id=1 \
    --model_prefix=model.$SRC-$TGT \
    --vocab_size=$VOCAB_SIZE \
    --input=<(cat $TRAIN_DIR/clean.para.{$SRC,$TGT}.gz | pigz -dc) \
    --input_sentence_size=1000000000 \
    --train_extremely_large_corpus \
    --byte_fallback \
    --num_threads $SLURM_CPUS_PER_TASK \

mv model.$SRC-$TGT.model $MODEL_DIR/model.$SRC-$TGT.spm
mv model.$SRC-$TGT.vocab $MODEL_DIR/model.$SRC-$TGT.vocab

# Create links for the backtranslation
ln -s model.$SRC-$TGT.spm $MODEL_DIR/model.$TGT-$SRC.spm
ln -s model.$SRC-$TGT.vocab $MODEL_DIR/model.$TGT-$SRC.vocab
