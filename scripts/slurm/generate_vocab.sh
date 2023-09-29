#!/usr/bin/env bash
#SBATCH --job-name=generate_vocab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=20G
#SBATCH --partition=small
# TODO: logging (logfiles - maybe in the ``pipeline'' script?)
set -euo pipefail
. config/pipeline.config.sh
. config/pipeline.functions.sh

srun $MARIAN_DIR/bin/spm_train \
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
wait

mv model.$SRC-$TGT.model $MODEL_DIR/model.$SRC-$TGT.model
mv model.$SRC-$TGT.vocab $MODEL_DIR/model.$SRC-$TGT.vocab

# Create links for the backtranslation
ln -s $MODEL_DIR/model.$SRC-$TGT.model $MODEL_DIR/model.$TGT-$SRC.model
ln -s $MODEL_DIR/model.$SRC-$TGT.vocab $MODEL_DIR/model.$TGT-$SRC.vocab
