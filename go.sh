#!/bin/bash

lng1=cs
lng2=en

# STEP 1 generate vocab(s)
PREFIX="--model_prefix=model.${lng1}-${lng2}"
VOCAB_SIZE=32000

spm_train \
  --bos_id=-1 \
  --eos_id=0 \
  --unk_id=1 \
  ${PREFIX} \
  --vocab_size=${VOCAB_SIZE} \
  --input=<(sed 's/\t/\n/g' <data/train-${lng1}-${lng2}.tsv) \
  --input_sentence_size=20000000 \
  --train_extremely_large_corpus \
  --byte_fallback

# train_model() {
#     seed=$1
#     src_data=$2
#     tgt_data=$3
#     model_dir=$4

#     marian -c config.yml \
# 	   --seed $seed \
# 	   --train-sets $src_data $tgt_data \
# 	   --valid-sets data/dev.{$src,$tgt}.txt \
# 	   --model $model_dir/model.npz \
# 	   --num_devices 8
# }

# STEP 2 train model in direction lng1 -> lng2
bts=5
for bt_iteration in {1..bts}; do
  if [ src = lng1 ]; then tgt=lng2; else tgt=lng1; fi

  seed=1

  # train lng1 -> lng2
  marian -c config.yml \
    --seed $seed \
    --train-sets data/train-$((bt_iteration - 1)).{$src,$tgt}.txt \
    --valid-sets data/dev.{$src,$tgt}.txt \
    --model model-${bt_iteration}/model.npz \
    --num_devices 8

  # translate lng1 (mono) -> lng2 (synth i-th iteration)
  marian-decoder \
    -m model-${bt_iteration}/model.npz \
    -i data/mono.$src.txt \
    -o data/synth-${bt_iteration}.$tgt.txt \
    --maxi-batch 100 --maxi-batch-sort src \
    --beam-size 6 \
    --quiet-translation \
    --max-length-factor 3 --max-length-crop \
    --max-length 300 \
    --num-devices 8

  # prepare data (mix of auth + synth from i-th iteration)
  cat <(paste data/train.$src.txt data/train.$tgt.txt) \
    <(paste mono.$src.txt synth-${bt_iteration}.$tgt.txt) |
    shuf |
    tee >(cut -f1 data/train-${bt_iteration}.$tgt.txt) \
      >(cut -f2 data/train-${bt_iteration}.$src.txt) \
      >/dev/null

  # switch src and tgt, continue
  src=tgt
done

for src in lng1 lng2; do
  if [ src = lng1 ]; then
    tgt=lng2
  else
    tgt=lng1
  fi

  for seed in 7 222 1337 2131; do
    model_dir=model_${src}-${tgt}-${seed}
    mkdir -p model_dir

    marian -c config.yml \
      --seed $seed \
      --train-sets=data/train.{$src,$tgt}.txt \
      --valid-sets=data/dev.{$src,$tgt}.txt \
      --model $model_dir/model.npz \
      --num_devices 8
  done
done
