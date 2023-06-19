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
  --input=<(sed 's/\t/\n/g' < data/train-${lng1}-${lng2}.tsv) \
  --input_sentence_size=20000000 \
  --train_extremely_large_corpus \
  --byte_fallback


for src in lng1 lng2; do
    if [ src = lng1 ] ; then
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
