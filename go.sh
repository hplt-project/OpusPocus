#!/bin/bash

lng1=cs
lng2=en

# STEP 1 generate vocab(s)






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
