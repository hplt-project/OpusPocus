#!/bin/bash

set -euo pipefail

# Requirements:
#  - We need to keep in TSV format for OpusTrainer
basedir=$(dirname "$0")
marian_root=marian-dev
spm_root=$marian_root/build-cpu

lng1=en
lng2=he

# Requires:
# 1. Already have written filters for your downloaded datastes through opuscleaner
# 2. Configurations for training, backtranslation, and decoding, as well as model arch
# 3. Selected dev/test sets and evaluation metric/stopping criteria

get_seeded_random() {
  # source of repeatable randomness for shuf
  seed="$1"
  openssl enc -aes-256-ctr -pass pass:"$seed" -nosalt \
    </dev/zero 2>/dev/null
}

# Devset and test set are given as an initial state
# We either
# 1. Use some already established dataset
# 2. Extract dev/devtest _before_ cleaning, and tidy them into some "cleaner" dev/devtest

# STEP 0 apply opuscleaner
raw_data="data/train-parts"  # by default OpusCleaner downloads data to this dir
clean_dest="data/clean"
mkdir -p ${clean_dest}/para
for pipeline in ${raw_data}/*.filters.json; do

  prefix=$(basename $pipeline)
  prefix=${prefix/%.filters.json/}

  (opuscleaner-clean $pipeline --parallel 8 2> >(tee ${clean_dest}/para/${prefix}.log >&2)) | \
    pigz -c >${clean_dest}/para/opuscleaner.${prefix}.tsv.gz

  # REMOVE DEV/TEST FROM THESE OUTPUTS
  # Compute hashes of source and target of dev/test set.
  # If _either_ matches the line in the training data matches, omit it
  pigz -dc ${clean_dest}/para/opuscleaner.${prefix}.tsv.gz | \
    $basedir/decontaminate.py --min-length 25 \
			      data/dev/dev.${lng1}-${lng2}.tsv \
			      data/dev/devtest.${lng1}-${lng2}.tsv | \
    pigz -c > ${clean_dest}/para/${prefix}.tsv.gz

done

# Get the prefixes for "clean" labelled datasets from opuscleaner
mkdir -p data/train
clean_ds=$(python -c "import json; ds=json.load(open('${raw_data}/categories.json'))['mapping']['clean']; print(' '.join(ds))")
echo "Clean datasets: $clean_ds"
for ds_prefix in $clean_ds; do
  clean_ds_file="${clean_dest}/para/${ds_prefix}.tsv.gz"

  if [ ! -e "$clean_ds_file" ]; then
    echo "WARNING! Missing $clean_ds_file" >&2
    continue
  fi

  echo "Adding $clean_ds_file to training data" >&2
  cat $clean_ds_file
done >data/train/clean.$lng1-$lng2.tsv.gz

# STEP 1 generate vocab(s)
spm_prefix="spm.${lng1}-${lng2}"
# TODO if vocab size exceeds real vocab size, spm fails.
spm_vocab_size=8000

$spm_root/spm_train \
  --bos_id=-1 \
  --eos_id=0 \
  --unk_id=1 \
  --model_prefix=${spm_prefix} \
  --vocab_size=${spm_vocab_size} \
  --input=<(pigz -cd data/train/clean.$lng1-$lng2.tsv.gz | sed 's/\t/\n/') \
  --input_sentence_size=20000000 \
  --train_extremely_large_corpus \
  --byte_fallback


# STEP 2.pre
# When OpusCleaner supports mono-lingual, we should do cleaning here too
# However, we also need to remove the dev/devtets from the monolingual data
# Mono is cleaned+merged earlier
for ln in $lng1 $lng2; do
  pigz -dc \
    data/raw/mono.$ln.txt.gz | decontaminate.py \
				 data/dev/dev.${lng1}-${lng2}.tsv \
				 data/dev/devtest.${lng1}-${lng2}.tsv |
    pigz -c ${clean_dest}/mono.$lng1-$lng2.tsv.gz
done

# STEP 2 Train models for iterative backtranslation
# Generate iterative translated mono text
# Note: We do not use these BT-models for the teacher because they only see
# clean parallel authentic data, and parallel from synthetic mono text
bts=5
for bt_iteration in {1..$bts}; do
  if [ $src = $lng1 ]; then tgt=$lng2; else tgt=$lng1; fi

  seed=1

  # train lng1 -> lng2
  marian -c config-bt.yml \
    --seed $seed \
    --train-sets data/train-$((bt_iteration - 1)).{$src,$tgt}.txt \
    --valid-sets data/dev/dev.{$src,$tgt}.txt \
    --model model-${bt_iteration}/model.npz \
    --num_devices 8

  # translate lng1 (mono) -> lng2 (synth i-th iteration)
  marian-decoder \
    -m model-${bt_iteration}/model.npz \
    -i data/clean/mono.$src.txt \
    -o data/synth-${bt_iteration}.$tgt.txt \
    --maxi-batch 100 --maxi-batch-sort src \
    --beam-size 6 \
    --quiet-translation \
    --max-length-factor 3 --max-length-crop \
    --max-length 300 \
    --num-devices 8

  # We must apply some light cleaning on the output of the
  # iterative backtranslation to filter out length-ratios
  bash clean-para.sh <(paste data/clean/mono.$src.txt synth-${bt_iteration}.$tgt.txt) \
    >cleaned-synth-${bt_iteration}.tsv

  # prepare data (mix of auth + synth from i-th iteration)
  cat <(paste data/train.$src.txt data/train.$tgt.txt) \
    cleaned-synth-${bt_iteration}.tsv |
    shuf |
    tee >(cut -f1 data/train-${bt_iteration}.$tgt.txt) \
      >(cut -f2 data/train-${bt_iteration}.$src.txt) \
      >/dev/null

  # switch src and tgt, continue
  src=$tgt
done


# Recycle BT-models for distillation to bitext students



for src in $lng1 $lng2; do
  if [ $src = $lng1 ]; then tgt=$lng2; else tgt=$lng1; fi

  for seed in 7 222 1337 2131; do
    model_dir=model_${src}-${tgt}-${seed}
    mkdir -p model_dir

    opustrainer -c config_teacher.yml marian
    # config_teacher.yml will specify a training schedule of
    # auth_clean
    # synth
    # auth_crawl (our new data [ + some old data? ])

  done
done
