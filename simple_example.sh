#!/usr/bin/env bash
set -euo pipefail

SRC=$1
TGT=$2
DATA_VERSION=${3:-"v0"}

PIPELINE_DIR=experiments/$SRC-$TGT/simple/$DATA_VERSION
mkdir -p $PIPELINE_DIR

./go.py init \
    --pipeline-config config/pipeline.simple.yaml\
    --pipeline-dir $PIPELINE_DIR\
    --pipeline simple \
    --src-lang $SRC \
    --tgt-lang $TGT \
    --raw-data-dir data/$SRC-$TGT/raw/$DATA_VERSION/para \
    --valid-data-dir data/$SRC-$TGT/valid \
    --test-data-dir data/$SRC-$TGT/test \
    --marian-dir /project/project_465000574/software/marian-320dd390 \
    --marian-config config/marian.train.teacher.base.yml \
    --opustrainer-config config/opustrainer.teacher.base.yml
./go.py run \
    --runner sbatch \
    --runner-opts '--account=project_465000574 --partition=small-g --time=24:00:00' \
    --pipeline-dir $PIPELINE_DIR
