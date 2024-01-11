#!/usr/bin/env bash
set -euo pipefail
LOG_LEVEL="info"

SRC=$1
TGT=$2

DATA_VERSION=${3:-"v0"}

RAW_DATA_DIR="data/$SRC-$TGT/raw/$DATA_VERSION"
VALID_DIR="data/$SRC-$TGT/valid"
TEST_DIR="data/$SRC-$TGT/test"

MARIAN_CONFIG="config/marian.train.teacher.12x6.yml"

PIPELINE_DIR=experiments/$SRC-$TGT/simple/$DATA_VERSION
mkdir -p $PIPELINE_DIR

./go.py init \
    --pipeline simple \
    --pipeline-dir $PIPELINE_DIR \
    --pipeline-config config/pipeline.simple.yml\
    --src-lang $SRC \
    --tgt-lang $TGT \
    --raw-data-dir $RAW_DATA_DIR \
    --valid-data-dir $VALID_DIR \
    --test-data-dir $TEST_DIR \
    --marian-config $MARIAN_CONFIG \
    --log-level $LOG_LEVEL
./go.py run \
    --runner sbatch \
    --runner-opts '--account=project_465000574 --partition=small-g --time=24:00:00' \
    --pipeline-dir $PIPELINE_DIR \
    --log-level $LOG_LEVEL
