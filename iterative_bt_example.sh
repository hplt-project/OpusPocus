#!/usr/bin/env bash
set -euo pipefail
LOG_LEVEL="debug"

SRC=$1
TGT=$2
NUM_ITER=${3:-"1"}

EXPERIMENT_LABEL=${4:-"iter_bt"}
DATA_VERSION=${4:-"v0"}
MARIAN_CONFIG_VERSION=${5:-"12x12"}

RAW_PARA_DATA_DIR="data/$SRC-$TGT/raw/$DATA_VERSION"
RAW_SRC_DATA_DIR="data/$SRC/raw"
RAW_TGT_DATA_DIR="data/$TGT/raw"
VALID_DIR="data/$SRC-$TGT/valid"
TEST_DIR="data/$SRC-$TGT/test"

MARIAN_CONFIG="config/marian.train.teacher.$MARIAN_CONFIG_VERSION.yml"

PIPELINE_DIR=experiments/$SRC-$TGT/$EXPERIMENT_LABEL.$MARIAN_CONFIG_VERSION.$DATA_VERSION
mkdir -p $PIPELINE_DIR

./go.py init \
    --pipeline iterative_backtranslation \
    --pipeline-dir $PIPELINE_DIR \
    --pipeline-config config/pipeline.iterative_bt.yml \
    --src-lang $SRC \
    --tgt-lang $TGT \
    --raw-data-parallel-dir $RAW_PARA_DATA_DIR \
    --raw-data-src-dir $RAW_SRC_DATA_DIR \
    --raw-data-tgt-dir $RAW_TGT_DATA_DIR \
    --valid-data-dir $VALID_DIR \
    --test-data-dir $TEST_DIR \
    --marian-config $MARIAN_CONFIG \
    --n-iterations $NUM_ITER \
    --log-level $LOG_LEVEL
./go.py run \
    --runner sbatch \
    --runner-opts '--account=project_465000574 --partition=small-g --time=24:00:00' \
    --pipeline-dir $PIPELINE_DIR \
    --log-level $LOG_LEVEL
