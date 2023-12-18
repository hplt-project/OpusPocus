#!/usr/bin/env bash
set -euo pipefail

SRC=$1
TGT=$2
NUM_ITER=${3:-"1"}

DATA_VERSION=${4:-"v0"}
RAW_DATA_DIR="data/$SRC-$TGT/raw/$DATA_VERSION"
VALID_DIR="data/$SRC-$TGT/valid"
TEST_DIR="data/$SRC-$TGT/test"

PYTHON_VENV_DIR="/project/project_465000574/software/opuspocus-env"

MARIAN_DIR="/project/project_465000574/software/marian-320dd390"
MARIAN_CONFIG="config/marian.train.teacher.base.yml"
OPUSTRAINER_CONFIG="config/opustrainer.teacher.base.yml"

PIPELINE_DIR=experiments/$SRC-$TGT/iterbt/$DATA_VERSION
mkdir -p $PIPELINE_DIR

./go.py init \
    --pipeline-config config/pipeline.iterbt.yml \
    --pipeline-dir $PIPELINE_DIR \
    --pipeline iterative_backtranslation \
    --src-lang $SRC \
    --tgt-lang $TGT \
    --raw-data-parallel-dir $RAW_DATA_DIR/para \
    --raw-data-src-dir $RAW_DATA_DIR/$SRC \
    --raw-data-tgt-dir $RAW_DATA_DIR/$TGT \
    --valid-data-dir $VALID_DIR \
    --test-data-dir $TEST_DIR \
    --python-venv-dir $PYTHON_VENV_DIR \
    --marian-dir $MARIAN_DIR \
    --marian-config $MARIAN_CONFIG \
    --opustrainer-config $OPUSTRAINER_CONFIG
./go.py run \
    --runner sbatch \
    --runner-opts '--account=project_465000574 --partition=small-g --time=24:00:00' \
    --pipeline-dir $PIPELINE_DIR
