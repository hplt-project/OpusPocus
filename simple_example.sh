#!/usr/bin/env bash
set -euo pipefail

SRC=$1
TGT=$2

DATA_VERSION=${3:-"v0"}
RAW_DATA_DIR="data/$SRC-$TGT/raw/$DATA_VERSION"
VALID_DIR="data/$SRC-$TGT/valid"
TEST_DIR="data/$SRC-$TGT/test"

PYTHON_VENV_DIR="/project/project_465000574/software/opuspocus-env"

MARIAN_DIR="/project/project_465000574/software/marian-320dd390"
MARIAN_CONFIG="config/marian.train.teacher.base.yml"
OPUSTRAINER_CONFIG="config/opustrainer.teacher.base.yml"

PIPELINE_DIR=experiments/$SRC-$TGT/simple/$DATA_VERSION
mkdir -p $PIPELINE_DIR

./go.py init \
    --pipeline-config config/pipeline.simple.yml\
    --pipeline-dir $PIPELINE_DIR\
    --pipeline simple \
    --src-lang $SRC \
    --tgt-lang $TGT \
    --raw-data-dir $RAW_DATA_DIR/para \
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
