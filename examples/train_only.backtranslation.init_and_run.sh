#!/usr/bin/env bash

SRC=en
TGT=nn
EXP_DIR="experiments/$SRC-$TGT/train_only.backtranslation"

echo Initializing pipeline... >&2
./go.py init --pipeline-config config/pipeline.train.backtranslation.yml --pipeline-dir $EXP_DIR
echo >&2

echo Running pipeline... >&2
./go.py run --pipeline-dir $EXP_DIR --runner bash