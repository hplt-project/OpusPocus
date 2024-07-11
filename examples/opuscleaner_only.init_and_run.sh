#!/usr/bin/env bash

SRC=en
TGT=is
EXP_DIR="experiments/$SRC-$TGT/opuscleaner_only"

export PYTHONPATH="/home/bmalik/OpusPocus/opuspocus/:$PYTHONPATH"

echo Initializing pipeline... >&2
./go.py init --pipeline-config config/pipeline.opuscleaner_only.yml --pipeline-dir $EXP_DIR
echo >&2

echo Running pipeline... >&2
./go.py run --pipeline-dir $EXP_DIR --runner bash
