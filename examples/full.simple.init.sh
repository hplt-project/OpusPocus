#!/usr/bin/env bash

SRC=${1:-en}
TGT=${2:-nn}
PIPELINE_DIR="experiments/full.simple.$SRC-$TGT"
echo "Initializing pipeline with overwrite --pipeline-dir $PIPELINE_DIR" >&2
./go.py init --pipeline-config config/pipeline.full.simple.yml --pipeline-dir $PIPELINE_DIR 
