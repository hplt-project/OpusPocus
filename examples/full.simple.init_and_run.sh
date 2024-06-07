#!/usr/bin/env bash

SRC=${1:-en}
TGT=${2:-nn}
PIPELINE_DIR="experiments/$SRC-$TGT/full.simple.init_and_run"
echo "Initializing pipeline with overwrite --pipeline-dir $PIPELINE_DIR" >&2
./go.py \
	init \
	--pipeline-dir $PIPELINE_DIR \
	--pipeline-config config/pipeline.full.simple.yml
./go.py \
	run \
	--pipeline-dir $PIPELINE_DIR \
	--runner bash
