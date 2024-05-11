#!/usr/bin/env bash

SRC=${1:-en}
TGT=${2:-nn}
PIPELINE_DIR="experiments/$SRC-$TGT/full.simple"
echo "Initializing pipeline with overwrite --pipeline-dir $PIPELINE_DIR" >&2
./go.py \
	--pipeline-dir $PIPELINE_DIR \
	init \
		--pipeline-config config/pipeline.full.simple.yml
