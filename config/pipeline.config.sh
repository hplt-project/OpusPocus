#!/usr/bin/env bash
# MT Pipeline Directory (And Other) Configuration
set -euo pipefail
ROOT_DIR=$(pwd)

SEED=42
PROJECT_NUM="465000574"

# TOOLS
OPUS_ENV="/project/project_$PROJECT_NUM/software/opus-env"
[[ ! -d $OPUS_ENV ]] && bash /project/project_$PROJECT_NUM/src/opus-env/build.sh
export PATH="$OPUS_ENV/bin:$PATH"
MARIAN_DIR="/project/project_$PROJECT_NUM/software/marian-320dd390"
SCRIPTS="$ROOT_DIR/scripts"
SLURM_SCRIPTS="$ROOT_DIR/scripts/slurm"  # HPC-specific processing step scripts


## VARIABLES AND PATHS ##

# Directories
LP="${SRC}-${TGT}"
EXP_DIR="$LP/$EXP_NAME"

RAW_DATA_DIR="$EXP_DIR/data/raw"
RAW_MONO_DATA_DIR="$RAW_DATA_DIR/mono"
RAW_PARA_DATA_DIR="$RAW_DATA_DIR/para"

CLEAN_DATA_DIR="$EXP_DIR/data/clean"
CLEAN_MONO_DATA_DIR="$CLEAN_DATA_DIR/mono"
CLEAN_PARA_DATA_DIR="$CLEAN_DATA_DIR/para"

TRAIN_DIR="$EXP_DIR/data/train"
VALID_DIR="$EXP_DIR/data/valid"
TEST_DIR="$EXP_DIR/data/test"

MODEL_DIR="$EXP_DIR/model"

# Marian
MODEL="base"
VOCAB_SIZE=64000
SPLIT_SIZE=500000
