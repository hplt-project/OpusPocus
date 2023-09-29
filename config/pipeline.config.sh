#!/usr/bin/env bash
# MT Pipeline Directory (And Other) Configuration
set -euo pipefail
ROOT_DIR=$(pwd)

SEED=42

# TOOLS
OPUS_ENV="/project/project_462000067/software/opus-env"
#[[ ! -d $OPUS_ENV ]] && bash /project/project_462000067/src/opus-env/build.sh
export PATH="$OPUS_ENV/bin:$PATH"
MARIAN_DIR="/project/project_465000574/software/marian-320dd390"
SCRIPTS="$ROOT_DIR/scripts"
SLURM_SCRIPTS="$ROOT_DIR/scripts/slurm"  # HPC-specific processing step scripts


# VARIABLES AND PATHS
PROJECT_NUM="462000067"

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
VOCAB_SIZE=64000
