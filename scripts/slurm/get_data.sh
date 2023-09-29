#!/usr/bin/env bash
# Setup the data structure and get (download) datasets
# TODO: import sacrebleu
set -euo pipefail

export SRC=$1
export TGT=$2
export EXP_NAME=$3
. config/pipeline.config.sh
. config/pipeline.functions.sh


echo "Creating the directory structure..." >&2
mkdir -p \
    $RAW_MONO_DATA_DIR \
    $RAW_PARA_DATA_DIR \
    $CLEAN_MONO_DATA_DIR \
    $CLEAN_PARA_DATA_DIR \
    $TRAIN_DIR \
    $VALID_DIR \
    $TEST_DIR \
    $MODEL_DIR

echo "Downloading monolingual data..." >&2
scripts/download_mono_en.sh

## Download testsests ##
NTREX_DIR="NTREX"
[[ ! -d  $NTREX_DIR ]] \
    && "Downloading NTREX dataset." >&2 \
    && git clone https://github.com/MicrosoftTranslator/NTREX.git
# TODO: copy/link the relevant testsets to the internal dir structure
# (requires mapping from ISO 639-1 to ISO 639-1)

FLORES_DIR="flores200_dataset"
[[ ! -d $FLORES_DIR ]] \
    && echo "Downloading Flores-200 dataset." >&2 \
    && wget --trust-server-names https://tinyurl.com/flores200dataset \
    && tar xzvf flores200_dataset.tar.gz \
    && rm flores200_dataset.tar.gz
# TODO: copy/link the relevant testsets to the internal dir structure
# (requires mapping from ISO 639-1 to ISO 639-1)

# TODO sacrebleu testsets download

echo "Populate the $RAW_DATA_DIR directory with .filters.json specifications." >&2 
