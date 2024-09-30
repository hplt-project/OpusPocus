#!/usr/bin/env bash
SRC=en
TGT=eu
DATA_DIR=data/en-eu

mkdir -p $DATA_DIR

# Prepare the training data
wget https://data.hplt-project.org/one/bitext/en-eu.raw.gz
mkdir -p $DATA_DIR/raw
zcat en-eu.raw.gz | cut -f3 | gzip -c > $DATA_DIR/raw/raw.en.gz
zcat en-eu.raw.gz | cut -f4 | gzip -c > $DATA_DIR/raw/raw.eu.gz
rm en-eu.raw.gz

# Prepare the validation data
wget https://github.com/openlanguagedata/flores/releases/download/v2.0-rc.2/floresp-v2.0-rc.2.zip
unzip -P "multilingual machine translation" floresp-v2.0-rc.2.zip && rm floresp-v2.0-rc.2.zip
mkdir -p $DATA_DIR/valid $DATA_DIR/test
cp -v floresp-v2.0-rc.2/dev/dev.eng_Latn $DATA_DIR/valid/flores200.dev.en-eu.en
cp -v floresp-v2.0-rc.2/dev/dev.eus_Latn $DATA_DIR/valid/flores200.dev.en-eu.eu 
cp -v floresp-v2.0-rc.2/devtest/devtest.eng_Latn $DATA_DIR/test/flores200.devtest.en-eu.en 
cp -v floresp-v2.0-rc.2/devtest/devtest.eus_Latn $DATA_DIR/test/flores200.devtest.en-eu.eu
