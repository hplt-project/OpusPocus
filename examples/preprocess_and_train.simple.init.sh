#!/usr/bin/env bash

./go.py init --pipeline-config config/pipeline.preprocess.simple.yml
./go.py init --pipeline-config config/pipeline.train.simple.yml
