#!/usr/bin/env bash
# Set state of pipeline steps with step.state==$2 to FAILED
# Useful before rerunning partially failed pipeline

PIPELINE_DIR=$1
STATE=${2:-"RUNNING"}

for state_file in $PIPELINE_DIR/s.*/step.state; do
    sed -i "s/$STATE/FAILED/" $state_file
done
