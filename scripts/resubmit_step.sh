#!/usr/bin/env bash
# Resubmits step and updates its dependants
set -euo pipefail

STEP_DIR=$1
SLURM_PARAMS=$2

SLURM_JOBID=$(cat $STEP_DIR/step.jobid)

new_jid=$( sbatch --parsable $SLURM_PARAMS $STEP_DIR/step.command )

#rm -r $STEP_DIR/logs/*

echo $new_jid > $STEP_DIR/step.jobid
echo RUNNING > $STEP_DIR/step.state

# Update the dependent steps
for job in $(squeue --me --format "%i %E" | grep ":$SLURM_JOBID" | grep -v ^$new_jid | cut -d" " -f1); do
    echo Updating dependencies of job $job... >&2
    update_str=$(squeue --me --format "%i %E" \
        | grep ^$job \
        | cut -d" " -f2 \
        | sed "s/([^)]*)//g;s/$SLURM_JOBID/$new_jid/" \
    )
    scontrol update JobId=$job dependency=$update_str
done
