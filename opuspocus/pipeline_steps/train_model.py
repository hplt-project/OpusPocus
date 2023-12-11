from typing import Optional

import logging
from pathlib import Path
from opuspocus.pipeline_steps import (
    CorpusStep,
    GenerateVocabStep,
    register_step
)

logger = logging.getLogger(__name__)

SLURM_RESUBMIT_TIME=600  # resubmit N seconds before job finishes


@register_step('train_model')
class TrainModelStep(OpusPocusStep):
    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        marian_dir: Path,
        valid_data_dir: Path,
        src_lang: str,
        tgt_lang: str,
        marian_config: Path,
        opustrainer_config: Path,
        vocab_step: GenerateVocabStep,
        train_corpus_step: CorpusStep,
        model_init_step: Optional['TrainModelStep'] = None,
        seed: int = 42,
        train_dataset: str = 'clean.para',
        valid_dataset: str = 'flores200.dev',
        suffix: str = None
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            marian_dir=marian_dir,
            valid_data_dir=valid_data_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            marian_config=marian_config,
            opustrainer_config=opustrainer_config,
            vocab_step=vocab_step,
            train_corpus_step=train_corpus_step,
            model_init_step=model_init_step,
            seed=seed,
            train_dataset=train_dataset,
            valid_dataset=valid_dataset,
            suffix=suffix
        )
        if self.dependencies['model_init_step'] is not None:
            self.model_init_path = self.dependencies['model_init_step'].model_path
        self.input_dir = self.dependencies['train_corpus_step'].output_dir

        self.vocab_size = self.dependencies['vocab_step'].vocab_size

        # Check existence of the valid dataset
        for lang in [self.src_lang, self.tgt_lang]:
            valid_dataset_path = Path(
                self.valid_data_dir,
                '{dset}.{src}-{tgt}.{lang}'.format(
                    dset=self.valid_dataset,
                    src=self.src_lang,
                    tgt=self.tgt_lang,
                    lang=lang
                )
            )
            if not valid_dataset_path.exists():
                raise FileNotFoundError(
                    'Dataset file {} does not exist'.format(valid_dataset_path)
                )

        self.model_path = Path(self.output_dir, 'model.npz')
        self.tmp_dir = Path(self.step_dir, 'tmp.d')

    @property
    def vocab_path(self) -> Path:
        vocab_dir = self.dependencies['vocab_step'].output_dir

        # TODO: this should be fetched from the dependency in case that
        # file naming changes in the future
        vocab_path = Path(
            vocab_dir, 'model.{}-{}.spm'.format(self.src_lang, self.tgt_lang)
        )
        return vocab_path

    @property
    def step_name(self):
        name = 's.{}.{}-{}'.format(
            self.step, self.src_lang, self.tgt_lang
        )
        if self.suffix is not None:
            name += '.{}'.format(self.suffix)
        return name

    def get_command_str(self) -> str:
        model_init = ''
        if hasattr(self, 'model_init_path'):
            model_init = '--pretrained-model {}'.format(self.model_init_path)

        return """#!/usr/bin/env bash
#SBATCH --job-name=train_model
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --gpus-per-node=8
#SBATCH --mem=20G
#SBATCH -o {logdir}/slurm.%j.log
#SBATCH -e {logdir}/slurm.%j.log
# TODO: training recovery
# TODO: replace the hardwired flores-200
set -euo pipefail

SRC="{src_lang}"
TGT="{tgt_lang}"

TRAIN_DIR="{indir}"
OUTPUT_DIR="{outdir}"
VALID_DIR="{valdir}"
LOG_DIR="{logdir}"

STATE_FILE="{state_file}"

SCRIPT_DIR="scripts"
MARIAN_DIR="{marian_dir}"
SEED="{seed}"

#OPUSTRAINER_CONFIG_FILE="{opustrainer_config}"
MARIAN_CONFIG_FILE="{marian_config}"

MODEL_FILE="{model_file}"
VOCAB_FILE="{vocab_file}"
VOCAB_SIZE="{vocab_size}"

VALID_OUT_FILE="$LOG_DIR/model.valid.out"
TRAIN_LOG_FILE="$LOG_DIR/model.train.log"
VALID_LOG_FILE="$LOG_DIR/model.valid.log"

TRAIN_PREFIX="$TRAIN_DIR/{train_dset}"
VALID_PREFIX="$VALID_DIR/{valid_dset}"
RESUBMIT_TIME_LEFT={resubmit_time}

TEMP_DIR="{tmpdir}/$SLURM_JOBID"
mkdir -p $TEMP_DIR

fail() {{
    echo $1 >&2
    exit 1
}}

cleanup() {{
    exit_code=$?
    rm -r $TEMP_DIR
    if [[ $exit_code -gt 0 ]]; then
        exit $exit_code
    fi
    echo DONE > $STATE_FILE
    exit 0
}}

err_cleanup() {{
    exit_code=$?
    # Set the step state and exit
    echo FAILED > $STATE_FILE
    exit $exit_code
}}

trap err_cleanup ERR
trap cleanup EXIT

compute_opt="--cpu-threads 1"
[[ $SLURM_GPUS_PER_NODE -gt 0 ]] \\
    && compute_opt="--devices $(seq 0 1 $(expr $SLURM_GPUS_PER_NODE - 1))"

# TODO: Use OpusTrainer instead
$MARIAN_DIR/bin/marian \\
    -c $MARIAN_CONFIG_FILE \\
    --seed $SEED \\
    --data-threads $SLURM_CPUS_PER_TASK \\
    --model $MODEL_FILE \\
    --vocabs $VOCAB_FILE $VOCAB_FILE \\
    --dim-vocabs $VOCAB_SIZE \\
    --tempdir $TEMP_DIR \\
    --train-sets $TRAIN_PREFIX.{{$SRC,$TGT}}.gz \\
    --valid-sets $VALID_PREFIX.$SRC-$TGT.{{$SRC,$TGT}} \\
    --valid-translation-output $VALID_OUT_FILE \\
    --log-level info \\
    --log $TRAIN_LOG_FILE \\
    --valid-log $VALID_LOG_FILE \\
    {model_init} $compute_opt &
pid=$!

# Wait for the time limit to run out
while [[ $(python $SCRIPT_DIR/slurm_time_to_seconds.py $(squeue -h -j $SLURM_JOBID -o %L)) -gt $RESUBMIT_TIME_LEFT ]]; do
    sleep 60s
    # Exit if Marian finished
    ps -p $pid > /dev/null || exit 0
done

echo "Training termination due to SLURM time limit." >&2
echo "Submitting a continuation job..." >&2

# Terminate the training and resubmit
kill -15 $pid
new_jid=$(sbatch \\
    --parsable \\
    --dependency="afterany:$SLURM_JOBID" \\
    --account=$SLURM_JOB_ACCOUNT \\
    --partition=$SLURM_JOB_PARTITION \\
    --time=$(squeue -h -j $SLURM_JOBID -o %l) \\
    `pwd`/step.command \\
)
echo $jid > `pwd`/step.jobid

# Update the job dependencies
for job in `sqeueu --me --format "%i $E" | grep ":$SLURM_JOBID" | grep -v ^$new_jid | cut -d" " -f1`; do
    echo Updating dependencies of job $job... >&2
    update_str=$(squeue --me --format "%i %E" \\
        | grep ^$job \\
        | cut -d" " -f2 \\
        | sed "s/([^)]*)//g;s/$SLURM_JOBID/$new_jid/" \\
    )
    scontrol update JobId=$job dependency=$update_str
done

# Explicitly exit with non-zero status
exit 0
        """.format(
            state_file=str(Path(self.step_dir, self.state_file)),
            src_lang=self.src_lang,
            tgt_lang=self.tgt_lang,
            indir=str(self.input_dir),
            outdir=str(self.output_dir),
            valdir=str(self.valid_data_dir),
            logdir=str(self.log_dir),
            marian_dir=str(self.marian_dir),
            seed=self.seed,
            opustrainer_config='TODO',
            marian_config=str(self.marian_config),
            train_dset=self.train_dataset,
            valid_dset=self.valid_dataset,
            model_file=str(self.model_path),
            vocab_file=str(self.vocab_path),
            vocab_size=self.vocab_size,
            resubmit_time=SLURM_RESUBMIT_TIME,
            tmpdir=str(self.tmp_dir),
            model_init=model_init,
        )
