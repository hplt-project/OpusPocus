from typing import Optional

import logging
from pathlib import Path
from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.pipeline_steps.generate_vocab import GenerateVocabStep
from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep


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
        python_venv_dir: Path,
        src_lang: str,
        tgt_lang: str,
        marian_config: Path,
        opustrainer_config: Path,
        vocab_step: GenerateVocabStep,
        train_corpus_step: CorpusStep,
        train_category: str = 'clean',
        model_init_step: Optional['TrainModelStep'] = None,
        seed: int = 42,
        valid_dataset: str = 'flores200.dev',
        suffix: str = None
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            marian_dir=marian_dir,
            valid_data_dir=valid_data_dir,
            python_venv_dir=python_venv_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            marian_config=marian_config,
            opustrainer_config=opustrainer_config,
            vocab_step=vocab_step,
            train_corpus_step=train_corpus_step,
            model_init_step=model_init_step,
            seed=seed,
            train_category=train_category,
            valid_dataset=valid_dataset,
            suffix=suffix
        )
        self.input_dir = self.dependencies['train_corpus_step'].output_dir

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

    @property
    def train_corpus_step(self) -> CorpusStep:
        return self.dependencies['train_corpus_step']

    @property
    def model_init_path(self) -> Path:
        if self.dependencies['model_init_step'] is not None:
            return self.dependencies['model_init_step'].model_path
        return None

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
    def vocab_size(self) -> int:
        return self.dependencies['vocab_step'].vocab_size

    @property
    def model_path(self) -> Path:
        return Path(self.output_dir, 'model.npz')

    @property
    def tmp_dir(self) -> Path:
        return Path(self.step_dir, 'tmp.d')

    @property
    def step_name(self):
        name = 's.{}.{}-{}'.format(
            self.step, self.src_lang, self.tgt_lang
        )
        if self.suffix is not None:
            name += '.{}'.format(self.suffix)
        return name

    def _cmd_header_str(self) -> str:
        return super()._cmd_header_str(
            n_cpus=8,
            n_gpus=8,
            mem=40
        )

    def _cmd_vars_str(self) -> str:
        return """export PATH="{python_venv}/bin:$PATH"

SRC="{src_lang}"
TGT="{tgt_lang}"

TRAIN_DATA_DIR="{indir}"
OUTPUT_DIR="{outdir}"
VALID_DIR="{valdir}"
LOG_DIR="{logdir}"

STEP_DIR="{step_dir}"
SCRIPT_DIR="scripts"
MARIAN_DIR="{marian_dir}"
SEED="{seed}"

OPUSTRAINER_CONFIG_FILE="{opustrainer_config}"
MARIAN_CONFIG_FILE="{marian_config}"

MODEL_FILE="{model_file}"
VOCAB_FILE="{vocab_file}"
VOCAB_SIZE="{vocab_size}"

VALID_OUT_FILE="$LOG_DIR/model.valid.out"
TRAIN_LOG_FILE="$LOG_DIR/model.train.log"
VALID_LOG_FILE="$LOG_DIR/model.valid.log"

TRAIN_DATASETS="{train_dsets}"
VALID_PREFIX="$VALID_DIR/{valid_dset}"
RESUBMIT_TIME_LEFT={resubmit_time}

TEMP_DIR="{tmpdir}/$SLURM_JOBID"
mkdir -p $TEMP_DIR
        """.format(
            python_venv=self.python_venv_dir,
            src_lang=self.src_lang,
            tgt_lang=self.tgt_lang,
            indir=self.input_dir,
            outdir=self.output_dir,
            valdir=self.valid_data_dir,
            logdir=self.log_dir,
            step_dir=self.step_dir,
            marian_dir=self.marian_dir,
            seed=self.seed,
            opustrainer_config='TODO',
            marian_config=self.marian_config,
            model_file=self.model_path,
            vocab_file=self.vocab_path,
            vocab_size=self.vocab_size,
            train_dsets=' '.join(
                self.train_corpus_step.category_mapping[self.train_category]
            ),
            valid_dset=self.valid_dataset,
            resubmit_time=SLURM_RESUBMIT_TIME,
            tmpdir=self.tmp_dir
        )

    def _cmd_traps_str(self) -> str:
        return """cleanup() {{
    exit_code=$?
    if [[ $exit_code -gt 0 ]]; then
        exit $exit_code
    fi

    rm {tmpdir}/train.*.gz
    echo DONE > {state_file}
    exit 0
}}

err_cleanup() {{
    exit_code=$?
    # Set the step state and exit
    echo FAILED > {state_file}
    exit $exit_code
}}

trap err_cleanup ERR
trap cleanup EXIT
        """.format(
            tmpdir=self.tmp_dir,
            state_file=Path(self.step_dir, self.state_file)
        )

    def _cmd_body_str(self) -> str:
        model_init = ''
        if self.model_init_path is not None:
            model_init = '--pretrained-model {}'.format(self.model_init_path)

        dset_list = self.train_corpus_step.category_mapping[self.train_category]
        train_data_opt = '--train-sets '
        train_data_opt += ' '.join([
            '<(zcat {})'.format(' '.join([
                '{}/{}.{}.gz'.format(self.input_dir, dset, lang)
                for dset in dset_list
            ]))
            for lang in [self.src_lang, self.tgt_lang]
        ])

        return """
for lang in $SRC $TGT; do
    for dset in $TRAIN_DATASETS; do
        cat $TRAIN_DATA_DIR/$dset.$lang.gz
    done > $TEMP_DIR/train.$lang.gz
done

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
    --train-sets $TEMP_DIR/train.{{$SRC,$TGT}}.gz \\
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
    ps -p $pid > /dev/null || (wait $pid; exit $?)
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
    $STEP_DIR/step.command \\
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
        """.format(
            model_init=model_init,
            train_data_opt=train_data_opt
        )
