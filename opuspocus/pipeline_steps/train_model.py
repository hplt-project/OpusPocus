from typing import Optional

import logging
from opuspocus.pipeline_steps import (
    GenerateVocabStep,
    OpusPocusStep,
    build_step,
    register_step
)

logger = logging.getLogger(__name__)

SLURM_RESUBMIT_TIME=600  # resubmit N seconds before job finishes



@register_step('train_model')
class TrainModelStep(OpusPocusStep):

    def __init__(
        self,
        step,
        args,
        iteration: int,
        vocab_step: GenerateVocabStep,
        train_corpus_step: OpusPocusStep,
        model_init_step: Optional[TrainModelStep] = None,
    ):
        super().__init__(step, args)
        self.iteration = iteration

        self.dependencies = {
            'vocab_step': vocab_step,
            'train_corpus_step': train_corpus_step,
            'model_init_step': model_init_step,
        }
        self.input_dir = self.dependencies['train_corpus_step'].output_dir

        self.src_lang = self.dependencies['train_corpus_step'].src_lang
        self.tgt_lang = self.dependencies['train_corpus_step'].tgt_lang

        self.vocab_path = get_vocab_path()
        self.vocab_size = self.dependencies['vocab_step'].vocab_size

        self.seed = args.seed
        self.marian_dir = Path(args.marian_dir)

        # TODO: these should be copied to the step_dir
        self.opustrainer_config = Path(args.opustrainer_config_file)
        self.marian_config = Path(args.marian_config_file)

        self.valid_data_dir = Path(args.valid_data_dir)
        if not self.valid_data_dir.exists():
            raise ValueError(
                'File {} does not exist'.format(self.decontaminate_script)
            )

        self.model_path = Path(self.output_dir, 'model.npz')
        self.tmp_dir = Path(self.step_dir, 'tmp.d')

        if self.dependencies['model_init_step'] is not None:
            self.model_init_path = self.dependencies['model_init_step'].model_path

    def get_vocab_path(self):
        assert self.dependencies is not None
        vocab_dir = self.dependencies['vocab_step'].output_dir

        # TODO: this should be fetched fromt he dependency in case that
        # file naming changes in the future
        vocab_path = Path(
            vocab_dir, 'model.{}-{}.spm'.format(self.src_lang, self.tgt_lang)
        )

    @property
    def step_name(self):
        return 's.{}.{}-{}.{}'.format(
            self.step, self.src_lang, self.tgt_lang, self.iteration
        )

    def get_command_str(self) -> str:
        return """
            #!/usr/bin/env bash
            #SBATCH --job-name=train_model
            #SBATCH --nodes=1
            #SBATCH --ntasks=1
            #SBATCH --cpus-per-task=8
            #SBATCH --gpus-per-node=8
            #SBATCH --mem=20G
            #SBATCH -o {logdir}/slurm.%j.log
            #SBATCH -e {logdir}/slurm.%j.log
            # TODO: training recovery
            # TODO: iteration N training initialization by iteration N-1 model
            # TODO: replace the hardwired flores-200
            set -euo pipefail

            SRC="{src_lang}"
            TGT="{tgt_lang}"
            ITER="{iteration}"

            TRAIN_DIR="{indir}"
            OUTPUT_DIR="{outdir}"
            LOG_DIR="{logdir}"

            STATE_FILE="{state_file}"

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

            TRAIN_PREFIX="$TRAIN_DIR/clean.para"
            VALID_PREFIX="$VALID_DIR/flores-200.dev"
            RESUBMIT_TIME_LEFT={resubmit_time}

            TEMP_DIR="{tmpdir}/$SLURM_JOBID"
            mkdir -p $TEMP_DIR

            fail() {
                echo $1 >&2
                exit 1
            }

            cleanup() {
                exit_code=$?
                if [[ $exit_code -gt 0 ]]; then
                    exit $exit_code
                fi
                rm -r $TEMP_DIR
                echo DONE > $STATE_FILE
                exit 0
            }

            err_cleanup() {
                # Set the step state and exit
                echo FAILED > $STATE_FILE
                exit $1
            }

            trap err_cleanup ERR
            trap cleanup EXIT

            compute_opt="--cpu-threads 1"
            [[ $SLURM_GPUS_PER_NODE -gt 0 ]] \
                && compute_opt="--devices $(seq 0 1 $(expr $SLURM_GPUS_PER_NODE - 1))"

            # TODO: Use OpusTrainer instead
            $MARIAN_DIR/bin/marian \
                -c $MARIAN_CONFIG_FILE \
                --seed $SEED \
                --data-threads $SLURM_CPUS_PER_TASK \
                --model $MODEL_FILE \
                --vocabs $VOCAB_FILE $VOCAB_FILE \
                --dim-vocabs $VOCAB_SIZE \
                --tempdir $TEMP_DIR \
                --train-sets $TRAIN_PREFIX.{$SRC,$TGT}.gz \
                --valid-sets $VALID_PREFIX.{$SRC,$TGT} \
                --valid-translation-output $VALID_OUT_FILE \
                --log-level info \
                --log $TRAIN_LOG_FILE \
                --valid-log $VALID_LOG_FILE \
                {model_init} $compute_opt &
            pid=$!

            # Wait for the time limit to run out
            # while [[ $(python $SCRIPTS/slurm_time_to_seconds.py $(squeue -h -j $SLURM_JOBID -o %L)) -gt $RESUBMIT_TIME_LEFT ]]; do
                sleep 60s
                # Exit if Marian finished
                # ps -p $pid > /dev/null || exit 0
            done

            echo "Training termination due to SLURM time limit." >&2
            echo "Submitting a continuation job..." >&2

            # Terminate the training and resubmit
            kill -15 $pid
            new_jid=$(sbatch \
                --parsable \
                --dependency="afterany:$SLURM_JOBID" \
                --account=$SLURM_JOB_ACCOUNT \
                --partition=$SLURM_JOB_PARTITION \
                --time=$(squeue -h -j $SLURM_JOBID -o %l) \
                `pwd`/step.command \
            )
            echo $jid > `pwd`/step.jobid

            # Update the job dependencies
            for job in `sqeueu --me --format "%i $E" | grep ":$SLURM_JOBID" | grep -v ^$new_jid | cut -d" " -f1`; do
                echo Updating dependencies of job $job... >&2
                update_str=$(squeue --me --format "%i %E" \
                    | grep ^$job \
                    | cut -d" " -f2 \
                    | sed "s/([^)]*)//g;s/$SLURM_JOBID/$new_jid/"\
                )
                scontrol update JobId=$job dependency=$update_str
            done
        """.format(
            state_file=str(Path(self.step_dir, self.state_file)),
            src_lang=self.src_lang,
            tgt_lang=self.tgt_lang,
            iteration=self.iteration,
            indir=str(self.input_dir),
            outdir=str(self.output_dir),
            logdir=str(self.log_dir),
            marian_dir=str(self.marian_dir),
            seed=self.seed,
            opustrainer_config="TODO",
            marian_config=str(self.marian_config),
            model_file=str(self.model_path),
            vocab_file=str(self.vocab_path),
            vocab_size=self.vocab_size,
            resubmit_time=SLURM_RESUBMIT_TIME,
            tmpdir=str(self.tmp_dir),
            model_init=(
                "--pretrained-model {}".format(self.model_init_path)
                if self.model_init_path is not None else ""
            )
        )
