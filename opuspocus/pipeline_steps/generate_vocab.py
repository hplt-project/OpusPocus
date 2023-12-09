from typing import List

import logging
from pathlib import Path
from opuspocus.pipeline_steps import (
    OpusPocusStep,
    build_step,
    register_step
)

logger = logging.getLogger(__name__)


@register_step('generate_vocab')
class GenerateVocabStep(OpusPocusStep):
    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        src_lang: str,
        tgt_lang: str,
        datasets: List[str],
        marian_dir: Path,
        corpus_step: OpusPocusStep,
        seed: int = 42,
        vocab_size: int = 64000,
        suffix: str = None
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            datasets=datasets,
            marian_dir=marian_dir,
            corpus_step=corpus_step,
            seed=seed,
            vocab_size=vocab_size,
            suffix=suffix
        )
        self.input_dir = self.dependencies['corpus_step'].output_dir

    @property
    def step_name(self):
        return 's.{}.{}-{}'.format(self.step, self.src_lang, self.tgt_lang)

    def get_command_str(self) -> str:
        datasets=','.join(self.datasets)
        if ',' in datasets:
            datasets = '{' + datasets + '}'

        return """#!/usr/bin/env bash
#SBATCH --job-name=generate_vocab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=20G
#SBATCH -o {logdir}/slurm.%j.log
#SBATCH -e {logdir}/slurm.%j.log
# TODO: logging (logfiles - maybe in the ``pipeline'' script?)
set -euo pipefail

SRC="{src_lang}"
TGT="{tgt_lang}"

TRAIN_DIR="{indir}"
OUTPUT_DIR="{outdir}"
LOG_DIR="{logdir}"

STATE_FILE="{state_file}"

MARIAN_DIR="{marian_dir}"
SEED="{seed}"
VOCAB_SIZE="{vocab_size}"

cleanup() {{
    exit_code=$?
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

# TODO: test existence of input corpus

$MARIAN_DIR/bin/spm_train \\
    --random_seed=$SEED \\
    --bos_id=-1 \\
    --eos_id=0 \\
    --unk_id=1 \\
    --model_prefix=$OUTPUT_DIR/model.$SRC-$TGT \\
    --vocab_size=$VOCAB_SIZE \\
    --input=<(cat $TRAIN_DIR/{datasets}.{{$SRC,$TGT}}.gz | pigz -dc) \\
    --input_sentence_size=1000000000 \\
    --train_extremely_large_corpus \\
    --byte_fallback \\
    --num_threads $SLURM_CPUS_PER_TASK

mv $OUTPUT_DIR/model.$SRC-$TGT.model \\
    $OUTPUT_DIR/model.$SRC-$TGT.spm
# Create links for the backtranslation
ln -s model.$SRC-$TGT.spm $OUTPUT_DIR/model.$TGT-$SRC.spm
ln -s model.$SRC-$TGT.vocab $OUTPUT_DIR/model.$TGT-$SRC.vocab

# Explicitly exit with non-zero status
exit 0
        """.format(
            state_file=str(Path(self.step_dir, self.state_file)),
            src_lang=self.src_lang,
            tgt_lang=self.tgt_lang,
            indir=str(self.input_dir),
            outdir=str(self.output_dir),
            logdir=str(self.log_dir),
            marian_dir=self.marian_dir,
            seed=self.seed,
            vocab_size=self.vocab_size,
            datasets=datasets,
        )
