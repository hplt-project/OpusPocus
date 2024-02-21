from typing import List

import logging
from pathlib import Path
from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep


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
        corpus_step: CorpusStep,
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

    def _cmd_header_str(self) -> str:
        return super()._cmd_header_str(
            n_cpus=8,
            mem=80
        )

    def _cmd_vars_str(self) -> str:
        return """
SRC="{src_lang}"
TGT="{tgt_lang}"

TRAIN_DIR="{indir}"
OUTPUT_DIR="{outdir}"
LOG_DIR="{logdir}"

MARIAN_DIR="{marian_dir}"
SEED="{seed}"
VOCAB_SIZE="{vocab_size}"
        """.format(
            src_lang=self.src_lang,
            tgt_lang=self.tgt_lang,
            indir=self.input_dir,
            outdir=self.output_dir,
            logdir=self.log_dir,
            marian_dir=self.marian_dir,
            seed=self.seed,
            vocab_size=self.vocab_size,
        )

    def _cmd_body_str(self) -> str:
        # List the training dataset prefixes
        datasets=','.join(self.datasets)
        if ',' in datasets:
            datasets = '{' + datasets + '}'

        # Compose the body_cmd string
        return """# TODO: test existence of input corpus

$MARIAN_DIR/bin/spm_train \\
    --random_seed=$SEED \\
    --bos_id=-1 \\
    --eos_id=0 \\
    --unk_id=1 \\
    --model_prefix=$OUTPUT_DIR/model.$SRC-$TGT \\
    --vocab_size=$VOCAB_SIZE \\
    --input=<(cat $TRAIN_DIR/{datasets}.{{$SRC,$TGT}}.gz | pigz -dc) \\
    --input_sentence_size=10000000 \\
    --shuffle_input_sentence=true \\
    --train_extremely_large_corpus \\
    --byte_fallback \\
    --num_threads $SLURM_CPUS_PER_TASK

mv $OUTPUT_DIR/model.$SRC-$TGT.model \\
    $OUTPUT_DIR/model.$SRC-$TGT.spm
# Create links for the backtranslation
ln -s model.$SRC-$TGT.spm $OUTPUT_DIR/model.$TGT-$SRC.spm
ln -s model.$SRC-$TGT.vocab $OUTPUT_DIR/model.$TGT-$SRC.vocab
        """.format(datasets=datasets)
