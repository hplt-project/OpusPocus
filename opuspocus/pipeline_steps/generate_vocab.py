from typing import List

import logging
from pathlib import Path
from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep
from opuspocus.utils import RunnerResources


logger = logging.getLogger(__name__)


@register_step('generate_vocab')
class GenerateVocabStep(OpusPocusStep):
    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        src_lang: str,
        tgt_lang: str,
        datasets: List[str],
        marian_dir: Path,
        corpus_step: CorpusStep,
        seed: int = 42,
        vocab_size: int = 64000,
    ):
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            datasets=datasets,
            marian_dir=marian_dir,
            corpus_step=corpus_step,
            seed=seed,
            vocab_size=vocab_size,
        )

    def init_step(self) -> None:
        super().init_step()
        for dset in self.datasets:
            if dset not in self.corpus_step.dataset_list:
                raise ValueError(
                    'Dataset {} is not registered in the {} categories.json'
                    .format(dset, self.corpus_step.step_label)
                )

    @property
    def corpus_step(self) -> OpusPocusStep:
        return self.dependencies['corpus_step']

    @property
    def input_dir(self) -> Path:
        return self.corpus_step.output_dir

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
    --num_threads ${cpus_var_name}

mv $OUTPUT_DIR/model.$SRC-$TGT.model \\
    $OUTPUT_DIR/model.$SRC-$TGT.spm
# Create links for the backtranslation
ln -s model.$SRC-$TGT.spm $OUTPUT_DIR/model.$TGT-$SRC.spm
ln -s model.$SRC-$TGT.vocab $OUTPUT_DIR/model.$TGT-$SRC.vocab
        """.format(
            cpus_var_name=RunnerResources.get_env_name('cpus'),
            datasets=datasets
        )
