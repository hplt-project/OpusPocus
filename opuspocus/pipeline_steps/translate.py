from typing import Optional

import logging
from pathlib import Path
from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.pipeline_steps.generate_vocab import GenerateVocabStep
from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep
from opuspocus.pipeline_steps.train_model import TrainModelStep


logger = logging.getLogger(__name__)


@register_step('translate')
class TranslateStep(CorpusStep):
    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        marian_dir: Path,
        src_lang: str,
        tgt_lang: str,
        previous_corpus_step: CorpusStep,
        model_step: TrainModelStep,
        output_shard_size: Optional[int] = None,
        model_suffix: str = 'best-chrf',
        suffix: str = None
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            marian_dir=marian_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            previous_corpus_step=previous_corpus_step,
            model_step=model_step,
            output_shard_size=output_shard_size,
            model_suffix=model_suffix,
            suffix=suffix
        )

    @property
    def input_dir(self) -> Path:
        if self.prev_corpus_step.is_sharded:
            return self.prev_corpus_step.output_shard_dir
        return self.prev_corpus_step.output_dir

    def init_dataset_list(self) -> None:
        import shutil
        print(self.prev_corpus_step.categories_path)
        if self.prev_corpus_step.categories_path.exists():
            shutil.copy(
                self.prev_corpus_step.categories_path,
                self.categories_path
            )
        shutil.copy(
            self.prev_corpus_step.dataset_list_path,
            self.dataset_list_path
        )

    @property
    def model_step(self) -> OpusPocusStep:
        return self.dependencies['model_step']

    @property
    def model_config_path(self) -> Path:
        return Path(
            self.model_step.model_path,
            '{}.npz.decoder.yml'.format(self.model_suffix)
        )

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
            mem=20
        )

    def _cmd_vars_str(self) -> str:
        return """
SRC="{src_lang}"
TGT="{tgt_lang}"

INPUT_DIR="{indir}"
OUTPUT_DIR="{outdir}"
LOG_DIR="{logdir}"

MARIAN_DIR="{marian_dir}"
CONFIG_FILE="{marian_config}"
        """.format(
            src_lang=self.src_lang,
            tgt_lang=self.tgt_lang,
            indir=self.input_dir,
            outdir=self.output_dir,
            logdir=self.log_dir,
            marian_dir=self.marian_dir,
            marian_config=self.model_config_path,
        )

    def _cmd_body_str(self) -> str:
        return """
has_equal_num_lines {{
    f_one=$1
    f_two=$2

    f_one_lines=$(pigz -dc $f_one | wc -l)
    f_two_lines=$(pigz -dc $f_two | wc -l)
    if [[ $f_in_lines -eq $f_out_lines ]]; then
        return 0
    else
        return 1
    fi
}}

compute_opt="--cpu-threads 1"
[[ $SLURM_GPUS_PER_NODE -gt 0 ]] \\
    && compute_opt="--devices $(seq 0 1 $(expr $SLURM_GPUS_PER_NODE - 1))"

# TODO: parallelization + output merge

# Translate every source file in the input directory
for f_in in $INPUT_DIR/*$SRC.gz; do
    f_name=$(basename $f_in)
    f_out="$OUTPUT_DIR/${{$f_name/.$SRC/.$TGT}}"
    if [[ -e $f_out ]]; then
        if has_equal_num_lines $f_in $f_out; then
            continue
        else
            echo "Number of lines does not match ($f_in; $f_out). Removing output." >&2
            rm $f_out
        fi
    fi
    $MARIAN_DIR/marian-decoder \\
        -c $CONFIG_FILE \\
        -i $f_in \\
        -o >(pigz -c > $f_out) \\
        --log $LOG_DIR/$f_name.log \\
        -b 4 \\
        $compute_opt

    # Sanity check
    if has_equal_num_lines $f_in $f_out; then
        continue
    else
        echo "Number of lines does not match ($f_in; $f_out). Removing output." >&2
        echo "Terminating..." >&2
        exit 1
    fi

    ln $f_in $OUTPUT_DIR/$fname
done

# TODO: script resubmission loop?
        """
