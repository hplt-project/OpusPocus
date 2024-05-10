from typing import Optional

import logging
from pathlib import Path

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep


logger = logging.getLogger(__name__)


@register_step('clean')
class CleanCorpusStep(CorpusStep):
    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        previous_corpus_step: CorpusStep,
        python_venv_dir: Path,
        src_lang: str,
        tgt_lang: str = None,
        output_shard_size: Optional[int] = None,
        opuscleaner_cmd: str = 'opuscleaner-clean',
        gzipped: bool = True,
    ):
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            previous_corpus_step=previous_corpus_step,
            python_venv_dir=python_venv_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            output_shard_size=output_shard_size,
            opuscleaner_cmd=opuscleaner_cmd,
            gzipped=gzipped,
        )

    def register_categories(self) -> None:
        """Create a dataset list using the datasets listed in categories.json file.

        OpusCleaner server app creates a categories.json file listing locally
        available datasets and their user-specified categorization.
        """
        import shutil
        shutil.copy(
            self.prev_corpus_step.categories_path,
            self.categories_path
        )

        # Sanity check: .filters.json files exist
        for dset in self.dataset_list:
            dset_filter_path = Path(
                self.input_dir, '{}.filters.json'.format(dset)
            )
            if not dset_filter_path.exists():
                raise FileNotFoundError(dset_filter_path)

    def _cmd_header_str(self) -> str:
        return super()._cmd_header_str(
            n_cpus=8,
            mem=20,
        )

    def _cmd_vars_str(self) -> str:
        tgt_def_str = ''
        if self.tgt_lang is not None:
            tgt_def_str = 'TGT="{}"'.format(self.tgt_lang)

        return """export PATH="{python_venv}/bin:$PATH"

SRC="{src_lang}"
{tgt_def_str}

INPUT_DIR="{indir}"
OUTPUT_DIR="{outdir}"
LOG_DIR="{logdir}"

OPUSCLEANER="{opuscleaner}"
        """.format(
            python_venv=self.python_venv_dir,
            src_lang=self.src_lang,
            tgt_def_str=tgt_def_str,
            indir=self.input_dir,
            outdir=self.output_dir,
            logdir=self.log_dir,
            opuscleaner=self.opuscleaner_cmd,
        )

    def _cmd_body_str(self) -> str:
        # TODO: parallelize (using hyperqueue)

        # Opuscleaner output processing
        opuscleaner_out_str = """>( \\
        tee \\
            >(cut -f1 | gzip -c > $OUTPUT_DIR/$dataset.$SRC.gz) \\
            > /dev/null \\
    )"""
        if self.tgt_lang is not None:
            opuscleaner_out_str = """>( \\
        tee \\
            >(cut -f1 | gzip -c > $OUTPUT_DIR/$dataset.$SRC.gz) \\
            >(cut -f2 | gzip -c > $OUTPUT_DIR/$dataset.$TGT.gz) \\
            > /dev/null \\
    )"""

        # Sanity check of the OpusCleaner output
        sanity_check_str = ''
        if self.tgt_lang is not None:
            sanity_check_str = """# Sanity Check
    src_lines=$(zcat $OUTPUT_DIR/$dataset.$SRC.gz | wc -l)
    tgt_lines=$(zcat $OUTPUT_DIR/$dataset.$TGT.gz | wc -l)
    [[ $src_lines -ne $tgt_lines ]] \\
        && echo "Lines in the output files do not match ($src_lines != $tgt_lines) >&2" \\
        && exit 1
"""

        # Compose the body_cmd string
        return """
for filter_file in $INPUT_DIR/*filters.json; do
    dataset=$(basename $filter_file)
    dataset=${{dataset/.filters.json/}}

    ## Run OpusCleaner ##
    echo "Cleaning $dataset..." >&2
    $OPUSCLEANER \\
        $filter_file \\
        --parallel $SLURM_CPUS_PER_TASK \\
        -b $INPUT_DIR \\
    > {opuscleaner_out_str} \\
    2> >(tee $LOG_DIR/opuscleaner.$dataset.log >&2)

    {sanity_check_str}
done
        """.format(
            opuscleaner_out_str=opuscleaner_out_str,
            sanity_check_str=sanity_check_str,
        )
