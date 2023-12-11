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
        pipeline_dir: Path,
        previous_corpus_step: CorpusStep,
        src_lang: str,
        tgt_lang: str,
        python_venv_dir: Path,
        opuscleaner_cmd: str = 'opuscleaner-clean',
        suffix: str = None
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            previous_corpus_step=previous_corpus_step,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            python_venv_dir=python_venv_dir,
            opuscleaner_cmd=opuscleaner_cmd,
            suffix=suffix
        )

    def init_dataset_list(self) -> None:
        """Create a dataset list using the datasets listed in categories.json file.

        OpusCleaner server app creates a categories.json file listing locally
        available datasets and their user-specified categorization.
        """
        import shutil
        import json
        import yaml

        # Sanity check: categories.json exists
        if not self.previous_corpus_step.categories_path.exists():
            raise FileNotFoundError(
                self.previous_corpus_step.categories_path.exists()
            )
        shutil.copy(
            self.previous_corpus_step.categories_path,
            self.categories_path
        )

        datasets = [
            dset for dset in mapping_values
            for mapping_values in self.category_mapping.values()
        ]

        # Sanity check: filters.json files extist
        for dset in datasets:
            dset_filter_path = Path(
                self.input_dir, '{}.filters.json'.format(dset)
            )
            if not dset_filter_path.exists():
                raise FileNotFoundError(dset_filter_path)

        yaml.dump(datasets, open(self.dataset_list_path, 'w'))

    def get_command_str(self) -> str:
        # TODO: refactor using self.compose_cmd method

        # Conditional parts of the command
        tgt_def_str = ''
        opuscleaner_out_str = ''
        sanity_check_str = ''
        if self.tgt_lang is not None:
            tgt_def_str = 'TGT="{}"'.format(self.tgt_lang)
            opuscleaner_out_str = (
                '>(cut -f2 | gzip -c > $OUTPUT_DIR/$dataset.$TGT.gz)'
            )
            sanity_check_str = """    # Sanity Check
    src_lines=$(zcat $OUTPUT_DIR/$dataset.$SRC.gz | wc -l)
    tgt_lines=$(zcat $OUTPUT_DIR/$dataset.$TGT.gz | wc -l)
    [[ $src_lines -ne $tgt_lines ]] \\
        && fail "Lines in the output files do not match ($src_lines != $tgt_lines)"
"""

        # Step Command
        return """#!/usr/bin/env bash
#SBATCH --job-name=opuscleaner-clean
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=20G
#SBATCH -o {logdir}/slurm.%j.log
#SBATCH -e {logdir}/slurm.%j.log
# Preprocess and cleans the data (mainly using opuscleaner)
# TODO: replace non-parallel gzip with pigz
set -euo pipefail
export PATH="{python_venv}/bin:$PATH"

SRC="{src_lang}"
{tgt_def_str}

INPUT_DIR="{indir}"
OUTPUT_DIR="{outdir}"
LOG_DIR="{logdir}"

STATE_FILE="{state_file}"

OPUSCLEANER="{opuscleaner}"

fail() {{
    echo $1 >&2
    exit 1
}}

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

for filter_file in $INPUT_DIR/*filters.json; do
    dataset=$(basename $filter_file)
    dataset=${{dataset/.filters.json/}}

    ## Run OpusCleaner ##
    echo "Cleaning $dataset..." >&2
    $OPUSCLEANER \\
        $filter_file \\
        --parallel $SLURM_CPUS_PER_TASK \\
        -b $INPUT_DIR \\
    > >( \\
        tee \\
            >(cut -f1 | gzip -c > $OUTPUT_DIR/$dataset.$SRC.gz) \\
            {opuscleaner_out_str} \\
            > /dev/null \\
    ) \\
    2> >(tee $LOG_DIR/opuscleaner.$dataset.log >&2)

{sanity_check_str}
done

# Explicitly exit with non-zero status
exit 0
        """.format(
            tgt_def_str=tgt_def_str,
            opuscleaner_out_str=opuscleaner_out_str,
            sanity_check_str=sanity_check_str,
            state_file=str(Path(self.step_dir, self.state_file)),
            python_venv=str(self.python_venv_dir),
            src_lang=self.src_lang,
            indir=str(self.input_dir),
            outdir=str(self.output_dir),
            logdir=str(self.log_dir),
            opuscleaner=str(self.opuscleaner_cmd),
        )
