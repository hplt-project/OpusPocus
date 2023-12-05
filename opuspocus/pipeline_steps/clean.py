import os
import glob
import logging
from pathlib import Path
from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep


logger = logging.getLogger(__name__)


class CleanCorpusStep(OpusPocusStep):
    """
    TODO: split into individual corpus-steps
    TODO: reduce code duplicates from mono/para split
    """
    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        python_venv_dir: Path,
        opuscleaner_cmd: str = 'opuscleaner-clean',
        suffix: str = None,
        **kwargs
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            python_venv_dir=python_venv_dir,
            opuscleaner_cmd=opuscleaner_cmd,
            suffix=suffix,
            **kwargs
        )


@register_step('clean_para')
class CleanCorpusParaStep(CleanCorpusStep):
    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        src_lang: str,
        tgt_lang: str,
        python_venv_dir: Path,
        raw_data_dir: Path,
        opuscleaner_cmd: str = 'opuscleaner-clean',
        suffix: str = None
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            python_venv_dir=python_venv_dir,
            raw_data_dir=raw_data_dir,
            opuscleaner_cmd=opuscleaner_cmd,
            suffix=suffix
        )
        if not self.raw_data_dir.exists():
            raise ValueError(
                'Directory {} does not exist.'.format(self.raw_data_dir)
            )

    @property
    def step_name(self):
        name = 's.{}.{}-{}'.format(
            self.step,
            self.src_lang,
            self.tgt_lang
        )
        if self.suffix is not None:
            name += '.{}'.format(self.suffix)
        return name

    def get_command_str(self) -> str:
        return """#!/usr/bin/env bash
#SBATCH --job-name=opuscleaner-clean
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=20G
#SBATCH -o {logdir}/slurm.%j.log
#SBATCH -e {logdir}/slurm.%j.log
# Preprocess and clean the data (mainly using opuscleaner)
# TODO: replace non-parallel gzip with pigz
set -euo pipefail
export PATH="{python_venv}/bin:$PATH"

SRC="{src_lang}"
TGT="{tgt_lang}"

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
    $OUTPUT_DIR/*
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
            >(cut -f2 | gzip -c > $OUTPUT_DIR/$dataset.$TGT.gz) \\
            > /dev/null \\
    ) \\
    2> >(tee $LOG_DIR/opuscleaner.$dataset.log >&2)
     # Validate Output
    src_lines=$(zcat $OUTPUT_DIR/$dataset.$SRC.gz | wc -l)
    tgt_lines=$(zcat $OUTPUT_DIR/$dataset.$TGT.gz | wc -l)
    [[ $src_lines -ne $tgt_lines ]] \\
        && fail "Lines in the output files do not match ($src_lines != $tgt_lines)"
done

# create link to the corpus categories file
ln $INPUT_DIR/categories.json $OUTPUT_DIR/categories.json

echo DONE > $STATE_FILE
        """.format(
            state_file=str(Path(self.step_dir, self.state_file)),
            python_venv=str(self.python_venv_dir),
            src_lang=self.src_lang,
            tgt_lang=self.tgt_lang,
            indir=str(self.raw_data_dir),
            outdir=str(self.output_dir),
            logdir=str(self.log_dir),
            opuscleaner=str(self.opuscleaner_cmd),
        )


@register_step('clean_mono')
class CleanCorpusMonoStep(CleanCorpusStep):
    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        lang: str,
        python_venv_dir: Path,
        raw_data_dir: Path,
        opuscleaner_cmd: str = 'opuscleaner-clean',
        suffix: str = None
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            python_venv_dir=python_venv_dir,
            lang=lang,
            raw_data_dir=raw_data_dir,
            opuscleaner_cmd=opuscleaner_cmd,
            suffix=suffix
        )
        if not self.raw_data_dir.exists():
            raise ValueError(
                'Directory {} does not exist.'.format(self.raw_data_dir)
            )

    @property
    def step_name(self):
        name = 's.{}.{}'.format(self.step, self.lang)
        if self.suffix is not None:
            name += '.{}'.format(self.suffix)
        return name

    def get_command_str(self) -> str:
        return """#!/usr/bin/env bash
#SBATCH --job-name=opuscleaner-clean
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=20G
#SBATCH -o {logdir}/slurm.%j.log
#SBATCH -e {logdir}/slurm.%j.log
# Preprocess and clean the data (mainly using opuscleaner)
# TODO: replace non-parallel gzip with pigz
set -euo pipefail
export PATH="{python_venv}/bin:$PATH"

LANG="{lang}"

INPUT_DIR="{indir}"
OUTPUT_DIR="{outdir}"
LOG_DIR="{logdir}"

STATE_FILE="{state_file}"

OPUSCLEANER="{opuscleaner}"

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
    rm -r $OUTPUT_DIR/*
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
            >(cut -f1 | gzip -c > $OUTPUT_DIR/$dataset.$LANG.gz) \\
            > /dev/null \\
    ) \\
    2> >(tee $LOG_DIR/opuscleaner.$dataset.log >&2)
done

# create link to the corpus categories file
ln $INPUT_DIR/categories.json $OUTPUT_DIR/categories.json
        """.format(
            state_file=str(Path(self.step_dir, self.state_file)),
            python_venv=str(self.python_venv_dir),
            lang=self.lang,
            indir=str(self.raw_data_dir),
            outdir=str(self.output_dir),
            logdir=str(self.log_dir),
            opuscleaner=str(self.opuscleaner_cmd),
        )
