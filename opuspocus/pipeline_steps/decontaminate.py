from typing import List

import os
import glob
import logging
from pathlib import Path
from opuspocus.pipeline_steps import (
    OpusPocusStep,
    build_step,
    register_step
)

logger = logging.getLogger(__name__)


class DecontaminateCorpusStep(OpusPocusStep):
    """
    TODO: split into individual corpus-steps
    TODO: reduce code duplicates from mono/para split
    """
    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        python_venv_dir: Path,
        valid_data_dirs: List[Path],
        corpus_step: OpusPocusStep,
        decontaminate_path: Path = Path('scripts/decontaminate.py'),
        min_length: int = 25,
        suffix: str = None,
        **kwargs
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            python_venv_dir=python_venv_dir,
            valid_data_dirs=valid_data_dirs,
            corpus_step=corpus_step,
            decontaminate_path=decontaminate_path,
            min_length=min_length,
            suffix=suffix,
            **kwargs
        )
        self.input_dir = self.dependencies['corpus_step'].output_dir

        for valid_dir in self.valid_data_dirs:
            if not valid_dir.exists():
                raise ValueError(
                    'Directory {} does not exist'.format(valid_dir)
                )

        if not self.decontaminate_path.exists():
            raise ValueError(
                'File {} does not exist'.format(self.decontaminate_path)
            )


@register_step('decontaminate_para')
class DecontaminateCorpusParaStep(DecontaminateCorpusStep):
    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        src_lang: str,
        tgt_lang: str,
        python_venv_dir: Path,
        valid_data_dirs: List[Path],
        corpus_step: OpusPocusStep,
        decontaminate_path: Path = Path('scripts/decontaminate.py'),
        min_length: int = 25,
        suffix: str = None
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            python_venv_dir=python_venv_dir,
            valid_data_dirs=valid_data_dirs,
            corpus_step=corpus_step,
            decontaminate_path=decontaminate_path,
            min_length=min_length,
            suffix=suffix
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
#SBATCH --job-name=decontaminate
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=5G
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
VALID_DIRS="{valdirs}"
LOG_DIR="{logdir}"

STATE_FILE="{state_file}"

MIN_LENGTH="{min_length}"
DECONTAMINATE="{decontaminate}"

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
    rm -r $OUTPUT_DIR/*
    exit $exit_code
}}

trap err_cleanup ERR
trap cleanup EXIT

for dataset in $INPUT_DIR/*.$SRC.gz; do
    dataset=$(basename $dataset)
    dataset=${{dataset%%.$SRC.gz}}

    echo "Decontaminating $dataset..." >&2
    valid_dsets=""
    for valid_dir in $VALID_DIRS; do
        for dset in $valid_dir/*$SRC; do
            path_prefix=${{dset%%.$SRC}}
            [[ -e $path_prefix.$SRC-$TGT ]] \\
                || paste $path_prefix.$SRC $path_prefix.$TGT \\
                    | tr -d $'\\r' \\
                    > $path_prefix.$SRC-$TGT
            valid_dsets="$path_prefix.$SRC-$TGT $valid_dsets"
        done
    done
    paste \\
        <(zcat $INPUT_DIR/$dataset.$SRC.gz) \\
        <(zcat $INPUT_DIR/$dataset.$TGT.gz) \\
    | python $DECONTAMINATE \\
        --min-length $MIN_LENGTH \\
        $valid_dsets \\
    > >( \\
        tee \\
            >(cut -f1 | gzip -c > $OUTPUT_DIR/$dataset.$SRC.gz) \\
            >(cut -f2 | gzip -c > $OUTPUT_DIR/$dataset.$TGT.gz) \\
            > /dev/null \\
    )
    2> >(tee $LOG_DIR/decontaminate.$dataset.log >&2)

    # Validate Output
    src_lines=$(zcat $OUTPUT_DIR/$dataset.$SRC.gz | wc -l)
    tgt_lines=$(zcat $OUTPUT_DIR/$dataset.$TGT.gz | wc -l)
    [[ $src_lines -ne $tgt_lines ]] \\
            && fail "Lines in the output files do not match ($src_lines != $tgt_lines)"
done

# create link to the corpus categories file
ln $INPUT_DIR/categories.json $OUTPUT_DIR/categories.json
        """.format(
            state_file=str(Path(self.step_dir, self.state_file)),
            python_venv=str(self.python_venv_dir),
            src_lang=self.src_lang,
            tgt_lang=self.tgt_lang,
            indir=str(self.input_dir),
            outdir=str(self.output_dir),
            valdirs=' '.join([str(v_dir) for v_dir in self.valid_data_dirs]),
            logdir=str(self.log_dir),
            min_length=self.min_length,
            decontaminate=str(self.decontaminate_path),
        )


@register_step('decontaminate_mono')
class DecontaminateCorpusMonoStep(DecontaminateCorpusStep):
    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        lang: str,
        python_venv_dir: Path,
        valid_data_dirs: List[Path],
        corpus_step: OpusPocusStep,
        decontaminate_path: Path = Path('scripts/decontaminate.py'),
        min_length: int = 25,
        suffix: str = None
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            lang=lang,
            python_venv_dir=python_venv_dir,
            valid_data_dirs=valid_data_dirs,
            corpus_step=corpus_step,
            decontaminate_path=decontaminate_path,
            min_length=min_length,
            suffix=suffix
        )

    @property
    def step_name(self):
        name = 's.{}.{}'.format(self.step, self.lang)
        if self.suffix is not None:
            name += '.{}'.format(self.suffix)
        return name

    def get_command_str(self) -> str:
        return """#!/usr/bin/env bash
#SBATCH --job-name=decontaminate
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=5G
#SBATCH -o {logdir}/slurm.%j.log
#SBATCH -e {logdir}/slurm.%j.log
# Preprocess and clean the data (mainly using opuscleaner)
# TODO: replace non-parallel gzip with pigz
set -euo pipefail
export PATH="{python_venv}/bin:$PATH"

LANG="{lang}"

INPUT_DIR="{indir}"
OUTPUT_DIR="{outdir}"
VALID_DIRS="{valdirs}"
LOG_DIR="{logdir}"

STATE_FILE="{state_file}"

MIN_LENGTH="{min_length}"
DECONTAMINATE="{decontaminate}"

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

for dataset in $INPUT_DIR/*.$LANG.gz; do
    dataset=$(basename $dataset)
    dataset=${{dataset/.$LANG.gz}}

    valid_dirs=""
    for valid_dir in $VALID_DIRS; do
        valid_dirs="$valid_dir/*$LANG $valid_dirs"
    done

    echo "Decontaminating $dataset..." >&2
    zcat $INPUT_DIR/$dataset.$LANG.gz \\
    | python $DECONTAMINATE \\
        --min-length $MIN_LENGTH \\
        $valid_dirs \\
    > >( \\
        tee \\
            >(cut -f1 | gzip -c > $OUTPUT_DIR/$dataset.$LANG.gz) \\
            > /dev/null \\
    )
    2> >(tee $LOG_DIR/decontaminate.$dataset.log >&2)
done

# create link to the corpus categories file
ln $INPUT_DIR/categories.json $OUTPUT_DIR/categories.json
        """.format(
            state_file=str(Path(self.step_dir, self.state_file)),
            python_venv=str(self.python_venv_dir),
            lang=self.lang,
            indir=str(self.input_dir),
            outdir=str(self.output_dir),
            valdirs=' '.join([str(v_dir) for v_dir in self.valid_data_dirs]),
            logdir=str(self.log_dir),
            min_length=self.min_length,
            decontaminate=str(self.decontaminate_path),
        )
