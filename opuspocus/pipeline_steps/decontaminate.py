from typing import List

import os
import glob
import logging
from pathlib import Path
from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep


logger = logging.getLogger(__name__)


@register_step('decontaminate')
class DecontaminateCorpusStep(CorpusStep):
    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        previous_corpus_step: CorpusStep,
        src_lang: str,
        tgt_lang: str,
        python_venv_dir: Path,
        valid_data_dirs: List[Path],
        decontaminate_path: Path = Path('scripts/decontaminate.py'),
        min_length: int = 25,
        gzipped: bool = True,
        suffix: str = None
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            previous_corpus_step=previous_corpus_step,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            python_venv_dir=python_venv_dir,
            valid_data_dirs=valid_data_dirs,
            decontaminate_path=decontaminate_path,
            min_length=min_length,
            gzipped=gzipped,
            suffix=suffix
        )
        for valid_dir in self.valid_data_dirs:
            if not valid_dir.exists():
                raise FileNotFoundError(
                    'Directory {} does not exist'.format(valid_dir)
                )

        if not self.decontaminate_path.exists():
            raise FileNotFoundError(
                'File {} does not exist'.format(self.decontaminate_path)
            )

    def init_dataset_list(self) -> None:
        import shutil

        # Carry over the datasets from the previous step
        shutil.copy(
            self.prev_corpus_step.dataset_list_path,
            self.dataset_list_path
        )

        # Use and carry over categories.json if available.
        if self.prev_corpus_step.categories_path.exists():
            # Use and carry over categories.json when available.
            shutil.copy(
                self.prev_corpus_step.categories_path,
                self.categories_path
            )

            # Sanity check: the dataset_list and categories.json should contain
            # same datasets
            datasets = [
                dset for mapping_values in self.category_mapping.values()
                for dset in mapping_values
            ]
            for dset in self.dataset_list:
                if dset not in datasets:
                    raise ValueError(
                        'Dataset listed in the {} but not in {} file.'.format(
                            self.dataset_list_path,
                            self.prev_corpus_step.categories_path
                        )
                    )
           
    def get_command_str(self) -> str:
        # TODO: refactor using the self.compose_cmd method

        # Conditional parts of the command
        tgt_def_str = ''
        valid_data_str = """valid_dsets=""
    for valid_dir in $VALID_DIRS; do
        valid_dsets="$valid_dir/*$SRC $valid_dsets"
    done
"""
        decontaminate_inp_str = 'zcat $INPUT_DIR/$dataset.$SRC.gz'
        decontaminate_out_str = ''
        sanity_check_str = '' 
        if self.tgt_lang is not None:
            tgt_def_str = 'TGT="{}"'.format(self.tgt_lang)
            valid_data_str = """valid_dsets=""
    for valid_dir in $VALID_DIRS; do
        for dset in $valid_dir/*$SRC; do
            path_prefix=${dset%%.$SRC}
            [[ -e $path_prefix.$SRC-$TGT ]] \\
                || paste $path_prefix.$SRC $path_prefix.$TGT \\
                    | tr -d $'\\r' \\
                    > $path_prefix.$SRC-$TGT
            valid_dsets="$path_prefix.$SRC-$TGT $valid_dsets"
        done
    done
"""
            decontaminate_in_str = """paste \\
        <(zcat $INPUT_DIR/$dataset.$SRC.gz) \\
        <(zcat $INPUT_DIR/$dataset.$TGT.gz)"""
            decontaminate_out_str = '>(cut -f2 | gzip -c > $OUTPUT_DIR/$dataset.$TGT.gz)'
            sanity_check_str = """# Sanity Check
    src_lines=$(zcat $OUTPUT_DIR/$dataset.$SRC.gz | wc -l)
    tgt_lines=$(zcat $OUTPUT_DIR/$dataset.$TGT.gz | wc -l)
    [[ $src_lines -ne $tgt_lines ]] \\
            && fail "Lines in the output files do not match ($src_lines != $tgt_lines)"
"""

        # Step Command
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
{tgt_def_str}

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
    exit $exit_code
}}

trap err_cleanup ERR
trap cleanup EXIT

for dataset in $INPUT_DIR/*.$SRC.gz; do
    dataset=$(basename $dataset)
    dataset=${{dataset%%.$SRC.gz}}

    echo "Decontaminating $dataset..." >&2
    {valid_data_str}
    {decontaminate_in_str} \\
    | python $DECONTAMINATE \\
        --min-length $MIN_LENGTH \\
        ${{valid_dsets%% }} \\
    > >( \\
        tee \\
            >(cut -f1 | gzip -c > $OUTPUT_DIR/$dataset.$SRC.gz) \\
            {decontaminate_out_str} \\
            > /dev/null \\
    )
    2> >(tee $LOG_DIR/decontaminate.$dataset.log >&2)

    {sanity_check_str}
done

# Explicitly exit with non-zero status
exit 0
        """.format(
            tgt_def_str=tgt_def_str,
            valid_data_str=valid_data_str,
            decontaminate_in_str=decontaminate_in_str,
            decontaminate_out_str=decontaminate_out_str,
            sanity_check_str=sanity_check_str,
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
