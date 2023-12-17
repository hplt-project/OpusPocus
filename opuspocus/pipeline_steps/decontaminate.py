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
        python_venv_dir: Path,
        valid_data_dirs: List[Path],
        src_lang: str,
        tgt_lang: str = None,
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

    def _cmd_header_str(self) -> str:
        return super()._cmd_header_str(
            n_cpus=8,
            mem=5,
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
VALID_DIRS="{valdirs}"
LOG_DIR="{logdir}"

MIN_LENGTH="{min_length}"
DECONTAMINATE="{decontaminate}"
        """.format(
            python_venv=self.python_venv_dir,
            src_lang=self.src_lang,
            tgt_def_str=tgt_def_str,
            indir=self.input_dir,
            outdir=self.output_dir,
            valdirs=' '.join([str(v_dir) for v_dir in self.valid_data_dirs]),
            logdir=self.log_dir,
            min_length=self.min_length,
            decontaminate=self.decontaminate_path,
        )

    def _cmd_body_str(self) -> str:
        # TODO: parallelize (using hyperqueue)

        # List the validation datasets
        valid_data_str = """valid_dsets=""
    for valid_dir in $VALID_DIRS; do
        valid_dsets="$valid_dir/*$SRC $valid_dsets"
    done
"""
        if self.tgt_lang is not None:
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
        # decontaminate.py input preprocessing
        decontaminate_in_str = 'zcat $INPUT_DIR/$dataset.$SRC.gz'
        if self.tgt_lang is not None:
            decontaminate_in_str = """paste \\
        <(zcat $INPUT_DIR/$dataset.$SRC.gz) \\
        <(zcat $INPUT_DIR/$dataset.$TGT.gz)"""

        # decontaminate.py output postprocessing
        decontaminate_out_str = """>( \\
        tee \\
            >(cut -f1 | gzip -c > $OUTPUT_DIR/$dataset.$SRC.gz) \\
            > /dev/null \\
    )"""
        if self.tgt_lang is not None:
            decontaminate_out_str = """>( \\
        tee \\
            >(cut -f1 | gzip -c > $OUTPUT_DIR/$dataset.$SRC.gz) \\
            >(cut -f2 | gzip -c > $OUTPUT_DIR/$dataset.$TGT.gz) \\
            > /dev/null \\
    )"""

        # Sanity check of the decontaminate.py output
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
for dataset in $INPUT_DIR/*.$SRC.gz; do
    dataset=$(basename $dataset)
    dataset=${{dataset%%.$SRC.gz}}

    echo "Decontaminating $dataset..." >&2
    {valid_data_str}
    {decontaminate_in_str} \\
    | python $DECONTAMINATE \\
        --min-length $MIN_LENGTH \\
        ${{valid_dsets%% }} \\
    > {decontaminate_out_str} \\
    2> >(tee $LOG_DIR/decontaminate.$dataset.log >&2)

    {sanity_check_str}
done
        """.format(
            valid_data_str=valid_data_str,
            decontaminate_in_str=decontaminate_in_str,
            decontaminate_out_str=decontaminate_out_str,
            sanity_check_str=sanity_check_str,
        )
