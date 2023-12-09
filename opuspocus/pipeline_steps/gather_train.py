import logging
from pathlib import Path
from opuspocus.pipeline_steps import (
    OpusPocusStep,
    build_step,
    register_step
)

logger = logging.getLogger(__name__)


@register_step('gather_train')
class GatherStep(OpusPocusStep):
    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        src_lang: str,
        tgt_lang: str,
        corpus_step: OpusPocusStep,
        suffix: str = None,
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            corpus_step=corpus_step,
            suffix=suffix
        )

        self.input_dir = self.dependencies['corpus_step'].output_dir

    @property
    def step_name(self):
        name = 's.{}.{}-{}'.format(
            self.step,
            self.src_lang,
            self.tgt_lang,
        )
        if self.suffix is not None:
            name += '.{}'.format(self.suffix)
        return name

    def get_command_str(self) -> str:
        return """#!/usr/bin/env bash
#SBATCH --job-name=gather_train
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH -o {logdir}/slurm.%j.log
#SBATCH -e {logdir}/slurm.%j.log
# Gather the individual datasets into training groups based
# on the opuscleaner categories
set -euo pipefail

SRC="{src_lang}"
TGT="{tgt_lang}"

INPUT_DIR="{indir}"
OUTPUT_DIR="{outdir}"
LOG_DIR="{logdir}"

STATE_FILE="{state_file}"

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

categories_json="$INPUT_DIR/categories.json"
categories=$(python -c "import json, sys; print(' '.join([x['name'] for x in json.load(open('$categories_json', 'r'))['categories']]))")
for category in $categories; do
    for l in $SRC $TGT; do
        datasets=$(python -c "import json, sys; print(' '.join(json.load(open('$categories_json', 'r'))['mapping']['$category']))")
        f_out="$OUTPUT_DIR/$category.para.$l.gz"
        for dset in $datasets; do
            ds_file=$INPUT_DIR/$dset.$l.gz
            [[ ! -e "$ds_file" ]] \\
                && fail_and_rm "Missing $ds_file..." $f_out
            echo "Adding $ds_file" >&2
            cat $ds_file
        done > $f_out
    done
    src_lines=$(zcat $OUTPUT_DIR/$category.para.$SRC.gz | wc -l)
    tgt_lines=$(zcat $OUTPUT_DIR/$category.para.$TGT.gz | wc -l)
    [[ $src_lines -ne $tgt_lines ]] \\
        && fail_and_rm "Lines in the output files (dataset $category) do not match ($src_lines != $tgt_lines)"
done

# Explicitly exit with non-zero status
exit 0
        """.format(
            state_file=str(Path(self.step_dir, self.state_file)),
            src_lang=self.src_lang,
            tgt_lang=self.tgt_lang,
            indir=str(self.input_dir),
            outdir=str(self.output_dir),
            logdir=str(self.log_dir)
        )
