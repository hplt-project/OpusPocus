import logging
from pathlib import Path
from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep


logger = logging.getLogger(__name__)


@register_step('gather')
class GatherStep(CorpusStep):
    """Gather the input corpora and merge them into datasets based
    on the OpusCleaner categories labels.

    TODO: Monolingual dataset support (?)
    """

    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        previous_corpus_step: CorpusStep,
        src_lang: str,
        tgt_lang: str = None,
        gzipped: bool = True,
        suffix: str = None,
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            previous_corpus_step=previous_corpus_step,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            gzipped=gzipped,
            suffix=suffix
        )

    def init_dataset_list(self) -> None:
        """Extract the dataset names.

        Dataset names are extracted using the mapping labels
        in the categories.json input file. After this step,
        categories.json is dropped.
        """
        import yaml
        if not self.prev_corpus_step.categories_path.exists():
            raise FileNotFoundError(
                self.prev_corpus_step.categories_path()
            )
        datasets = [
            '{}.{}-{}'.format(cat, self.src_lang, self.tgt_lang)
            if self.tgt_lang is not None
            else '{}.{}'.format(cat, self.src_lang)
            for cat in self.prev_corpus_step.categories
        ]
        yaml.dump(datasets, open(self.dataset_list_path, 'w'))

    def get_command_str(self) -> str:
        # TODO: refactor using self.compose_cmd method

        # Conditional parts of the command
        tgt_def_str = ''
        sanity_check_str = ''
        if self.tgt_lang is not None:
            tgt_def_str = 'TGT="{}"'.format(self.tgt_lang)
            sanity_check_str = """    # Sanity Check
    src_lines=$(zcat $OUTPUT_DIR/$category*.$SRC.gz | wc -l)
    tgt_lines=$(zcat $OUTPUT_DIR/$category*.$TGT.gz | wc -l)
    [[ $src_lines -ne $tgt_lines ]] \\
        && echo "Lines in the output files (dataset $category) do not match ($src_lines != $tgt_lines)" >&2 \\
        && rm $OUTPUT_DIR/$category*.gz \\
        && exit 1
"""

        # Step Command
        return """#!/usr/bin/env bash
#SBATCH --job-name=gather
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
{tgt_def_str}

INPUT_DIR="{indir}"
OUTPUT_DIR="{outdir}"
LOG_DIR="{logdir}"

CATEGORIES_PATH="{categories_path}"
CATEGORIES="{categories}"

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

for category in $CATEGORIES; do
    for l in {languages}; do
        datasets=$(python -c "import json; print(' '.join(json.load(open('$CATEGORIES_PATH', 'r'))['mapping']['$category']))")
        f_out="$OUTPUT_DIR/$category{langpair}.$l.gz"
        for dset in $datasets; do
            ds_file=$INPUT_DIR/$dset.$l.gz
            [[ ! -e "$ds_file" ]] \\
                && echo "Missing $ds_file..." >&2 \\
                && rm $f_out \\
                && exit 1
            echo "Adding $ds_file" >&2
            cat $ds_file
        done > $f_out
    done
    {sanity_check_str}
done

# Explicitly exit with non-zero status
exit 0
        """.format(
            tgt_def_str=tgt_def_str,
            sanity_check_str=sanity_check_str,
            state_file=str(Path(self.step_dir, self.state_file)),
            src_lang=self.src_lang,
            tgt_lang=self.tgt_lang,
            indir=str(self.input_dir),
            outdir=str(self.output_dir),
            logdir=str(self.log_dir),
            categories_path=str(self.prev_corpus_step.categories_path),
            categories=' '.join(self.prev_corpus_step.categories),
            languages=' '.join(self.languages),
            langpair=(
                '.{}-{}'.format(self.src_lang, self.tgt_lang)
                if self.tgt_lang is not None else ''
            )
        )
