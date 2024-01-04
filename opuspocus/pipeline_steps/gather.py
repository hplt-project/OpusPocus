from typing import Optional

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
        python_venv_dir: Path,
        src_lang: str,
        tgt_lang: str = None,
        output_shard_size: Optional[int] = None,
        gzipped: bool = True,
        suffix: str = None,
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            previous_corpus_step=previous_corpus_step,
            python_venv_dir=python_venv_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            output_shard_size=output_shard_size,
            gzipped=gzipped,
            suffix=suffix
        )

    def register_categories(self) -> None:
        """Extract the dataset names.

        Dataset names are extracted using the mapping labels
        in the categories.json input file. After this step,
        categories.json is dropped.
        """
        categories_dict = {
            'categories': [
                c_dict for c_dict in self.prev_corpus_step.categories_dict['categories']
                if c_dict['name'] in self.prev_corpus_step.category_mapping
            ],
            'mapping': {}
        }
        for c_dict in categories_dict['categories']:
            dset_name = c_dict['name']
            if self.tgt_lang is not None:
                dset_name = '{}.{}-{}'.format(
                    c_dict['name'], self.src_lang, self.tgt_lang
                )
            categories_dict['mapping'][c_dict['name']] = [dset_name]
        self.save_categories_dict(categories_dict)

    def _cmd_header_str(self) -> str:
        return super()._cmd_header_str(
            n_cpus=1,
            mem=10,
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

CATEGORIES_PATH="{categories_path}"
CATEGORIES="{categories}"
        """.format(
            python_venv=self.python_venv_dir,
            src_lang=self.src_lang,
            tgt_def_str=tgt_def_str,
            indir=self.input_dir,
            outdir=self.output_dir,
            logdir=self.log_dir,
            categories_path=self.prev_corpus_step.categories_path,
            categories=' '.join(self.categories),
        )

    def _cmd_body_str(self) -> str:
        # TODO: refactor using self.compose_cmd method

        # Sanity check of the step output
        sanity_check_str = ''
        if self.tgt_lang is not None:
            sanity_check_str = """# Sanity Check
    src_lines=$(zcat $OUTPUT_DIR/$category*.$SRC.gz | wc -l)
    tgt_lines=$(zcat $OUTPUT_DIR/$category*.$TGT.gz | wc -l)
    [[ $src_lines -ne $tgt_lines ]] \\
        && echo "Lines in the output files (dataset $category) do not match ($src_lines != $tgt_lines)" >&2 \\
        && rm $OUTPUT_DIR/$category*.gz \\
        && exit 1
"""

        # Compose the body_cmd string
        return """
for category in $CATEGORIES; do
    for l in {languages}; do
        datasets=$(python -c "import json; print(' '.join(json.load(open('$CATEGORIES_PATH', 'r'))['mapping']['$category'] ))")
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
        """.format(
            sanity_check_str=sanity_check_str,
            src_lang=self.src_lang,
            tgt_lang=self.tgt_lang,
            languages=' '.join(self.languages),
            langpair=(
                '.{}-{}'.format(self.src_lang, self.tgt_lang)
                if self.tgt_lang is not None else ''
            )
        )
