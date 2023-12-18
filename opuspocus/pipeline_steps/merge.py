from typing import List

import logging
from pathlib import Path
from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep


logger = logging.getLogger(__name__)


def extend_dataset_name(d_name, label):
    # Dataset name should be a '.' separated name with last element
    # contains the language/langpair identification.
    d_name_list = d_name.split('.')
    d_name_list = d_name_list[:-1] + [label] + d_name_list[-1:]
    return '.'.join(d_name_list)


@register_step('merge')
class MergeStep(CorpusStep):
    """Merge two corpus steps into a single one.

    Takes the other_corpus_step output_dir contents and adds them
    to the contents of the previous_corpus_step output_dir.

    This is mainly a helper step for training with backtranslation.
    """

    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        previous_corpus_step: CorpusStep,
        previous_corpus_label: str,
        other_corpus_step: CorpusStep,
        other_corpus_label: str,
        src_lang: str,
        tgt_lang: str = None,
        gzipped: bool = True,
        suffix: str = None,
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            previous_corpus_step=previous_corpus_step,
            previous_corpus_label=previous_corpus_label,
            other_corpus_step=other_corpus_step,
            other_corpus_label=other_corpus_label,
            corpus_labels=corpus_labels,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            gzipped=gzipped,
            suffix=suffix
        )

    @property
    def other_corpus_step(self) -> CorpusStep:
        return self.dependencies['other_corpus_step']

    def init_dataset_list(self) -> None:
        """Extract the dataset names.

        Dataset names are extracted using the mapping labels
        in the categories.json input file.
        """
        import yaml
        dataset_list = []

        # register previous_corpus_step datasets
        for dataset_list, corpus_label in [
            (self.prev_corpus_step.dataset_list, self.previous_corpus_label),
            (self.other_corpus_step.dataset_list, self.other_corpus_label)
        ]:
            for dataset in dataset_list:
                # Dataset name should be a '.' separated name with last element
                # contains the language/langpair identification.
                dataset_list.append(extend_dataset_name(dataset, corpus_label))
        self.save_dataset_list(dataset_list)
        self.merge_categories()

    def merge_categories(self) -> None:
        categories_dict = self.prev_corpus_label.categories_dict
        for category in self.other_corpus_step.categories:
            if category not in self.prev_corpus_step.categories:
                categories_dict.append({'name': category})
        categories_dict['mapping'] = {
            c_map_name : [
                extend_dataset_name(dataset, self.previous_corpus_label)
                for dataset in c_map_value
            ]
            for c_map_name, c_map_value in categories_dict['mapping'].values()
        }
        for c_map_name, c_map_value in (
            self.other_corpus_step.category_mapping.values()
        ):
            if c_map_name not in categories_dict['mapping']:
                categories_dict[c_map_name] = [
                    extend_dataset_name(dataset, self.other_corpus_label)
                    for dataset in c_map_value
                ]
            else:
                for dataset in c_map_value:
                    categories_dict['mapping'][c_map_name].append(
                        extend_dataset_name(dataset, self.other_corpus_label)
                    )
        self.save_categories_dict(categories_dict)

    def _cmd_vars_str(self) -> str:
        tgt_def_str = ''
        if self.tgt_lang is not None:
            tgt_def_str = 'TGT="{}"'.format(self.tgt_lang)

        return """
SRC="{src_lang}"
{tgt_def_str}
LANGUAGES="{languages}"

INPUT_DIR="{indir}"
OTHER_INPUT_DIR="{other_indir}"
OUTPUT_DIR="{outdir}"
LOG_DIR="{logdir}"

STEP_LABEL="{label}"
OTHER_STEP_LABEL="{other_label}"
DATASET_LIST"{dset_list}"
OTHER_DATASET_LIST="{other_dset_list}"

        """.format(
            src_lang=self.src_lang,
            tgt_def_str=tgt_def_str,
            languages=self.languages,
            indir=self.prev_corpus_step.output_dir,
            other_indir=self.other_corpus_step.output_dir,
            outdir=self.output_dir,
            logdir=self.log_dir,
            label=self.previous_corpus_label,
            other_label=self.other_corpus_label,
            dset_list=' '.join(self.prev_corpus_step.dataset_list),
            other_dset_list=' '.join(self.other_corpus_step.dataset_list)
        )

    def _cmd_body_str(self) -> str:
        return """
for dset in $DATASET_LIST; do
    dset_new=$(echo $dset | sed "s/\\(\\.[^\\.]*\\)$/.$STEP_LABEL\\1/")
    for lang in $LANGUAGES; do
        ln $INPUT_DIR/$dset.$lang $OUTPUT_DIR/$dset_new.$lang
    done
done

for dset in $OTHER_DATASET_LIST; do
    dset_new=$(echo $dset | sed "s/\\(\\.[^\\.]*\\)$/.$OTHER_STEP_LABEL\\1/")
    for lang in $LANGUAGES; do
        ln $INPUT_DIR/$dset.$lang $OUTPUT_DIR/$dset_new.$lang
    done
done"""
