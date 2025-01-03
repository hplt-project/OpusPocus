from pathlib import Path
from typing import List

from attrs import define

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.utils import concat_files


@register_step("gather")
@define(kw_only=True)
class GatherCorpusStep(CorpusStep):
    """Gather the input corpora and merge them into datasets based on the OpusCleaner categories labels."""

    def register_categories(self) -> None:
        """Extract the dataset names.

        Dataset names are extracted using the mapping labels
        in the categories.json input file. After this step,
        categories.json is dropped.
        """
        categories_dict = {
            "categories": [
                c_dict
                for c_dict in self.prev_corpus_step.categories_dict["categories"]
                if (
                    c_dict["name"] in self.prev_corpus_step.category_mapping
                    and self.prev_corpus_step.category_mapping[c_dict["name"]]
                )
            ],
            "mapping": {},
        }
        for c_dict in categories_dict["categories"]:
            dset_name = c_dict["name"]
            if self.tgt_lang is not None:
                dset_name = "{}.{}-{}".format(c_dict["name"], self.src_lang, self.tgt_lang)
            categories_dict["mapping"][c_dict["name"]] = [dset_name]
        self.save_categories_dict(categories_dict)

    def get_command_targets(self) -> List[Path]:
        """Get the list of output corpora using the categories defined in the prev_corpus_step categories.json."""
        langpair = ""
        if self.tgt_lang is not None:
            langpair = f".{self.src_lang}-{self.tgt_lang}"
        return [
            Path(self.output_dir, f"{category}{langpair}.{lang}.gz")
            for category in self.categories
            for lang in self.languages
        ]

    def command(self, target_file: Path) -> None:
        """Concatenate files that belong to the specified category.

        We infer the input file list based on the target_file name and the list of datasets that correspond to the
        category_mapping used to create the target_file filename.
        """
        target_stem_split = target_file.stem.split(".")
        category = ".".join(target_stem_split[:-1])
        if self.tgt_lang is not None:
            category = ".".join(target_stem_split[:-2])
        lang = target_stem_split[-1]

        concat_files(
            [Path(self.input_dir, f"{dset}.{lang}.gz") for dset in self.prev_corpus_step.category_mapping[category]],
            target_file,
        )
