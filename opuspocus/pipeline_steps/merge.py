from pathlib import Path
from typing import List

from attrs import define, field, validators

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep


@register_step("merge")
@define(kw_only=True)
class MergeCorpusStep(CorpusStep):
    """Class for merging of two corpus steps into a single one.

    Takes the other_corpus_step output_dir contents and adds them
    to the contents of the prev_corpus_step output_dir.

    This is mainly a helper step for training with backtranslation.

    # TODO(varisd): support merging more than two corpus steps.
    """

    prev_corpus_label: str = field(validator=validators.instance_of(str))
    other_corpus_step: CorpusStep = field()
    other_corpus_label: str = field(validator=validators.instance_of(str))
    merge_categories: bool = field(default=False)

    @other_corpus_step.validator
    def _inherited_from_corpus_step(self, attribute: str, value: CorpusStep) -> None:
        # TODO(varisd): remove duplicate code (similar to corpus_step.py validator)
        if not issubclass(type(value), CorpusStep):
            err_msg = f"{attribute} value must contain class instance that inherits from CorpusStep."
            raise TypeError(err_msg)

    def register_categories(self) -> None:
        """Infer new categories.json by merging the categories.json files from the prev_corpus_step
        and other_corpus_step.

        We avoid naming collissions by using the prev_corpus_label and other_corpus_label as the prefix for the
        MergeStep corpus files.
        """
        categories_dict = {"categories": [], "mapping": {}}

        # Register categories
        for cat in self.prev_corpus_step.categories:
            cat_val = cat
            if not self.merge_categories:
                cat_val = f"{self.prev_corpus_label}.{cat}"
            categories_dict["categories"].append({"name": cat_val})
        for cat in self.other_corpus_step.categories:
            cat_val = cat
            if not self.merge_categories:
                cat_val = f"{self.other_corpus_label}.{cat}"
            if cat_val not in categories_dict["categories"]:
                categories_dict["categories"].append({"name": cat_val})

        # Register mapping
        for cat, dset_list in self.prev_corpus_step.category_mapping.items():
            cat_val = cat
            if not self.merge_categories:
                cat_val = f"{self.prev_corpus_label}.{cat}"
            categories_dict["mapping"][cat_val] = [f"{self.prev_corpus_label}.{dset_name}" for dset_name in dset_list]
        for cat, dset_list in self.other_corpus_step.category_mapping.items():
            cat_val = cat
            if not self.merge_categories:
                cat_val = f"{self.other_corpus_label}.{cat}"
            if cat_val not in categories_dict["mapping"]:
                categories_dict["mapping"][cat_val] = []
            for dset_name in dset_list:
                categories_dict["mapping"][cat_val].append(f"{self.other_corpus_label}.{dset_name}")

        self.save_categories_dict(categories_dict)

    def get_command_targets(self) -> List[Path]:
        """One target_file per corpus linked from prev_corpus_step or other_corpus_step
        (with its after-merge naming).
        """
        return [Path(self.output_dir, f"{dset}.{lang}.gz") for dset in self.dataset_list for lang in self.languages]

    def command(self, target_file: Path) -> None:
        """Create a target_file by hardlinking it to its original corpus file.

        We infer the original corpus filename from the target_file.
        """
        target_filename = target_file.stem + target_file.suffix
        source_filename = ".".join(target_filename.split(".")[1:])
        source_label = target_filename.split(".")[0]
        if source_label == self.prev_corpus_label:
            target_file.hardlink_to(Path(self.prev_corpus_step.output_dir, source_filename))
        elif source_label == self.other_corpus_label:
            target_file.hardlink_to(Path(self.other_corpus_step.output_dir, source_filename))
        else:
            err_msg = f"Unknown corpus label ({source_label})."
            raise ValueError(err_msg)
