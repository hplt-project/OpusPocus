from typing import List, Optional

import logging
from pathlib import Path
from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep


logger = logging.getLogger(__name__)


def extend_dataset_name(dset_name, label):
    return "{}.{}".format(label, dset_name)


@register_step("merge")
class MergeCorpusStep(CorpusStep):
    """Merge two corpus steps into a single one.

    Takes the other_corpus_step output_dir contents and adds them
    to the contents of the previous_corpus_step output_dir.

    This is mainly a helper step for training with backtranslation.
    """

    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        previous_corpus_step: CorpusStep,
        previous_corpus_label: str,
        other_corpus_step: CorpusStep,
        other_corpus_label: str,
        src_lang: str,
        tgt_lang: str = None,
        output_shard_size: Optional[int] = None,
    ):
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            previous_corpus_step=previous_corpus_step,
            previous_corpus_label=previous_corpus_label,
            other_corpus_step=other_corpus_step,
            other_corpus_label=other_corpus_label,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            output_shard_size=output_shard_size,
        )

    @property
    def other_corpus_step(self) -> CorpusStep:
        return self.dependencies["other_corpus_step"]

    def register_categories(self) -> None:
        categories_dict = {}
        categories_dict["categories"] = [
            {"name": cat} for cat in self.prev_corpus_step.categories
        ]

        # Merge the category lists
        for cat in self.other_corpus_step.categories:
            if cat not in self.prev_corpus_step.categories:
                categories_dict["categories"].append({"name": cat})

        categories_dict["mapping"] = {}
        for cat, dset_list in self.prev_corpus_step.category_mapping.items():
            categories_dict["mapping"][cat] = [
                extend_dataset_name(dset_name, self.previous_corpus_label)
                for dset_name in dset_list
            ]
        for cat, dset_list in self.other_corpus_step.category_mapping.items():
            if cat not in categories_dict["mapping"]:
                categories_dict["mapping"][cat] = []
            for dset_name in dset_list:
                categories_dict["mapping"][cat].append(
                    extend_dataset_name(dset_name, self.other_corpus_label)
                )
        self.save_categories_dict(categories_dict)

    def get_command_targets(self) -> List[Path]:
        return [
            Path(self.output_dir, "{}.{}.gz".format(dset, lang))
            for dset in self.dataset_list
            for lang in self.languages
        ]

    def command(self, target_file: Path) -> None:
        target_filename = target_file.stem + target_file.suffix
        source_filename = ".".join(target_filename.split(".")[1:])
        source_label = target_filename.split(".")[0]
        if source_label == self.previous_corpus_label:
            target_file.hardlink_to(
                Path(self.prev_corpus_step.output_dir, source_filename)
            )
        elif source_label == self.other_corpus_label:
            target_file.hardlink_to(
                Path(self.other_corpus_step.output_dir, source_filename)
            )
        else:
            raise ValueError("Unknown corpus label ({}).".format(source_label))
