import logging
import shutil
from argparse import Namespace
from pathlib import Path
from typing import List

from attrs import define, field, validators

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.tools.decontaminate import main as decontaminate_main
from opuspocus.utils import cut_file, paste_files

logger = logging.getLogger(__name__)


@register_step("decontaminate")
@define(kw_only=True)
class DecontaminateCorpusStep(CorpusStep):
    """Class implementing training dataset decontamination (removing test data examples)>"""

    valid_data_step: CorpusStep = field(validator=validators.optional(validators.instance_of(CorpusStep)))
    test_data_step: CorpusStep = field(validator=validators.optional(validators.instance_of(CorpusStep)))
    min_length: int = field(default=25)

    def __attrs_post_init__(self) -> None:
        """Check that at least one of the valid/test steps is defined."""
        if self.valid_data_step is None and self.test_data_step is None:
            logger.warning(
                "No valid_data_step or test_data_step was provided. Step %s will do notning when executed.",
                self.step_label,
            )

    def get_valid_test_corpora(self) -> List[Path]:
        """Collect paths to all available valid/test corpora."""
        valid_corpora = []
        test_corpora = []
        if self.valid_data_step is not None:
            valid_corpora = []
            for dset in self.valid_data_step.dataset_list:
                infile = Path(
                    self.tmp_dir,
                    "valid.{}.{}.gz".format(dset, "-".join(self.languages)),
                )
                paste_files(
                    [Path(self.valid_data_step.output_dir, f"{dset}.{lang}.gz") for lang in self.languages],
                    infile,
                )
                valid_corpora.append(infile)
        if self.test_data_step is not None:
            test_corpora = []
            for dset in self.test_data_step.dataset_list:
                infile = Path(self.tmp_dir, "test.{}.{}.gz".format(dset, "-".join(self.languages)))
                paste_files(
                    [Path(self.test_data_step.output_dir, f"{dset}.{lang}.gz") for lang in self.languages],
                    infile,
                )
                test_corpora.append(infile)
        return valid_corpora + test_corpora

    def register_categories(self) -> None:
        """Copy the categories from the previous step."""
        shutil.copy(self.prev_corpus_step.categories_path, self.categories_path)

    def get_command_targets(self) -> List[Path]:
        """One target file per each decontaminated dataset."""
        return [Path(self.output_dir, f"{dset}.{self.src_lang}.gz") for dset in self.dataset_list]

    def command(self, target_file: Path) -> None:
        """Invoke tools/decontaminate.py to remove training examples similar to ones in the valid/test files.

        We infer the input files (source-side, target-side) using the target_file.
        """
        dset_name = ".".join(target_file.stem.split(".")[:-1])

        # Combine the corpora before decontaminating
        infile = Path(self.tmp_dir, "input.{}.gz".format("-".join(self.languages)))
        outfile = Path(self.tmp_dir, "output.{}.gz".format("-".join(self.languages)))

        paste_files(
            [Path(self.input_dir, f"{dset_name}.{lang}.gz") for lang in self.languages],
            infile,
        )

        # Run decontamination
        args = Namespace(
            **{
                "mono": False,
                "input_file": str(infile),
                "output_file": str(outfile),
                "min_length": self.min_length,
                "test_files": ",".join([str(f) for f in self.get_valid_test_corpora()]),
            }
        )
        decontaminate_main(args)

        cut_file(
            outfile,
            [Path(self.output_dir, f"{dset_name}.{lang}.gz") for lang in self.languages],
        )
