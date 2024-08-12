from typing import List, Optional

import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.utils import cut_filestream, RunnerResources

logger = logging.getLogger(__name__)


@register_step("clean")
class CleanCorpusStep(CorpusStep):
    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        previous_corpus_step: CorpusStep,
        python_venv_dir: Path,
        src_lang: str,
        tgt_lang: Optional[str] = None,
        output_shard_size: Optional[int] = None,
        opuscleaner_cmd: str = "opuscleaner-clean",
    ):
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            previous_corpus_step=previous_corpus_step,
            python_venv_dir=python_venv_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            output_shard_size=output_shard_size,
            opuscleaner_cmd=opuscleaner_cmd,
        )

    def register_categories(self) -> None:
        """Create a dataset list using the datasets listed in categories.json file.

        OpusCleaner server app creates a categories.json file listing locally
        available datasets and their user-specified categorization.
        """
        shutil.copy(self.prev_corpus_step.categories_path, self.categories_path)

    def get_command_targets(self) -> List[Path]:
        return [Path(self.output_dir, "{}.{}.gz".format(dset, self.src_lang)) for dset in self.dataset_list]

    def command(self, target_file: Path) -> None:
        # TODO: use OpusCleaner Python API instead when available
        target_filename = target_file.stem + target_file.suffix
        dataset = ".".join(str(target_filename).split(".")[:-2])
        input_file = Path(self.input_dir, "{}.filters.json".format(dataset))

        opuscleaner_bin_path = Path(self.python_venv_dir, "bin", self.opuscleaner_cmd)
        if not input_file.exists():
            logger.info("%s file not found. Copying input corpora to output.", input_file)
            for lang in self.languages:
                Path(self.output_dir, "{}.{}.gz".format(dataset, lang)).hardlink_to(
                    Path(self.input_dir, "{}.{}.gz".format(dataset, lang))
                )
            return

        # Run OpusCleaner
        proc = subprocess.Popen(
            [
                str(opuscleaner_bin_path),
                str(input_file),
                "--parallel",
                os.environ[RunnerResources.get_env_name("cpus")],
                "-b",
                str(self.input_dir),
            ],
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
            env=os.environ,
            text=True,
        )

        # Get the correct order of languages
        languages = [file.split(".")[-2] for file in json.load(open(input_file, "r"))["files"]]
        # TODO(varisd): replace these asserts with something more clever
        for lang in self.languages:
            assert lang in languages
        for lang in languages:
            assert lang in self.languages

        # Split OpusCleaner output into files
        output_files = [Path(self.output_dir, "{}.{}.gz".format(dataset, lang)) for lang in languages]
        cut_filestream(input_stream=proc.stdout, output_files=output_files)

        # Check the return code
        rc = proc.poll()
        if rc:
            raise Exception("Process {} exited with non-zero value.".format(proc.pid))
