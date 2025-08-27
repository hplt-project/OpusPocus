import json
import logging
import os
import shutil
import signal
import subprocess
import sys
from pathlib import Path
from typing import List

from attrs import define, field

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.runner_resources import RunnerResources
from opuspocus.utils import cut_filestream

logger = logging.getLogger(__name__)


@register_step("clean")
@define(kw_only=True)
class CleanCorpusStep(CorpusStep):
    """Class implementing dataset cleaning using OpusCleaner."""

    opuscleaner_cmd: str = field(default="opuscleaner-clean")

    def register_categories(self) -> None:
        """Create a dataset list using the datasets listed in categories.json file.

        OpusCleaner server app creates a categories.json file listing locally
        available datasets and their user-specified categorization.
        """
        shutil.copy(self.prev_corpus_step.categories_path, self.categories_path)

    def get_command_targets(self) -> List[Path]:
        """One target file per each processed dataset."""
        return [Path(self.output_dir, f"{dset}.{self.src_lang}.gz") for dset in self.dataset_list]

    def command(self, target_file: Path) -> None:
        """Invoke OpusCleaner to process corpus based on the target_file.

        We infer the input corpus file, target-side corpus file and .filter.json file using the target_file.
        """
        # TODO: use OpusCleaner Python API instead when available
        target_filename = target_file.stem + target_file.suffix
        dataset = ".".join(str(target_filename).split(".")[:-2])
        input_file = Path(self.input_dir, f"{dataset}.filters.json")

        opuscleaner_cmd = "opuscleaner-clean"
        if not input_file.exists():
            logger.info("%s file not found. Copying input corpora to output.", input_file)
            for lang in self.languages:
                Path(self.output_dir, f"{dataset}.{lang}.gz").hardlink_to(Path(self.input_dir, f"{dataset}.{lang}.gz"))
            return

        # Run OpusCleaner
        proc = subprocess.Popen(
            [
                str(opuscleaner_cmd),
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

        # Propagate the termination signal to the child process
        def step_terminate_handler(signum, _):  # noqa: ANN001, ANN202
            logger.debug("Received signal %i, gracefully terminating OpusCleaner child process...", signum)
            proc.terminate()
            err_msg = f"{self.step_label}.command received signal {signum}. Terminating..."
            raise InterruptedError(err_msg)

        signal.signal(signal.SIGUSR1, step_terminate_handler)
        signal.signal(signal.SIGTERM, step_terminate_handler)

        # Get the correct order of languages
        languages = [file.split(".")[-2] for file in json.load(open(input_file))["files"]]  # noqa: PTH123, SIM115
        # TODO(varisd): replace these asserts with something more clever
        for lang in self.languages:
            assert lang in languages
        for lang in languages:
            assert lang in self.languages

        # Split OpusCleaner output into files
        output_files = [Path(self.output_dir, f"{dataset}.{lang}.gz") for lang in languages]
        cut_filestream(input_stream=proc.stdout, output_files=output_files)

        # Check the return code
        rc = proc.poll()
        if rc:
            err_msg = f"Process {proc.pid} exited with non-zero value."
            raise Exception(err_msg)  # noqa: TRY002
