import logging
import os
import shutil
import signal
import subprocess
import sys
from pathlib import Path
from typing import List

from attrs import Attribute, define, field, validators

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.pipeline_steps.train_model import TrainModelStep
from opuspocus.runner_resources import RunnerResources
from opuspocus.utils import open_file, read_shard, save_filestream

logger = logging.getLogger(__name__)


@register_step("translate")
@define(kw_only=True)
class TranslateCorpusStep(CorpusStep):
    """Class implementing dataset translation using a provided NMT model."""

    model_step: TrainModelStep = field()

    marian_dir: Path = field(converter=Path)
    beam_size: int = field(default=4, validator=validators.gt(0))
    model_suffix: str = field(default="best-chrf")

    @marian_dir.validator
    def _path_exists(self, _: str, value: Path) -> None:
        if not value.exists():
            err_msg = f"Provided path ({value}) does not exist."
            raise FileNotFoundError(err_msg)

    @model_step.validator
    def _inherited_from_train_model_step(self, attribute: Attribute, value: TrainModelStep) -> None:
        if not issubclass(type(value), TrainModelStep):
            err_msg = f"{attribute.name} value must contain a class instance that inherits from TrainModelStep"
            raise TypeError(err_msg)

    @marian_dir.default
    def _inherit_marian_dir_from_train_model(self) -> Path:
        return self.model_step.marian_dir

    @property
    def model_config_path(self) -> Path:
        """Location of the training config file."""
        return Path(f"{self.model_step.model_path}.{self.model_suffix}.npz.decoder.yml")

    def register_categories(self) -> None:
        shutil.copy(self.prev_corpus_step.categories_path, self.categories_path)

    def infer_input(self, tgt_file: Path) -> Path:
        """Infer the input files (including sharing if enabled) given a target_file."""
        tgt_filename_stem_split = tgt_file.stem.split(".")

        lang_idx = -1  # non-sharded
        if tgt_filename_stem_split[-2] == "gz":
            lang_idx = -3  # sharded

        # Constuct the source-side filename
        src_filename_stem_split = tgt_filename_stem_split
        src_filename_stem_split[lang_idx] = self.src_lang
        src_filename = ".".join([*src_filename_stem_split, "gz"])
        src_file = Path(tgt_file.parent, src_filename)

        if src_file.exists():
            return src_file

        if tgt_filename_stem_split[-2] == "gz":
            # Write the relevant shard lines into the source-side file

            # TODO(varisd): right now, we create the source-side shards and the
            #   "copy" of their respective input_file is created by a merge in
            #   the command_postprocess. Ideally in the future, we would like
            #   to hardling the input source-side file instead.

            shard_idx = int(tgt_filename_stem_split[-1])
            input_filename = ".".join(src_filename_stem_split[:-1])
            shard_lines = read_shard(
                Path(self.input_dir, input_filename),
                self.prev_corpus_step.line_index_dict[input_filename],
                shard_idx * self.shard_size,
                self.shard_size,
            )
            with open_file(src_file, "w") as fh:
                for line in shard_lines:
                    print(line, end="", file=fh)
        else:
            # Hardlink the source-side corpus
            src_file.hardlink_to(Path(self.input_dir, src_filename))

        return src_file

    def get_command_targets(self) -> List[Path]:
        """One file per each translated dataset.

        If shard_size > 1, return a target_file for each translated output dataset shard instead.
        The shards will be merged at the end of the main_task execution in the CorpusStep.main_task_postprocess()
        method.
        """
        if self.shard_size is not None:
            return [
                shard_file_path
                for dset in self.dataset_list
                for shard_file_path in self.infer_dataset_output_shard_path_list(f"{dset}.{self.tgt_lang}.gz")
            ]
        return [Path(self.output_dir, f"{dset}.{self.tgt_lang}.gz") for dset in self.dataset_list]

    def command(self, target_file: Path) -> None:
        """Invoke Marian's decode program to translate the input dataset.

        The input file for the provided target_file is infered using TranslateStep.infer_input() method
        """
        env = os.environ
        n_cpus = env[RunnerResources.get_env_name("cpus")]
        n_gpus = 0
        if RunnerResources.get_env_name("gpus") in env:
            n_gpus = int(env[RunnerResources.get_env_name("gpus")])

        # Hardlink source file
        input_file = self.infer_input(target_file)

        # Prepare the command
        marian_path = Path(self.marian_dir, "build", "marian-decoder")
        cmd = [
            str(marian_path),
            "-c",
            str(self.model_config_path),
            "--data-threads",
            str(n_cpus),
            "-i",
            str(input_file),
            "--log",
            f"{self.log_dir}/{target_file.stem}.log",
            "-b",
            str(self.beam_size),
        ]

        # GPU option
        if n_gpus:
            cmd += ["--devices"] + [str(i) for i in range(n_gpus)]
        else:
            cmd += ["--cpu-threads", str(n_cpus)]

        # Execute the command
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=sys.stderr, env=env, text=True)

        # Propagate the termination signal to the child process
        def step_terminate_handler(signum, _):  # noqa: ANN001, ANN202
            logger.debug("Received signal %i, gracefully terminating Marian child process...", signum)
            proc.terminate()
            err_msg = f"{self.step_label}.command received signal {signum}. Terminating..."
            raise InterruptedError(err_msg)

        signal.signal(signal.SIGUSR1, step_terminate_handler)
        signal.signal(signal.SIGTERM, step_terminate_handler)

        save_filestream(input_stream=proc.stdout, output_file=target_file)

        # Check the return code
        rc = proc.poll()
        if rc:
            err_msg = f"Process {proc.pid} exited with non-zero value."
            raise Exception(err_msg)  # noqa: TRY002

    @property
    def default_resources(self) -> RunnerResources:
        return RunnerResources(gpus=1, mem="20g")
