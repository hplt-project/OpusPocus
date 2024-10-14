import logging
import os
import shutil
import signal
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep
from opuspocus.pipeline_steps.train_model import TrainModelStep
from opuspocus.utils import RunnerResources, open_file, read_shard, save_filestream

logger = logging.getLogger(__name__)


@register_step("translate")
class TranslateCorpusStep(CorpusStep):
    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        marian_dir: Path,
        src_lang: str,
        tgt_lang: str,
        previous_corpus_step: CorpusStep,
        model_step: TrainModelStep,
        beam_size: int = 4,
        shard_size: Optional[int] = None,
        model_suffix: str = "best-chrf",
    ) -> None:
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            marian_dir=marian_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            previous_corpus_step=previous_corpus_step,
            model_step=model_step,
            beam_size=beam_size,
            shard_size=shard_size,
            model_suffix=model_suffix,
        )

    @property
    def model_step(self) -> OpusPocusStep:
        return self.dependencies["model_step"]

    @property
    def model_config_path(self) -> Path:
        return Path(f"{self.model_step.model_path}.{self.model_suffix}.npz.decoder.yml")

    def register_categories(self) -> None:
        shutil.copy(self.prev_corpus_step.categories_path, self.categories_path)

    def infer_input(self, tgt_file: Path) -> Path:
        tgt_filename_stem_split = tgt_file.stem.split(".")

        lang_idx = -1  # non-sharded
        if tgt_filename_stem_split[-2] == "gz":
            lang_idx = -3  # sharded

        # Constuct the source-side filename
        src_filename_stem_split = tgt_filename_stem_split
        src_filename_stem_split[lang_idx] = self.src_lang
        src_filename = ".".join([*src_filename_stem_split, "gz"])
        src_file = Path(tgt_file.parent, src_filename)

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
        if self.shard_size is not None:
            return [
                shard_file_path
                for dset in self.dataset_list
                for shard_file_path in self.get_input_dataset_shard_path_list(f"{dset}.{self.tgt_lang}.gz")
            ]
        return [Path(self.output_dir, f"{dset}.{self.tgt_lang}.gz") for dset in self.dataset_list]

    def command(self, target_file: Path) -> None:
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
