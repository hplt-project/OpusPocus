from typing import List, Optional

from pathlib import Path
import logging
import os
import shutil
import signal
import subprocess
import sys

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep
from opuspocus.pipeline_steps.train_model import TrainModelStep
from opuspocus.utils import RunnerResources, save_filestream, subprocess_wait

logger = logging.getLogger(__name__)


@register_step("translate")
class TranslateStep(CorpusStep):
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
        output_shard_size: Optional[int] = None,
        model_suffix: str = "best-chrf",
    ):
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
            output_shard_size=output_shard_size,
            model_suffix=model_suffix,
        )

    @property
    def model_step(self) -> OpusPocusStep:
        return self.dependencies["model_step"]

    @property
    def _inherits_sharded(self) -> bool:
        return True

    @property
    def model_config_path(self) -> Path:
        return Path(
            "{}.{}.npz.decoder.yml".format(
                self.model_step.model_path,
                self.model_suffix
            )
        )

    def register_categories(self) -> None:
        shutil.copy(self.prev_corpus_step.categories_path, self.categories_path)

    def infer_input(self, tgt_file: Path) -> Path:
        offset = 2
        if self.is_sharded:
            offset = 3
        tgt_filename = tgt_file.stem + tgt_file.suffix
        src_filename = ".".join(
            tgt_filename.split(".")[:-offset]
            + [self.src_lang]
            + tgt_filename.split(".")[-(offset - 1):]
        )
        if self.prev_corpus_step.is_sharded:
            src_file = Path(self.shard_dir, src_filename)
            if not src_file.exists():
                src_file.hardlink_to(
                    Path(self.input_shard_dir, src_filename)
                )
        else:
            src_file = Path(self.output_dir, src_filename)
            if not src_file.exists():
                src_file.hardlink_to(
                    Path(self.prev_corpus_step.output_dir, src_filename)
                )
        return src_file

    def get_command_targets(self) -> List[Path]:
        if self.is_sharded:
            targets = []
            for dset in self.dataset_list:
                dset_filename = "{}.{}.gz".format(dset, self.tgt_lang)
                targets.extend([
                    shard_file
                    for shard_file in self.get_shard_list(dset_filename)
                ])
            return targets
        return [
            Path(self.output_dir, "{}.{}.gz".format(dset, self.tgt_lang))
            for dset in self.dataset_list
        ]

    def command_preprocess(self) -> None:
        if self.is_sharded:
            shard_dict = {}
            for d_fname, s_list in self.prev_corpus_step.shard_index.items():
                s_fname_list = [f.stem + f.suffix for f in s_list]
                shard_dict[d_fname] = s_fname_list
                d_fname_target = ".".join(
                    d_fname.split(".")[:-2] + [self.tgt_lang, "gz"]
                )
                shard_dict[d_fname_target] = [
                    ".".join(
                        shard.split(".")[:-3]
                        + [self.tgt_lang]
                        + shard.split(".")[-2:]
                    ) for shard in s_fname_list
                ]
            self.save_shard_dict(shard_dict)

    def command(self, target_file: Path) -> None:
        env = os.environ
        n_cpus = env[RunnerResources.get_env_name("cpus")]
        n_gpus = 0
        if RunnerResources.get_env_name("gpus") in env:
            n_gpus = int(env[RunnerResources.get_env_name("gpus")])

        # Hardlink source file
        input_file = self.infer_input(target_file)

        # Prepare the command
        marian_path = Path(self.marian_dir, "bin", "marian-decoder")
        cmd = [
            str(marian_path),
            "-c",
            str(self.model_config_path),
            "--data-threads",
            str(n_cpus),
            "-i",
            str(input_file),
            "--log",
            "{}/{}.log".format(self.log_dir, target_file.stem),
            "-b",
            str(self.beam_size),
        ]

        # GPU option
        if n_gpus:
            cmd += ["--devices"] + [str(i) for i in range(n_gpus)]
        else:
            cmd += ["--cpu-threads", str(n_cpus)]

        # Execute the command
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
            env=env,
            text=True
        )

        def terminate_signal(signalnum, handler):
            logger.debug("Received SIGTERM, terminating child process...")
            proc.terminate()

        signal.signal(signal.SIGTERM, terminate_signal)

        save_filestream(input_stream=proc.stdout, output_file=target_file)

        # Check the return code
        rc = proc.poll()
        if rc:
            raise Exception(
                "Process {} exited with non-zero value.".format(proc.pid)
            )
