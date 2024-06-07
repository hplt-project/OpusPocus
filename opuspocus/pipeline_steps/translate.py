from typing import List, Optional

from pathlib import Path
import logging
import shutil

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep
from opuspocus.pipeline_steps.train_model import TrainModelStep


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
    def input_dir(self) -> Path:
        if self.prev_corpus_step.is_sharded:
            return self.prev_corpus_step.shard_dir
        return self.prev_corpus_step.output_dir

    @property
    def model_config_path(self) -> Path:
        return Path(
            self.model_step.model_path, "{}.npz.decoder.yml".format(self.model_suffix)
        )

    def register_categories(self) -> None:
        shutil.copy(self.prev_corpus_step.categories_path, self.categories_path)

    def get_command_targets(self) -> List[Path]:
        file_list = []
        for dset in self.dataset_list:
            src_filename = "{}.{}.gz".format(dset, self.src_lang)
            if self.prev_corpus_step.is_sharded:
                shard_stems = [
                    ".".join(shard.stem.split(".")[:-1] + [self.tgt_lang])
                    for shard in self.prev_corpus_step.get_shard_files(src_filename)
                ]
                file_list.extend(
                    [Path(self.shard_dir, stem + ".gz") for stem in shard_stems]
                )
            else:
                file_list.append(self.output_dir, "")

    def infer_input(self, target_file: Path) -> Path:
        filename = target_file.stem + target_file.suffix
        if self.prev_corpus_step.is_sharded:
            return Path(self.prev_corpus_step.shard_dir, filename)
        return Path(self.prev_corpus_step.output_dir, filename)

    def get_command_targets(self) -> List[Path]:
        if self.is_sharded:
            [
                Path(self.shard_dir, shard_filename)
                for dset in self.dataset_list
                for shard_filename in self.get_shard_list[dset]
            ]
        return [
            Path(self.output_dir, "{}.{}.gz".format(dset, self.tgt_lang))
            for dset in self.dataset_list
        ]

    def command(self, target_file: Path) -> None:
        env = os.environ()
        n_cpus = env[RunnerResources.get_env_name("cpus")]
        n_gpus = RunnerResources.n_gpus

        # Hardlink source file
        input_file = self.infer_input(target_file)

        # Prepare the command
        marian_path = Path(self.marian_dir, "bin", "marian-decoder")
        cmd = [
            str(marian_path),
            "-c",
            str(self.model_config_path),
            "-i",
            str(input_file),
            "-o",
            ">(pigz -c > {})".format(target_file),
            "--log",
            "{}/{}.log".format(self.log_dir, target_file.stem),
            "-b",
            self.beam_size,
        ]

        # Execute the command
        proc = subprocess.Popen(
            cmd, stdout=sys.stdout, stderr=sys.stderr, env=env, text=True
        )
        output, errors = proc.communicate()
        if proc.returncode or errors:
            raise Exception("Process {} exited with non-zero value.".format(proc.pid))
