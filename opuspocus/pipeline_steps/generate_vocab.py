import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep
from opuspocus.utils import (
    RunnerResources,
    concat_files,
    decompress_file,
    subprocess_wait,
)

logger = logging.getLogger(__name__)


@register_step("generate_vocab")
class GenerateVocabStep(OpusPocusStep):
    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        src_lang: str,
        tgt_lang: str,
        datasets: List[str],
        marian_dir: Path,
        corpus_step: CorpusStep,
        seed: int = 42,
        vocab_size: int = 64000,
    ) -> None:
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            datasets=datasets,
            marian_dir=marian_dir,
            corpus_step=corpus_step,
            seed=seed,
            vocab_size=vocab_size,
        )

    def init_step(self) -> None:
        super().init_step()
        for dset in self.datasets:
            if dset not in self.corpus_step.dataset_list:
                logger.debug(
                    "Step %s categories.json: %s",
                    self.corpus_step.step_label,
                    self.corpus_step.categories_dict,
                )
                raise ValueError(  # noqa: TRY003
                    f"Dataset {dset} is not registered in the {self.corpus_step.step_label} categories.json"  # noqa: EM102
                )

    @property
    def corpus_step(self) -> OpusPocusStep:
        return self.dependencies["corpus_step"]

    @property
    def input_dir(self) -> Path:
        return self.corpus_step.output_dir

    @property
    def languages(self) -> List[str]:
        return [self.src_lang, self.tgt_lang]

    def get_command_targets(self) -> List[Path]:
        return [Path(self.output_dir, f"model.{self.src_lang}-{self.tgt_lang}.spm")]

    def command(self, target_file: Path) -> None:
        spm_train_path = Path(self.marian_dir, "bin", "spm_train")
        model_prefix = f"{self.output_dir}/{target_file.stem}"
        n_cpus = int(os.environ[RunnerResources.get_env_name("cpus")])

        train_concat_gz = Path(self.tmp_dir, "train_concat.gz")
        train_concat = Path(self.tmp_dir, train_concat_gz.stem)
        concat_files(
            [Path(self.input_dir, f"{dset}.{lang}.gz") for dset in self.datasets for lang in self.languages],
            train_concat_gz,
        )
        decompress_file(train_concat_gz, train_concat)

        # Train subword model
        # TODO: make this Unix non-exclusive
        cmd = [
            str(spm_train_path),
            f"--random_seed={self.seed}",
            "--bos_id=-1",
            "--eos_id=0",
            "--unk_id=1",
            f"--model_prefix={model_prefix}",
            f"--vocab_size={self.vocab_size}",
            f"--input={str(train_concat)}",
            "--input_sentence_size=10000000",
            "--shuffle_input_sentence=true",
            "--train_extremely_large_corpus",
            "--byte_fallback",
            f"--num_threads={n_cpus}",
        ]
        proc = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr, env=os.environ, text=True)
        subprocess_wait(proc)

        # Rename the output file
        shutil.move(model_prefix + ".model", model_prefix + ".spm")

        for suffix in ["spm", "vocab"]:
            Path(
                self.output_dir,
                f"model.{self.tgt_lang}-{self.src_lang}.{suffix}",
            ).symlink_to(Path(self.output_dir, target_file.stem + suffix))
