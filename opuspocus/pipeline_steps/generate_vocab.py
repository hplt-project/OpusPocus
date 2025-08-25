import logging
import os
import shutil
import signal
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

from attrs import Attribute, define, field, validators

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep
from opuspocus.runner_resources import RunnerResources
from opuspocus.utils import (
    concat_files,
    decompress_file,
    subprocess_wait,
)

logger = logging.getLogger(__name__)


@register_step("generate_vocab")
@define(kw_only=True)
class GenerateVocabStep(OpusPocusStep):
    """Class implementing sentencepiece vocabulary generation."""

    corpus_step: CorpusStep = field()

    src_lang: str = field(validator=validators.instance_of(str))
    tgt_lang: str = field(validator=validators.optional(validators.instance_of(str)))
    marian_dir: Path = field(converter=Path)
    datasets: List[str] = field(factory=list)
    seed: int = field(default=42)
    vocab_size: int = field(default=64000, validator=validators.gt(0))

    @corpus_step.validator
    def _inherited_from_corpus_step(self, attribute: Attribute, value: CorpusStep) -> None:
        # TODO(varisd): remove duplicate code (similar to corpus_step.py validator)
        if not issubclass(type(value), CorpusStep):
            err_msg = f"{attribute.name} value must contain class instance that inherits from CorpusStep."
            raise TypeError(err_msg)

    @src_lang.default
    def _inherit_src_lang_from_corpus_step(self) -> Optional[str]:
        return self.corpus_step.src_lang

    @tgt_lang.default
    def _inherit_tgt_lang_from_corpus_step(self) -> Optional[str]:
        return self.corpus_step.tgt_lang

    def init_step(self) -> None:
        # we need to set default datasets value before calling super,
        # which saves the step parameters for later pipeline manipulation
        if not self.datasets:
            self.datasets = self.corpus_step.dataset_list

        super().init_step()
        for dset in self.datasets:
            if dset not in self.corpus_step.dataset_list:
                logger.debug(
                    "Step %s categories.json: %s",
                    self.corpus_step.step_label,
                    self.corpus_step.categories_dict,
                )
                err_msg = f"Dataset {dset} is not registered in the {self.corpus_step.step_label} categories.json."
                raise ValueError(err_msg)

    @property
    def input_dir(self) -> Path:
        """Previous step's output_dir."""
        return self.corpus_step.output_dir

    @property
    def vocab_path(self) -> Path:
        """Sentencepiece vocabulary model file path."""
        return Path(self.output_dir, f"model.{self.src_lang}-{self.tgt_lang}.spm")

    @property
    def languages(self) -> List[str]:
        """Provide the model's language list."""
        return [self.src_lang, self.tgt_lang]

    def get_command_targets(self) -> List[Path]:
        """The only target is the sentencepiece vocabulary."""
        return [self.vocab_path]

    def command(self, target_file: Path) -> None:
        """Invoke Marian's spm_train to create the sentencepiece model (and its related files)."""
        spm_train_path = Path(self.marian_dir, "build", "spm_train")
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
            f"--input={train_concat!s}",
            "--input_sentence_size=10000000",
            "--shuffle_input_sentence=true",
            "--train_extremely_large_corpus",
            "--byte_fallback",
            f"--num_threads={n_cpus}",
        ]
        proc = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr, env=os.environ, text=True)

        # Propagate the termination signal to the child process
        def step_terminate_handler(signum, _):  # noqa: ANN001, ANN202
            logger.debug("Received signal %i, gracefully terminating Marian child process...", signum)
            proc.terminate()
            err_msg = f"{self.step_label}.command received signal {signum}. Terminating..."
            raise InterruptedError(err_msg)

        signal.signal(signal.SIGUSR1, step_terminate_handler)
        signal.signal(signal.SIGTERM, step_terminate_handler)

        subprocess_wait(proc)

        # Rename the output file
        shutil.move(model_prefix + ".model", model_prefix + ".spm")

        for suffix in ["spm", "vocab"]:
            Path(
                self.output_dir,
                f"model.{self.tgt_lang}-{self.src_lang}.{suffix}",
            ).symlink_to(Path(self.output_dir, f"{target_file.stem}.{suffix}"))

    @property
    def default_resources(self) -> RunnerResources:
        return RunnerResources(mem="50g")
