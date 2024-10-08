import logging
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.pipeline_steps.generate_vocab import GenerateVocabStep
from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep
from opuspocus.utils import RunnerResources, concat_files, subprocess_wait

logger = logging.getLogger(__name__)

SLURM_RESUBMIT_TIME = 600  # resubmit N seconds before job finishes


@register_step("train_model")
class TrainModelStep(OpusPocusStep):
    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        marian_dir: Path,
        src_lang: str,
        tgt_lang: str,
        marian_config: Path,
        opustrainer_config: Path,
        vocab_step: GenerateVocabStep,
        train_corpus_step: CorpusStep,
        valid_corpus_step: CorpusStep,
        model_init_step: Optional["TrainModelStep"] = None,
        seed: int = 42,
        train_category: str = "clean",
        valid_dataset: str = "flores200.dev",
    ) -> None:
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            marian_dir=marian_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            marian_config=marian_config,
            opustrainer_config=opustrainer_config,
            vocab_step=vocab_step,
            train_corpus_step=train_corpus_step,
            valid_corpus_step=valid_corpus_step,
            model_init_step=model_init_step,
            seed=seed,
            train_category=train_category,
            valid_dataset=valid_dataset,
        )
        # TODO: check language compatibility

    def init_step(self) -> None:
        super().init_step()
        if self.valid_dataset not in self.valid_corpus_step.dataset_list:
            err_msg = (
                f"Dataset {self.valid_dataset} is not registered in {self.valid_corpus_step.step_label}"
                "categories.json"
            )
            raise ValueError(err_msg)

    @property
    def train_corpus_step(self) -> CorpusStep:
        return self.dependencies["train_corpus_step"]

    @property
    def valid_corpus_step(self) -> CorpusStep:
        return self.dependencies["valid_corpus_step"]

    @property
    def vocab_step(self) -> OpusPocusStep:
        return self.dependencies["vocab_step"]

    @property
    def input_dir(self) -> Path:
        return self.train_corpus_step.output_dir

    @property
    def train_datasets(self) -> List[str]:
        return self.train_corpus_step.category_mapping[self.train_category]

    @property
    def valid_data_dir(self) -> Path:
        return self.valid_corpus_step.output_dir

    @property
    def model_init_path(self) -> Path:
        if self.dependencies["model_init_step"] is not None:
            return self.dependencies["model_init_step"].model_path
        return None

    @property
    def vocab_path(self) -> Path:
        vocab_dir = self.vocab_step.output_dir

        # TODO: this should be fetched from the dependency in case that
        # file naming changes in the future
        vocab_path = Path(vocab_dir, f"model.{self.src_lang}-{self.tgt_lang}.spm")
        return vocab_path  # noqa: RET504

    @property
    def vocab_size(self) -> int:
        return self.vocab_step.vocab_size

    @property
    def model_path(self) -> Path:
        return Path(self.output_dir, "model.npz")

    @property
    def languages(self) -> List[str]:
        return [self.src_lang, self.tgt_lang]

    @property
    def langpair(self) -> str:
        return "-".join(self.languages)

    @property
    def default_resources(self) -> RunnerResources:
        return RunnerResources(gpus=1)

    def get_command_targets(self) -> List[Path]:
        return [self.model_path]

    def command(self, target_file: Path) -> None:
        model_path = target_file  # for better readability

        env = os.environ
        n_cpus = int(env[RunnerResources.get_env_name("cpus")])
        n_gpus = 0
        if RunnerResources.get_env_name("gpus") in env:
            n_gpus = int(env[RunnerResources.get_env_name("gpus")])

        # Prepare the command
        marian_path = Path(self.marian_dir, "bin", "marian")
        cmd = [
            str(marian_path),
            "-c",
            str(self.marian_config),
            "--seed",
            str(self.seed),
            "--data-threads",
            str(n_cpus),
            "--model",
            str(model_path),
            "--vocabs",
            str(self.vocab_path),
            str(self.vocab_path),
            "--dim-vocabs",
            str(self.vocab_size),
            "--tempdir",
            str(self.tmp_dir),
            "--valid-translation-output",
            f"{self.log_dir}/valid.out",
            "--log-level",
            "info",
            "--log",
            f"{self.log_dir}/train.log",
            "--valid-log",
            f"{self.log_dir}/valid.log",
        ]

        # Training data
        # TODO: Data concatenation should be removed when opustrainer support
        #       is added
        train_paths = [Path(self.tmp_dir, f"train.{lang}.gz") for lang in self.languages]
        if not all([p.exists() for p in train_paths]):  # noqa: C419
            for lang, output_file in zip(self.languages, train_paths):
                concat_files(
                    [Path(self.input_dir, f"{dset}.{lang}.gz") for dset in self.train_datasets],
                    output_file,
                )
        cmd += ["--train-sets"] + [str(p) for p in train_paths]

        # Validation data
        cmd += ["--valid-sets"] + [f"{self.valid_data_dir}/{self.valid_dataset}.{lang}.gz" for lang in self.languages]

        # GPU option
        if n_gpus:
            cmd += ["--devices"] + [str(i) for i in range(n_gpus)]
        else:
            cmd += ["--cpu-threads", str(n_cpus)]

        # Initial checkpoint option
        if self.model_init_path is not None:
            cmd += ["--pretrained-model", str(self.model_init_path)]

        # Execute the command
        proc = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr, env=env, text=True)

        # Propagate the termination signal to the child process
        def step_terminate_handler(signum, _):  # noqa: ANN001, ANN202
            logger.debug("Received signal %i, gracefully terminating Marian child process...", signum)
            proc.terminate()
            err_msg = f"{self.step_label}.command received signal {signum}. Terminating..."
            raise InterruptedError(err_msg)

        signal.signal(signal.SIGUSR1, step_terminate_handler)
        signal.signal(signal.SIGTERM, step_terminate_handler)
        subprocess_wait(proc)
