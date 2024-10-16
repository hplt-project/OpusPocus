import argparse
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.pipeline_steps.generate_vocab import GenerateVocabStep
from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep
from opuspocus.tools import opustrainer_trainer
from opuspocus.utils import RunnerResources, paste_files

logger = logging.getLogger(__name__)

SLURM_RESUBMIT_TIME = 600  # resubmit N seconds before job finishes
DEFAULT_MODIFIERS = [{"UpperCase": 0.01}, {"TitleCase": 0.01}]


@register_step("train_model")
class TrainModelStep(OpusPocusStep):
    opustrainer_config_file = "opustrainer.config.yml"
    marian_config_file = "marian.config.yml"

    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        marian_dir: Path,
        src_lang: str,
        tgt_lang: str,
        marian_config: Path,
        vocab_step: GenerateVocabStep,
        train_corpus_step: CorpusStep,
        valid_corpus_step: CorpusStep,
        model_init_step: Optional["TrainModelStep"] = None,
        opustrainer_config: Optional[Path] = None,
        seed: int = 42,
        max_epochs: Optional[int] = None,
        train_categories: Optional[List[str]] = None,
        train_category_ratios: Optional[List[float]] = None,
        train_modifiers: List[Dict[str, Any]] = DEFAULT_MODIFIERS,
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
            vocab_step=vocab_step,
            train_corpus_step=train_corpus_step,
            valid_corpus_step=valid_corpus_step,
            model_init_step=model_init_step,
            opustrainer_config=opustrainer_config,
            seed=seed,
            max_epochs=max_epochs,
            train_categories=train_categories,
            train_category_ratios=train_category_ratios,
            train_modifiers=train_modifiers,
            valid_dataset=valid_dataset,
        )
        if opustrainer_config is None and (train_categories is None or train_category_ratios is None):
            err_msg = (
                "'train_categories' and 'train_category_ratios' must be provided if 'opustrainer_config' is NoneType."
            )
            raise ValueError(err_msg)

        if (train_categories is not None) != (train_category_ratios is not None):
            err_msg = "'train_categories' and 'train_category_ratios' must be either both specified or both None."
            raise ValueError(err_msg)
        if train_categories is not None and train_category_ratios is not None:
            if len(train_categories) != len(train_category_ratios):
                err_msg = "'train_categories' and 'train_category_ratios' lists must have same number of elements."
                raise ValueError(err_msg)
            if sum(train_category_ratios) != 1.0:
                err_msg = "'train_category_ratios' list elements must sum up to 1."
                raise ValueError(err_msg)

        if max_epochs is not None and max_epochs < 0:
            err_msg = "'max_epochs' must be a positive integer."
            raise ValueError(err_msg)

    def init_step(self) -> None:
        super().init_step()
        if self.valid_dataset not in self.valid_corpus_step.dataset_list:
            err_msg = (
                f"Dataset {self.valid_dataset} is not registered in {self.valid_corpus_step.step_label}"
                "categories.json"
            )
            raise ValueError(err_msg)

        for cat in self.train_categories:
            if cat not in self.train_corpus_step.categories:
                err_msg = (
                    f"One of the 'train_categories' ({cat}) is not part of the 'train_corpus_step' categories "
                    f"({self.train_corpus_step.categories})."
                )
                raise ValueError(err_msg)

        for cat in self.train_corpus_step.categories:
            if len(self.train_corpus_step.category_mapping[cat]) > 1:
                logger.warning(
                    "Category %s contains more than one corpus. Only the first corpus in the list will be "
                    "considered (%s). To aggregate corpora from individual categories, use GatherCorpusStep prior to "
                    "training.",
                    cat,
                    self.train_corpus_step.category_mapping[cat][0],
                )

        # Prepare OpusTrainer config
        if self.opustrainer_config is not None:
            shutil.copy(self.opustrainer_config, self.opustrainer_config_path)
        else:
            with self.opustrainer_config_path.open("w") as fh:
                yaml.dump(self._generate_opustrainer_config(), fh)

        # Prepare Marian NMT config
        with self.marian_config_path.open("w") as fh:
            yaml.dump(self._generate_marian_config(), fh)

    def _generate_opustrainer_config(self) -> Dict[str, Any]:
        config = {"seed": self.seed, "stages": ["main"], "modifiers": self.train_modifiers, "num_fields": 2}

        categories = self.train_categories
        if categories is None:
            categories = self.train_corpus_step.categories
        config["datasets"] = {
            cat: str(Path(self.tmp_dir, str(self.train_corpus_step.category_mapping[cat][0]) + ".tsv.gz"))
            for cat in categories
        }

        n_epochs = "inf"
        if self.max_epochs is not None:
            n_epochs = f"{self.max_epochs}"

        ratios = self.train_category_ratios
        if ratios is None:
            ratios = [1.0 / len(categories)] * len(categories)
        config["main"] = [f"{cat} {ratio:f}" for cat, ratio in zip(categories, ratios)] + [
            f"until {self.train_categories[0]} {n_epochs}"
        ]
        return config

    @property
    def opustrainer_config_path(self) -> Path:
        return Path(self.step_dir, self.opustrainer_config_file)

    def _generate_marian_config(self) -> Dict[str, Any]:
        with self.marian_config.open("r") as fh:
            config = yaml.safe_load(fh)
        config["seed"] = self.seed
        config["model"] = str(self.model_path)
        config["vocabs"] = [str(self.vocab_path), str(self.vocab_path)]
        config["dim-vocabs"] = self.vocab_size
        config["tempdir"] = str(self.tmp_dir)
        config["valid-translation-output"] = f"{self.log_dir}/valid.out"
        config["log-level"] = "info"
        config["log"] = f"{self.log_dir}/train.log"
        config["valid-log"] = f"{self.log_dir}/valid.log"
        config["valid-sets"] = f"{self.valid_data_dir}/{self.valid_dataset}.tsv.gz"
        return config

    @property
    def opustrainer_config_dict(self) -> Dict[str, Any]:
        with self.opustrainer_config_path.open("r") as fh:
            return yaml.safe_load(fh)

    @property
    def opustrainer_dataset_paths(self) -> List[Path]:
        return [Path(dset) for dset in self.opustrainer_config_dict["datasets"].values()]

    @property
    def valid_dataset_path(self) -> Path:
        return Path(f"{self.valid_data_dir}/{self.valid_dataset}.tsv.gz")

    @property
    def marian_config_path(self) -> Path:
        return Path(self.step_dir, self.marian_config_file)

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
    def valid_data_dir(self) -> Path:
        return self.valid_corpus_step.output_dir

    @property
    def model_init_path(self) -> Path:
        if self.dependencies["model_init_step"] is not None:
            return self.dependencies["model_init_step"].model_path
        return None

    @property
    def vocab_path(self) -> Path:
        return self.vocab_step.vocab_path

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
        return [Path(str(self.model_path) + ".DONE")]

    def command(self, target_file: Path) -> None:
        env = os.environ
        n_cpus = int(env[RunnerResources.get_env_name("cpus")])
        n_gpus = 0
        if RunnerResources.get_env_name("gpus") in env:
            n_gpus = int(env[RunnerResources.get_env_name("gpus")])

        trainer = [
            f"{self.marian_dir}/build/marian",
            "-t",
            "stdin",
            "-c",
            f"{self.marian_config_path}",
            "--data-threads",
            f"{n_cpus}",
        ]

        # GPU option
        if n_gpus:
            trainer += ["--devices"] + [str(i) for i in range(n_gpus)]
        else:
            trainer += ["--cpu-threads", str(n_cpus)]

        # Initial checkpoint option
        if self.model_init_path is not None:
            trainer = ["--pretrained-model", self.model_init_path]

        # Prepare training datasets TSV files
        for dset_path in self.opustrainer_dataset_paths:
            if dset_path.exists():
                continue
            logger.info("Creating dataset %s...", dset_path)
            dset = ".".join(dset_path.stem.split(".")[:-1])
            in_files = [Path(self.train_corpus_step.output_dir, f"{dset}.{lang}.gz") for lang in self.languages]
            paste_files(in_files, dset_path)

        # Prepare valid dataset TSV file
        if not self.valid_dataset_path.exists():
            logger.info("Creating dataset %s...", self.valid_dataset_path)
            dset = ".".join(self.valid_dataset_path.stem.split(".")[:-1])
            in_files = [Path(self.valid_corpus_step.output_dir, f"{dset}.{lang}.gz") for lang in self.languages]
            paste_files(in_files, self.valid_dataset_path)

        args = argparse.Namespace(
            **{
                "config": str(self.opustrainer_config_path),
                "state": None,
                "sync": False,
                "temporary_directory": str(self.tmp_dir),
                "do_not_resume": False,
                "shuffle": True,
                "trainer": trainer,
            }
        )
        rc = opustrainer_trainer.main(args)
        if rc != 0:
            err_msg = f"Opustrainer exited with non-zero return code ({rc})"
            raise subprocess.SubprocessError(err_msg)

        target_file.touch()  # touch target file so we know that the training finished
