import argparse
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List

import yaml
from attrs import Attribute, Factory, converters, define, field, validators

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.pipeline_steps.generate_vocab import GenerateVocabStep
from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep
from opuspocus.tools import opustrainer_trainer
from opuspocus.utils import RunnerResources, paste_files

logger = logging.getLogger(__name__)


@register_step("train_model")
@define(kw_only=True)
class TrainModelStep(OpusPocusStep):
    """Class implementing model training using OpusTrainer."""

    vocab_step: GenerateVocabStep = field()
    train_corpus_step: CorpusStep = field()
    valid_corpus_step: CorpusStep = field()
    model_init_step: "TrainModelStep" = field(default=None)

    src_lang: str = field(validator=validators.instance_of(str))
    tgt_lang: str = field(validator=validators.instance_of(str))
    marian_dir: Path = field(converter=Path)

    marian_config: Path = field(converter=Path)
    opustrainer_config: Path = field(default=None, converter=converters.optional(Path))
    seed: int = field(default=42)
    max_epochs: int = field(default=None, validator=validators.optional(validators.gt(0)))
    train_categories: List[str] = field(factory=list)
    train_category_ratios: List[float] = field(factory=list)
    train_modifiers: List[Dict[str, Any]] = field(default=Factory(lambda: [{"UpperCase": 0.01}, {"TitleCase": 0.01}]))
    valid_dataset: str = field(default="flores200.dev")

    _opustrainer_config_file = "opustrainer.config.yml"
    _marian_config_file = "marian.config.yml"

    @marian_dir.validator
    @marian_config.validator
    @opustrainer_config.validator
    def _path_exists(self, attribute: Attribute, value: Path) -> None:
        if value is None and attribute.name != "opustrainer_config":
            err_msg = f"`{attribute.name}` value must be type Path (NoneType was provided)."
            raise ValueError(err_msg)
        if value is not None and not value.exists():
            err_msg = f"Provided path ({value}) does not exist."
            raise FileNotFoundError(err_msg)

    @train_corpus_step.validator
    @valid_corpus_step.validator
    def _inherited_from_corpus_step(self, attribute: Attribute, value: CorpusStep) -> None:
        if not issubclass(type(value), CorpusStep):
            err_msg = f"{attribute.name} value must contain a class instance that inherits from CorpusStep."
            raise TypeError(err_msg)

    @vocab_step.validator
    def _inherited_from_vocab_step(self, attribute: Attribute, value: GenerateVocabStep) -> None:
        if not issubclass(type(value), GenerateVocabStep):
            err_msg = f"{attribute.name} value must contain a class instance that inherits from GenerateVocabStep."
            raise TypeError(err_msg)

    @model_init_step.validator
    def _is_none_or_inherited_from_train_model_step(self, attribute: Attribute, value: "TrainModelStep") -> None:
        if value is not None and not issubclass(type(value), type(self)):
            err_msg = (
                f"{attribute.name} value must contain NoneType or a class instance that inherits from TrainModelStep."
            )
            raise TypeError(err_msg)

    @src_lang.default
    def _inherit_src_lang_from_train_step(self) -> str:
        return self.train_corpus_step.src_lang

    @tgt_lang.default
    def _inherit_tgt_lang_from_train_step(self) -> str:
        return self.train_corpus_step.tgt_lang

    @marian_dir.default
    def _inherit_marian_dir_from_generate_vocab(self) -> Path:
        return self.vocab_step.marian_dir

    def __attrs_post_init__(self) -> None:
        if self.opustrainer_config is None and (self.train_categories is None or self.train_category_ratios is None):
            err_msg = (
                "'train_categories' and 'train_category_ratios' must be provided if 'opustrainer_config' is NoneType."
            )
            raise ValueError(err_msg)
        if (self.train_categories is not None) != (self.train_category_ratios is not None):
            err_msg = "'train_categories' and 'train_category_ratios' must be either both specified or both None."
            raise ValueError(err_msg)
        if self.train_categories is not None and self.train_category_ratios is not None:
            if len(self.train_categories) != len(self.train_category_ratios):
                err_msg = "'train_categories' and 'train_category_ratios' lists must have same number of elements."
                raise ValueError(err_msg)
            if sum(self.train_category_ratios) != 1.0:
                err_msg = "'train_category_ratios' list elements must sum up to 1."
                raise ValueError(err_msg)

    def init_step(self) -> None:
        """Check whether the train/valid datasets are present in the provided CorpusStep(s) and make copies of the
        provided config files or prepare the default ones.
        """
        super().init_step()
        if self.valid_dataset not in self.valid_corpus_step.dataset_list:
            err_msg = (
                f"Dataset {self.valid_dataset} is not registered in {self.valid_corpus_step.step_label} "
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
        """Generate OpusTrainer config file base on the provided TrainModelStep parameters."""
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

    def _generate_marian_config(self) -> Dict[str, Any]:
        """Generate the Marian training configuration file.

        Use the provided config file and replace relevant values with the TraimModelStep's context, i.e. vocabulary
        size, location, valid data location, etc.
        """
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
    def opustrainer_config_path(self) -> Path:
        """OpusTrainer config path."""
        return Path(self.step_dir, self._opustrainer_config_file)

    @property
    def marian_config_path(self) -> Path:
        """Marian config path."""
        return Path(self.step_dir, self._marian_config_file)

    @property
    def opustrainer_config_dict(self) -> Dict[str, Any]:
        """Contents of the OpusTrainer config file."""
        with self.opustrainer_config_path.open("r") as fh:
            return yaml.safe_load(fh)

    @property
    def opustrainer_dataset_paths(self) -> List[Path]:
        """List of training datasets in OpusTrainer config."""
        return [Path(dset) for dset in self.opustrainer_config_dict["datasets"].values()]

    @property
    def input_dir(self) -> Path:
        """Training corpora location."""
        return self.train_corpus_step.output_dir

    @property
    def valid_data_dir(self) -> Path:
        """Validation step's output directory."""
        return self.valid_corpus_step.output_dir

    @property
    def valid_dataset_path(self) -> Path:
        """Path to the validation dataset."""
        return Path(f"{self.valid_data_dir}", f"{self.valid_dataset}.tsv.gz")

    @property
    def model_init_path(self) -> Path:
        """Location of the model used for training initialization."""
        if self.model_init_step is not None:
            return self.model_init_step.model_path
        return None

    @property
    def vocab_path(self) -> Path:
        """VocabStep's vocabulary model file path."""
        return self.vocab_step.vocab_path

    @property
    def vocab_size(self) -> int:
        """Vocabulary size."""
        return self.vocab_step.vocab_size

    @property
    def model_path(self) -> Path:
        """Location of the output Marian model."""
        return Path(self.output_dir, "model.npz")

    @property
    def languages(self) -> List[str]:
        """List of languages."""
        return [self.src_lang, self.tgt_lang]

    @property
    def langpair(self) -> str:
        """Language pair string."""
        return "-".join(self.languages)

    def get_command_targets(self) -> List[Path]:
        """At the end of the TrainModelStep.command() execution, create a flag file that indicates end of training."""
        return [Path(str(self.model_path) + ".DONE")]

    def command(self, target_file: Path) -> None:
        """Invoke tools/opustrainer_train.py OpusTrainer wrapper to train a Marian model.

        Executes the following steps:
            1. Creates the trainer string for the OpusTrainer wrapper (setting training compute parameters for Marian).
            2. Combines the source and target corpora into single tab-separated temporary files.
            3. Executes the the OpusTrainer wrapper

        Creates a flag file (target_file) after the training successfully terminates.
        """
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

    @property
    def default_resources(self) -> RunnerResources:
        return RunnerResources(gpus=1)
