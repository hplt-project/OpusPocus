import logging
from pathlib import Path
from typing import Any, List

import sacrebleu
from attrs import Factory, define, field, validators

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep
from opuspocus.utils import open_file

logger = logging.getLogger(__name__)


@register_step("evaluate")
@define(kw_only=True)
class EvaluateStep(OpusPocusStep):
    """Class implementing translation evaluation."""

    src_lang: str = field(validator=validators.instance_of(str))
    tgt_lang: str = field(validator=validators.instance_of(str))
    translated_corpus_step: CorpusStep = field()
    reference_corpus_step: CorpusStep = field()
    datasets: List[str] = field(factory=list)
    seed: int = field(default=42)
    metrics: List[str] = field(default=Factory(lambda: ["BLEU", "CHRF"]))

    _available_metrics = sacrebleu.metrics.METRICS

    @metrics.validator
    def _is_available(self, _: str, value: Any) -> None:  # noqa: ANN401
        for metric in value:
            if metric not in self._available_metrics:
                raise ValueError(
                    f"Unknown metric: {metric}.\n" + "Supported metrics: {}".format(",".join(self._available_metrics))
                )

    @translated_corpus_step.validator
    @reference_corpus_step.validator
    def _inherited_from_corpus_step(self, attribute: str, value: CorpusStep) -> None:
        # TODO(varisd): remove duplicate code (similar to corpus_step.py validator)
        if not issubclass(type(value), CorpusStep):
            err_msg = f"{attribute} value must contain class instance that inherits from CorpusStep."
            raise TypeError(err_msg)

    def init_step(self) -> None:
        super().init_step()
        if self.datasets is None:
            self.datasets = self.translated_step.dataset_list
        for dset in self.datasets:
            if dset not in self.translated_step.dataset_list:
                err_msg = f"Dataset {dset} is not registered in the {self.translated_step.step_label} categories.json."
                raise ValueError(err_msg)
            if dset not in self.reference_step.dataset_list:
                err_msg = f"Dataset {dset} is not registered in the {self.reference_step.step_label} categories.json."
                raise ValueError(err_msg)

    @property
    def translated_step(self) -> OpusPocusStep:
        """Attribute alias."""
        return self.translated_corpus_step

    @property
    def reference_step(self) -> OpusPocusStep:
        """Attribute alias."""
        return self.reference_corpus_step

    @property
    def languages(self) -> List[str]:
        return [self.src_lang, self.tgt_lang]

    def get_command_targets(self) -> List[Path]:
        return [Path(self.output_dir, f"{metric}.{dset}.txt") for dset in self.datasets for metric in self.metrics]

    def command(self, target_file: Path) -> None:
        metric_label = target_file.stem.split(".")[0]
        dset = ".".join(target_file.stem.split(".")[1:])
        metric = self._available_metrics[metric_label]()

        sys = [
            line.rstrip("\n")
            for line in open_file(
                Path(
                    self.translated_step.output_dir,
                    f"{dset}.{self.tgt_lang}.gz",
                ),
                "r",
            ).readlines()
        ]
        # TODO: multi-reference support
        ref = [
            line.rstrip("\n")
            for line in open_file(
                Path(
                    self.reference_step.output_dir,
                    f"{dset}.{self.tgt_lang}.gz",
                ),
                "r",
            ).readlines()
        ]
        with open_file(target_file, "w") as fh:
            print(metric.corpus_score(sys, [ref]), file=fh)
            print(metric.get_signature(), file=fh)
