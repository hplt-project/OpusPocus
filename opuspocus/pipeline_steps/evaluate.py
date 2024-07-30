from typing import List, Optional

import logging
import sacrebleu
from pathlib import Path

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep

logger = logging.getLogger(__name__)


@register_step("evaluate")
class EvaluateStep(OpusPocusStep):
    AVAILABLE_METRICS = sacrebleu.metrics.METRICS

    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        src_lang: str,
        tgt_lang: str,
        translated_corpus_step: CorpusStep,
        reference_corpus_step: CorpusStep,
        datasets: Optional[List[str]] = None,
        seed: int = 42,
        metrics: List[str] = ["BLEU", "CHRF"],
    ):
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            datasets=datasets,
            translated_corpus_step=translated_corpus_step,
            reference_corpus_step=reference_corpus_step,
            seed=seed,
            metrics=metrics,
        )
        for metric in self.metrics:
            if metric not in self.AVAILABLE_METRICS:
                raise ValueError(
                    "Unknown metric: {}.\n".format(metric)
                    + "Supported metrics: {}".format(",".join(self.AVAILABLE_METRICS))
                )

    def init_step(self) -> None:
        super().init_step()
        if self.dataset is None:
            self.dataset = self.translated_step.dataset_list
        for dset in self.datasets:
            if dset not in self.translated_step.dataset_list:
                raise ValueError(
                    "Dataset {} is not registered in the {} categories.json".format(
                        dset, self.translated_step.step_label
                    )
                )
            if dset not in self.reference_step.dataset_list:
                raise ValueError(
                    "Dataset {} is not registered in the {} categories.json".format(
                        dset, self.reference_step.step_label
                    )
                )

    @property
    def translated_step(self) -> OpusPocusStep:
        return self.dependencies["translated_corpus_step"]

    @property
    def reference_step(self) -> OpusPocusStep:
        return self.dependencies["reference_corpus_step"]

    @property
    def languages(self) -> List[str]:
        return [self.src_lang, self.tgt_lang]

    def get_command_targets(self) -> List[Path]:
        return [
            Path(self.output_dir, "{}.{}.txt".format(metric, dset))
            for dset in self.datasets
            for metric in self.metrics
        ]

    def command(self, target_file: Path) -> None:
        metric_label = target_file.stem.split(".")[0]
        dset = target_file.stem.split(".")[1:]
        metric = self.AVAILABLE_METRICS[metric_label]()

        sys = [
            line.rstrip("\n")
            for line in open(
                Path(
                    self.translated_step.output_dir,
                    "{}.{}.gz".format(dset, self.tgt_lang),
                ),
                "r",
            ).readlines()
        ]
        # TODO: multi-reference support
        ref = [
            line.rstrip("\n")
            for line in open(
                Path(
                    self.reference_step.output_dir,
                    "{}.{}.gz".format(dset, self.tgt_lang),
                ),
                "r",
            ).readlines()
        ]
        with open(target_file, "w") as fh:
            print(metric.corpus_score(sys, [ref]), file=fh)
            print(metric.get_signature(), file=fh)