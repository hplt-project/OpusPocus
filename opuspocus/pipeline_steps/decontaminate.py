from typing import List, Optional


import gzip
import logging
import shutil
import subprocess
from argparse import Namespace
from pathlib import Path
from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.tools.decontaminate import main as decontaminate_main


logger = logging.getLogger(__name__)


@register_step('decontaminate')
class DecontaminateCorpusStep(CorpusStep):
    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        previous_corpus_step: CorpusStep,
        src_lang: str,
        tgt_lang: Optional[str] = None,
        valid_data_step: Optional[CorpusStep] = None,
        test_data_step: Optional[CorpusStep] = None,
        output_shard_size: Optional[int] = None,
        min_length: int = 25,
    ):
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            previous_corpus_step=previous_corpus_step,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            valid_data_step=valid_data_step,
            test_data_step=test_data_step,
            output_shard_size=output_shard_size,
            min_length=min_length,
        )
        if self.valid_step is None and self.test_step is None:
            logger.warn(
                'No valid_data_step or test_data_step was provided. '
                'Step {} will do notning when executed'.format(self.step_label)
            )

    @property
    def valid_step(self) -> CorpusStep:
        return self.dependencies['valid_data_step']

    @property
    def test_step(self) -> CorpusStep:
        return self.dependencies['test_data_step']

    def get_valid_test_corpora(self, language: str) -> List[Path]:
        valid_corpora = []
        test_corpora = []
        if self.valid_step is not None:
            valid_corpora = [
                Path(
                    self.valid_step.output_dir,
                    '{}.{}.gz'.format(dset, language)
                )
                for dset in self.valid_step.dataset_list
            ]
        if self.test_step is not None:
            test_corpora = [
                Path(
                    self.test_step.output_dir,
                    '{}.{}.gz'.format(dset, language)
                )
                for dset in self.test_step.dataset_list
            ]
        return valid_corpora + test_corpora

    def register_categories(self) -> None:
        shutil.copy(
            self.prev_corpus_step.categories_path,
            self.categories_path
        )

    def get_command_targets(self) -> List[Path]:
        return [
            Path(self.output_dir, '{}.{}.gz'.format(dset, self.src_lang))
            for dset in self.dataset_list
        ]

    def command(self, target_file: Path) -> None:
        dset_name = '.'.join(target_file.stem.split('.')[:-1])
        for lang in self.languages:
            args = Namespace(**{
                'mono': True,
                'input_file': '{}/{}.{}.gz'.format(
                    self.input_dir, dset_name, lang
                ),
                'output_file': '{}/{}.{}.gz'.format(
                    self.output_dir,dset_name, lang
                ),
                'min_length': self.min_length,
                'test_files': ','.join([
                    str(f) for f in self.get_valid_test_corpora(lang)
                ])
            })
            decontaminate_main(args)
