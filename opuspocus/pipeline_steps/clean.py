from typing import List, Optional

import gzip
import logging
import os
import shutil
import subprocess
from pathlib import Path

from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.utils import RunnerResources


logger = logging.getLogger(__name__)


@register_step('clean')
class CleanCorpusStep(CorpusStep):
    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        previous_corpus_step: CorpusStep,
        python_venv_dir: Path,
        src_lang: str,
        tgt_lang: str = None,
        output_shard_size: Optional[int] = None,
        opuscleaner_cmd: str = 'opuscleaner-clean',
    ):
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            previous_corpus_step=previous_corpus_step,
            python_venv_dir=python_venv_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            output_shard_size=output_shard_size,
            opuscleaner_cmd=opuscleaner_cmd,
        )

    def register_categories(self) -> None:
        """Create a dataset list using the datasets listed in categories.json file.

        OpusCleaner server app creates a categories.json file listing locally
        available datasets and their user-specified categorization.
        """
        shutil.copy(
            self.prev_corpus_step.categories_path,
            self.categories_path
        )

        # Sanity check: .filters.json files exist
        for dset in self.dataset_list:
            dset_filter_path = Path(
                self.input_dir, '{}.filters.json'.format(dset)
            )
            if not dset_filter_path.exists():
                raise FileNotFoundError(dset_filter_path)

    def get_input_file_list(self) -> List[Path]:
        return [
            Path(self.input_dir, '{}.filters.json'.format(dset))
            for dset in self.dataset_list
        ]

    def command(
        self,
        input_file: Path,
        runner: 'OpusPocusRunner' = None
    ) -> None:
        # TODO: use OpusCleaner Python API instead when available
        input_filename = input_file.stem + input_file.suffix

        dataset = '.'.join(str(input_filename).split('.')[:-2])
        opuscleaner_bin_path =  Path(
            self.python_venv_dir, 'bin', self.opuscleaner_cmd
        )

        # Run OpusCleaner
        proc = subprocess.Popen(
            [
                str(opuscleaner_bin_path),
                str(input_file),
                '--parallel',
                os.environ[RunnerResources.get_env_name('cpus')],
                '-b', str(self.input_dir)
            ],
            stdout=subprocess.PIPE,
            stderr=open(Path(self.log_dir, '{}.err'.format(dataset)), 'w'),
            env=os.environ,
            text=True
        )
        output, _ = proc.communicate()

        # Open Output Files
        output_src_path = Path(
            self.output_dir, '{}.{}.gz'.format(dataset, self.src_lang)
        )
        output_src_fh = gzip.open(output_src_path, 'wt')
        output_tgt_fh = None
        if self.tgt_lang is not None:
            output_tgt_path = Path(
                self.output_dir, '{}.{}.gz'.format(dataset, self.tgt_lang)
            )
            output_tgt_fh = gzip.open(output_tgt_path, 'wt')

        # Write Output
        for line in output:
            line = line.strip().split('\t')
            print(line[0], file=output_src_fh)
            if output_tgt_fh is not None:
                print(line[1], file=output_tgt_fh)
