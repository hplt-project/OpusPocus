from typing import Optional

import logging
import os
import shutil
from pathlib import Path
from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep


logger = logging.getLogger(__name__)


@register_step('raw')
class RawCorpusStep(CorpusStep):
    """Step used for transforming input data directories CorpusStep.

    The resulting CorpusStep can be later processed by other CorpusStep steps
    or used as an input for OpusPocusSteps.
    """
    default_category = 'clean'

    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        raw_data_dir: Path,
        src_lang: str,
        tgt_lang: str = None,
        output_shard_size: Optional[int] = None,
    ):
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            raw_data_dir=raw_data_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            output_shard_size=output_shard_size,
        )

    def register_categories(self) -> None:
        """Extract the dataset names from the raw_data_dir.

        Use categories.json, if available. Otherwise, scan the input direcotory
        for corpus files.
        """
        categories_path = Path(self.raw_data_dir, self.categories_file)
        if categories_path.exists():

            logger.info(
                'OpusCleaner\'s categories.json found. Copying.'
            )
            shutil.copy(categories_path, self.categories_path)
        else:
            logger.info(
                'categories.json not found. Scanning for datasets.'
            )
            categories_dict = {
                'categories': [{ 'name' : self.default_category }],
                'mapping': { self.default_category : [] }
            }
            # TODO: support other than .gz files
            for lang in self.languages:
                for corpus_path in self.raw_data_dir.glob('*.{}.gz'.format(lang)):
                    categories_dict['mapping'][self.default_category].append(
                        '.'.join(corpus_path.name.split('.')[:-2])
                    )
            self.save_categories_dict(categories_dict)
        for dset in self.dataset_list:
            # Hardlink corpus files
            for lang in self.languages:
                corpus_path = Path(
                    self.raw_data_dir, '{}.{}.gz'.format(dset, lang)
                )
                if not corpus_path.exists():
                    FileNotFoundError(corpus_path)
                os.link(
                    corpus_path.resolve(),
                    Path(self.output_dir, '{}.{}.gz'.format(dset, lang))
                )

            # Copy .filters.json files, if available
            filters_path = Path(
                self.raw_data_dir, '{}.filters.json'.format(dset)
            )
            if filters_path.exists():
                shutil.copy(
                    filters_path,
                    Path(self.output_dir, '{}.filters.json'.format(dset))
                )

    def command(
        self, input_file: Path = None, runner: 'OpusPocusRunner' = None
    ) -> None:
        pass
