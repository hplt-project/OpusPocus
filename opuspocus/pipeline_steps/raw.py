from typing import List, Optional

import gzip
import logging
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
        tgt_lang: Optional[str] = None,
        output_shard_size: Optional[int] = None,
        compressed: bool = True
    ):
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            raw_data_dir=raw_data_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            output_shard_size=output_shard_size,
            compressed=compressed
        )

    def register_categories(self) -> None:
        """Extract the dataset names from the raw_data_dir.

        Use categories.json, if available. Otherwise, scan the input direcotory
        for corpus files.
        """
        categories_path = Path(self.raw_data_dir, self.categories_file)
        if categories_path.exists():
            logger.info(
                '[{}] OpusCleaner\'s categories.json found. Copying.'
                .format(self.step_label)
            )
            shutil.copy(categories_path, self.categories_path)
        else:
            logger.info(
                '[{}] categories.json not found. Scanning for datasets.'
                .format(self.step_label)
            )
            categories_dict = {
                'categories': [{ 'name' : self.default_category }],
                'mapping': { self.default_category : [] }
            }
            suffix = '.{}'.format(self.src_lang)
            if self.compressed:
                suffix += '.gz'
            for corpus_path in self.raw_data_dir.glob('*{}'.format(suffix)):
                corpus_prefix = '.'.join(corpus_path.name.split('.')[:-1])
                if self.compressed:
                    corpus_prefix = '.'.join(corpus_path.name.split('.')[:-2])
                categories_dict['mapping'][self.default_category].append(corpus_prefix)
            self.save_categories_dict(categories_dict)

    def get_command_targets(self) -> List[Path]:
        return [
            Path(self.output_dir, '{}.{}.gz'.format(dset, lang))
            for dset in self.dataset_list for lang in self.languages
        ]

    def command(self, target_file: Path) -> None:
        # Hardlink compressed files
        target_filename = target_file.stem + target_file.suffix
        lang = target_file.stem.split('.')[-1]

        # Copy .filters.json files, if available
        # Only do this once (for src lang) in bilingual corpora
        if lang == self.src_lang:
            filters_filename = '.'.join(
                target_file.stem.split('.')[:-1] + ['filters.json']
            )
            filters_path = Path(self.raw_data_dir, filters_filename)
            if filters_path.exists():
                shutil.copy(
                    filters_path,
                    Path(self.output_dir, filters_filename)
                )

        if self.compressed:
            corpus_path = Path(self.raw_data_dir, target_filename)
            if not corpus_path.exists():
                raise FileNotFoundError(corpus_path)

            Path(
                self.output_dir, target_filename
            ).hardlink_to(corpus_path.resolve())
        else:
            corpus_path = Path(self.raw_data_dir, target_file.stem)
            if not corpus_path.exists():
                raise FileNotFoundError(corpus_path)

            compressed_path = Path(self.output_dir, target_filename)
            with open(corpus_path, 'r') as f_in:
                with gzip.open(compressed_path, 'wt') as f_out:
                    for line in f_in:
                        print(line, end='', file=f_out)
