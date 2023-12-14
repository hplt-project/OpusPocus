import logging
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

    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        raw_data_dir: Path,
        src_lang: str,
        tgt_lang: str = None,
        gzipped: bool = True,
        suffix: str = None,
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            raw_data_dir=raw_data_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            gzipped=gzipped,
            suffix=suffix
        )

    def init_dataset_list(self) -> None:
        """Extract the dataset names from the raw_data_dir.

        Use categories.json, if available. Otherwise, scan the input direcotory
        for corpus files.
        """
        import os
        import yaml

        categories_path = Path(self.raw_data_dir, self.categories_file)
        if categories_path.exists():
            import shutil

            logger.info(
                'OpusCleaner\'s categories.json found. Copying.'
            )
            shutil.copy(categories_path, self.categories_path)

            logger.info(
                'Copying datasets\' .filter.json files.'
            )            
            datasets = [
                d for d_list in self.category_mapping.values() for d in d_list
            ]
            for dset in datasets:
                filt = Path(self.raw_data_dir, '{}.filters.json'.format(dset))
                if not filt.exists():
                    FileNotFoundError(filt)
                shutil.copy(filt, Path(self.output_dir, filt.name))

            logger.info(
                'Creating links to the datasets\' corpora.'
            )
            for dset in datasets:
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
        else:
            logger.info(
                'categories.json not found. Scanning for datasets.'
            )
            for lang in self.languages:
                for corpus_path in self.raw_data_dir.glob('*.{}.gz'.format(lang)):
                    os.link(
                        corpus_path.resolve(),
                        Path(self.output_dir, corpus_path.name)
                    )
            datasets = [
                '.'.join(c.name.split('.')[:-2])
                for c in self.output_dir.glob('*.{}.gz'.format(self.src_lang))
            ]
        yaml.dump(datasets, open(self.dataset_list_path, 'w'))

    def get_command_str(self) -> str:
        return """#!/usr/bin/env bash
echo DONE > {state_file}
        """.format(state_file=str(Path(self.step_dir, self.state_file)))
