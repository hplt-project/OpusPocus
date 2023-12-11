from typing import Any, Dict, List, Optional, get_type_hints

import json
import logging
from pathlib import Path

from opuspocus.pipelines_steps.opuspocus_step import OpusPocusStep
from opuspocus.command_utils import build_subprocess
from opuspocus.utils import print_indented


logger = logging.getLogger(__name__)


class CorpusStep(OpusPocusStep):
    dataset_list_file = 'dataset_list.yaml'
    categories_file = 'categories.json'

    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        previous_corpus_step: 'CorpusStep',
        src_lang: str,
        tgt_lang: str = None,
        gzipped: bool = True,
        suffix: str = None,
        **kwargs
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            previous_corpus_step=previous_corpus_step,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            gzipped=gzipped,
            suffix=suffix,
            **kwargs,
        )
        self.input_dir = self.dependencies['previous_corpus_step'].output_dir

    @property
    def categories_path(self) -> Path:
        return Path(self.output_dir, self.categories_file)

    @property
    def categories_dict(self) -> Optional[Dict[str, Any]]:
        if not self.categories_path.exists():
            return None
        return json.load(open(self.categories_path, 'r'))

    @property
    def categories(self) -> Optional[List[str]]:
        if self.categories_dict is None:
            return None
        return [cat['name'] for cat in self.categories_dict['categories']]

    @property
    def category_mapping(self) -> Optional[Dict[str, List[str]]]:
        if self.categories_dict is None
            return None
        return self.categories_dict['mapping']

    @property
    def dataset_list_path(self) -> Path:
        return Path(self.output_dir, self.categories_file)

    @property
    def dataset_list(self) -> List[str]:
        dataset_list = yaml.safe_load(open(self.dataset_list_path, 'r'))
        return dataset_list

    def init_dataset_list(self) -> None:
        """Step-specific code for listing its available datasets."""
        NotImplementedError()

    @property
    def languages(self) -> List[str]:
        if self.tgt_lang is not None:
            return [self.src_lang, self.tgt_lang]
        return [self.src_lang]

    @property
    def step_name(self) -> str:
        name = 's.{}'.format(self.step)
        if tgt_lang is not None:
            name += '.{}-{}'.format(self.src_lang, self.tgt_lang)
        else:
            name += '.{}'.format(self.src_lang)
        if self.suffix is not None:
            name += '.{}'.format(self.suffix)
        return name
   
    def init_step(self) -> None:
        super().init_step()

        # Set state to incomplete again until finished initializing.
        self.set_state('INIT_INCOMPLETE')

        self.init_dataset_list()
        self.set_state('INITED')

    def _cmd_exit_str(self) -> str:
        """
        Check whether all the datasets files are present and are not empty.
        """

        return """# Sanity check: Check the dataset existence and whether
# they are not empty
for dset in {datasets}; do
    for lang in {languages}; do
        dset_path="$OUTPUT_DIR/$dset.$lang{gzip_suf}"
        [[ -e $dset_path ]] || ( \\
            echo "Dataset $dset_path does not exist." >&2 \\
            && exit 1 \\
        )

        [[ `zcat $dset_path | wc -l` -eq 0]] && ( \\
            echo "Datset $dset_path is empty." >&2 \\
            && exit 1 \\
        )
    done
done

# By default, return zero code.
exit 0
""".format(
            datasets=' '.join(self.dataset_list),
            languages=' '.join(self.languages),
            gzip_suf=('.gz' if self.gzipped else '')
        )
