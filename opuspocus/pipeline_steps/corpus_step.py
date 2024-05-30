from typing import Any, Dict, List, Optional, get_type_hints
from typing_extensions import TypedDict

import gzip
import json
import logging
import yaml
from pathlib import Path

from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep, StepState
from opuspocus.utils import (
    file_to_shards,
    open_file,
    shards_to_file
)


logger = logging.getLogger(__name__)

# TODO: can we future-proof this against type changes in OpusCleaner?
CategoryEntry = TypedDict(
    'CategoryEntry',
    {
        'name': str
    }
)
CategoriesDict = TypedDict(
    'CategoriesDict',
    {
        'categories': List[CategoryEntry],
        'mapping': Dict[str, List[str]],
    }
)


class CorpusStep(OpusPocusStep):
    """Base class for corpus-producing pipeline steps.

    This superclass provides additional functionality used by steps that create
    or modify corpora, i.e. corpus cleaning, corpus translation, etc.

    Compared to OpusPocusStep, it provides additional functionality, such as
    input sharding or indication of the corpora provided by the step at the end
    of its execution.
    Moving this common functionality from the corpus processing steps into
    a single superclass reduces code duplicity.
    """
    categories_file = 'categories.json'
    shard_dirname = 'shards'
    shard_index_file = 'shard_index.yml'

    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        src_lang: str,
        tgt_lang: Optional[str] = None,
        previous_corpus_step: Optional['CorpusStep'] = None,
        output_shard_size: Optional[int] = None,
        **kwargs
    ):
        """Object initialization.

        Definition of common corpus step attributes such as corpora language,
        previous corpus step or sharding size.
        """
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            previous_corpus_step=previous_corpus_step,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            output_shard_size=output_shard_size,
            **kwargs,
        )
        if self._inherits_sharded and output_shard_size is not None:
            logger.warn(
                'Step {} always inherits sharding from its '
                'previous_corpus_step ({}). Ignoring the output_shard_size '
                'parameter...'
                .format(self.step, self.prev_corpus_step.step_label)
            )
            if self.prev_corpus_step.is_sharded:
                self.output_shard_size = self.previous_corpus_step.shard_size

    @property
    def prev_corpus_step(self) -> 'CorpusStep':
        """Shortcut to the previous corpus step dependency."""
        return self.dependencies['previous_corpus_step']

    @property
    def input_dir(self) -> Optional[Path]:
        """Shortcut to the input directory (previous step's output)."""
        if self.prev_corpus_step is None:
            return None
        return self.prev_corpus_step.output_dir

    @property
    def input_shard_dir(self) -> Optional[Path]:
        if self.prev_corpus_step is None:
            return None
        return self.prev_corpus_step.shard_dir

    @property
    def _inherits_sharded(self) -> bool:
        """TODO: This should be implemented as an interface/subclass.
        Something along corpus steps that can/cannot process sharded
        prev_corpus_step steps.
        """
        return False

    @property
    def is_sharded(self) -> bool:
        """Indicates whether the step output is sharded."""
        return self.output_shard_size is not None

    @property
    def shard_dir(self) -> Path:
        """Path to the sharded output directory."""
        if self.is_sharded:
            return Path(self.output_dir, self.shard_dirname)
        return None

    @property
    def shard_size(self) -> int:
        """Syntactic sugar for output_shard_size."""
        return self.output_shard_size

    @property
    def shard_index(self) -> Optional[Dict[str, List[Path]]]:
        if not self.is_sharded:
            return []
        shard_dict = yaml.safe_load(
            open(Path(self.shard_dir, self.shard_index_file), 'r')
        )
        return {
            k: [f for f in v]
            for k, v in shard_dict.items()
        }

    def save_shard_index(self, shard_dict: Dict[str, List[str]]) -> None:
        assert self.is_sharded
        yaml.dump(
            shard_dict,
            open(Path(self.shard_dir, self.shard_index), 'w')
        )

    def get_shard_list(self, dset_filename: str) -> List[Path]:
        return self.shard_map(dset_filename)

    def command_postprocess(self) -> None:
        """TODO"""
        if self.is_sharded and not self._inherits_sharded:
            # Shard Output
            shard_map = {}
            for dset in self.dataset_list:
                for lang in self.languages:
                    dset_path = Path(
                        self.output_dir, '{}.{}.gz'.format(dset, lang)
                    )
                    dset_filename = dset_path.stem + dset_path.suffix
                    shard_map[dset_filename] = file_to_shards(
                        file_path=dset_path,
                        shard_dir=self.shard_dir,
                        shard_size=self.shard_size,
                    )
            self.save_shard_map(shard_map)
        elif self._inherits_sharded:
            Path(self.shard_dir, self.shard_index).hardlink_to(
                Path(
                    self.prev_corpus_step.shard_dir,
                    self.prev_corpus_step.shard_index
                ).resolve()
            )
            # Merge Shards
            for dset in self.dataset_list:
                for lang in self.languages:
                    dset_file_path = Path(
                        self.output_dir, '{}.{}.gz'.format(dset, lang)
                    )
                    dset_filename = dset_file_path.stem + dset_file_path.suffix
                    shards_to_file(
                        self.get_shard_list(dset_filename),
                        self.shard_dir,
                        dset_file_path
                    )

    def create_directories(self) -> None:
        """Create the internal directory structure.

        If the output is sharded, additional directory for sharded output
        is created.
        """
        super().create_directories()
        if self.shard_dir is not None:
            self.shard_dir.mkdir()

    @property
    def categories_path(self) -> Path:
        """Full path to the categories.json."""
        return Path(self.output_dir, self.categories_file)

    @property
    def categories_dict(self) -> Optional[CategoriesDict]:
        """Shortcut for the categories.json contents."""
        if not self.categories_path.exists():
            return None
        return self.load_categories_dict()

    @property
    def categories(self) -> Optional[List[str]]:
        """Shortcut for the categories list in categories.json."""
        if self.categories_dict is None:
            return None
        return [cat['name'] for cat in self.categories_dict['categories']]

    @property
    def category_mapping(self) -> Optional[Dict[str, List[str]]]:
        """Return the mapping between the categories and the list of corpora
        in a give category.
        """
        if self.categories_dict is None:
            return None
        return self.categories_dict['mapping']

    @property
    def dataset_list(self) -> List[str]:
        """Return the list of step datasets (indicated by categories.json)."""
        return [
            dset for dset_list in self.category_mapping.values()
            for dset in dset_list
        ]

    # Loading and Saving abstractions
    # (if we want to change the file format in the future)
    def load_categories_dict(self) -> CategoriesDict:
        """Load categories.json file."""
        return json.load(open(self.categories_path, 'r'))

    def save_categories_dict(self, categories_dict: CategoriesDict) -> None:
        """Save the categories dict into categories.json.

        TODO: add syntax checking for the categories_dict parameter
        """
        json.dump(categories_dict, open(self.categories_path, 'w'), indent=2)

    def init_step(self) -> None:
        """Step initialization method.

        Duplicate code of OpusPocusStep.init_step with the additional
        categories.json initialization.
        """
        # TODO: refactor opuscleaner_step.init_step to reduce code duplication
        self.state = self.load_state()
        if self.state is not None:
            if self.has_state(StepState.INITED):
                logger.info('Step already initialized. Skipping...')
                return
            else:
                raise ValueError(
                    'Trying to initialize step in a {} state.'.format(self.state)
                )
        # Set state to incomplete until finished initializing.
        self.create_directories()
        self.set_state(StepState.INIT_INCOMPLETE)

        self.init_dependencies()
        self.init_categories_file()
        self.save_parameters()
        self.save_dependencies()
        self.create_command()

        # Initialize state
        logger.info('[{}] Step Initialized.'.format(self.step_label))
        self.set_state(StepState.INITED)

    def init_categories_file(self) -> None:
        """Initialize the categories.json file."""
        self.register_categories()
        if not self.categories_path.exists():
            raise FileNotFoundError(
                '{} not found after initialization. Perhaps there is an issue '
                'with the register_categories derived method implementation? '
                ''.format(self.categories_file)
            )

    def register_categories(self) -> None:
        """Step-specific code for listing corpora available in the step output.
        Produces categories.json
        """
        NotImplementedError()

    @property
    def languages(self) -> List[str]:
        """Provide the corpora lanugages."""
        if self.tgt_lang is not None:
            return [self.src_lang, self.tgt_lang]
        return [self.src_lang]
