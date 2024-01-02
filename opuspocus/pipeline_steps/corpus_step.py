from typing import Any, Dict, List, Optional, get_type_hints
from typing_extensions import TypedDict

import json
import logging
import yaml
from pathlib import Path

from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep
from opuspocus.command_utils import build_subprocess
from opuspocus.utils import print_indented


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
    categories_file = 'categories.json'
    shard_dirname = 'shards'

    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        src_lang: str,
        tgt_lang: Optional[str] = None,
        previous_corpus_step: Optional['CorpusStep'] = None,
        output_shard_size: Optional[int] = None,
        suffix: Optional[str] = None,
        **kwargs
    ):
        super().__init__(
            step=step,
            pipeline_dir=pipeline_dir,
            previous_corpus_step=previous_corpus_step,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            output_shard_size=output_shard_size,
            suffix=suffix,
            **kwargs,
        )

    @property
    def input_dir(self) -> Optional[Path]:
        if self.prev_corpus_step is not None:
            return self.prev_corpus_step.output_dir
        return None

    @property
    def is_sharded(self) -> bool:
        return self.output_shard_size is not None

    @property
    def shard_dir(self) -> Path:
        if self.is_sharded:
            return Path(self.output_dir, self.shard_dirname)
        return None

    @property
    def shard_size(self) -> int:
        """Shortcut for output_shard_size."""
        return self.output_shard_size

    def create_directories(self) -> None:
        super().create_directories()
        if self.shard_dir is not None:
            self.shard_dir.mkdir()

    @property
    def prev_corpus_step(self) -> 'CorpusStep':
        # Alternative to calling the prev corpus depencency
        return self.dependencies['previous_corpus_step']

    @property
    def categories_path(self) -> Path:
        return Path(self.output_dir, self.categories_file)

    @property
    def categories_dict(self) -> Optional[CategoriesDict]:
        if not self.categories_path.exists():
            return None
        return self.load_categories_dict()

    @property
    def categories(self) -> Optional[List[str]]:
        if self.categories_dict is None:
            return None
        return [cat['name'] for cat in self.categories_dict['categories']]

    @property
    def category_mapping(self) -> Optional[Dict[str, List[str]]]:
        if self.categories_dict is None:
            return None
        return self.categories_dict['mapping']

    @property
    def dataset_list(self) -> List[str]:
        return [
            dset for dset_list in self.category_mapping.values()
            for dset in dset_list
        ]

    # Loading and Saving abstractions
    # (if we want to change the file format in the future)
    def load_categories_dict(self) -> CategoriesDict:
        return json.load(open(self.categories_path, 'r'))

    def save_categories_dict(self, categories_dict: CategoriesDict) -> None:
        json.dump(categories_dict, open(self.categories_path, 'w'), indent=2)

    def init_step(self) -> None:
        # TODO: refactor opuscleaner_step.init_step to reduce code duplication
        self.state = self.load_state()
        if self.state is not None:
            if self.has_state('INITED'):
                logger.info('Step already initialized. Skipping...')
                return
            else:
                raise ValueError(
                    'Trying to initialize step in a {} state.'.format(self.state)
                )
        # Set state to incomplete until finished initializing.
        self.create_directories()
        self.set_state('INIT_INCOMPLETE')

        self.init_dependencies()
        self.init_categories_file()
        self.save_parameters()
        self.save_dependencies()
        self.create_command()

        # Initialize state
        logger.info('[{}.init] Step Initialized.'.format(self.step))
        self.set_state('INITED')

    def init_categories_file(self) -> None:
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
        if self.tgt_lang is not None:
            return [self.src_lang, self.tgt_lang]
        return [self.src_lang]

    @property
    def step_name(self) -> str:
        name = 's.{}'.format(self.step)
        if self.tgt_lang is not None:
            name += '.{}-{}'.format(self.src_lang, self.tgt_lang)
        else:
            name += '.{}'.format(self.src_lang)
        if self.suffix is not None:
            name += '.{}'.format(self.suffix)
        return name
   
    def _cmd_exit_str(self) -> str:
        """
        Check whether all the datasets files are present and are not empty.
        """
        sharding_str = ''
        if self.is_sharded:
            sharding_str = """
        # Shard the output for the future steps
        SHARD_DIR="{shard_dir}"
        zcat $dset_path \\
            | split -a4 -dl {shard_size} --filter='gzip -c > $FILE.gz' - $SHARD_DIR/$dset_file.

        [[ $dset_lines -ne $($zcat $SHARD_DIR/$dset_file.* | wc -l) ]] && ( \\
            echo "Original file and sum of shards $SHARD_DIR/$dset_file contain different number of lines" >&2 \\
            && exit 1
        )
            """.format(
                    shard_size=self.shard_size,
                    shard_dir=self.shard_dir,
                )

        return """# Sanity check: Check the dataset existence and whether
# they are not empty
OUTPUT_DIR="{outdir}"
for dset in {datasets}; do
    for lang in {languages}; do
        dset_file="$dset.$lang.gz"
        dset_path="$OUTPUT_DIR/$dset_file"
        [[ -e $dset_path ]] || ( \\
            echo "Dataset $dset_path does not exist." >&2 \\
            && exit 1 \\
        )

        dset_lines=$(zcat $dset_path | wc -l)
        [[ $dset_lines -eq 0 ]] && ( \\
            echo "Datset $dset_path is empty." >&2 \\
            && exit 1 \\
        )
        {sharding_str}
    done
done

# By default, return zero code.
exit 0
""".format(
            outdir=self.output_dir,
            datasets=' '.join(self.dataset_list),
            languages=' '.join(self.languages),
            sharding_str=sharding_str,
        )
