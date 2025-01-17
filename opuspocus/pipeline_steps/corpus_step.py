import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

from attrs import Attribute, define, field, validators
from typing_extensions import TypedDict

from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep, StepState
from opuspocus.utils import clean_dir, concat_files, file_line_index, read_shard

logger = logging.getLogger(__name__)


# TODO(varisd): can we future-proof this against type changes in OpusCleaner?
class CategoryEntry(TypedDict):
    name: str


class CategoriesDict(TypedDict):
    categories: List[CategoryEntry]
    mapping: Dict[str, List[str]]


@define(kw_only=True)
class CorpusStep(OpusPocusStep):
    """Base class for corpus-producing pipeline steps.

    This superclass provides additional functionality used by steps that create
    or modify corpora, i.e. corpus cleaning, corpus translation, etc.

    Compared to OpusPocusStep, it provides additional functionality, such as
    file sharding or indication of the corpora provided by the step at the end
    of its execution.
    """

    prev_corpus_step: "CorpusStep" = field(default=None)

    src_lang: str = field(validator=validators.instance_of(str))
    tgt_lang: str = field(validator=validators.optional(validators.instance_of(str)))
    shard_size: int = field(validator=validators.optional(validators.gt(0)))

    _categories_file = "categories.json"

    @prev_corpus_step.validator
    def _none_or_inherited_from_corpus_step(self, attribute: Attribute, value: Optional["CorpusStep"]) -> None:
        if value is not None and not issubclass(type(value), CorpusStep):
            err_msg = f"{attribute.name} value must contain NoneType or a class instance that inherits from CorpusStep"
            raise ValueError(err_msg)

    @src_lang.default
    def _inherit_src_lang_from_prev_step(self) -> Optional[str]:
        if self.prev_corpus_step is not None:
            return self.prev_corpus_step.src_lang
        return None

    @tgt_lang.default
    def _inherit_tgt_lang_from_prev_step(self) -> Optional[str]:
        if self.prev_corpus_step is not None:
            return self.prev_corpus_step.tgt_lang
        return None

    @shard_size.default
    def _inherit_shard_size_from_prev_step(self) -> Optional[int]:
        if self.prev_corpus_step is not None:
            return self.prev_corpus_step.shard_size
        return None

    @property
    def input_dir(self) -> Optional[Path]:
        """Previous step's output directory."""
        if self.prev_corpus_step is None:
            return None
        return self.prev_corpus_step.output_dir

    @property
    def categories_path(self) -> Path:
        """Full path to the categories.json."""
        return Path(self.output_dir, self._categories_file)

    @property
    def categories_dict(self) -> Optional[CategoriesDict]:
        """Contents of the categories.json file."""
        if not self.categories_path.exists():
            return None
        return json.load(self.categories_path.open("r"))

    @property
    def categories(self) -> Optional[List[str]]:
        """List of categories in categories.json file."""
        if self.categories_dict is None:
            return None
        return [cat["name"] for cat in self.categories_dict["categories"]]

    @property
    def category_mapping(self) -> Optional[Dict[str, List[str]]]:
        """Return the mapping between the categories and the list of corpora
        in a give category.
        """
        if self.categories_dict is None:
            return None
        return self.categories_dict["mapping"]

    @property
    def dataset_list(self) -> List[str]:
        """Return the list of step datasets (indicated by categories.json)."""
        return [dset for dset_list in self.category_mapping.values() for dset in dset_list]

    @property
    def dataset_filename_list(self) -> List[str]:
        """Full list of all the output_dir dataset filenames."""
        return [f"{dset}.{lang}.gz" for dset in self.dataset_list for lang in self.languages]

    @property
    def line_index_dict(self) -> Dict[str, List[int]]:
        """Provide a list of file seek indices for each registered dataset.

        The indices can be used to seek shard inputs for the respective output shard files.

        Return:
            A dictionary with keys reflecting the .output_dir filenames
            and values containing the lists of seek indices indicating
            beginning of line in the respective files.
        """
        assert self.state == StepState.DONE, (
            f"{self.step_label}.output_dir dataset's line index can only be construceted "
            "after the step successfully finished execution."
        )

        idx_dict = {}
        for f_name in self.dataset_filename_list:
            idx_dict[f_name] = file_line_index(Path(self.output_dir, f_name))
        return idx_dict

    def main_task_postprocess(self) -> None:
        """By default, merge all sharded output datasets into the single dataset files."""
        super().main_task_postprocess()

        # By default, all dataset files must be available after a successful
        # step command execution. If not, there must be sharded output that can
        # be concatenated into the target file
        for f_name in self.dataset_filename_list:
            target_file = Path(self.output_dir, f_name)
            if not target_file.exists():
                concat_files(self.infer_dataset_output_shard_path_list(f_name), target_file)

    def read_shard_from_dataset_file(self, filename: str, start: int, shard_size: int) -> List[str]:
        """Provides input by reading a part of an input (CorpusStep.prev_corpus_step) dataset corpus with regard
        to the output dataset shard.

        Args:
            filename (str): filename within a directory (without full path)
            start (int): starting line in the dataset
            shard_size (int): number of returned lines

        Return:
            List of lines extracted from the filename.
        """
        if filename not in self.dataset_filename_list:
            dlist_str = " ".join(self.dataset_filename_list)
            err_msg = f"{filename} is not in the list of dataset files ({dlist_str})"
            raise ValueError(err_msg)

        file_path = Path(self.output_dir, filename)
        if not file_path.exists():
            err_msg = f"File {file_path} does not exists"
            raise FileNotFoundError(err_msg)

        return read_shard(file_path, self.line_index_dict[filename], start, shard_size)

    def infer_dataset_output_shard_path_list(self, filename: str) -> List[Path]:
        """Return a list of output shard file paths useful for parallel data processing.

        The output shard filenames are computed based on the size of the respective input CorpusStep.prev_corpus_step
        dataset. The CorpusStep suporting sharding should implement OpusPocusStep.command() in a way that fetches
        the relevant shard input using the CorpusStep.prev_corpus_step.line_index_dict method.

        Args:
            filename: dataset's filename

        Returns:
            List of full paths to the respective dataset output shards.
        """
        # Get list of indexed filenames based on the prev_corpus_step dataset size
        assert self.prev_corpus_step is not None, (
            f"No {self.step_label}.prev_corpus_step was provided. Sharding of the {filename} dataset "
            f"in the {self.step_label}.output_dir is determined using "
            f"{self.step_label}.previvous_corpus_step.output_dir {filename} file"
        )
        n_lines = len(self.prev_corpus_step.line_index_dict[filename])
        n_shards = n_lines // self.shard_size
        if n_lines % self.shard_size != 0:
            n_shards += 1
        return [Path(self.tmp_dir, f"{filename}.{i}.gz") for i in range(n_shards)]

    def save_categories_dict(self, categories_dict: CategoriesDict) -> None:
        """Save the categories dict into categories.json."""
        # TODO(varisd): add syntax checking for the categories_dict parameter
        json.dump(categories_dict, self.categories_path.open("w"), indent=2)

    def init_step(self) -> None:
        """Step initialization method.

        Duplicate code of OpusPocusStep.init_step with the additional
        categories.json initialization.
        """
        # TODO: refactor opuscleaner_step.init_step to reduce code duplication
        if self.state is StepState.DONE:
            logger.info("[%s] Step is in %s state. Skipping...", self.step_label, self.state)
            return
        if self.state is StepState.INIT_INCOMPLETE:
            logger.warning("[%s] Step is in %s state. Re-initializing...", self.step_label, self.state)
            clean_dir(self.step_dir)
        if self.state is not None:
            if self.has_state(StepState.INITED):
                logger.info("[%s] Step already initialized. Skipping...", self.step_label)
                return
            err_msg = f"Trying to initialize step ({self.step_label}) in a {self.state} state."
            raise ValueError(err_msg)
        # Set state to incomplete until finished initializing.
        self.create_directories()
        self.state = StepState.INIT_INCOMPLETE

        self.init_dependencies()
        self.init_categories_file()
        self.save_parameters()
        self.save_dependencies()
        self.create_cmd_file()

        # Initialize state
        logger.info("[%s] Step Initialized.", self.step_label)
        self.state = StepState.INITED

    def init_categories_file(self) -> None:
        """Initialize the categories.json file."""
        self.register_categories()
        if not self.categories_path.exists():
            err_msg = (
                f"{self._categories_file} not found after initialization. Perhaps there is an issue "
                "with the {self.__name__}.register_categories() derived method implementation?"
            )
            raise FileNotFoundError(err_msg)

    def register_categories(self) -> None:
        """Step-specific code for listing corpora available in the step output.
        Produces categories.json
        """
        raise NotImplementedError()

    @property
    def languages(self) -> List[str]:
        """List of the corpora lanugages."""
        if self.tgt_lang is not None:
            return [self.src_lang, self.tgt_lang]
        return [self.src_lang]
