import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

from typing_extensions import TypedDict

from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep, StepState
from opuspocus.utils import concat_files, file_line_index, read_shard

logger = logging.getLogger(__name__)


# TODO(varisd): can we future-proof this against type changes in OpusCleaner?
class CategoryEntry(TypedDict):
    name: str


class CategoriesDict(TypedDict):
    categories: List[CategoryEntry]
    mapping: Dict[str, List[str]]


class CorpusStep(OpusPocusStep):
    """Base class for corpus-producing pipeline steps.

    This superclass provides additional functionality used by steps that create
    or modify corpora, i.e. corpus cleaning, corpus translation, etc.

    Compared to OpusPocusStep, it provides additional functionality, such as
    file sharding or indication of the corpora provided by the step at the end
    of its execution.
    """

    categories_file = "categories.json"

    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        src_lang: str,
        tgt_lang: Optional[str] = None,
        previous_corpus_step: Optional["CorpusStep"] = None,
        shard_size: Optional[int] = None,
        **kwargs,  # noqa: ANN003
    ) -> None:
        """Object initialization.

        Definition of common corpus step attributes such as corpora language,
        previous corpus step or sharding size.

        Args:
            step: string used for step class registration
            step_label: unique label of the step instance
            pipeline_dir: root directory of a pipeline
            src_lang: source-side language
            tgt_lang: target-side language
            prev_corpus_step: previous CorpusStep (being processed by the step)
            shard_size: number of lines per individual dataset shards
        """
        if src_lang is None:
            err_msg = "src_lang value cannot by NoneType."
            raise ValueError(err_msg)
        if shard_size is not None and shard_size <= 0:
            err_msg = f"shard_size must be a positive integer value (value: {shard_size})."
            raise ValueError(err_msg)
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            previous_corpus_step=previous_corpus_step,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            shard_size=shard_size,
            **kwargs,
        )

    @property
    def prev_corpus_step(self) -> "CorpusStep":
        """Shortcut to the previous corpus step dependency."""
        return self.dependencies["previous_corpus_step"]

    @property
    def input_dir(self) -> Optional[Path]:
        """Shortcut to the input directory (previous step's output)."""
        if self.prev_corpus_step is None:
            return None
        return self.prev_corpus_step.output_dir

    @property
    def line_index_dict(self) -> Dict[str, List[int]]:
        """Provides a list of file seek indices for each registered dataset.

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

    def read_shard_from_dataset_file(self, filename: str, start: int, shard_size: int) -> List[str]:
        """Provides input by reading a part of an input (.prev_corpus_step)
        dataset corpus with regard to the output dataset shard.

        Args:
            filename: filename within a directory (without full path)
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

    def get_input_dataset_shard_path_list(self, filename: str) -> List[Path]:
        """Gets a list of shard files useful for prallel data processing.

        The shard filenames are computed based on the size of the respective
        input .prev_corpus_step dataset. The CorpusStep suporting sharding
        should implement .command in a way that fetches the relevant shard
        input using the .prev_corpus_step.line_index_dict method.

        Args:
            filename: dataset's filename

        Returns:
            List of full paths to the respective dataset output shards.
        """
        # Get list of indexed filenames based on the prev_corpus_step dataset size
        assert self.prev_corpus_step is not None, (
            f"({self.step_label}).previous_corpus_step is not specified. Sharding "
            f"of the {filename} dataset in the ({self.step_label}).output_dir is "
            f"determined using ({self.step_label}).previvous_corpus_step.output_dir "
            f"{filename} file"
        )
        n_lines = len(self.prev_corpus_step.line_index_dict[filename])
        n_shards = n_lines // self.shard_size
        if n_lines % self.shard_size != 0:
            n_shards += 1
        return [Path(self.tmp_dir, f"{filename}.{i}.gz") for i in range(n_shards)]

    def command_postprocess(self) -> None:
        """By default merges all output dataset shards (if shard_size is not
        None) into the single dataset file.
        """
        super().command_postprocess()

        # By default, all dataset files must be available after a successful
        # step command execution. If not, there must be sharded output that can
        # be concatenated into the target file
        for f_name in self.dataset_filename_list:
            target_file = Path(self.output_dir, f_name)
            if not target_file.exists():
                concat_files(self.get_input_dataset_shard_path_list(f_name), target_file)

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

    # Loading and Saving abstractions
    # (if we want to change the file format in the future)
    def load_categories_dict(self) -> CategoriesDict:
        """Load categories.json file."""
        return json.load(open(self.categories_path))  # noqa: PTH123, SIM115

    def save_categories_dict(self, categories_dict: CategoriesDict) -> None:
        """Save the categories dict into categories.json.

        TODO: add syntax checking for the categories_dict parameter
        """
        json.dump(categories_dict, open(self.categories_path, "w"), indent=2)  # noqa: PTH123, SIM115

    def init_step(self) -> None:
        """Step initialization method.

        Duplicate code of OpusPocusStep.init_step with the additional
        categories.json initialization.
        """
        # TODO: refactor opuscleaner_step.init_step to reduce code duplication
        if self.state is not None:
            if self.has_state(StepState.INITED):
                logger.info("Step already initialized. Skipping...")
                return
            else:  # noqa: RET505
                err_msg = f"Trying to initialize step in a {self.state} state."
                raise ValueError(err_msg)
        # Set state to incomplete until finished initializing.
        self.create_directories()
        self.state = StepState.INIT_INCOMPLETE

        self.init_dependencies()
        self.init_categories_file()
        self.save_parameters()
        self.save_dependencies()
        self.create_command()

        # Initialize state
        logger.info("[%s] Step Initialized.", self.step_label)
        self.state = StepState.INITED

    def init_categories_file(self) -> None:
        """Initialize the categories.json file."""
        self.register_categories()
        if not self.categories_path.exists():
            err_msg = (
                f"{self.categories_file} not found after initialization. Perhaps there is an issue "
                "with the register_categories derived method implementation?"
            )
            raise FileNotFoundError(err_msg)

    def register_categories(self) -> None:
        """Step-specific code for listing corpora available in the step output.
        Produces categories.json
        """
        NotImplementedError()  # noqa: PLW0133

    @property
    def languages(self) -> List[str]:
        """Provide the corpora lanugages."""
        if self.tgt_lang is not None:
            return [self.src_lang, self.tgt_lang]
        return [self.src_lang]
