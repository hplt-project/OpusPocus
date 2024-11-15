from pathlib import Path
from typing import List

import pytest
from attrs import define, field

from opuspocus import pipeline_steps
from opuspocus.pipeline_steps import StepState, build_step, register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.runners.debug import DebugRunner
from opuspocus.utils import count_lines, open_file, read_shard

# TODO(varisd): test categories.json load/save
# TODO(varisd): stuff related to the abstract methods (e.g. creating
#   categories.json, etc) should be generalized to be tested with each
#   CorpusStep implementation

N_LANGUAGES_MONO = 1
N_LANGUAGES_BI = 2


@register_step("foo_corpus")
@define(kw_only=True)
class FooCorpusStep(CorpusStep):
    """Mock that copies the input dataset files into the .output_dir, or copies
    the dataset files from the .prev_corpus_step .output_dir
    """

    dataset_files: List[Path] = field(factory=list)

    _categories: frozenset[str] = ("foo", "bar")

    @dataset_files.validator
    def files_are_gzipped(self, _, value: List[Path]) -> None:
        if value is not None:
            for file in value:
                assert file.suffix == ".gz"

    def register_categories(self) -> None:
        """Register categories based on the presence of .prev_corpus_step."""
        if self.prev_corpus_step is not None:
            self.categories_path.hardlink_to(self.prev_corpus_step.categories_path)
            return

        assert self.dataset_files is not None
        cat_dict = {}
        cat_dict["categories"] = [{"name": cat} for cat in self._categories]
        cat_dict["mapping"] = {cat: [] for cat in self._categories}

        dset = ".".join(self.dataset_files[0].stem.split(".")[:-1])
        cat_dict["mapping"][self._categories[0]] = [dset]
        self.save_categories_dict(cat_dict)

    def get_command_targets(self) -> List[Path]:
        """Create targets."""
        if self.shard_size is not None and self.prev_corpus_step is not None:
            return [
                shard
                for f_name in self.dataset_filename_list
                for shard in self.infer_dataset_output_shard_path_list(f_name)
            ]
        return [Path(self.output_dir, f_name) for f_name in self.dataset_filename_list]

    def command(self, target_file: Path) -> None:
        """Process command, either normally or using the file shards."""
        target_filename_stem_split = target_file.stem.split(".")
        if target_filename_stem_split[-2] == "gz":
            assert self.prev_corpus_step is not None
            with open_file(target_file, "w") as fh:
                idx = int(target_filename_stem_split[-1])
                input_filename = ".".join(target_filename_stem_split[:-1])
                shard_lines = read_shard(
                    Path(self.input_dir, input_filename),
                    self.prev_corpus_step.line_index_dict[input_filename],
                    idx * self.shard_size,
                    self.shard_size,
                )
                for line in shard_lines:
                    print(line, end="", file=fh)
        else:
            assert self.dataset_files is not None
            lang = target_filename_stem_split[-1]
            src_files = self.dataset_files
            if lang == src_files[0].stem.split(".")[-1]:
                target_file.hardlink_to(src_files[0])
            elif lang == src_files[1].stem.split(".")[-1]:
                target_file.hardlink_to(src_files[1])
            else:
                pytest.fail("Unknown target_file languages")


@pytest.fixture()
def train_data_parallel_tiny_dataset(train_data_parallel_tiny):
    """Get the name of the mock dataset for testing."""
    assert train_data_parallel_tiny[0].suffix == ".gz"
    return ".".join(train_data_parallel_tiny[0].stem.split(".")[:-1])


@pytest.fixture(params=["monolingual", "bilingual"])
def foo_corpus_languages(request, languages):
    """List of languages based on the version of tested corpus."""
    if request.param == "monolingual":
        return [languages[0]]
    return languages


@pytest.mark.parametrize(
    ("foo_languages", "shard_size"), [(("en", "fr"), "-1"), (("en", "fr"), "0"), (("null", "fr"), "1")]
)
def test_foo_corpus_invalid_values(foo_languages, shard_size, train_data_parallel_tiny, tmp_path_factory):
    """Test invalid langauge combination or shard_size values."""
    pipeline_steps.STEP_INSTANCE_REGISTRY = {}
    shard_size = None if shard_size == "null" else int(shard_size)

    pipeline_dir = tmp_path_factory.mktemp("foo.mock.{}".format("-".join(foo_languages)))
    src_lang = None if foo_languages[0] == "null" else foo_languages[0]
    tgt_lang = foo_languages[1]

    exception_type = ValueError
    if src_lang is None:
        exception_type = TypeError

    with pytest.raises(exception_type):
        build_step(
            step="foo_corpus",
            step_label="foo.test",
            pipeline_dir=pipeline_dir,
            **{
                "dataset_files": train_data_parallel_tiny,
                "src_lang": src_lang,
                "tgt_lang": tgt_lang,
                "prev_corpus_step": None,
                "shard_size": shard_size,
            },
        )


@pytest.fixture()
def foo_corpus_step_inited(foo_corpus_languages, train_data_parallel_tiny, tmp_path_factory):
    """Create and initialize the mock corpus."""
    pipeline_steps.STEP_INSTANCE_REGISTRY = {}
    pipeline_dir = tmp_path_factory.mktemp("foo.mock.{}".format("-".join(foo_corpus_languages)))

    src_lang = foo_corpus_languages[0]
    tgt_lang = None
    if len(foo_corpus_languages) == N_LANGUAGES_BI:
        tgt_lang = foo_corpus_languages[1]
    step = build_step(
        step="foo_corpus",
        step_label="foo.test",
        pipeline_dir=pipeline_dir,
        **{
            "dataset_files": train_data_parallel_tiny,
            "src_lang": src_lang,
            "tgt_lang": tgt_lang,
            "prev_corpus_step": None,
            "shard_size": None,
        },
    )
    step.init_step()
    return step


@pytest.fixture(params=["1", "3", "dataset", "dataset+1"])
def foo_shard_size(request, foo_corpus_step_inited):
    """Provides sharding test examples."""
    dset_size = count_lines(foo_corpus_step_inited.dataset_files[0])
    if request.param == "dataset":
        return dset_size
    if request.param == "dataset+1":
        return dset_size + 1
    return int(request.param)


@pytest.fixture()
def bar_corpus_step_inited(foo_shard_size, foo_corpus_step_inited):
    """Create and initialize the follow up mock corpus step."""
    step = build_step(
        step="foo_corpus",
        step_label="bar.test",
        pipeline_dir=foo_corpus_step_inited.pipeline_dir,
        **{
            "dataset_files": None,
            "src_lang": foo_corpus_step_inited.src_lang,
            "tgt_lang": foo_corpus_step_inited.tgt_lang,
            "prev_corpus_step": foo_corpus_step_inited,
            "shard_size": foo_shard_size,
        },
    )
    step.init_step()
    return step


@pytest.fixture(params=["foo_corpus", "bar_corpus"])
def corpus_step_inited(request, foo_corpus_step_inited, bar_corpus_step_inited):
    """Fixture wrapper. Returns a respective corpus step based on the param
    value.
    """
    if request.param == "foo_corpus":
        return foo_corpus_step_inited
    if request.param == "bar_corpus":
        return bar_corpus_step_inited
    pytest.fail("Unknown corpus label")


@pytest.fixture(params=["foo_corpus", "bar_corpus"])
def corpus_step_done(
    request,
    foo_corpus_step_inited,
    bar_corpus_step_inited,
):
    """Run the respective corpus steps and return a finished version."""
    if request.param == "foo_corpus":
        runner = DebugRunner("debug", foo_corpus_step_inited.pipeline_dir)
        runner.submit_step(foo_corpus_step_inited)
        return foo_corpus_step_inited
    if request.param == "bar_corpus":
        runner = DebugRunner("debug", bar_corpus_step_inited.pipeline_dir)
        runner.submit_step(bar_corpus_step_inited)
        return bar_corpus_step_inited
    pytest.fail("Unknown corpus label")


def test_corpus_step_inited(corpus_step_inited):
    """Test whether the initialization was successful."""
    assert corpus_step_inited.state == StepState.INITED


def test_output_filename_format(corpus_step_inited):
    """Test the correctness of the filename format."""
    for f_name in corpus_step_inited.dataset_filename_list:
        f_name_split = f_name.split(".")
        assert f_name_split[-1] == "gz"
        assert f_name_split[-2] in corpus_step_inited.languages


def test_corpus_step_inited_categories_file_exists(corpus_step_inited):
    """Test the presents of categories.json."""
    assert corpus_step_inited.categories_path.exists


def test_corpus_step_inited_categories(corpus_step_inited):
    """Test the correctness of categories in categories.json."""
    ref_sorted = sorted(corpus_step_inited._categories)  # noqa: SLF001
    hyp_sorted = sorted(corpus_step_inited.categories)
    assert hyp_sorted == ref_sorted


def test_corpus_step_inited_mapping(corpus_step_inited, train_data_parallel_tiny_dataset):
    """Test the correctness of the dataset mapping in categories.json."""
    category = corpus_step_inited._categories[0]  # noqa: SLF001
    ref_sorted = sorted([train_data_parallel_tiny_dataset])
    hyp_sorted = sorted(corpus_step_inited.category_mapping[category])
    assert hyp_sorted == ref_sorted


def test_corpus_step_inited_dataset_list(corpus_step_inited, train_data_parallel_tiny_dataset):
    """Test whether the list of registered datasets is correct."""
    ref_sorted = sorted([train_data_parallel_tiny_dataset])
    hyp_sorted = sorted(corpus_step_inited.dataset_list)
    assert hyp_sorted == ref_sorted


def test_foo_corpus_step_inited_line_index_fail(foo_corpus_step_inited):
    """Test whether the seek line indexing fails with not DONE step."""
    with pytest.raises(AssertionError):
        # if this does not fail, it should at least return None value
        assert foo_corpus_step_inited.line_index_dict is None


def test_foo_corpus_step_inited_shard_list_fail(foo_corpus_step_inited):
    """Test whether the sharding without .prev_corpus_step fails as
    expected.
    """
    for f_name in foo_corpus_step_inited.dataset_filename_list:
        with pytest.raises(AssertionError):
            foo_corpus_step_inited.infer_dataset_output_shard_path_list(f_name)


def test_languages(corpus_step_inited):
    """Test whether the corpus languages were set correctly."""
    languages = corpus_step_inited.languages
    if len(languages) == N_LANGUAGES_MONO:
        assert corpus_step_inited.tgt_lang is None
    elif len(languages) == N_LANGUAGES_BI:
        assert corpus_step_inited.tgt_lang == languages[1]
    else:
        pytest.fail("Unexpected number of languages")


def test_corpus_step_done(corpus_step_done):
    """Test whether the runner execution was successful."""
    assert corpus_step_done.state == StepState.DONE


def test_corpus_step_done_output_exist(corpus_step_done):
    """Test whether the step output exists."""
    for f_name in corpus_step_done.dataset_filename_list:
        assert Path(corpus_step_done.output_dir, f_name).exists()


def test_corpus_step_done_output_compressed(corpus_step_done):
    """Test whether the output files are compressed."""
    for f_name in corpus_step_done.dataset_filename_list:
        file_path = Path(corpus_step_done.output_dir, f_name)
        with file_path.open("rb") as fh:
            assert fh.read(2) == b"\x1f\x8b"


def test_dataset_filename_list_len(corpus_step_done):
    """Test whether the filename list has correct length."""
    n_dsets = len(corpus_step_done.dataset_list)
    n_langs = len(corpus_step_done.languages)
    list_length = len(corpus_step_done.dataset_filename_list)
    assert n_dsets * n_langs == list_length
