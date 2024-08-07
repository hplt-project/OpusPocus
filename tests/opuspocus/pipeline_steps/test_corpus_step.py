import pytest

from typing import List, Optional
from pathlib import Path

import opuspocus.pipeline_steps as pipeline_steps
from opuspocus.pipeline_steps import build_step, register_step, StepState
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.runners.debug import DebugRunner
from opuspocus.utils import count_lines


@register_step("foo")
class FooCorpusStep(CorpusStep):
    """Mock that copies the input dataset files into the output_dir."""

    CATEGORIES = ["foo", "bar"]

    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        src_lang: str,
        tgt_lang: Optional[str] = None,
        previous_corpus_step: Optional[CorpusStep] = None,
        dataset_files: List[Path] = None,
        shard_size: Optional[int] = None,
    ):
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            dataset_files=dataset_files,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            previous_corpus_step=previous_corpus_step,
            shard_size=shard_size,
        )
        for file in self.dataset_files:
            assert file.suffix == ".gz"

    def register_categories(self) -> None:
        if self.prev_corpus_step is not None:
            self.categories_path.hardlink_to(
                self.prev_corpus_step.categories_path
            )
            return

        assert self.dataset_files is not None
        cat_dict = {}
        cat_dict["categories"] = [{"name": cat} for cat in self.CATEGORIES]
        cat_dict["mapping"] = {cat: [] for cat in self.CATEGORIES}

        dset = ".".join(self.dataset_files[0].stem.split(".")[:-1])
        cat_dict["mapping"][self.CATEGORIES[0]] = [dset]
        self.save_categories_dict(cat_dict)

    def get_command_targets(self) -> List[Path]:
        if self.shard_size is not None and self.prev_corpus_step is not None:
            return [
                shard for f_name in self.dataset_filename_list
                for shard in self.prev_corpus_step.get_dataset_filename_shard_list
            ]
        return [
            Path(self.output_dir, f_name)
            for f_name in self.dataset_filename_list
        ]

    def command(self, target_file: Path) -> None:
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
                    self.shard_size
                )
                for line in shard_lines:
                    print(line, file=fh)
        else:
            assert self.dataset_files is not None
            lang = target_filename_stem_split[-1]
            src_files = self.dataset_files
            if lang == src_files[0].stem.split(".")[-1]:
                target_file.hardlink_to(src_files[0])
            elif lang == src_files[1].stem.split(".")[-1]:
                target_file.hardlink_to(src_files[1])
            else:
                assert False


@pytest.fixture(scope="function")
def train_data_parallel_tiny_dataset(train_data_parallel_tiny):
    assert train_data_parallel_tiny[0].suffix == ".gz"
    return ".".join(train_data_parallel_tiny[0].stem.split(".")[:-1])


@pytest.fixture(scope="function", params=["monolingual", "bilingual"])
def foo_corpus_languages(request, languages):
    if request.param == "monolingual":
        return [languages[0]]
    return languages


@pytest.fixture(scope="function")
def foo_corpus_step_inited(
    foo_corpus_languages,
    train_data_parallel_tiny, tmp_path_factory
):
    setattr(pipeline_steps, "STEP_INSTANCE_REGISTRY", {})
    pipeline_dir = tmp_path_factory.mktemp(
        "foo.mock.{}".format("-".join(foo_corpus_languages))
    )

    src_lang = foo_corpus_languages[0]
    tgt_lang = None
    if len(foo_corpus_languages) == 2:
        tgt_lang = foo_corpus_languages[1]
    step = build_step(
        step="foo",
        step_label="foo.test",
        pipeline_dir=pipeline_dir,
        **{
            "dataset_files": train_data_parallel_tiny,
            "src_lang": src_lang,
            "tgt_lang": tgt_lang,
            "previous_corpus_step": None,
            "shard_size": None,
        }
    )
    step.init_step()
    return step


@pytest.mark.parametrize(
    "foo_languages,shard_size",
    [
        ("en,fr", "-1"), ("en,fr", "0"), ("null,fr", "null")
    ]
)
def test_foo_corpus_invalid_values(
    foo_languages, shard_size, train_data_parallel_tiny, tmp_path_factory
):
    setattr(pipeline_steps, "STEP_INSTANCE_REGISTRY", {})
    if shard_size == "null":
        shard_size = None
    else:
        shard_size = int(shard_size)

    foo_languages = foo_languages.split(",")
    pipeline_dir = tmp_path_factory.mktemp(
        "foo.mock.{}".format("-".join(foo_languages))
    )
    src_lang = None if foo_languages[0] == "null" else foo_languages[0]
    tgt_lang = foo_languages[1]
    with pytest.raises(ValueError):
        build_step(
            step="foo",
            step_label="foo.test",
            pipeline_dir=pipeline_dir,
            **{
                "dataset_files": train_data_parallel_tiny,
                "src_lang": src_lang,
                "tgt_lang": tgt_lang,
                "previous_corpus_step": None,
                "shard_size": shard_size,
            }
        )

# TODO(varisd): test categories.json load/save


def test_foo_corpus_step_inited(foo_corpus_step_inited):
    assert foo_corpus_step_inited.state == StepState.INITED


def test_foo_corpus_step_inited_categories_file_exists(foo_corpus_step_inited):
    assert foo_corpus_step_inited.categories_path.exists


def test_foo_corpus_step_inited_categories(foo_corpus_step_inited):
    ref_sorted = sorted(foo_corpus_step_inited.CATEGORIES)
    hyp_sorted = sorted(foo_corpus_step_inited.categories)
    assert hyp_sorted == ref_sorted


def test_foo_corpus_step_inited_mapping(
    foo_corpus_step_inited, train_data_parallel_tiny_dataset
):
    category = foo_corpus_step_inited.CATEGORIES[0]
    ref_sorted = sorted([train_data_parallel_tiny_dataset])
    hyp_sorted = sorted(foo_corpus_step_inited.category_mapping[category])
    assert hyp_sorted == ref_sorted


def test_foo_corpus_step_inited_dataset_list(
    foo_corpus_step_inited, train_data_parallel_tiny_dataset
):
    ref_sorted = sorted([train_data_parallel_tiny_dataset])
    hyp_sorted = sorted(foo_corpus_step_inited.dataset_list)


@pytest.fixture(scope="function")
def foo_corpus_step_done(foo_corpus_step_inited):
    runner = DebugRunner("debug", foo_corpus_step_inited.pipeline_dir)
    runner.submit_step(foo_corpus_step_inited)
    return foo_corpus_step_inited


def test_foo_corpus_step_done(foo_corpus_step_done):
    assert foo_corpus_step_done.state == StepState.DONE


def test_foo_corpus_step_done_output_exist(foo_corpus_step_done):
    for f_name in foo_corpus_step_done.dataset_filename_list:
        assert Path(foo_corpus_step_done.output_dir, f_name).exists()


def test_foo_corpus_step_done_output_compressed(foo_corpus_step_done):
    for f_name in foo_corpus_step_done.dataset_filename_list:
        file_path = Path(foo_corpus_step_done.output_dir, f_name)
        with open(file_path, 'rb') as fh:
            assert fh.read(2) == b"\x1f\x8b"

# shard_size + prev_corpus_step invalid combinations
# shard naming (dset.lang.gz.idx.gz)
# file naming (dset.lang.gz)

def test_languages(foo_corpus_step_inited):
    languages = foo_corpus_step_inited.languages
    if len(languages) == 1:
        assert foo_corpus_step_inited.tgt_lang is None
    elif len(languages) == 2:
        assert foo_corpus_step_inited.tgt_lang == languages[1]
    else:
        assert False
