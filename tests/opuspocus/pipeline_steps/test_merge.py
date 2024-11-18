from pathlib import Path

import pytest

from opuspocus.pipeline_steps import StepState, build_step
from opuspocus.runners.debug import DebugRunner


@pytest.fixture(params=[True, False])
def merge_step_inited(request, train_data_parallel_tiny_raw_step_inited):
    """Create and initialize the merge step."""
    step = build_step(
        step="merge",
        step_label=f"merge.{request.param}.test",
        pipeline_dir=train_data_parallel_tiny_raw_step_inited.pipeline_dir,
        **{
            "src_lang": train_data_parallel_tiny_raw_step_inited.src_lang,
            "tgt_lang": train_data_parallel_tiny_raw_step_inited.tgt_lang,
            "prev_corpus_step": train_data_parallel_tiny_raw_step_inited,
            "prev_corpus_label": "previous",
            "other_corpus_step": train_data_parallel_tiny_raw_step_inited,
            "other_corpus_label": "other",
            "merge_categories": request.param,
        },
    )
    step.init_step()
    return step


def test_merge_step_inited(merge_step_inited):
    """Test whether the step was initialized successfully."""
    assert merge_step_inited.state == StepState.INITED


@pytest.fixture()
def merge_step_done(merge_step_inited):
    """Execute the merge step."""
    runner = DebugRunner("debug", merge_step_inited.pipeline_dir)
    runner.submit_step(merge_step_inited)
    return merge_step_inited


def test_merge_step_done(merge_step_done):
    """Test whether the step execution finished successfully."""
    assert merge_step_done.state == StepState.DONE


def test_datasets_merged(merge_step_done):
    """All datasets from the original steps are listable in the merged step (with new dataset names)."""
    dset_set = set(merge_step_done.prev_corpus_step.dataset_list + merge_step_done.other_corpus_step.dataset_list)
    for dataset in merge_step_done.dataset_list:
        dataset_orig = ".".join(dataset.split(".")[1:])
        assert dataset_orig in dset_set


def test_dataset_files_merged(merge_step_done):
    """All dataset files from the original steps are present in the merged step output dir."""
    for dataset in merge_step_done.dataset_list:
        for lang in merge_step_done.languages:
            dset_path = Path(merge_step_done.output_dir, f"{dataset}.{lang}.gz")
            assert dset_path.exists()


def test_categories_merged(merge_step_done):
    """All the categories from the original files are in the merged step categories."""
    categories_set = set(merge_step_done.prev_corpus_step.categories + merge_step_done.other_corpus_step.categories)
    for cat in merge_step_done.categories:
        if merge_step_done.merge_categories:
            assert cat in categories_set
        else:
            label, orig_cat = cat.split(".")
            assert orig_cat in merge_step_done.dependencies[f"{label}_corpus_step"].categories
