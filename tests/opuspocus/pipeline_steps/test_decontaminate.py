from pathlib import Path

import pytest

from opuspocus.pipeline_steps import StepState, build_step
from opuspocus.runners.debug import DebugRunner
from opuspocus.utils import count_lines


@pytest.fixture()
def decontaminate_step_inited(train_data_parallel_tiny_raw_step_inited):
    """Create and initialize the decontaminate step."""
    step = build_step(
        step="decontaminate",
        step_label="decontaminate.test",
        pipeline_dir=train_data_parallel_tiny_raw_step_inited.pipeline_dir,
        **{
            "prev_corpus_step": train_data_parallel_tiny_raw_step_inited,
            "src_lang": train_data_parallel_tiny_raw_step_inited.src_lang,
            "tgt_lang": train_data_parallel_tiny_raw_step_inited.tgt_lang,
            "valid_data_step": train_data_parallel_tiny_raw_step_inited,
            "test_data_step": train_data_parallel_tiny_raw_step_inited,
        },
    )
    step.init_step()
    return step


def test_decontaminate_step_inited(decontaminate_step_inited):
    """Test whether the step was initialized successfully."""
    assert decontaminate_step_inited.state == StepState.INITED


@pytest.fixture()
def decontaminate_step_done(decontaminate_step_inited):
    """Execute the decontaminate step."""
    runner = DebugRunner("debug", decontaminate_step_inited.pipeline_dir)
    runner.submit_step(decontaminate_step_inited)
    return decontaminate_step_inited


def test_decontaminate_step_done(decontaminate_step_done):
    """Test whether the step was executed successfully."""
    assert decontaminate_step_done.state == StepState.DONE


def test_decontaminate_step_done_corpus_lines(decontaminate_step_done):
    """The output corpora should have identical number of lines."""
    # TODO(varisd): this can probably be generalized to all CorpusStep objects
    for dset in decontaminate_step_done.dataset_list:
        src_lines = count_lines(
            Path(
                decontaminate_step_done.output_dir,
                f"{dset}.{decontaminate_step_done.src_lang}.gz",
            )
        )
        tgt_lines = count_lines(
            Path(
                decontaminate_step_done.output_dir,
                f"{dset}.{decontaminate_step_done.tgt_lang}.gz",
            )
        )
        assert src_lines == tgt_lines
