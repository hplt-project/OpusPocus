import pytest

from pathlib import Path

from opuspocus.pipeline_steps import build_step, StepState
from opuspocus.runners.debug import DebugRunner
from opuspocus.utils import count_lines


@pytest.fixture(scope="session")
def decontaminate_step(raw_step_train_minimal):
    """Build mock decontaminate step."""
    return build_step(
        step="decontaminate",
        step_label="decontaminate.test",
        pipeline_dir=raw_step_train_minimal.pipeline_dir,
        **{
            "previous_corpus_step": raw_step_train_minimal,
            "src_lang": raw_step_train_minimal.src_lang,
            "tgt_lang": raw_step_train_minimal.tgt_lang,
            "valid_data_step": raw_step_train_minimal,
            "test_data_step": raw_step_train_minimal,
        },
    )


@pytest.fixture(scope="session")
def decontaminate_step_inited(decontaminate_step):
    """Initialize the step."""
    decontaminate_step.init_step()
    return decontaminate_step


def test_decontaminate_step_inited(decontaminate_step_inited):
    """Test the initialization."""
    assert decontaminate_step_inited.state == StepState.INITED


@pytest.fixture(scope="session")
def decontaminate_step_done(decontaminate_step_inited):
    """Run the step."""
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
                "{}.{}.gz".format(dset, decontaminate_step_done.src_lang),
            )
        )
        tgt_lines = count_lines(
            Path(
                decontaminate_step_done.output_dir,
                "{}.{}.gz".format(dset, decontaminate_step_done.tgt_lang),
            )
        )
        assert src_lines == tgt_lines
