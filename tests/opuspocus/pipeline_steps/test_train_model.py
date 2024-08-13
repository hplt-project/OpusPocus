import pytest

from opuspocus.pipeline_steps import StepState
from opuspocus.runners.debug import DebugRunner

# TODO(varisd): test cpu vs gpu run (single vs multi)
#   (for this we will probably require a separate CPU/GPU compilations of
#   Marian)
# TODO(varisd): test model tuning (loading a model and continuing traning)


@pytest.fixture()
def train_model_step_inited(train_data_parallel_tiny_model_step_inited):
    return train_data_parallel_tiny_model_step_inited


def test_train_model_step_inited(train_model_step_inited):
    """Test whether the step was initialized successfully."""
    assert train_model_step_inited.state == StepState.INITED


@pytest.fixture()
def train_model_step_done(train_model_step_inited):
    """Execute the train_model step."""
    runner = DebugRunner("debug", train_model_step_inited.pipeline_dir)
    runner.submit_step(train_model_step_inited)
    return train_model_step_inited


def test_train_model_step_done(train_model_step_done):
    """Test whether the step execution finished successfully."""
    assert train_model_step_done.state == StepState.DONE


@pytest.mark.xfail(reason="not implemented")
def test_train_model_step_done_model(train_model_step_done):  # noqa: ARG001
    """Check whether the train_step model was saved correctly."""
    pass
