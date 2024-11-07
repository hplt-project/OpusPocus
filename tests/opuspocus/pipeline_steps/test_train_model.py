import pytest

from opuspocus.pipeline_steps import StepState, build_step
from opuspocus.runners.debug import DebugRunner

# TODO(varisd): test cpu vs gpu run (single vs multi)
#   (for this we will probably require a separate CPU/GPU compilations of
#   Marian)
# TODO(varisd): test model tuning (loading a model and continuing traning)


@pytest.fixture()
def train_model_step_inited(train_data_parallel_tiny_model_step_inited):
    """Inited TrainModelStep instance."""
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


@pytest.mark.parametrize(
    "params_invalid",
    [
        {"train_categories": ["clean"], "train_category_ratios": None},
        {"train_category_ratios": [0.6, 0.4], "train_categories": None},
        {"train_categories": ["clean"], "train_category_ratios": [0.1, 0.9]},
        {"train_categories": ["clean"], "train_category_ratios": [0.9]},
        {"train_categories": ["foo", "bar"], "train_category_ratios": [0.9, 0.1]},
        {"train_categories": ["clean"], "train_category_ratios": [1.0], "max_epochs": -1},
    ],
)
def test_invalid_parameters_fail(params_invalid, train_data_parallel_tiny_model_step_inited):
    """Fail building with invalid build parameters."""
    param_dict = train_data_parallel_tiny_model_step_inited.get_parameters_dict()
    for k, v in train_data_parallel_tiny_model_step_inited.dependencies.items():
        param_dict[k] = v
    for k, v in params_invalid.items():
        param_dict[k] = v

    pipeline_dir = param_dict["pipeline_dir"]
    del param_dict["step"]
    del param_dict["step_label"]
    del param_dict["pipeline_dir"]
    with pytest.raises(ValueError):  # noqa: PT011, PT012
        step = build_step(step="train_model", step_label="train_model.fail", pipeline_dir=pipeline_dir, **param_dict)
        step.init_step()
