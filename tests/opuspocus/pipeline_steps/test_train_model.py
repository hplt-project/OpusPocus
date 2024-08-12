import pytest

from opuspocus.pipeline_steps import StepState, build_step
from opuspocus.runners.debug import DebugRunner

# TODO(varisd): test cpu vs gpu run (single vs multi)
#   (for this we will probably require a separate CPU/GPU compilations of
#   Marian)
# TODO(varisd): test model tuning (loading a model and continuing traning)


@pytest.fixture(scope="function", params=["marian_cpu_dir", "marian_gpu_dir"])
def train_model_step_inited(
    request,
    train_data_parallel_tiny_raw_step_inited,
    train_data_parallel_tiny_vocab_step_inited,
    marian_tiny_config_file,
    opustrainer_tiny_config_file,
):
    """Create and initialize the train_model step."""
    marian_dir = request.getfixturevalue(request.param)

    step = build_step(
        step="train_model",
        step_label=f"train_model.{request.param}.test",
        pipeline_dir=train_data_parallel_tiny_raw_step_inited.pipeline_dir,
        **{
            "marian_dir": marian_dir,
            "src_lang": train_data_parallel_tiny_raw_step_inited.src_lang,
            "tgt_lang": train_data_parallel_tiny_raw_step_inited.tgt_lang,
            "marian_config": marian_tiny_config_file,
            "vocab_step": train_data_parallel_tiny_vocab_step_inited,
            "opustrainer_config": opustrainer_tiny_config_file,
            "train_corpus_step": train_data_parallel_tiny_raw_step_inited,
            "valid_corpus_step": train_data_parallel_tiny_raw_step_inited,
            "train_category": train_data_parallel_tiny_raw_step_inited.categories[0],
            "valid_dataset": train_data_parallel_tiny_raw_step_inited.dataset_list[0],
        },
    )
    step.init_step()
    return step


def test_train_model_step_inited(train_model_step_inited):
    """Test whether the step was initialized successfully."""
    assert train_model_step_inited.state == StepState.INITED


@pytest.fixture(scope="function")
def train_model_step_done(train_model_step_inited):
    """Execute the train_model step."""
    runner = DebugRunner("debug", train_model_step_inited.pipeline_dir)
    runner.submit_step(train_model_step_inited)
    return train_model_step_inited


def test_train_model_step_done(train_model_step_done):
    """Test whether the step execution finished successfully."""
    assert train_model_step_done.state == StepState.DONE


@pytest.mark.xfail(reason="not implemented")
def test_train_model_step_done_model(train_model_step_done):
    """Check whether the train_step model was saved correctly."""
    assert False  # noqa: B011
