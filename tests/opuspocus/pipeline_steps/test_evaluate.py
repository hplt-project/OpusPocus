import pytest

from opuspocus.pipeline_steps import StepState, build_step
from opuspocus.pipeline_steps.evaluate import EvaluateStep
from opuspocus.runners.debug import DebugRunner


@pytest.fixture(scope="function", params=EvaluateStep.AVAILABLE_METRICS)
def evaluate_step_inited(request, train_data_parallel_tiny_raw_step_inited):
    """Create and initialize the evaluate step."""
    step = build_step(
        step="evaluate",
        step_label="evaluate.{}.test".format(request.param),
        pipeline_dir=train_data_parallel_tiny_raw_step_inited.pipeline_dir,
        **{
            "src_lang": train_data_parallel_tiny_raw_step_inited.src_lang,
            "tgt_lang": train_data_parallel_tiny_raw_step_inited.tgt_lang,
            "datasets": train_data_parallel_tiny_raw_step_inited.dataset_list,
            "translated_corpus_step": train_data_parallel_tiny_raw_step_inited,
            "reference_corpus_step": train_data_parallel_tiny_raw_step_inited,
            "metrics": [request.param],
        },
    )
    step.init_step()
    return step


def test_evaluate_step_unknown_metric(train_data_parallel_tiny_raw_step_inited):
    """Step construction fails when presented with unknown metric."""
    with pytest.raises(ValueError):
        build_step(
            step="evaluate",
            step_label="evaluate.foo.test",
            pipeline_dir=train_data_parallel_tiny_raw_step_inited.pipeline_dir,
            **{
                "src_lang": train_data_parallel_tiny_raw_step_inited.src_lang,
                "tgt_lang": train_data_parallel_tiny_raw_step_inited.tgt_lang,
                "datasets": train_data_parallel_tiny_raw_step_inited.dataset_list,
                "translated_corpus_step": train_data_parallel_tiny_raw_step_inited,
                "reference_corpus_step": train_data_parallel_tiny_raw_step_inited,
                "metrics": ["foo"],
            },
        )


def test_evaluate_step_inited(evaluate_step_inited):
    """Test whether the step was initialized successfully."""
    assert evaluate_step_inited.state == StepState.INITED


@pytest.fixture(scope="function")
def evaluate_step_done(evaluate_step_inited):
    """Execute the evaluate step."""
    runner = DebugRunner("debug", evaluate_step_inited.pipeline_dir)
    runner.submit_step(evaluate_step_inited)
    return evaluate_step_inited


def test_evaluate_step_done(evaluate_step_done):
    """Test whether the step execution finished successfully."""
    assert evaluate_step_done.state == StepState.DONE
