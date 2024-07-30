import pytest

from opuspocus.options import parse_run_args
from opuspocus.runners import (
    build_runner,
    load_runner,
    OpusPocusRunner,
    RUNNER_REGISTRY
)


@pytest.fixture(scope="function", params=RUNNER_REGISTRY.keys())
def parsed_runner_args(request, pipeline_minimal_inited):
    """Create default runner arguments."""
    return parse_run_args([
        "--pipeline-dir",
        pipeline_minimal_inited.pipeline_dir,
        "--runner",
        request.param
    ])


def test_build_runner_method(parsed_runner_args):
    """Create runner with default args."""
    runner = build_runner(
        parsed_runner_args.runner,
        parsed_runner_args.pipeline_dir,
        parsed_runner_args
    )
    assert isinstance(runner, OpusPocusRunner)


def test_load_runner_method(parsed_runner_args):
    """Reload runner for further pipeline execution manipulation."""
    runner = build_runner(
        parsed_runner_args.runner,
        parsed_runner_args.pipeline_dir,
        parsed_runner_args
    )
    runner.save_parameters()

    runner_loaded = load_runner(parsed_runner_args.pipeline_dir)
    assert runner == runner_loaded


def test_load_runner_before_save(pipeline_minimal_inited):
    """Fail loading runner that was not previously created."""
    with pytest.raises(FileNotFoundError):
        runner = load_runner(pipeline_minimal_inited.pipeline_dir)


#    for runner_default


# def test_list_parameters():


# def test_pipeline_class_...():
