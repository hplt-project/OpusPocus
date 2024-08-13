import pytest

from opuspocus.options import parse_run_args
from opuspocus.runners import (
    RUNNER_REGISTRY,
    OpusPocusRunner,
    build_runner,
    load_runner,
)

# TODO(varisd): add more tests (test_list_parameters, etc.)


@pytest.fixture(scope="module", params=RUNNER_REGISTRY.keys())
def parsed_runner_args(request, pipeline_preprocess_tiny_inited, opuspocus_hq_server_dir, hyperqueue_dir):
    """Create default runner arguments."""
    extra = []
    if request.param == "hyperqueue":
        extra = [
            "--hq-server-dir",
            str(opuspocus_hq_server_dir),
            "--hq-path",
            f"{hyperqueue_dir!s}/bin/hq",
        ]

    args = parse_run_args(
        [  # noqa: RUF005
            "--pipeline-dir",
            pipeline_preprocess_tiny_inited.pipeline_dir,
            "--runner",
            request.param,
        ]
        + extra
    )
    return args  # noqa: RET504


def test_build_runner_method(parsed_runner_args):
    """Create runner with default args."""
    runner = build_runner(parsed_runner_args.runner, parsed_runner_args.pipeline_dir, parsed_runner_args)
    assert isinstance(runner, OpusPocusRunner)


def test_load_runner_before_save(pipeline_preprocess_tiny_inited):
    """Fail loading runner that was not previously created."""
    with pytest.raises(FileNotFoundError):
        load_runner(pipeline_preprocess_tiny_inited.pipeline_dir)


def test_load_runner_method(parsed_runner_args):
    """Reload runner for further pipeline execution manipulation."""
    runner = build_runner(parsed_runner_args.runner, parsed_runner_args.pipeline_dir, parsed_runner_args)
    runner.save_parameters()

    runner_loaded = load_runner(parsed_runner_args.pipeline_dir)
    assert runner == runner_loaded
