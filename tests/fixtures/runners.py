from pathlib import Path

import pytest

from opuspocus.options import parse_run_args
from opuspocus.runners import RUNNER_REGISTRY, build_runner


@pytest.fixture(params=RUNNER_REGISTRY.keys())
def runner_args(request, foo_step):
    """Create default runner arguments."""
    if request.param == "slurm" and not (Path("/bin/sbatch").exists() or Path("/usr/bin/sbatch").exists()):
        pytest.skip(reason="Requires SLURM to be available...")

    extra = []
    if request.param == "bash":
        extra += ["--run-tasks-in-parallel"]  # not setting this to true can break some runner-related tests
    args = [
        "--runner",
        request.param,
    ] + extra
    return args


@pytest.fixture()
def parsed_step_runner_args(runner_args, foo_step):
    """Create default mock step runner arguments."""
    return parse_run_args(runner_args + ["--pipeline-dir", str(foo_step.pipeline_dir)])


@pytest.fixture()
def parsed_pipeline_runner_args(runner_args, foo_pipeline):
    """Create default mock pipeline runner arguments."""
    return parse_run_args(runner_args + ["--pipeline-dir", str(foo_pipeline.pipeline_dir)])


@pytest.fixture()
def foo_step_runner(parsed_step_runner_args):
    """Create a mock step runner."""
    runner = build_runner(parsed_step_runner_args)
    runner.save_parameters()
    return runner


@pytest.fixture()
def foo_pipeline_runner(parsed_pipeline_runner_args):
    """Create a mock pipeline runner."""
    return build_runner(parsed_pipeline_runner_args)
