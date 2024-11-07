from pathlib import Path

import pytest

from opuspocus.options import parse_run_args
from opuspocus.runners import RUNNER_REGISTRY, build_runner


@pytest.fixture(params=RUNNER_REGISTRY.keys())
def parsed_runner_args(request, foo_step):
    """Create default runner arguments."""
    if request.param == "slurm" and not Path("/bin/sbatch").exists():
        pytest.skip(reason="Requires SLURM to be available...")

    extra = []
    args = parse_run_args(
        [  # noqa: RUF005
            "--pipeline-dir",
            str(foo_step.pipeline_dir),
            "--runner",
            request.param,
        ]
        + extra
    )
    return args  # noqa: RET504


@pytest.fixture()
def foo_runner(parsed_runner_args):
    """Create a mock runner."""
    return build_runner(parsed_runner_args.runner, parsed_runner_args.pipeline_dir, parsed_runner_args)
