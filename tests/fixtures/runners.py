from pathlib import Path

import pytest

from opuspocus.options import parse_run_args
from opuspocus.runners import build_runner


# @pytest.fixture(params=RUNNER_REGISTRY.keys())
@pytest.fixture(params=["bash", "slurm"])
# @pytest.fixture(params=["slurm"])
def parsed_runner_args(request, foo_step_inited):
    """Create default runner arguments."""
    if request.param == "slurm" and not Path("/bin/sbatch").exists():
        pytest.skip(reason="Requires SLURM to be available...")

    extra = []
    if request.param == "hyperqueue":
        hq_dir = request.getfixturevalue("hyperqueue_dir")
        hq_server_dir = request.getfixturevalue("opuspocus_hq_server_dir")
        extra = [
            "--hq-server-dir",
            str(hq_server_dir),
            "--hq-path",
            f"{hq_dir!s}/bin/hq",
        ]

    args = parse_run_args(
        [  # noqa: RUF005
            "--pipeline-dir",
            str(foo_step_inited.pipeline_dir),
            "--runner",
            request.param,
        ]
        + extra
    )
    return args  # noqa: RET504


@pytest.fixture()
def foo_runner(parsed_runner_args):
    return build_runner(parsed_runner_args.runner, parsed_runner_args.pipeline_dir, parsed_runner_args)
