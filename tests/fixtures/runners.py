import pytest

from opuspocus.options import parse_run_args
from opuspocus.runners import RUNNER_REGISTRY, build_runner


#@pytest.fixture(params=RUNNER_REGISTRY.keys())
@pytest.fixture(params=["bash"])
def parsed_runner_args(request, foo_step_inited):
    """Create default runner arguments."""
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
    runner = build_runner(parsed_runner_args.runner, parsed_runner_args.pipeline_dir, parsed_runner_args)
    return runner
