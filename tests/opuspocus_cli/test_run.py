import time
from pathlib import Path

import pytest

from opuspocus.config import PipelineConfig
from opuspocus.options import ERR_RETURN_CODE
from opuspocus.pipeline_steps import StepState
from opuspocus.pipelines import PipelineInitError, PipelineStateError
from opuspocus_cli import main

SLEEP_TIME_WAIT = 2


def test_run_default_values(foo_pipeline_config_file):
    """Execute 'run' command with default values (required values are provided)."""
    rc = main(["run", "--pipeline-config", str(foo_pipeline_config_file), "--runner", "bash"])
    assert rc == 0


def test_run_missing_config_fail():
    """Fail 'run' execution when pipeline-config option is missing."""
    rc = main(["run", "--runner", "bash"])
    assert rc == ERR_RETURN_CODE


def test_run_nonempty_directory_exists_fail(foo_pipeline):
    """Fail execution when pipeline exists and is not in the INITED state."""
    with Path(foo_pipeline.pipeline_dir, "mock_file.txt").open("w") as fh:
        print("Some text.", file=fh)
    config_path = Path(foo_pipeline.pipeline_dir.parent, foo_pipeline._config_file)  # noqa: SLF001
    PipelineConfig.save(foo_pipeline.pipeline_config, config_path)

    with pytest.raises(PipelineInitError):
        main(
            [
                "run",
                "--pipeline-dir",
                str(foo_pipeline.pipeline_dir),
                "--pipeline-config",
                str(config_path),
                "--runner",
                "bash",
            ]
        )


@pytest.mark.timeout(40)
@pytest.mark.parametrize(
    "pipeline_in_state",
    ["foo_pipeline_partially_inited", "foo_pipeline_inited", "foo_pipeline_failed", "foo_pipeline_done"],
)
def test_run_pipeline_in_state(pipeline_in_state, request):
    """Run a pipeline in the initialized or failed state."""
    pipeline = request.getfixturevalue(pipeline_in_state)
    rc = main(
        [
            "run",
            "--pipeline-dir",
            str(pipeline.pipeline_dir),
            "--runner",
            "bash",
        ]
    )
    assert rc == 0
    # Wait for the execution to finish to avoid problems during the cleanup
    while pipeline.state == StepState.RUNNING:
        time.sleep(SLEEP_TIME_WAIT)


@pytest.mark.parametrize(
    "pipeline_in_state",
    ["foo_pipeline_running"],
)
def test_run_pipeline_in_state_fail(pipeline_in_state, request):
    """Fail running a pipeline in running or done states without the rerun flag."""
    pipeline = request.getfixturevalue(pipeline_in_state)
    with pytest.raises(PipelineStateError):
        main(
            [
                "run",
                "--pipeline-dir",
                str(pipeline.pipeline_dir),
                "--pipeline-config",
                str(pipeline.pipeline_config_path),
                "--runner",
                "bash",
            ]
        )


@pytest.mark.timeout(40)
@pytest.mark.parametrize(
    "pipeline_in_state",
    [
        "foo_pipeline_partially_inited",
        "foo_pipeline_inited",
        "foo_pipeline_failed",
        "foo_pipeline_running",
        "foo_pipeline_done",
    ],
)
def test_run_pipeline_in_state_reinit(pipeline_in_state, request):
    """Fully reinitialize and run a pipeline in a specific state, canceling the previous run."""
    pipeline = request.getfixturevalue(pipeline_in_state)

    argv = [
        "run",
        "--pipeline-dir",
        str(pipeline.pipeline_dir),
        "--runner",
        "bash",
        "--reinit",
    ]
    if pipeline_in_state == "foo_pipeline_running":
        argv += ["--stop-previous-run"]

    rc = main(argv)
    assert rc == 0
    while pipeline.state == StepState.RUNNING:
        time.sleep(SLEEP_TIME_WAIT)


@pytest.mark.parametrize(
    ("pipeline_in_state", "warn"),
    [
        ("foo_pipeline_partially_inited", True),
        ("foo_pipeline_inited", True),
        ("foo_pipeline_failed", True),
        ("foo_pipeline_running", False),
        ("foo_pipeline_done", True),
    ],
)
def test_run_pipeline_in_state_stop(pipeline_in_state, warn, request):
    """Rerun a pipeline in a specific state, canceling the previous run."""
    pipeline = request.getfixturevalue(pipeline_in_state)
    cmd = [
        "run",
        "--pipeline-dir",
        str(pipeline.pipeline_dir),
        "--runner",
        "bash",
        "--stop-previous-run",
    ]
    if warn:
        with pytest.warns(UserWarning):
            rc = main(cmd)
    else:
        rc = main(cmd)
    assert rc == 0
