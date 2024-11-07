from pathlib import Path

import pytest

from opuspocus.pipelines import PipelineConfig, PipelineInitError
from opuspocus_cli import main


def test_run_default_values(foo_pipeline_config_file):
    """Execute 'run' command with default values (required values are provided)."""
    rc = main(["run", "--pipeline-config", str(foo_pipeline_config_file), "--runner", "bash"])
    assert rc == 0


@pytest.mark.parametrize("values", [("config"), ("runner"), ("")])
def test_run_missing_required_values_fail(values, foo_pipeline_config_file):
    """Fail 'run' execution when required values are missing."""
    cmd = ["run"]
    values_arr = values.split(",")
    if "config" in values_arr:
        cmd += ["--pipeline-config", str(foo_pipeline_config_file)]
    if "runner" in values_arr:
        cmd += ["--runner", "bash"]
    with pytest.raises(SystemExit):
        main(cmd)


def test_run_nonempty_directory_exists_fail(foo_pipeline):
    """Fail execution when pipeline exists and is not in the INITED state."""
    with Path(foo_pipeline.pipeline_dir, "mock_file.txt").open("w") as fh:
        print("Some text.", file=fh)
    config_path = Path(foo_pipeline.pipeline_dir.parent, foo_pipeline.config_file)
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


@pytest.mark.parametrize(
    "pipeline_in_state",
    ["foo_pipeline_partially_inited", "foo_pipeline_inited", "foo_pipeline_failed", "foo_pipeline_done"],
)
def test_run_pipeline_in_state(pipeline_in_state, request):
    """Run a pipeline in the initialized or failed state."""
    pipeline = request.getfixturevalue(pipeline_in_state)
    pipeline_config = Path(pipeline.pipeline_dir, pipeline.config_file)
    rc = main(
        [
            "run",
            "--pipeline-dir",
            str(pipeline.pipeline_dir),
            "--pipeline-config",
            str(pipeline_config),
            "--runner",
            "bash",
        ]
    )
    assert rc == 0


@pytest.mark.parametrize(
    "pipeline_in_state",
    ["foo_pipeline_running"],
)
def test_run_pipeline_in_state_fail(pipeline_in_state, request):
    """Fail running a pipeline in running or done states without the rerun flag."""
    pipeline = request.getfixturevalue(pipeline_in_state)
    pipeline_config = Path(pipeline.pipeline_dir, pipeline.config_file)
    with pytest.raises(SystemExit):
        main(
            [
                "run",
                "--pipeline-dir",
                str(pipeline.pipeline_dir),
                "--pipeline-config",
                str(pipeline_config),
                "--runner",
                "bash",
            ]
        )


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
    pipeline_config = Path(pipeline.pipeline_dir, pipeline.config_file)
    rc = main(
        [
            "run",
            "--pipeline-dir",
            str(pipeline.pipeline_dir),
            "--pipeline-config",
            str(pipeline_config),
            "--runner",
            "bash",
            "--reinit",
        ]
    )
    assert rc == 0


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
    pipeline_config = Path(pipeline.pipeline_dir, pipeline.config_file)

    cmd = [
        "run",
        "--pipeline-dir",
        str(pipeline.pipeline_dir),
        "--pipeline-config",
        str(pipeline_config),
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
