import pytest
from pathlib import Path

from opuspocus.pipelines import PipelineInitError
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


def test_run_nonempty_directory_exists_fail(foo_pipeline_inited):
    """Fail execution when pipeline exists and is not in the INITED state."""
    pipeline_dir = Path(foo_pipeline_inited.pipeline_dir.parent, "mock_dir")
    pipeline_dir.mkdir()
    with Path(pipeline_dir, "mock_file.txt").open("w") as fh:
        print("Some text.", file=fh)
    pipeline_config = Path(foo_pipeline_inited.pipeline_dir, foo_pipeline_inited.config_file)

    with pytest.raises(PipelineInitError):
        main([
            "run",
            "--pipeline-dir",
            str(pipeline_dir),
            "--pipeline-config",
            str(pipeline_config),
            "--runner",
            "bash"
        ])


@pytest.mark.parametrize(
    "pipeline_in_state",
    [
        "foo_pipeline_partially_inited",
        "foo_pipeline_inited",
        "foo_pipeline_failed"
    ]
)
def test_run_pipeline_in_state(pipeline_in_state, request):
    pipeline = request.getfixturevalue(pipeline_in_state)
    pipeline_config = Path(pipeline.pipeline_dir, pipeline.config_file)
    rc = main([
        "run",
        "--pipeline-dir",
        str(pipeline.pipeline_dir),
        "--pipeline-config",
        str(pipeline_config),
        "--runner",
        "bash"
    ])
    assert rc == 0


@pytest.mark.parametrize(
    "pipeline_in_state",
    [
        "foo_pipeline_running",
        "foo_pipeline_done",
    ]
)
def test_run_pipeline_in_state_fail(pipeline_in_state, request):
    pipeline = request.getfixturevalue(pipeline_in_state)
    pipeline_config = Path(pipeline.pipeline_dir, pipeline.config_file)
    with pytest.raises(ValueError):
        main([
            "run",
            "--pipeline-dir",
            str(pipeline.pipeline_dir),
            "--pipeline-config",
            str(pipeline_config),
            "--runner",
            "bash"
        ])


@pytest.mark.parametrize(
    "pipeline_in_state",
    [
        "foo_pipeline_partially_inited",
        "foo_pipeline_inited",
        "foo_pipeline_failed",
        "foo_pipeline_running",
        "foo_pipeline_done"
    ]
)
def test_run_pipeline_in_state_reinit(pipeline_in_state, request):
    pipeline = request.getfixturevalue(pipeline_in_state)
    pipeline_config = Path(pipeline.pipeline_dir, pipeline.config_file)
    rc = main([
        "run",
        "--pipeline-dir",
        str(pipeline.pipeline_dir),
        "--pipeline-config",
        str(pipeline_config),
        "--runner",
        "bash",
        "--reinit"
    ])
    assert rc == 0


@pytest.mark.parametrize(
    "pipeline_in_state",
    [
        "foo_pipeline_inited",
        "foo_pipeline_failed",
        "foo_pipeline_running",
        "foo_pipeline_done"
    ]
)
def test_run_pipeline_in_state_reinit(pipeline_in_state, request):
    pipeline = request.getfixturevalue(pipeline_in_state)
    pipeline_config = Path(pipeline.pipeline_dir, pipeline.config_file)
    rc = main([
        "run",
        "--pipeline-dir",
        str(pipeline.pipeline_dir),
        "--pipeline-config",
        str(pipeline_config),
        "--runner",
        "bash",
        "--rerun"
    ])
    assert rc == 0
