import pytest

from opuspocus_cli import main


@pytest.mark.parametrize(
    ("pipeline_in_state", "warn"),
    [
        ("foo_pipeline", True),
        ("foo_pipeline_partially_inited", True),
        ("foo_pipeline_inited", True),
        ("foo_pipeline_failed", True),
        ("foo_pipeline_done", True),
        ("foo_pipeline_running", False),
    ],
)
def test_stop_default_values(pipeline_in_state, warn, request):
    """Stop pipeline in a specific state using default CLI option values."""
    pipeline = request.getfixturevalue(pipeline_in_state)
    pipeline_dir = pipeline.pipeline_dir

    cmd = ["stop", "--pipeline-dir", str(pipeline_dir)]
    if warn:
        with pytest.warns(UserWarning):
            rc = main(cmd)
    else:
        rc = main(cmd)
    assert rc == 0
