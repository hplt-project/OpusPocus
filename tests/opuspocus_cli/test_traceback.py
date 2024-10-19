import pytest

from opuspocus_cli.traceback import main


@pytest.mark.parametrize(
    "pipeline_in_state",
    [
        "foo_pipeline_partially_inited",
        "foo_pipeline_inited",
        "foo_pipeline_failed",
        "foo_pipeline_done",
        "foo_pipeline_running",
    ]
)
def test_stop_default_values(pipeline_in_state, request):
    """Print traceback - depencency graph of the pipelien with step states and (optional) step parameters."""
    # TODO(varisd): properly test the output format
    pipeline = request.getfixturevalue(pipeline_in_state)
    pipeline_dir = pipeline.pipeline_dir

    rc = main(["traceback", "--pipeline-dir", str(pipeline_dir)])
    assert rc == 0
