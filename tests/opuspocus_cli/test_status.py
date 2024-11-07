import time

import pytest

from opuspocus.pipeline_steps import StepState
from opuspocus.pipelines import PipelineState
from opuspocus_cli import main

SLEEP_TIME = 2
STATUS_OUT_N_COLS = 3


@pytest.mark.parametrize(
    ("pipeline_in_state", "state"),
    [
        ("foo_pipeline_partially_inited", PipelineState.INIT_INCOMPLETE),
        ("foo_pipeline_inited", PipelineState.INITED),
        ("foo_pipeline_failed", PipelineState.FAILED),
        ("foo_pipeline_done", PipelineState.DONE),
        ("foo_pipeline_running", PipelineState.RUNNING),
    ],
)
def test_status_default_values(pipeline_in_state, state, capsys, request):
    """Print the pipeline state and the states of its individual steps in a parsable format."""
    pipeline = request.getfixturevalue(pipeline_in_state)
    pipeline_dir = pipeline.pipeline_dir

    if pipeline_in_state == "foo_pipeline_running":
        while pipeline.state == PipelineState.SUBMITTED:
            time.sleep(SLEEP_TIME)

    rc = main(["status", "--pipeline-dir", str(pipeline_dir)])
    assert rc == 0

    out_has_status = False
    for line in capsys.readouterr().out.split("\n")[:-1]:
        if "---" in line:
            continue  # skip toprule
        line_arr = line.split("|")
        assert len(line_arr) == STATUS_OUT_N_COLS

        assert "Step" in line_arr[1] or "Pipeline" in line_arr[1]
        assert line_arr[2] in StepState.list()

        if "OpusPocusPipeline" in line_arr[1]:
            out_has_status = True
            assert state.value in line
    assert out_has_status
