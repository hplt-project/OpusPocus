import pytest

from opuspocus.pipelines import PipelineState
from opuspocus_cli.status import main


@pytest.mark.parametrize(
    ("pipeline_in_state", "state"),
    [
        ("foo_pipeline_partially_inited", PipelineState.INIT_INCOMPLETE),
        ("foo_pipeline_inited", PipelineState.INITED),
        ("foo_pipeline_failed", PipelineState.FAILED),
        ("foo_pipeline_done", PipelineState.DONE),
        ("foo_pipeline_running", PipelineState.RUNNING)
    ]
)
def test_status_default_values(pipeline_in_state, state, capsys, request):
    """Print the pipeline state and the states of its individual steps in a parsable format."""
    pipeline = request.getfixturevalue(pipeline_in_state)
    pipeline_dir = pipeline.pipeline_dir

    rc = main(["status", "--pipeline-dir", str(pipeline_dir)])
    assert rc == 0

    out_has_status = False
    for line in capsys.readout:
        if '---' in line:
            continue  # skip toprule

        line_arr = line.split('|')
        assert len(line_arr) == 3

        assert "Step" in line_arr[1] or "Pipeline" in line_arr[1]
        assert line_arr[2] in StepState.__members__

        if "OpusPocusPipeline" in line_arr[1]:
            out_has_status = True
            assert str(state) in line
    assert out_has_status
