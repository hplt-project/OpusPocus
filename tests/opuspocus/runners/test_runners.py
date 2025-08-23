import time
from argparse import Namespace
from pathlib import Path

import pytest

from opuspocus.pipeline_steps import StepState
from opuspocus.pipelines import PipelineState
from opuspocus.runners import OpusPocusRunner, load_runner
from opuspocus.utils import open_file

SLEEP_TIME_WAIT = 1  # wait after submitting a job
SLEEP_TIME_SHORT = 15  # shorter waiting time
SLEEP_TIME_LONG = 120  # long waiting time (for job cancel, manipulation, etc.)


@pytest.fixture()
def foo_runner_for_step_submit(foo_runner, foo_step):
    """Mock runner for mock step-wise submissions."""
    foo_runner.pipeline_dir = foo_step.pipeline_dir
    foo_runner.save_parameters()
    return foo_runner


def test_build_runner_method(foo_runner):
    """Create runner with default args."""
    assert isinstance(foo_runner, OpusPocusRunner)


def test_load_runner_before_save_fail(pipeline_preprocess_tiny_inited):
    """Fail loading runner that was not previously created."""
    with pytest.raises(FileNotFoundError):
        load_runner(
            Namespace(**{"pipeline": Namespace(**{"pipeline_dir": pipeline_preprocess_tiny_inited.pipeline_dir})})
        )


def test_load_runner_method(foo_runner):
    """Reload runner for further pipeline execution manipulation."""
    foo_runner.save_parameters()

    runner_loaded = load_runner(Namespace(**{"pipeline": Namespace(**{"pipeline_dir": foo_runner.pipeline_dir})}))
    assert foo_runner == runner_loaded


def test_run_pipeline(foo_runner, foo_pipeline_inited):
    """Execute a pipeline."""
    foo_runner.run_pipeline(foo_pipeline_inited)
    assert foo_pipeline_inited.state in (PipelineState.SUBMITTED, PipelineState.RUNNING)


def test_stop_pipeline(foo_runner, foo_pipeline_inited):
    """Stop a running pipeline."""
    for s in foo_pipeline_inited.steps:
        s.sleep_time = SLEEP_TIME_LONG
        s.save_parameters()
    foo_runner.run_pipeline(foo_pipeline_inited)
    time.sleep(SLEEP_TIME_WAIT)

    foo_runner.stop_pipeline(foo_pipeline_inited)
    assert foo_pipeline_inited.state == PipelineState.FAILED


def test_stop_pipeline_with_nonmatching_runner_fail(foo_runner, foo_pipeline_inited):
    """Fail if you manipulate a pipeline with a different runner."""
    foo_runner.run_pipeline(foo_pipeline_inited)
    foo_runner.runner = "foobar"
    with pytest.raises(ValueError):  # noqa: PT011
        foo_runner.stop_pipeline(foo_pipeline_inited)


def test_submit_step(foo_runner_for_step_submit, foo_step_inited):
    """Submit a single step."""
    foo_runner_for_step_submit.submit_step(foo_step_inited)
    time.sleep(SLEEP_TIME_WAIT)
    assert foo_step_inited.state in (PipelineState.SUBMITTED, PipelineState.RUNNING)


def test_submitted_step_running(foo_runner_for_step_submit, foo_step_inited):
    """The running step should submit subtasks for each target file."""
    foo_runner_for_step_submit.submit_step(foo_step_inited)
    while foo_step_inited.state == StepState.SUBMITTED:
        time.sleep(SLEEP_TIME_WAIT)
    sub_info = foo_runner_for_step_submit.load_submission_info(foo_step_inited)
    for target_path in foo_step_inited.get_command_targets():
        assert str(target_path) in [subtask["file_path"] for subtask in sub_info["subtasks"]]


def test_submit_step_submission_info_structure(foo_runner_for_step_submit, foo_step_inited):
    """The step submission info should have a pre-defined structure."""
    # Split the asserts, create a fixture (move this to a different test file).
    sub_info = foo_runner_for_step_submit.submit_step(foo_step_inited)
    assert "runner" in sub_info
    assert "main_task" in sub_info
    assert "subtasks" in sub_info
    assert sub_info["main_task"]["file_path"] is None
    assert "id" in sub_info["main_task"]
    for t_info in sub_info["subtasks"]:
        assert t_info["file_path"] is not None
        assert "id" in t_info


def test_cancel_main_task(foo_runner_for_step_submit, foo_step_inited):
    """Cancel a running step via its main task."""
    foo_step_inited.sleep_time = SLEEP_TIME_LONG
    foo_step_inited.save_parameters()

    sub_info = foo_runner_for_step_submit.submit_step(foo_step_inited)
    time.sleep(SLEEP_TIME_WAIT)
    while foo_step_inited.state == StepState.SUBMITTED:
        time.sleep(SLEEP_TIME_WAIT)

    foo_runner_for_step_submit.cancel_task(sub_info["main_task"])
    time.sleep(SLEEP_TIME_WAIT)
    while foo_step_inited.state == StepState.RUNNING:
        time.sleep(SLEEP_TIME_WAIT)

    assert foo_step_inited.state == StepState.FAILED
    for t_info in sub_info["subtasks"]:
        assert not Path(t_info["file_path"]).exists()


def test_submit_running_step(foo_runner_for_step_submit, foo_step_inited):
    """Submitting a running step just returns submission info of the running step."""
    foo_step_inited.sleep_time = SLEEP_TIME_LONG
    foo_step_inited.save_parameters()
    sub_info = foo_runner_for_step_submit.submit_step(foo_step_inited)
    submit_again_sub_info = foo_runner_for_step_submit.submit_step(foo_step_inited)
    assert sub_info == submit_again_sub_info


@pytest.mark.parametrize("resubmit_finished", [True, False])
def test_submit_failed_step(foo_runner_for_step_submit, foo_step_inited, resubmit_finished):
    """Submit a failed step (keep or remove finished target files from the finished subtasks)."""
    foo_step_inited.state = StepState.FAILED
    files = foo_step_inited.get_command_targets()

    failed_str = "FAILED"
    with open_file(files[0], "w") as fh:
        print(failed_str, file=fh)
    sub_info = foo_runner_for_step_submit.submit_step(foo_step_inited, resubmit_finished_subtasks=resubmit_finished)
    time.sleep(SLEEP_TIME_WAIT)

    foo_runner_for_step_submit.wait_for_tasks([sub_info["main_task"]])
    assert foo_step_inited.state == StepState.DONE

    for file in files:
        assert file.exists()

    file_output = open_file(files[0], "r").readline().strip("\n")
    if not resubmit_finished:
        assert file_output == failed_str
    else:
        assert file_output == files[0].stem + files[0].suffix


def test_submit_step_with_dependency(foo_runner_for_step_submit, bar_step_inited):
    """Submit a step that has a different step as a dependency (waiting for the dependency to finish."""
    foo_runner_for_step_submit.submit_step(bar_step_inited)
    time.sleep(SLEEP_TIME_WAIT)
    for step in [bar_step_inited, bar_step_inited.dep_step]:
        assert step.state in (StepState.SUBMITTED, StepState.RUNNING)


@pytest.mark.parametrize("resubmit_finished", [True, False])
def test_resubmit_step(foo_runner, foo_pipeline_inited, resubmit_finished):
    """Cancel a running step with its immediate resubmission (adjusting the dependencies of other submitted steps)."""
    if foo_runner.runner == "bash":
        pytest.skip(reason="Not supported by Bash runner.")

    bar_step_inited = foo_pipeline_inited.targets[0]
    bar_step_inited.sleep_time = SLEEP_TIME_SHORT
    bar_step_inited.save_parameters()

    foo_step_inited = bar_step_inited.dep_step
    foo_step_inited.sleep_time = SLEEP_TIME_SHORT
    foo_step_inited.save_parameters()

    foo_runner.pipeline_dir = foo_pipeline_inited.pipeline_dir
    foo_runner.save_parameters()

    foo_sub_info = foo_runner.submit_step(foo_step_inited)
    bar_sub_info = foo_runner.submit_step(bar_step_inited)
    time.sleep(SLEEP_TIME_WAIT)

    new_foo_sub_info = foo_runner.resubmit_step(bar_step_inited.dep_step, resubmit_finished_subtasks=resubmit_finished)
    time.sleep(SLEEP_TIME_WAIT)
    assert foo_sub_info["main_task"]["id"] != new_foo_sub_info["main_task"]["id"]

    foo_runner.wait_for_tasks([bar_sub_info["main_task"]])
    for step in [foo_step_inited, bar_step_inited]:
        assert step.state == StepState.DONE
