import time
from pathlib import Path

import pytest

from opuspocus.pipeline_steps import StepState
from opuspocus.runners import OpusPocusRunner, load_runner
from opuspocus.utils import open_file

SLEEP_TIME_WAIT = 2  # wait after submitting a job
SLEEP_TIME_SHORT = 15  # shorter waiting time
SLEEP_TIME_LONG = 120  # long waiting time (for job cancel, manipulation, etc.)


@pytest.fixture()
def foo_runner_for_step_submit(foo_runner, foo_step):
    foo_runner.pipeline_dir = foo_step.pipeline_dir
    foo_runner.save_parameters()
    return foo_runner


def test_build_runner_method(foo_runner):
    """Create runner with default args."""
    assert isinstance(foo_runner, OpusPocusRunner)


def test_load_runner_before_save_fail(pipeline_preprocess_tiny_inited):
    """Fail loading runner that was not previously created."""
    with pytest.raises(FileNotFoundError):
        load_runner(pipeline_preprocess_tiny_inited.pipeline_dir)


def test_load_runner_method(foo_runner):
    """Reload runner for further pipeline execution manipulation."""
    foo_runner.save_parameters()

    runner_loaded = load_runner(foo_runner.pipeline_dir)
    assert foo_runner == runner_loaded


def test_run_pipeline(foo_runner, foo_pipeline_inited):
    foo_runner.run_pipeline(foo_pipeline_inited)
    for step in foo_pipeline_inited.steps:
        if foo_runner.runner == "bash":
            assert step.state in (StepState.SUBMITTED, StepState.RUNNING)


def test_stop_pipeline(foo_runner, foo_pipeline_inited):
    for s in foo_pipeline_inited.steps:
        s.sleep_time = SLEEP_TIME_LONG
        s.save_parameters()
    foo_runner.run_pipeline(foo_pipeline_inited)
    time.sleep(SLEEP_TIME_WAIT)

    foo_runner.stop_pipeline(foo_pipeline_inited)
    for step in foo_pipeline_inited.steps:
        assert step.state == StepState.FAILED


def test_stop_pipeline_with_nonmatching_runner_fail(foo_runner, foo_pipeline_inited):
    foo_runner.run_pipeline(foo_pipeline_inited)
    foo_runner.runner = "foobar"
    with pytest.raises(ValueError):  # noqa: PT011
        foo_runner.stop_pipeline(foo_pipeline_inited)


def test_submit_step(foo_runner_for_step_submit, foo_step_inited):
    foo_runner_for_step_submit.submit_step(foo_step_inited)
    time.sleep(SLEEP_TIME_WAIT)
    assert foo_step_inited.state in (StepState.SUBMITTED, StepState.RUNNING)


def test_submitted_step_running(foo_runner_for_step_submit, foo_step_inited):
    foo_runner_for_step_submit.submit_step(foo_step_inited)
    while foo_step_inited.state == StepState.SUBMITTED:
        time.sleep(SLEEP_TIME_WAIT)
    sub_info = foo_runner_for_step_submit.load_submission_info(foo_step_inited)
    for target_path in foo_step_inited.get_command_targets():
        assert str(target_path) in [subtask["file_path"] for subtask in sub_info["subtasks"]]


def test_submit_step_submission_info_structure(foo_runner_for_step_submit, foo_step_inited):
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
    foo_step_inited.sleep_time = SLEEP_TIME_LONG
    foo_step_inited.save_parameters()

    sub_info = foo_runner_for_step_submit.submit_step(foo_step_inited)
    time.sleep(SLEEP_TIME_WAIT)

    foo_runner_for_step_submit.cancel_task(sub_info["main_task"])
    time.sleep(SLEEP_TIME_WAIT)

    assert foo_step_inited.state == StepState.FAILED
    for t_info in sub_info["subtasks"]:
        assert not Path(t_info["file_path"]).exists()


def test_submit_running_step(foo_runner_for_step_submit, foo_step_inited):
    foo_step_inited.sleep_time = SLEEP_TIME_LONG
    foo_step_inited.save_parameters()
    sub_info = foo_runner_for_step_submit.submit_step(foo_step_inited)
    submit_again_sub_info = foo_runner_for_step_submit.submit_step(foo_step_inited)
    assert sub_info == submit_again_sub_info


@pytest.mark.parametrize("keep_finished", [True, False])
def test_submit_failed_step(foo_runner_for_step_submit, foo_step_inited, keep_finished):
    foo_step_inited.state = StepState.FAILED
    files = foo_step_inited.get_command_targets()

    failed_str = "FAILED"
    with open_file(files[0], "w") as fh:
        print(failed_str, file=fh)
    sub_info = foo_runner_for_step_submit.submit_step(foo_step_inited, keep_finished=keep_finished)
    time.sleep(SLEEP_TIME_WAIT)

    foo_runner_for_step_submit.wait_for_tasks([sub_info["main_task"]])
    assert foo_step_inited.state == StepState.DONE

    for file in files:
        assert file.exists()

    file_output = open_file(files[0], "r").readline().strip("\n")
    if keep_finished:
        assert file_output == failed_str
    else:
        assert file_output == files[0].stem + files[0].suffix


def test_submit_step_with_dependency(foo_runner_for_step_submit, bar_step_inited):
    foo_runner_for_step_submit.submit_step(bar_step_inited)
    time.sleep(SLEEP_TIME_WAIT)
    for step in [bar_step_inited, bar_step_inited.dep_step]:
        assert step.state in (StepState.SUBMITTED, StepState.RUNNING)


@pytest.mark.parametrize("keep_finished", [True, False])
def test_resubmit_step(foo_runner, foo_pipeline_inited, keep_finished):
    if foo_runner.runner == "bash":
        pytest.skip(reason="Not supported by Bash runner.")

    bar_step_inited = foo_pipeline_inited.default_targets[0]
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

    new_foo_sub_info = foo_runner.resubmit_step(bar_step_inited.dep_step, keep_finished=keep_finished)
    time.sleep(SLEEP_TIME_WAIT)
    assert foo_sub_info["main_task"]["id"] != new_foo_sub_info["main_task"]["id"]

    foo_runner.wait_for_tasks([bar_sub_info["main_task"]])
    for step in [foo_step_inited, bar_step_inited]:
        assert step.state == StepState.DONE
