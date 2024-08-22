from pathlib import Path

import pytest

from opuspocus.pipeline_steps import StepState
from opuspocus.runners import OpusPocusRunner, load_runner
from opuspocus.utils import open_file


def test_build_runner_method(foo_runner):
    """Create runner with default args."""
    assert isinstance(foo_runner, OpusPocusRunner)


def test_load_runner_before_save_fail(pipeline_preprocess_tiny_inited):
    """Fail loading runner that was not previously created."""
    with pytest.raises(FileNotFoundError):
        load_runner(pipeline_preprocess_tiny_inited.pipeline_dir)


def test_load_runner_method(foo_runner, foo_step_inited):
    """Reload runner for further pipeline execution manipulation."""
    foo_runner.save_parameters()

    runner_loaded = load_runner(foo_runner.pipeline_dir)
    assert foo_runner == runner_loaded


def test_run_pipeline(foo_runner, foo_pipeline_inited):
    foo_runner.run_pipeline(foo_pipeline_inited, foo_pipeline_inited.get_targets())
    for step in foo_pipeline_inited.steps:
        assert step.state in (StepState.SUBMITTED, StepState.RUNNING)


def test_stop_pipeline(foo_runner, foo_pipeline_inited):
    foo_runner.run_pipeline(foo_pipeline_inited, foo_pipeline_inited.get_targets())
    foo_runner.stop_pipeline(foo_pipeline_inited)
    for step in foo_pipeline_inited.steps:
        assert step.state == StepState.FAILED


def test_stop_pipeline_with_nonmatching_runner_fail(foo_runner, foo_pipeline_inited):
    foo_runner.run_pipeline(foo_pipeline_inited, foo_pipeline_inited.get_targets())
    foo_runner.runner = "foobar"
    with pytest.raises(ValueError):  # noqa: PT011
        foo_runner.stop_pipeline(foo_pipeline_inited)


def test_submit_step(foo_runner, foo_step_inited):
    foo_runner.submit_step(foo_step_inited)
    assert foo_step_inited.state in (StepState.SUBMITTED, StepState.RUNNING)


def test_submit_step_task_info_structure(foo_runner, foo_step_inited):
    # Split the asserts, create a fixture (move this to a different test file).
    task_info = foo_runner.submit_step(foo_step_inited)
    assert "runner" in task_info
    assert "main_task" in task_info
    assert "subtasks" in task_info
    assert task_info["main_task"]["file_path"] is None
    assert "id" in task_info["main_task"]
    for t_id in task_info["subtasks"]:
        assert t_id["file_path"] is not None
        assert "id" in t_id


def test_cancel_task(foo_step_inited, foo_runner):
    foo_runner.save_parameters()
    task_info = foo_runner.submit_step(foo_step_inited)
    import time

    time.sleep(2)

    for t_id in [*task_info["subtasks"], task_info["main_task"]]:
        foo_runner.cancel_task(t_id)
    for file in foo_step_inited.log_dir.iterdir():
        print(file)
        print("".join(open_file(file, "r").readlines()))

    assert foo_step_inited.state == StepState.FAILED
    for t_id in task_info:
        assert not Path(t_id.file_path).exists()


def test_submit_running_step(foo_step_inited, foo_runner):
    task_info = foo_runner.submit_step(foo_step_inited)
    resubmit_task_info = foo_runner.submit_step(foo_step_inited)
    assert task_info == resubmit_task_info


@pytest.mark.parametrize("keep_finished", [True, False])
def test_submit_failed_step(keep_finished, foo_step_inited, foo_runner):
    foo_step_inited.state = StepState.FAILED
    files = foo_step_inited.get_command_targets()

    failed_str = "FAILED"
    print(failed_str, file=open_file(files[0], "w"))
    task_info = foo_runner.submit_step(foo_step_inited, keep_finished=keep_finished)

    foo_runner.wait_for_tasks([task_info["main_task"]])
    assert foo_step_inited.state == StepState.DONE

    for file in files:
        assert file.exists()

    file_output = open_file(files[0], "r").readlines().strip("\n")
    if keep_finished:
        assert file_output == failed_str
    else:
        assert file_output == files[0].stem + files[0].suffix


def test_submit_step_with_dependency(bar_step_inited, foo_runner):
    foo_runner.submit_step(bar_step_inited)
    for step in [bar_step_inited, bar_step_inited.dep_step]:
        assert step.state in (StepState.SUBMITTED, StepState.RUNNING)


@pytest.mark.parametrize("keep_finished", [True, False])
def test_resubmit_task(keep_finished, bar_step_inited, foo_runner):
    foo_task_info = foo_runner.submit_step(bar_step_inited.dep_step)
    bar_task_info = foo_runner.submit_step(bar_step_inited)

    new_foo_task_info = foo_runner.resubmit_step(bar_step_inited.dep_step, keep_finished=keep_finished)
    assert foo_task_info["main_task"]["id"] != new_foo_task_info["main_task"]["id"]

    foo_runner.wait_for_tasks([bar_task_info["main_task"]])
    for step in [bar_step_inited, bar_step_inited.dep_step]:
        assert step.state == StepState.DONE
