import importlib
import py_compile
from pathlib import Path

import pytest

from opuspocus import pipeline_steps
from opuspocus.pipeline_steps import StepState, load_step
from opuspocus.runners import SubmissionInfo
from opuspocus.utils import open_file


def test_state_update_two_instances(foo_step_inited, monkeypatch):
    """Step state updates for all instances of a step (even the already loaded ones)."""
    foo_step_inited.state = StepState.FAILED

    monkeypatch.setattr(pipeline_steps, "STEP_INSTANCE_REGISTRY", {})
    new_foo_step = load_step(foo_step_inited.step_label, foo_step_inited.pipeline_dir)
    new_foo_step.state = StepState.DONE

    assert foo_step_inited.state == new_foo_step.state


@pytest.fixture()
def step_command_module(foo_step_inited, monkeypatch):
    """Reset the step registry and classes before each unit test run."""
    monkeypatch.setattr(
        pipeline_steps, "STEP_REGISTRY", {k: v for k, v in pipeline_steps.STEP_REGISTRY.items() if "foo" not in k}
    )
    monkeypatch.setattr(
        pipeline_steps, "STEP_CLASS_NAMES", {v for v in pipeline_steps.STEP_CLASS_NAMES if "Foo" not in v}
    )

    cmd_file = Path(foo_step_inited.step_dir, foo_step_inited.command_file)
    loader = importlib.machinery.SourceFileLoader("step_command", str(cmd_file))
    spec = importlib.util.spec_from_loader("step_command", loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def test_cmd_file_exists(foo_step_inited):
    """Command file exists after init_step() call."""
    assert Path(foo_step_inited.step_dir, foo_step_inited.command_file).exists()


def test_cmd_file_syntax_valid(foo_step_inited):
    """Command file does not have syntax errors."""
    cmd_file = Path(foo_step_inited.step_dir, foo_step_inited.command_file)
    py_compile.compile(cmd_file)
    assert True


@pytest.mark.parametrize("partially_done", [False, True])
def test_cmd_file_execute_main(step_command_module, foo_step_inited, foo_runner, partially_done):
    """Command file's main task method executes correctly without issues (standalone, no runner)."""
    foo_runner.save_parameters()
    foo_step_inited.SLEEP_TIME = 1

    finished_target = None
    if partially_done:
        finished_target = foo_step_inited.get_command_targets()[0]
        finished_target_str = "PREVIOUSLY_FINISHED"
        print(finished_target_str, file=open_file(finished_target, "w"))

    foo_runner.save_submission_info(
        foo_step_inited, SubmissionInfo(runner=foo_runner.runner, main_task=None, subtasks=[])
    )
    step_command_module.main(["foo_cmd"])
    assert foo_step_inited.state == StepState.DONE
    for i, target_file in enumerate(foo_step_inited.get_command_targets()):
        assert target_file.exists()
        output_str = open_file(target_file, "r").readline().rstrip("\n")
        if i == 0 and finished_target is not None:
            assert output_str == finished_target_str
        else:
            assert output_str == foo_step_inited.get_output_str(target_file)


def test_cmd_file_execute_sub(step_command_module, foo_step_inited):
    """Command file's subtask method executes correctly without issues (standalone, no runner)."""
    foo_step_inited.SLEEP_TIME = 1

    target_file = foo_step_inited.get_command_targets()[0]
    assert not target_file.exists()

    step_command_module.main(["foo_cmd", str(target_file)])
    assert target_file.exists()
    assert open_file(target_file, "r").readline().rstrip("\n") == foo_step_inited.get_output_str(target_file)
