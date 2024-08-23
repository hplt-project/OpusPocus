import pytest

import importlib
import py_compile
from pathlib import Path

from opuspocus import pipeline_steps
from opuspocus.pipeline_steps import StepState, load_step
from opuspocus.utils import open_file


def test_state_update_two_instances(foo_step_inited, monkeypatch):
    foo_step_inited.state = StepState.FAILED

    monkeypatch.setattr(pipeline_steps, "STEP_INSTANCE_REGISTRY", {})
    new_foo_step = load_step(foo_step_inited.step_label, foo_step_inited.pipeline_dir)
    new_foo_step.state = StepState.DONE

    assert foo_step_inited.state == new_foo_step.state


@pytest.fixture()
def step_command_module(foo_step_inited, monkeypatch):
    monkeypatch.setattr(
        pipeline_steps,
        "STEP_REGISTRY",
        {
            k: v for k, v in pipeline_steps.STEP_REGISTRY.items()
            if "foo" not in k
        }
    )
    monkeypatch.setattr(
        pipeline_steps,
        "STEP_CLASS_NAMES",
        set(v for v in pipeline_steps.STEP_CLASS_NAMES if "Foo" not in v)
    )

    cmd_file = Path(foo_step_inited.step_dir, foo_step_inited.command_file)
    loader = importlib.machinery.SourceFileLoader("step_command", str(cmd_file))
    spec = importlib.util.spec_from_loader("step_command", loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def test_cmd_file_exists(foo_step_inited):
    assert Path(foo_step_inited.step_dir, foo_step_inited.command_file).exists()


def test_cmd_file_syntax_valid(foo_step_inited):
    cmd_file = Path(foo_step_inited.step_dir, foo_step_inited.command_file)
    py_compile.compile(cmd_file)
    assert True


@pytest.mark.parametrize("partially_done", [False, True])
def test_cmd_file_execute_main(step_command_module, foo_step_inited, foo_runner, partially_done):
    foo_runner.save_parameters()
    foo_step_inited.SLEEP_TIME = 0

    finished_target = None
    if partially_done:
        finished_target = foo_step_inited.get_command_targets()[0]
        finished_target_str = "PREVIOUSLY_FINISHED"
        print(finished_target_str, file=open_file(finished_target, "w"))

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
    foo_step_inited.SLEEP_TIME = 0

    target_file = foo_step_inited.get_command_targets()[0]
    assert not target_file.exists()

    step_command_module.main(["foo_cmd", str(target_file)])
    assert target_file.exists()
    assert open_file(target_file, "r").readline().rstrip("\n") == foo_step_inited.get_output_str(target_file)


def test_command_file_partial_done_execute(step_command_module, foo_step_inited, foo_runner):
    foo_runner.save_parameters()
    step_command_module.main(["foo_cmd"])
