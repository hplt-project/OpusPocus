import pytest
import opuspocus.pipeline_steps as pipeline_steps
import opuspocus.runners as runners

# TODO: merge the register_step/register_runner test into a single
# parameterized testsuite

# @register_step

@pytest.fixture(scope="module")
def foo_step_cls():
    """Mock class that inherits from OpusPocusStep."""
    class FooStep(pipeline_steps.OpusPocusStep):
        pass
    return FooStep


@pytest.fixture(scope="function")
def foo_step_registries(monkeypatch):
    """Test the decorator with clean registry."""
    monkeypatch.setattr(pipeline_steps, "STEP_REGISTRY", {})
    monkeypatch.setattr(pipeline_steps, "STEP_CLASS_NAMES", set())


def test_register_step_name(foo_step_registries, foo_step_cls):
    step_name = "foo"
    pipeline_steps.register_step(step_name)(foo_step_cls)
    assert step_name in pipeline_steps.STEP_REGISTRY


def test_register_step_correct_subclass(foo_step_registries):
    class FooStep():
        pass
    with pytest.raises(ValueError) as e_info:
        pipeline_steps.register_step("foo")(FooStep)


def test_register_step_duplicate_class(foo_step_registries, foo_step_cls):
    pipeline_steps.register_step("foo")(foo_step_cls)
    with pytest.raises(ValueError) as e_info:
        pipeline_steps.register_step("bar")(foo_step_cls)


def test_register_step_duplicate_name(foo_step_registries, foo_step_cls):
    class BarStep(pipeline_steps.OpusPocusStep):
        pass
    pipeline_steps.register_step("foo")(foo_step_cls)
    with pytest.raises(ValueError) as e_info:
        pipeline_steps.register_step("foo")(BarStep)


# @register_runner

@pytest.fixture(scope="module")
def foo_runner_cls():
    """Mock class that inherits from OpusPocusRunner."""
    class FooRunner(runners.OpusPocusRunner):
        pass
    return FooRunner


@pytest.fixture(scope="function")
def foo_runner_registries(monkeypatch):
    """Test the decorator with clean registry."""
    monkeypatch.setattr(runners, "RUNNER_REGISTRY", {})
    monkeypatch.setattr(runners, "RUNNER_CLASS_NAMES", set())


def test_register_runner_name(foo_runner_registries, foo_runner_cls):
    runner_name = "foo"
    runners.register_runner(runner_name)(foo_runner_cls)
    assert runner_name in runners.RUNNER_REGISTRY


def test_register_runner_correct_subclass(foo_runner_registries):
    class FooRunner():
        pass
    with pytest.raises(ValueError) as e_info:
        runners.register_runner("foo")(FooRunner)


def test_register_runner_duplicate_class(foo_runner_registries, foo_runner_cls):
    runners.register_runner("foo")(foo_runner_cls)
    with pytest.raises(ValueError) as e_info:
        runners.register_runner("bar")(foo_runner_cls)


def test_register_runner_duplicate_name(foo_runner_registries, foo_runner_cls):
    class BarRunner(runners.OpusPocusRunner):
        pass
    runners.register_runner("foo")(foo_runner_cls)
    with pytest.raises(ValueError) as e_info:
        runners.register_runner("foo")(BarRunner)