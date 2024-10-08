import pytest

from opuspocus import pipeline_steps, runners

# TODO(varisd): merge the register_step/register_runner test into a single
#   parameterized testsuite

# @register_step


@pytest.fixture(scope="module")
def foo_step_cls():
    """Mock class that inherits from OpusPocusStep."""

    class FooStep(pipeline_steps.OpusPocusStep):
        pass

    return FooStep


def test_register_step_name(clear_registries, foo_step_cls):  # noqa: ARG001
    """Tests step class name registration."""
    step_name = "foo"
    pipeline_steps.register_step(step_name)(foo_step_cls)
    assert step_name in pipeline_steps.STEP_REGISTRY


def test_register_step_incorrect_subclass_fail(clear_registries):  # noqa: ARG001
    """Fail when the registered step class does not inherit from
    OpusPocusStep.
    """

    class FooStep:
        pass

    with pytest.raises(TypeError):
        pipeline_steps.register_step("foo")(FooStep)


def test_register_step_duplicate_class_fail(clear_registries, foo_step_cls):  # noqa: ARG001
    """Fail when trying to register duplicate step class."""
    pipeline_steps.register_step("foo")(foo_step_cls)
    with pytest.raises(ValueError):  # noqa: PT011
        pipeline_steps.register_step("bar")(foo_step_cls)


def test_register_step_duplicate_name_fail(clear_registries, foo_step_cls):  # noqa: ARG001
    """Fail when trying to register duplicate step name."""

    class BarStep(pipeline_steps.OpusPocusStep):
        pass

    pipeline_steps.register_step("foo")(foo_step_cls)
    with pytest.raises(ValueError):  # noqa: PT011
        pipeline_steps.register_step("foo")(BarStep)


# @register_runner


@pytest.fixture(scope="module")
def foo_runner_cls():
    """Mock class that inherits from OpusPocusRunner."""

    class FooRunner(runners.OpusPocusRunner):
        pass

    return FooRunner


def test_register_runner_name(clear_registries, foo_runner_cls):  # noqa: ARG001
    """Tests runner class name registration."""
    runner_name = "foo"
    runners.register_runner(runner_name)(foo_runner_cls)
    assert runner_name in runners.RUNNER_REGISTRY


def test_register_runner_incorrect_subclass_fail(clear_registries):  # noqa: ARG001
    """Fail when the registered runner class does not inherit from
    OpusPocusRunner.
    """

    class FooRunner:
        pass

    with pytest.raises(TypeError):
        runners.register_runner("foo")(FooRunner)


def test_register_runner_duplicate_class_fail(clear_registries, foo_runner_cls):  # noqa: ARG001
    """Fail when trying to register duplicate runner class."""
    runners.register_runner("foo")(foo_runner_cls)
    with pytest.raises(ValueError):  # noqa: PT011
        runners.register_runner("bar")(foo_runner_cls)


def test_register_runner_duplicate_name_fail(clear_registries, foo_runner_cls):  # noqa: ARG001
    """Fail when trying to register duplicate runner name."""

    class BarRunner(runners.OpusPocusRunner):
        pass

    runners.register_runner("foo")(foo_runner_cls)
    with pytest.raises(ValueError):  # noqa: PT011
        runners.register_runner("foo")(BarRunner)
