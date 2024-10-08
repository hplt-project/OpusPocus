import pytest

from opuspocus import pipeline_steps, runners

pytest_plugins = [
    "fixtures.configs",
    "fixtures.data",
    "fixtures.directories",
    "fixtures.pipelines",
    "fixtures.runners",
    "fixtures.steps",
]


@pytest.fixture()
def clear_instance_registry(monkeypatch):  # noqa: PT004
    """Clear the initialized step instances (to reuse step labels)."""
    monkeypatch.setattr(pipeline_steps, "STEP_INSTANCE_REGISTRY", {})


@pytest.fixture()
def clear_registries(monkeypatch):  # noqa: PT004
    """Clear all the registries."""
    monkeypatch.setattr(pipeline_steps, "STEP_REGISTRY", {})
    monkeypatch.setattr(pipeline_steps, "STEP_CLASS_NAMES", set())

    monkeypatch.setattr(runners, "RUNNER_REGISTRY", {})
    monkeypatch.setattr(runners, "RUNNER_CLASS_NAMES", set())


@pytest.fixture(scope="session")
def languages():
    """Default languages."""
    return ("en", "fr")
