import pytest
from argparse import Namespace
from opuspocus.runners import (
    OpusPocusRunner,
    build_runner,
    load_runner,
    RUNNER_REGISTRY
)


@pytest.fixture(scope="function", params=RUNNER_REGISTRY.keys())
def runner_default(runner, pipeline_minimal_initialized):
    return build_runner(
        runner.param,
        pipeline_minimal_initialized.pipeline_dir,
    )


@pytest.fixture(scope="function")
def runner_bash():
    return build_runner(
        "bash",
        pipeline_minimal_initialized.pipeline_dir,
    )


def test_parameter_save_load(runner_default):
    runner_default.save_parameters()
    params_load = runner_default.load_parameters()
#    for runner_default


#def test_list_parameters():


#def test_load_runner_method():


#def test_pipeline_class_...():
