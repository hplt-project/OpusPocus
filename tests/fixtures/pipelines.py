from argparse import Namespace
from pathlib import Path

import pytest

from opuspocus import pipeline_steps
from opuspocus.pipeline_steps import OpusPocusStep, StepState
from opuspocus.pipelines import OpusPocusPipeline, PipelineConfig, build_pipeline
from opuspocus.runners import build_runner
from opuspocus.runners.debug import DebugRunner

# NOTE(varisd): module-level (and lower) pipeline fixtures need to reset
#   pipeline_step registry to work correctly


class FooPipeline(OpusPocusPipeline):
    def __init__(self, bar_step: OpusPocusStep) -> None:
        foo_step = bar_step.dep_step
        self.pipeline_graph = {
            foo_step.step_label: foo_step,
            bar_step.step_label: bar_step,
        }
        self.default_targets = [bar_step]
        self.pipeline_config = PipelineConfig.create(
            bar_step.pipeline_dir,
            self.pipeline_graph,
            self.default_targets,
        )


@pytest.fixture()
def foo_pipeline(bar_step):
    return FooPipeline(bar_step)


@pytest.fixture()
def foo_pipeline_config_file(foo_pipeline, tmp_path_factory):
    config_file = Path(tmp_path_factory.mktemp("foo_config"), "config.yaml")
    PipelineConfig.save(foo_pipeline.pipeline_config, config_file)
    return config_file


@pytest.fixture()
def foo_pipeline_inited(foo_pipeline):
    foo_pipeline.init()
    return foo_pipeline


@pytest.fixture()
def foo_pipeline_partially_inited(foo_pipeline_inited):
    foo_pipeline_inited.steps[0].state = StepState.INIT_INCOMPLETE
    return foo_pipeline_inited


@pytest.fixture()
def foo_pipeline_running(foo_pipeline_inited):
    for s in foo_pipeline_inited.steps:
        s.SLEEP_TIME = 180
    pipeline_dir = foo_pipeline_inited.pipeline_dir
    runner = build_runner(
        "bash",
        pipeline_dir,
        Namespace(**{"runner": "bash", "pipeline_dir": str(pipeline_dir), "run_tasks_in_parallel": False}),
    )
    runner.run_pipeline(foo_pipeline_inited, foo_pipeline_inited.get_targets())
    return foo_pipeline_inited


@pytest.fixture()
def foo_pipeline_done(foo_pipeline_inited):
    pipeline_dir = foo_pipeline_inited.pipeline_dir
    runner = build_runner(
        "bash",
        pipeline_dir,
        Namespace(**{"runner": "bash", "pipeline_dir": str(pipeline_dir), "run_tasks_in_parallel": False}),
    )
    runner.run_pipeline(foo_pipeline_inited, foo_pipeline_inited.get_targets())
    tasks = [runner.load_submission_info(s)["main_task"] for s in foo_pipeline_inited.steps]
    runner.wait_for_tasks(tasks)
    return foo_pipeline_inited


@pytest.fixture()
def foo_pipeline_failed(foo_pipeline_done):
    for s in foo_pipeline_done.steps:
        s.state = StepState.FAILED
    return foo_pipeline_done


@pytest.fixture(scope="module")
def pipeline_preprocess_tiny_inited(
    pipeline_preprocess_tiny_config_file,
    tmp_path_factory,
):
    """Initialize mock dataset preprocessing pipeline."""
    pipeline_steps.STEP_INSTANCE_REGISTRY = {}
    pipeline_dir = Path(tmp_path_factory.mktemp("pipeline_prerocess_tiny"))
    args = Namespace(
        **{
            "pipeline_config": pipeline_preprocess_tiny_config_file,
            "pipeline_dir": pipeline_dir,
        }
    )
    pipeline = build_pipeline(args)
    pipeline.init()
    return pipeline


@pytest.fixture(scope="module")
def pipeline_preprocess_tiny_done(pipeline_preprocess_tiny_inited):
    """Run mock dataset preprocessing pipeline."""
    runner = DebugRunner("debug", pipeline_preprocess_tiny_inited.pipeline_dir)
    runner.run_pipeline(pipeline_preprocess_tiny_inited)
    return pipeline_preprocess_tiny_inited


@pytest.fixture(scope="module")
def pipeline_train_tiny_inited(
    pipeline_train_tiny_config_file,
    tmp_path_factory,
):
    """Initialize mock training pipeline."""
    pipeline_steps.STEP_INSTANCE_REGISTRY = {}
    pipeline_dir = Path(tmp_path_factory.mktemp("pipeline_train_tiny"))
    args = Namespace(
        **{
            "pipeline_config": pipeline_train_tiny_config_file,
            "pipeline_dir": pipeline_dir,
        }
    )
    pipeline = build_pipeline(args)
    pipeline.init()
    return pipeline


@pytest.fixture(scope="module")
def pipeline_train_tiny_done(pipeline_train_tiny_inited):
    """Run mock training pipeline."""
    runner = DebugRunner("debug", pipeline_train_tiny_inited.pipeline_dir)
    runner.run_pipeline(pipeline_train_tiny_inited)
    return pipeline_train_tiny_inited
