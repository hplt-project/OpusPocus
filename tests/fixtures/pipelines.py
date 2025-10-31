import time
from pathlib import Path

import pytest
from attrs import define, field, validators
from omegaconf import DictConfig

from opuspocus import pipeline_steps
from opuspocus.config import PipelineConfig
from opuspocus.pipeline_steps import OpusPocusStep, StepState
from opuspocus.pipelines import OpusPocusPipeline, build_pipeline
from opuspocus.runners import build_runner
from opuspocus.runners.debug import DebugRunner

# NOTE(varisd): we wait a bit at the end of each fixture to avoid
#       possible race conditions, e.g. task submission, state update
#       (is there a better solution)?
WAIT_TIME = 1


@define(kw_only=True)
class FooPipeline(OpusPocusPipeline):
    """Mock pipeline for lightweight unit testing."""

    bar_step: OpusPocusStep = field(validator=validators.instance_of(OpusPocusStep))
    foo_step: OpusPocusStep = field(validator=validators.instance_of(OpusPocusStep), init=False)
    pipeline_dir: Path = field(init=False)
    pipeline_config: PipelineConfig = field(init=False)

    @foo_step.default
    def _get_bar_steps_dep_step(self) -> OpusPocusStep:
        return self.bar_step.dep_step

    @pipeline_dir.default
    def _get_foo_pipeline_dir(self) -> Path:
        return self.bar_step.pipeline_dir

    @pipeline_config.default
    def _get_foo_config(self) -> PipelineConfig:
        return PipelineConfig.create(
            {
                "pipeline": {
                    "pipeline_dir": str(self.pipeline_dir),
                    "steps": [
                        self.foo_step.get_parameters_dict(exclude_dependencies=False),
                        self.bar_step.get_parameters_dict(exclude_dependencies=False),
                    ],
                    "targets": [self.bar_step.step_label],
                },
                "runner": {"runner": "bash"},
            }
        )


@pytest.fixture()
def foo_pipeline(bar_step):
    """Basic (two-step) mock pipeline."""
    pipeline_steps.STEP_INSTANCE_REGISTRY = {}
    return FooPipeline(bar_step=bar_step)


@pytest.fixture()
def foo_pipeline_config_file(foo_pipeline, tmp_path_factory):
    """Basic mock pipeline config file."""
    config_file = Path(tmp_path_factory.mktemp("foo_config"), "config.yaml")
    PipelineConfig.save(foo_pipeline.pipeline_config, config_file)
    return config_file


@pytest.fixture()
def foo_pipeline_inited(foo_pipeline):
    """Basic mock pipeline (INITED)."""
    foo_pipeline.init()
    time.sleep(WAIT_TIME)
    return foo_pipeline


@pytest.fixture()
def foo_pipeline_partially_inited(foo_pipeline_inited):
    """Basic mock pipeline (INIT_INCOMPLETE)."""
    foo_pipeline_inited.steps[0].state = StepState.INIT_INCOMPLETE
    time.sleep(WAIT_TIME)
    return foo_pipeline_inited


@pytest.fixture()
def foo_pipeline_running(foo_pipeline_inited):
    """Basic mock pipeline (RUNNING)."""
    for s in foo_pipeline_inited.steps:
        s.sleep_time = 180
        s.save_parameters()
    runner = build_runner(
        PipelineConfig.create(
            {
                "runner": {
                    "runner": "bash",
                    "run_tasks_in_parallel": True,
                    "runner_resources": None,
                },
                "pipeline": {"pipeline_dir": foo_pipeline_inited.pipeline_dir, "steps": []},
            }
        )
    )
    runner.run_pipeline(foo_pipeline_inited, None)
    time.sleep(WAIT_TIME)
    return foo_pipeline_inited


@pytest.fixture()
def foo_pipeline_done(foo_pipeline_inited):
    """Basic mock pipeline (DONE)."""
    runner = build_runner(
        PipelineConfig.create(
            {
                "runner": {"runner": "bash", "run_tasks_in_parallel": True, "runner_resources": None},
                "pipeline": {"pipeline_dir": foo_pipeline_inited.pipeline_dir, "steps": []},
            }
        )
    )
    runner.run_pipeline(foo_pipeline_inited)
    time.sleep(WAIT_TIME)
    tasks = [runner.load_submission_info(s)["main_task"] for s in foo_pipeline_inited.steps]
    runner.wait_for_tasks(tasks)
    return foo_pipeline_inited


@pytest.fixture()
def foo_pipeline_failed(foo_pipeline_done):
    """Basic mock pipeline (FAILED)."""
    for s in foo_pipeline_done.steps:
        s.state = StepState.FAILED
    time.sleep(WAIT_TIME)
    return foo_pipeline_done


@pytest.fixture()
def pipeline_preprocess_tiny(
    pipeline_preprocess_tiny_config_file,
    tmp_path_factory,
):
    """Mock Dataset Preprocessing pipeline."""
    pipeline_steps.STEP_INSTANCE_REGISTRY = {}
    pipeline_dir = Path(tmp_path_factory.mktemp("pipeline_preprocess_tiny"))
    config = PipelineConfig.load(
        pipeline_preprocess_tiny_config_file,
        DictConfig({"pipeline": {"pipeline_dir": pipeline_dir}, "runner": {"runner": "bash"}, "steps": []}),
    )
    return build_pipeline(config)


@pytest.fixture()
def pipeline_preprocess_tiny_inited(pipeline_preprocess_tiny):
    """Mock Dataset Preprocessing pipeline (INITED)."""
    pipeline_preprocess_tiny.init()
    return pipeline_preprocess_tiny


@pytest.fixture()
def pipeline_preprocess_tiny_done(pipeline_preprocess_tiny_inited):
    """Mock Dataset Preprocessing pipeline (DONE)."""
    runner = DebugRunner("debug", pipeline_preprocess_tiny_inited.pipeline_dir)
    runner.run_pipeline(pipeline_preprocess_tiny_inited)
    return pipeline_preprocess_tiny_inited


@pytest.fixture()
def pipeline_train_tiny(
    pipeline_train_tiny_config_file,
    tmp_path_factory,
):
    """Mock Training pipeline."""
    pipeline_steps.STEP_INSTANCE_REGISTRY = {}
    pipeline_dir = Path(tmp_path_factory.mktemp("pipeline_train_tiny"))
    config = PipelineConfig.load(
        pipeline_train_tiny_config_file, DictConfig({"pipeline": {"pipeline_dir": pipeline_dir}, "steps": []})
    )
    return build_pipeline(config)


@pytest.fixture()
def pipeline_train_tiny_inited(pipeline_train_tiny):
    """Mock Training pipeline (INITED)."""
    pipeline_train_tiny.init()
    return pipeline_train_tiny


@pytest.fixture()
def pipeline_train_tiny_done(pipeline_train_tiny_inited):
    """Mock Training pipeline (DONE)."""
    runner = DebugRunner("debug", pipeline_train_tiny_inited.pipeline_dir)
    runner.run_pipeline(pipeline_train_tiny_inited)
    return pipeline_train_tiny_inited
