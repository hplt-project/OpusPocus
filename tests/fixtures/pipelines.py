import pytest

import yaml
from argparse import Namespace
from pathlib import Path

import opuspocus.pipeline_steps as pipeline_steps
from opuspocus.pipelines import build_pipeline
from opuspocus.runners.debug import DebugRunner

# NOTE(varisd): module-level (and lower) pipeline fixtures need to reset
#   pipeline_step registry to work correctly


@pytest.fixture(scope="module")
def pipeline_preprocess_tiny_inited(
    pipeline_preprocess_tiny_config_file,
    tmp_path_factory,
):
    """Initialize mock dataset preprocessing pipeline."""
    setattr(pipeline_steps, "STEP_INSTANCE_REGISTRY", {})
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
    runner.run_pipeline(
        pipeline_preprocess_tiny_inited,
        pipeline_preprocess_tiny_inited.get_targets()
    )
    return pipeline_preprocess_tiny_inited


@pytest.fixture(scope="module")
def pipeline_train_tiny_inited(
    pipeline_train_tiny_config_file,
    tmp_path_factory,
):
    """Initialize mock training pipeline."""
    setattr(pipeline_steps, "STEP_INSTANCE_REGISTRY", {})
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
    runner.run_pipeline(
        pipeline_train_tiny_inited,
        pipeline_train_tiny_inited.get_targets(),
    )
    return pipeline_train_tiny_inited
