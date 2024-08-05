import pytest

import yaml
from argparse import Namespace
from pathlib import Path

from opuspocus.pipelines import build_pipeline, load_pipeline, PipelineConfig

# TODO(varisd): add more tests:
#   - test pipeline graph (building, structure comparison, cycle check, etc.)
#   - test status/traceback


def test_build_pipeline_method(
    pipeline_preprocess_tiny_config_file,
    pipeline_preprocess_tiny_inited,
    tmp_path_factory,
):
    """Build pipeline and compare it to a direct contructor call result."""
    pipeline_dir = Path(tmp_path_factory.mktemp("pipeline_preprocess_tiny"))
    args = Namespace(
        **{
            "pipeline_config": pipeline_preprocess_tiny_config_file,
            "pipeline_dir": pipeline_dir,
        }
    )
    pipeline = build_pipeline(args)
    pipeline.init()
    assert pipeline == pipeline_preprocess_tiny_inited


def test_load_pipeline_method(pipeline_preprocess_tiny_inited):
    """Load previously created pipeline and compare the two instances."""
    args = Namespace(
        **{"pipeline_dir": Path(pipeline_preprocess_tiny_inited.pipeline_dir)}
    )
    pipeline = load_pipeline(args)
    assert pipeline == pipeline_preprocess_tiny_inited


def test_load_pipeline_dir_not_exist():
    """Fail when trying to load pipeline not previously inited."""
    args = Namespace(**{"pipeline_dir": Path("nonexistent", "directory")})
    with pytest.raises(FileNotFoundError):
        load_pipeline(args)


def test_load_pipeline_dir_not_directory(pipeline_preprocess_tiny_inited):
    """Fail when trying to load pipeline from invalid directory."""
    args = Namespace(
        **{
            "pipeline_dir": Path(
                pipeline_preprocess_tiny_inited.pipeline_dir,
                pipeline_preprocess_tiny_inited.config_file,
            ),
        }
    )
    with pytest.raises(NotADirectoryError):
        load_pipeline(args)


def test_pipeline_class_init_graph(
    pipeline_preprocess_tiny_config_file, pipeline_preprocess_tiny_inited
):
    """Compare pipeline steps with the steps declared in config file."""
    config = PipelineConfig.load(pipeline_preprocess_tiny_config_file)
    config_labels = [s["step_label"] for s in config["pipeline"]["steps"]]
    assert len(config_labels) == len(pipeline_preprocess_tiny_inited.steps)
    for s in pipeline_preprocess_tiny_inited.steps:
        assert s.step_label in config_labels


def test_pipeline_class_init_default_targets(
    pipeline_preprocess_tiny_config_file, pipeline_preprocess_tiny_inited
):
    """Compare pipeline targets with targets declared in config file."""
    config = PipelineConfig.load(pipeline_preprocess_tiny_config_file)
    config_targets = config["pipeline"]["default_targets"]
    assert len(config_targets) == len(pipeline_preprocess_tiny_inited.default_targets)
    for target in pipeline_preprocess_tiny_inited.default_targets:
        assert target.step_label in config_targets
