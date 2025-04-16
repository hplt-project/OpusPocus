from argparse import Namespace
from pathlib import Path

import pytest

from opuspocus.config import PipelineConfig
from opuspocus.pipelines import build_pipeline, load_pipeline

# TODO(varisd): add more tests:
#   - test pipeline graph (building, structure comparison, cycle check, etc.)


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
    assert pipeline.pipeline_graph == pipeline_preprocess_tiny_inited.pipeline_graph


def test_load_pipeline_method(pipeline_preprocess_tiny_inited):
    """Load previously created pipeline and compare the two instances."""
    args = Namespace(**{"pipeline_dir": Path(pipeline_preprocess_tiny_inited.pipeline_dir)})
    pipeline = load_pipeline(args)
    assert pipeline.pipeline_graph == pipeline_preprocess_tiny_inited.pipeline_graph


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
                pipeline_preprocess_tiny_inited._config_file,  # noqa: SLF001
            ),
        }
    )
    with pytest.raises(NotADirectoryError):
        load_pipeline(args)


def test_pipeline_class_init_graph(pipeline_preprocess_tiny_config_file, pipeline_preprocess_tiny_inited):
    """Compare pipeline steps with the steps declared in config file."""
    config = PipelineConfig.load(pipeline_preprocess_tiny_config_file)
    config_labels = [s["step_label"] for s in config.pipeline["steps"]]
    assert len(config_labels) == len(pipeline_preprocess_tiny_inited.steps)
    for s in pipeline_preprocess_tiny_inited.steps:
        assert s.step_label in config_labels


def test_pipeline_class_init_default_targets(pipeline_preprocess_tiny_config_file, pipeline_preprocess_tiny_inited):
    """Compare pipeline targets with targets declared in config file."""
    config = PipelineConfig.load(pipeline_preprocess_tiny_config_file)
    config_targets = config.pipeline["targets"]
    assert len(config_targets) == len(pipeline_preprocess_tiny_inited.targets)
    for target in pipeline_preprocess_tiny_inited.targets:
        assert target.step_label in config_targets


def test_get_pipeline_targets(pipeline_preprocess_tiny_config_file, pipeline_preprocess_tiny_inited):
    """Pipeline returns target steps (step: OpusPocusStep) given the step labels (step_label: str)."""
    config = PipelineConfig.load(pipeline_preprocess_tiny_config_file)
    config_targets = config.pipeline["targets"]
    for target in pipeline_preprocess_tiny_inited.get_targets(config_targets):
        assert target in pipeline_preprocess_tiny_inited.targets


def test_get_unknown_pipeline_target_step_fail(pipeline_preprocess_tiny_inited):
    """Fail if a target step with unknown step_label was requested."""
    with pytest.raises(ValueError):  # noqa: PT011
        pipeline_preprocess_tiny_inited.get_targets("foo")
