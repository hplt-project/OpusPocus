import pytest
from argparse import Namespace
from pathlib import Path

from opuspocus.pipelines import build_pipeline, load_pipeline


def test_build_pipeline_method(config_minimal, pipeline_minimal, pipeline_dir):
    args = Namespace(**{"pipeline_config": config_minimal, "pipline_dir": pipeline_dir})
    pipeline = build_pipeline(args)
    assert pipeline == pipeline_minimal


def test_load_pipeline_method(pipeline_minimal_initialized):
    args = Namespace(**{"pipeline_dir": pipeline_minimal_initialized.pipeline_dir})
    pipeline = load_pipeline(args)
    assert pipeline == pipeline_minimal_initialized


def test_load_pipeline_dir_not_exist():
    args = Namespace(**{"pipeline_dir": Path("nonexistent", "directory")})
    with pytest.raises(FileNotFoundError):
        load_pipeline(args)


def test_load_pipeline_dir_not_directory(tmp_path):
    args = Namespace(
        **{
            "pipeline_dir": tmp_path,
        }
    )
    with pytest.raises(NotADirectoryError):
        load_pipeline(args)


def test_pipeline_class_init_graph(config_minimal, pipeline_minimal):
    config_labels = [s["step_label"] for s in config_minimal["pipeline"]["steps"]]
    assert len(config_labels) == len(pipeline_minimal.steps)
    for s in pipeline_minimal.steps:
        assert s.step_label in config_labels


def test_pipeline_class_init_default_targets(config_minimal, pipeline_minimal):
    config_targets = config_minimal["pipeline"]["default_targets"]
    assert len(config_targets) == len(pipeline_minimal.default_targets)
    for target_str in pipeline_minimal.default_steps:
        assert target_str in config_targets


# TODO: test build_pipeline_graph
# - normal build
# - compare structure (dependencies in config and in graph)
# - missing req
# - cycles


# TODO: test status/traceback?
