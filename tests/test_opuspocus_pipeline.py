import pytest
from argparse import Namespace
from opuspocus.pipelines import build_pipeline, load_pipeline, OpusPocusPipeline
from tests.utils import minimal_config, pipeline_dir


@pytest.fixture(scope="module")
def minimal_pipeline(minimal_config, pipeline_dir):
    return OpusPocusPipeline(minimal_config, pipeline_dir)


@pytest.fixture(scope="module")
def initialized_minimal_pipeline(minimal_pipeline):
    pipeline = minimal_pipeline.init()
    return pipeline


def test_build_pipeline_method(minimal_config, minimal_pipeline, pipeline_dir):
    args = Namespace(**{
        "pipeline_config": minimal_config,
        "pipline_dir": pipeline_dir
    })
    pipeline = build_pipeline(args)
    assert pipeline == minimal_pipeline


def test_load_pipeline_method(initialized_minimal_pipeline):
    args = Namespace(**{
        "pipeline_dir": pipeline_dir
    })
    pipeline = load_pipeline(args)
    assert pipeline == saved_pipeline


def test_pipeline_class_init_graph(minimal_config, minimal_pipeline):
    config_labels = [
        s["step_label"]
        for s in minimal_config["pipeline"]["steps"]
    ]
    assert len(config_labels) == len(minimal_pipeline.steps)
    for s in minimal_pipeline.steps:
        assert s.step_label in config_labels


def test_pipeline_class_init_default_targets(minimal_config, minimal_pipeline):
    config_targets = minimal_config["pipeline"]["default_targets"]
    assert len(config_targets) == len(minimal_pipeline.default_targets)
    for target_str in minimal_pipeline.default_steps:
        assert target_str in config_targets


# TODO: test build_pipeline_graph
# - normal build
# - compare structure (dependencies in config and in graph)
# - missing req
# - cycles



# TODO: test status/traceback?
