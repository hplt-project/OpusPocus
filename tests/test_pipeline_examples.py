import pytest
from pathlib import Path

from opuspocus.pipeline_steps import StepState
from opuspocus.pipelines import build_pipeline, load_pipeline
from opuspocus.runners import build_runner


PIPELINE_TRAIN_CONFIGS = [
    Path("config", "pipeline.train.simple.yml"),
    Path("config", "pipeline.train.backtranslation.yml")
]


## DATA PREPROCESSING ##

@pytest.fixture(scope="module")
def pipeline_preprocess_config(
    data_train_minimal,
    data_train_minimal_decompressed,
    languages
):
    """Edit example config file using mock dataset."""
    config_file = Path("config", "pipeline.preprocess.yaml")
    config = yaml.safe_load(open(config_file, "r"))

    config["global"]["src_lang"] = languages[0]
    config["global"]["tgt_lang"] = languages[1]

    data_dir = data_train_minimal[0].parent
    config["global"]["raw_para_dir"] = data_dir
    config["global"]["raw_mono_src_dir"] = data_dir
    config["global"]["raw_mono_tgt_dir"] = data_dir

    data_dir_decompressed = data_train_minimal_decompressed[0].parent
    config["global"]["valid_data_dir"] = data_dir_decompressed
    config["global"]["test_data_dir"] = data_dir_decompressed

    config["global"]["valid_dataset"] = data_train_minimal_decompressed[0].stem

    return config


@pytest.fixutre(scope="module")
def pipeline_preprocess_init(pipeline_preprocess_config, tmp_path_factory):
    """Initialize mock dataset preprocessing pipeline."""
    args = Namespace(**{
        "pipeline_config": pipeline_preprocess_config,
        "pipeline_dir": tmp_path_factory.mktemp("test_pipeline_preprocess")
    })
    pipeline = build_pipeline(args)
    pipeline.init()
    return pipeline


def test_pipeline_preprocess_init(pipeline_preprocess_init):
    """Test mock dataset preprocessing initialization."""
    for step in pipeline_preprocess_init.steps:
        assert step.state == StepState.INITED


@pytest.fixture(scope="module")
def pipeline_preprocess_done(pipeline_preprocess_init):
    """Run mock dataset preprocessing pipeline."""
    runner = build_runner("bash", pipeline_preprocess_init.pipeline_dir)
    runner.run_pipeline()
    runner.wait_for_tasks()
    return pipeline_preprocess_init


def test_pipeline_preprocess_done(pipeline_preprocess_done):
    """Test whether all mock data pipeline steps finished successfully."""
    for step in pipeline_preprocess_done.steps:
        assert step.state == StepState.DONE


## MODEL TRAINING ##

@pytest.fixture(scope="module", params=PIPELINE_TRAIN_CONFIGS)
def pipeline_train_config(request, pipeline_preprocess_done)
    config = yaml.safe_load(open(request.param, "r"))

    config["global"]["preprocess_pipeline_dir"] = pipeline_preprocess_done.pipeline_dir
    config["global"]["valid_dataset"] = test_pipeline_preprocess_done.pipeline_config["global"]["valid_dataset"]

    return config


@pytest.fixture(scope="module")
def pipeline_train_init(pipeline_train_config, tmp_path_factory):
    args = Namespace(**{
        "pipeline_config": pipeline_train_config,
        "pipeline_dir": tmp_path_factory.mktemp("test_pipeline_train")
    })
    pipeline = build_pipeline(args)
    pipeline.init()
    return pipeline


def test_pipeline_train_init(pipeline_train_init):
    for step in pipeline_train_init.steps:
        assert step.state == StepState.INITED


@pytest.fixture(scope="module")
def pipeline_train_done(pipeline_train_init):
    runner = build_runner("bash", pipeline_train_init.pipeline_dir)
    runner.run_pipeline()
    runner.wait_for_tasks()
    return pipeline_train_done


def test_pipeline_train_done(pipeline_train_done):
    for step in pipeline_train_done.steps:
        assert step.state == StepState.DONE
