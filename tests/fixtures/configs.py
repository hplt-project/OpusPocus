from pathlib import Path

import pytest
import yaml

PIPELINE_TRAIN_CONFIGS = [
    Path("config", "pipeline.train.simple.yml"),
    Path("config", "pipeline.train.backtranslation.yml"),
]


@pytest.fixture(scope="session")
def marian_tiny_config_file(config_dir):
    """Prepares small-data config for marian training."""
    # TODO(varisd): parametrize the fixture to test other (tiny)
    #   configurations?

    with Path("config", "marian.train.teacher.base.yml").open("r") as fh:
        config = yaml.safe_load(fh)

    config["workspace"] = 500

    config["dim-emb"] = 32
    config["transformer-heads"] = 4
    config["transformer-dim-ffn"] = 128
    config["transformer-dim-aan"] = 64
    config["max-length"] = 50

    config["mini-batch"] = 1
    config["disp-freq"] = "10u"
    config["save-freq"] = "20u"
    config["valid-freq"] = "5u"
    config["early-stopping"] = 2

    config["valid-mini-batch"] = 2
    config["valid-max-length"] = 100

    config_file = Path(config_dir, "marian.train.teacher.tiny.yml")
    with config_file.open("w") as fh:
        yaml.dump(config, fh)
    return config_file


@pytest.fixture(scope="module")
def pipeline_preprocess_tiny_config_file(
    config_dir,
    train_data_parallel_tiny,
    train_data_parallel_tiny_decompressed,
    languages,
    tmp_path_factory,
):
    """Prepares small-data preprocessing pipeline config for unit testing."""
    with Path("config", "pipeline.preprocess.yml").open("r") as fh:
        config = yaml.safe_load(fh)
    config["pipeline"]["pipeline_dir"] = str(tmp_path_factory.mktemp("pipeline_preprocess_tiny_default"))

    config["pipeline"]["src_lang"] = languages[0]
    config["pipeline"]["tgt_lang"] = languages[1]

    data_dir = str(train_data_parallel_tiny[0].parent)
    config["pipeline"]["raw_para_dir"] = data_dir
    config["pipeline"]["raw_mono_src_dir"] = data_dir
    config["pipeline"]["raw_mono_tgt_dir"] = data_dir

    data_dir_decompressed = str(train_data_parallel_tiny_decompressed[0].parent)
    config["pipeline"]["valid_data_dir"] = data_dir_decompressed
    config["pipeline"]["test_data_dir"] = data_dir_decompressed

    config_file = Path(config_dir, "pipeline.preprocess.tiny.yml")
    yaml.dump(config, config_file.open("w"))
    return config_file


@pytest.fixture(params=PIPELINE_TRAIN_CONFIGS)
def pipeline_train_tiny_config_file(
    request,
    config_dir,
    marian_dir,
    marian_tiny_config_file,
    pipeline_preprocess_tiny_done,
    languages,
    tmp_path_factory,
):
    """Prepares small-data training pipeline config for unit testing."""
    with request.param.open("r") as fh:
        config = yaml.safe_load(fh)
    config["pipeline"]["pipeline_dir"] = str(tmp_path_factory.mktemp("pipeline_train_tiny_default"))

    config["pipeline"]["original_config_file"] = str(request.param)
    config["pipeline"]["src_lang"] = languages[0]
    config["pipeline"]["tgt_lang"] = languages[1]

    config["pipeline"]["preprocess_pipeline_dir"] = str(pipeline_preprocess_tiny_done.pipeline_dir)
    config["pipeline"]["marian_config"] = str(marian_tiny_config_file)
    config["pipeline"]["marian_dir"] = str(marian_dir)
    config["pipeline"]["max_epochs"] = 2

    # NOTE(varisd): value chosen to satisfy generate_vocab training
    config["pipeline"]["vocab_size"] = 275

    # NOTE(varisd): A bit hacky way of getting the test dataset name from
    #   the preprocessing pipeline
    for step in pipeline_preprocess_tiny_done.steps:
        if "test" in step.step_label:
            config["pipeline"]["valid_dataset"] = step.dataset_list[0]

    if "shard_size" in config["pipeline"]:
        config["pipeline"]["shard_size"] = 2

    config_file = Path(config_dir, "pipeline.train.tiny.yml")
    yaml.dump(config, config_file.open("w"))
    return config_file
