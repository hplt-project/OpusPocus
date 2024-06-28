from pathlib import Path
import pytest
import yaml

from opuspocus.pipelines import OpusPocusPipeline

# TODO: add parameterization to some of these methods


@pytest.fixture(scope="session")
def languages():
    return ("en", "fr")


@pytest.fixture(scope="session")
def pipeline_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("test_pipeline")


@pytest.fixture(scope="session")
def data_train_minimal(tmp_path_factory, languages):
    src_file = Path(
        tmp_path_factory.mktemp("data"),
        "-".join(languages),
        "minimal",
        "train.src"
    )
    src_file.parent.mkdir(parents=True)
    with open(src_file, "w") as fh:
        print("\n".join([
            "the colorless ideas slept furiously",
            "pooh slept all night",
            "working class hero is something to be",
            "I am the working class walrus",
            "walrus for president"
        ]), file=fh)

    tgt_file = Path(src_file.parent, "train.tgt")
    with open(tgt_file, "w") as fh:
        print("\n".join([
            "les idées incolores dormaient furieusement",
            "le caniche dormait toute la nuit",
            "le héros de la classe ouvrière est quelque chose à être",
            "Je suis le morse de la classe ouvrière",
            "morse pour président"
        ]), file=fh)
    return (src_file, tgt_file)


### Change the following when dataclasses are properly implemented

@pytest.fixture(scope="session")
def config_minimal(
    tmp_path_factory,
    data_train_minimal,
    pipeline_dir,
    languages
):
    config_file = Path(
        tmp_path_factory.mktemp("test_configs"), "config_minimal.yml"
    )
    src_file, tgt_file = data_train_minimal

    step_label = "raw." + "-".join(languages)
    config = {
        "pipeline": {
            "pipeline_dir": str(pipeline_dir),
            "steps" : [
                {
                    "step": "raw",
                    "step_label": step_label,
                    "src_lang": languages[0],
                    "tgt_lang": languages[1],
                    "raw_data_dir": str(data_train_minimal[0].parent)
                }
            ],
            "default_targets": [step_label]
        }
    }
    yaml.dump(config, open(config_file, "w"))
    return config_file

###

@pytest.fixture(scope="module")
def pipeline_minimal(config_minimal, pipeline_dir):
    return OpusPocusPipeline(config_minimal, pipeline_dir)


@pytest.fixture(scope="module")
def pipeline_minimal_initialized(pipeline_minimal):
    pipeline = pipeline_minimal.init()
    return pipeline
