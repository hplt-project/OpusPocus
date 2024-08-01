from pathlib import Path
import pytest
import yaml

import opuspocus.pipeline_steps as pipeline_steps
import opuspocus.runners as runners
from opuspocus.pipelines import OpusPocusPipeline
from opuspocus.runners.bash import BashRunner
from opuspocus.utils import decompress_file, open_file

# TODO: add parameterization to some of these methods


@pytest.fixture(scope="session")
def marian_dir():
    marian_dir = Path("marian_dir")
    if not marian_dir.exists():
        pytest.skip(reason="A compiled version of Marian NMT must be available.")
    return marian_dir


@pytest.fixture(scope="function")
def clear_instance_registry(monkeypatch):
    """Clear the initialized step instances (to reuse step labels)."""
    monkeypatch.setattr(pipeline_steps, "STEP_INSTANCE_REGISTRY", {})


@pytest.fixture(scope="function")
def clear_registries(monkeypatch):
    """Clear all the registries."""
    monkeypatch.setattr(pipeline_steps, "STEP_REGISTRY", {})
    monkeypatch.setattr(pipeline_steps, "STEP_CLASS_NAMES", set())

    monkeypatch.setattr(runners, "RUNNER_REGISTRY", {})
    monkeypatch.setattr(runners, "RUNNER_CLASS_NAMES", set())


@pytest.fixture(scope="session")
def languages():
    return ("en", "fr")


@pytest.fixture(scope="function")
def pipeline_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("test_pipeline")


@pytest.fixture(scope="session")
def data_train_minimal(tmp_path_factory, languages):
    src_file = Path(
        tmp_path_factory.mktemp("data"),
        "-".join(languages),
        "minimal",
        "train.{}.gz".format(languages[0]),
    )
    src_file.parent.mkdir(parents=True)
    with open_file(src_file, "w") as fh:
        print(
            "\n".join(
                [
                    "the colorless ideas slept furiously",
                    "pooh slept all night",
                    "working class hero is something to be",
                    "I am the working class walrus",
                    "walrus for president",
                ]
            ),
            file=fh,
        )

    tgt_file = Path(src_file.parent, "train.{}.gz".format(languages[1]))
    with open_file(tgt_file, "w") as fh:
        print(
            "\n".join(
                [
                    "les idées incolores dormaient furieusement",
                    "le caniche dormait toute la nuit",
                    "le héros de la classe ouvrière est quelque chose à être",
                    "Je suis le morse de la classe ouvrière",
                    "morse pour président",
                ]
            ),
            file=fh,
        )
    return (src_file, tgt_file)


@pytest.fixture(scope="session")
def data_train_minimal_decompressed(data_train_minimal):
    src_file = Path(
        data_train_minimal[0].parent, "decompressed", data_train_minimal[0].stem
    )
    src_file.parent.mkdir(parents=True)
    decompress_file(data_train_minimal[0], src_file)

    tgt_file = Path(src_file.parent, data_train_minimal[1].stem)
    decompress_file(data_train_minimal[1], tgt_file)
    return (src_file, tgt_file)


### Change the following when dataclasses are properly implemented


@pytest.fixture(scope="function")
def config_file_minimal(tmp_path_factory, data_train_minimal, pipeline_dir, languages):
    config_file = Path(tmp_path_factory.mktemp("test_configs"), "config_minimal.yml")
    src_file, tgt_file = data_train_minimal

    step_label = "raw." + "-".join(languages)
    config = {
        "pipeline": {
            "pipeline_dir": str(pipeline_dir),
            "steps": [
                {
                    "step": "raw",
                    "step_label": step_label,
                    "src_lang": languages[0],
                    "tgt_lang": languages[1],
                    "raw_data_dir": str(data_train_minimal[0].parent),
                }
            ],
            "default_targets": [step_label],
        }
    }
    yaml.dump(config, open(config_file, "w"))
    return config_file


@pytest.fixture(scope="function")
def config_minimal(config_file_minimal):
    return yaml.safe_load(open(config_file_minimal, "r"))


###


@pytest.fixture(scope="function")
def pipeline_minimal(config_file_minimal, clear_instance_registry):
    config_dict = yaml.safe_load(open(config_file_minimal, "r"))
    pipeline_dir = config_dict["pipeline"]["pipeline_dir"]
    return OpusPocusPipeline(config_file_minimal, pipeline_dir)


@pytest.fixture(scope="function")
def pipeline_minimal_inited(pipeline_minimal):
    pipeline_minimal.init()
    return pipeline_minimal


@pytest.fixture(scope="function")
def pipeline_minimal_running(pipeline_minimal_inited):
    runner = BashRunner("bash", pipeline_minimal_inited.pipeline_dir)
    runner.run_pipeline(pipeline_minimal_inited, pipeline_minimal_inited.get_targets())
    return pipeline_minimal_inited


@pytest.fixture(scope="function")
def pipeline_minimal_done(pipeline_minimal_inited):
    runner = BashRunner("bash", pipeline_minimal_inited.pipeline_dir)
    runner.run_pipeline(pipeline_minimal_inited, pipeline_minimal_inited.get_targets())
    runner.wait_for_all_tasks(t.task_id for t in runner.submitted_tasks)
    return pipeline_minimal_inited
