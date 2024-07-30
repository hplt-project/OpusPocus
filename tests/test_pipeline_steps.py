import pytest

from opuspocus.pipeline_steps import (
    build_step,
    STEP_REGISTRY,
)
from opuspocus.pipeline_steps.raw import RawCorpusStep


@pytest.fixture(scope="function", params=STEP_REGISTRY.keys())
def step_default(step, tmp_path_factory):
    return build_step(
        step.param,
        "{}.test".format(step.param),
        tmp_path_factory.mktemp("empty_pipeline"),
    )


# def test_parameter_save_load(step_default):


# def test_list_parameters():


@pytest.fixture(scope="session")
def mock_step_parameters(languages, data_train_minimal):
    src_lang, tgt_lang = languages
    return {
        "step": "raw",
        "step_label": "raw.en",
        "src_lang": src_lang,
        "tgt_lang": tgt_lang,
        "raw_data_dir": str(data_train_minimal[0].parent),
    }


@pytest.fixture(scope="module")
def mock_step():
    return RawCorpusStep(**mock_step_parameters)


@pytest.fixture(scope="module")
def mock_step_inited(step):
    step.init()
    return step


# def test_build_method():
#    build_step(**


# def test_load_method():
