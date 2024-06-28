import pytest
from argparse import Namespace
from opuspocus.pipeline_steps import (
    OpusPocusStep,
    build_step,
    load_step,
    STEP_REGISTRY,
)


@pytest.fixture(scope="function", params=STEP_REGISTRY.keys())
def step_default(step, tmp_path_factory):
    return build_step(
        step.param,
        "{}.test".format(step.param),
        tmp_path_factory.mktemp("empty_pipeline")
    )


#def test_parameter_save_load(step_default):


#def test_list_parameters():




@pytest.fixture(scope="session")
def mock_step_parameters():
    src_lang, tgt_lang = languages
    return {
        "step": "raw",
        "step_label": "raw.en",
        "src_lang": src_lang,
        "tgt_lang": tgt_lang,
        "raw_data_dir": str(data_train_minimal[0].parent)
    }


@pytest.fixture(scope="module")
def mock_step():
    return RawStep(**mock_step_parameters)


@pytest.fixture(scope="module")
def mock_step_inited(step):
    step.init()
    return step


#def test_build_method():
#    build_step(**


#def test_load_method():
