import pytest

from opuspocus import pipeline_steps
from opuspocus.pipeline_steps import build_step
from tests.utils import teardown_step


@pytest.fixture()
def train_data_parallel_tiny_raw_step(tmp_path_factory, train_data_parallel_tiny_decompressed):
    """Mock step that loads the mock tiny dataset."""
    pipeline_steps.STEP_INSTANCE_REGISTRY = {}
    pipeline_dir = tmp_path_factory.mktemp("test_pipeline_steps")

    src_lang = train_data_parallel_tiny_decompressed[0].suffix.lstrip(".")
    tgt_lang = train_data_parallel_tiny_decompressed[1].suffix.lstrip(".")
    step = build_step(
        step="raw",
        step_label=f"raw.{src_lang}-{tgt_lang}.mock",
        pipeline_dir=pipeline_dir,
        **{
            "raw_data_dir": train_data_parallel_tiny_decompressed[0].parent,
            "src_lang": src_lang,
            "tgt_lang": tgt_lang,
            "compressed": False,
        },
    )
    yield step

    teardown_step(step)


@pytest.fixture()
def train_data_parallel_tiny_raw_step_inited(train_data_parallel_tiny_raw_step):
    """Mock step that loads the mock tiny dataset (INITED)."""
    train_data_parallel_tiny_raw_step.init_step()
    return train_data_parallel_tiny_raw_step


@pytest.fixture()
def train_data_parallel_tiny_vocab_step(train_data_parallel_tiny_raw_step_inited, marian_dir):
    """Mock Vocabulary step from the tiny dataset."""
    src_lang = train_data_parallel_tiny_raw_step_inited.src_lang
    tgt_lang = train_data_parallel_tiny_raw_step_inited.tgt_lang
    step = build_step(
        step="generate_vocab",
        step_label=f"generate_vocab.{src_lang}-{tgt_lang}",
        pipeline_dir=train_data_parallel_tiny_raw_step_inited.pipeline_dir,
        **{
            "src_lang": src_lang,
            "tgt_lang": tgt_lang,
            "datasets": train_data_parallel_tiny_raw_step_inited.dataset_list,
            "marian_dir": marian_dir,
            "corpus_step": train_data_parallel_tiny_raw_step_inited,
            "vocab_size": 300,
        },
    )
    yield step

    teardown_step(step)


@pytest.fixture()
def train_data_parallel_tiny_vocab_step_inited(train_data_parallel_tiny_vocab_step):
    """Mock Vocabulary step from the tiny dataset (INITED)."""
    train_data_parallel_tiny_vocab_step.init_step()
    return train_data_parallel_tiny_vocab_step


@pytest.fixture()
def train_data_parallel_tiny_train_model_step(
    train_data_parallel_tiny_raw_step,
    train_data_parallel_tiny_vocab_step,
    marian_tiny_config_file,
):
    """Mock Train Model step."""
    marian_dir = train_data_parallel_tiny_vocab_step.marian_dir
    step = build_step(
        step="train_model",
        step_label=f"train_model.{marian_dir}.test",
        pipeline_dir=train_data_parallel_tiny_raw_step.pipeline_dir,
        **{
            "marian_dir": marian_dir,
            "src_lang": train_data_parallel_tiny_raw_step.src_lang,
            "tgt_lang": train_data_parallel_tiny_raw_step.tgt_lang,
            "marian_config": marian_tiny_config_file,
            "vocab_step": train_data_parallel_tiny_vocab_step,
            "max_epochs": 10,
            "train_corpus_step": train_data_parallel_tiny_raw_step,
            "valid_corpus_step": train_data_parallel_tiny_raw_step,
            "train_categories": [train_data_parallel_tiny_raw_step.categories[0]],
            "train_category_ratios": [1.0],
            "valid_dataset": train_data_parallel_tiny_raw_step.dataset_list[0],
        },
    )
    yield step

    teardown_step(step)


@pytest.fixture()
def train_data_parallel_tiny_model_step_inited(train_data_parallel_tiny_train_model_step):
    """Mock Train Model step (INITED)."""
    train_data_parallel_tiny_train_model_step.init_step()
    return train_data_parallel_tiny_train_model_step
