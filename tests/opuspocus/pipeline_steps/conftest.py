import pytest

from opuspocus import pipeline_steps
from opuspocus.pipeline_steps import build_step


@pytest.fixture()
def train_data_parallel_tiny_raw_step_inited(tmp_path_factory, train_data_parallel_tiny_decompressed):
    """Load the mock tiny dataset."""
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
    step.init_step()
    return step


@pytest.fixture()
def train_data_parallel_tiny_vocab_step_inited(train_data_parallel_tiny_raw_step_inited, marian_dir):
    """Create the mock vocabulary from the tiny dataset."""
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
    step.init_step()
    return step


@pytest.fixture()
def train_data_parallel_tiny_model_step_inited(
    train_data_parallel_tiny_raw_step_inited,
    train_data_parallel_tiny_vocab_step_inited,
    marian_tiny_config_file,
    opustrainer_tiny_config_file,
):
    """Create the mock train_model step."""
    marian_dir = train_data_parallel_tiny_vocab_step_inited.marian_dir
    step = build_step(
        step="train_model",
        step_label=f"train_model.{marian_dir}.test",
        pipeline_dir=train_data_parallel_tiny_raw_step_inited.pipeline_dir,
        **{
            "marian_dir": marian_dir,
            "src_lang": train_data_parallel_tiny_raw_step_inited.src_lang,
            "tgt_lang": train_data_parallel_tiny_raw_step_inited.tgt_lang,
            "marian_config": marian_tiny_config_file,
            "vocab_step": train_data_parallel_tiny_vocab_step_inited,
            "opustrainer_config": opustrainer_tiny_config_file,
            "train_corpus_step": train_data_parallel_tiny_raw_step_inited,
            "valid_corpus_step": train_data_parallel_tiny_raw_step_inited,
            "train_categories": [train_data_parallel_tiny_raw_step_inited.categories[0]],
            "train_category_ratios": [1.0],
            "valid_dataset": train_data_parallel_tiny_raw_step_inited.dataset_list[0],
            "max_epochs": 1
        },
    )
    step.init_step()
    return step
