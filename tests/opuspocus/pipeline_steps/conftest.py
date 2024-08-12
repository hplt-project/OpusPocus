import pytest

import opuspocus.pipeline_steps as pipeline_steps
from opuspocus.pipeline_steps import build_step


@pytest.fixture(scope="function")
def train_data_parallel_tiny_raw_step_inited(tmp_path_factory, train_data_parallel_tiny_decompressed):
    """Load the mock tiny dataset."""
    setattr(pipeline_steps, "STEP_INSTANCE_REGISTRY", {})
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


@pytest.fixture(scope="function")
def train_data_parallel_tiny_vocab_step_inited(train_data_parallel_tiny_raw_step_inited, marian_cpu_dir):
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
            "marian_dir": marian_cpu_dir,
            "corpus_step": train_data_parallel_tiny_raw_step_inited,
            "vocab_size": 300,
        },
    )
    step.init_step()
    return step
