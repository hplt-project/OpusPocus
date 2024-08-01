import pytest

from opuspocus.pipeline_steps import build_step
from opuspocus.runners.debug import DebugRunner


@pytest.fixture(scope="session")
def raw_step_train_minimal(tmp_path_factory, data_train_minimal_decompressed):
    pipeline_dir = tmp_path_factory.mktemp("test_pipeline_steps")

    src_lang = data_train_minimal_decompressed[0].suffix.lstrip(".")
    tgt_lang = data_train_minimal_decompressed[1].suffix.lstrip(".")
    step = build_step(
        step="raw",
        step_label="raw.{}-{}.mock".format(src_lang, tgt_lang),
        pipeline_dir=pipeline_dir,
        **{
            "raw_data_dir": data_train_minimal_decompressed[0].parent,
            "src_lang": src_lang,
            "tgt_lang": tgt_lang,
            "compressed": False,
        }
    )
    step.init_step()
    return step


@pytest.fixture(scope="session")
def raw_step_train_minimal_reversed(tmp_path_factory, data_train_minimal_decompressed):
    pipeline_dir = tmp_path_factory.mktemp("test_pipeline_steps")

    src_lang = data_train_minimal_decompressed[1].suffix.lstrip(".")
    tgt_lang = data_train_minimal_decompressed[0].suffix.lstrip(".")
    step = build_step(
        step="raw",
        step_label="raw.{}-{}.mock".format(src_lang, tgt_lang),
        pipeline_dir=pipeline_dir,
        **{
            "raw_data_dir": data_train_minimal_decompressed[0].parent,
            "src_lang": src_lang,
            "tgt_lang": tgt_lang,
            "compressed": False,
        }
    )
    step.init_step()
    return step


@pytest.fixture(scope="session")
def vocab_step_minimal(raw_step_train_minimal, marian_dir):
    src_lang = raw_step_train_minimal.src_lang
    tgt_lang = raw_step_train_minimal.tgt_lang
    step = build_step(
        step="generate_vocab",
        step_label="generate_vocab.{}-{}".format(src_lang, tgt_lang),
        pipeline_dir=raw_step_train_minimal.pipeline_dir,
        **{
            "src_lang": src_lang,
            "tgt_lang": tgt_lang,
            "datasets": raw_step_train_minimal.dataset_list,
            "marian_dir": marian_dir,
            "corpus_step": raw_step_train_minimal,
            "vocab_size": 300,
        }
    )
    step.init_step()
    return step
