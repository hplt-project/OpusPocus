import pytest

from pathlib import Path

from opuspocus.pipeline_steps import build_step


@pytest.fixture(scope="session")
def raw_step_train_minimal(data_train_minimal_decompressed, tmp_path_factory):
    pipeline_dir = Path(tmp_path_factory.mktemp("pipeline_dir.unittest"))

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
        },
    )
    step.init_step()
    return step


@pytest.fixture(scope="session")
def raw_step_train_minimal_reversed(data_train_minimal_decompressed, tmp_path_factory):
    pipeline_dir = Path(tmp_path_factory.mktemp("pipeline_dir.unittest"))

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
        },
    )
    step.init_step()
    return step
