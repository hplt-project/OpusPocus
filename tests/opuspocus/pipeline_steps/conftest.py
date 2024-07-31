import pytest

from opuspocus.pipeline_steps.raw import RawCorpusStep


@pytest.fixture(scope="function")
def raw_step_train_minimal(
    pipeline_dir,
    data_train_minimal_decompressed
):
    src_lang = data_train_minimal_decompressed[0].suffix.lstrip(".")
    tgt_lang = data_train_minimal_decompressed[1].suffix.lstrip(".")
    step = RawCorpusStep(
        step="raw",
        step_label="raw.{}-{}".format(src_lang, tgt_lang),
        pipeline_dir=pipeline_dir,
        raw_data_dir=data_train_minimal_decompressed[0].parent,
        src_lang=src_lang,
        tgt_lang=tgt_lang,
        compressed=False
    )
    step.init_step()
    return step


@pytest.fixture(scope="function")
def raw_step_train_minimal_reversed(
    pipeline_dir,
    data_train_minimal_decompressed
):
    src_lang = data_train_minimal_decompressed[1].suffix.lstrip(".")
    tgt_lang = data_train_minimal_decompressed[0].suffix.lstrip(".")
    step = RawCorpusStep(
        step="raw",
        step_label="raw.{}-{}".format(src_lang, tgt_lang),
        pipeline_dir=pipeline_dir,
        raw_data_dir=data_train_minimal_decompressed[0].parent,
        src_lang=src_lang,
        tgt_lang=tgt_lang,
        compressed=False
    )
    step.init_step()
    return step
