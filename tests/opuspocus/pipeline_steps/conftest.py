import pytest

from opuspocus.pipeline_steps.raw import RawCorpusStep


@pytest.fixture(scope="module")
def raw_step_train_minimal(pipeline_dir, data_train_minimal):
    src_lang = data_train_minimal[0].suffix
    tgt_lang = data_train_minimal[1].suffix
    step = RawCorpusStep(
        step="raw",
        step_label="raw.{}-{}".format(src_lang, tgt_lang),
        pipeline_dir=pipeline_dir,
        raw_data_dir=data_train_minimal[0].parent,
        src_lang=src_lang,
        tgt_lang=tgt_lang,
    )
    step.init_step()
    return step


@pytest.fixture(scope="module")
def raw_step_train_minimal_reversed(pipeline_dir, data_train_minimal):
    src_lang = data_train_minimal[1].suffix
    tgt_lang = data_train_minimal[0].suffix
    step = RawCorpusStep(
        step="raw",
        step_label="raw.{}-{}".format(src_lang, tgt_lang),
        pipeline_dir=pipeline_dir,
        raw_data_dir=data_train_minimal[0].parent,
        src_lang=src_lang,
        tgt_lang=tgt_lang,
    )
    step.init_step()
    return step
