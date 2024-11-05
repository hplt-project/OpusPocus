from pathlib import Path

import pytest

from opuspocus.pipeline_steps import StepState, build_step
from opuspocus.runners.debug import DebugRunner
from opuspocus.utils import count_lines


@pytest.fixture(params=["null", "1", "3", "dataset"])
def translate_step_inited(
    request,
    train_data_parallel_tiny,
    train_data_parallel_tiny_raw_step_inited,
    train_data_parallel_tiny_model_step_inited,
):
    """Create and initialize the translate step."""
    dset_size = count_lines(train_data_parallel_tiny[0])
    shard_size = None
    if request.param == "dataset":
        shard_size = dset_size
    elif request.param != "null":
        shard_size = int(request.param)

    marian_dir = train_data_parallel_tiny_model_step_inited.marian_dir
    step = build_step(
        step="translate",
        step_label=f"translate.{marian_dir}.test",
        pipeline_dir=train_data_parallel_tiny_raw_step_inited.pipeline_dir,
        **{
            "marian_dir": marian_dir,
            "src_lang": train_data_parallel_tiny_raw_step_inited.src_lang,
            "tgt_lang": train_data_parallel_tiny_raw_step_inited.tgt_lang,
            "previous_corpus_step": train_data_parallel_tiny_raw_step_inited,
            "model_step": train_data_parallel_tiny_model_step_inited,
            "shard_size": shard_size,
            "model_suffix": "best-chrf",
        },
    )
    step.init_step()
    return step


def test_translate_step_inited(translate_step_inited):
    """Test whether the step was initialized successfully."""
    assert translate_step_inited.state == StepState.INITED


@pytest.fixture()
def translate_step_done(translate_step_inited):
    """Execute the translate step."""
    runner = DebugRunner("debug", translate_step_inited.pipeline_dir)
    runner.submit_step(translate_step_inited)
    return translate_step_inited


def test_translate_step_done(translate_step_done):
    """Test whether the step execution finished successfully."""
    assert translate_step_done.state == StepState.DONE


@pytest.mark.parametrize("lang", ["src_lang", "tgt_lang"])
def test_translate_step_done_output(translate_step_done, lang):
    """Translation output has the same number of lines."""
    # TODO(varisd): More robust output testing (?)
    for dset in translate_step_done.dataset_list:
        f_name = f"{dset}.{getattr(translate_step_done, lang)}.gz"
        src_lines = count_lines(Path(translate_step_done.input_dir, f_name))
        tgt_lines = count_lines(Path(translate_step_done.output_dir, f_name))
        assert src_lines == tgt_lines
