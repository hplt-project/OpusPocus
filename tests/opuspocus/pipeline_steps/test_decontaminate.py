import pytest

from opuspocus.pipeline_steps import StepState
from opuspocus.pipeline_steps.decontaminate import DecontaminateStep
from opuspocus.runners.bash import BashRunner
from opuspocus.utils import count_lines


@pytest.fixture(scope="function")
def decontaminate_step(request, raw_step_train_minimal):
    return DecontaminateStep(
        step="decontaminate",
        step_label="decontaminate.test",
        pipeline_dir=raw_step_train_minimal.pipeline_dir,
        previous_corpus_step=raw_step_train_minimal,
        src_lang=raw_step_train_minimal.src_lang,
        tgt_lang=raw_step_train_minimal.tgt_lang,
        valid_data_step=[raw_step_train_minimal],
        test_data_step=[raw_step_train_minimal],
    )


@pytest.fixture(scope="function")
def decontaminate_step_init(decontaminate_step):
    decontaminate_step.init_step()
    return decontaminate_step


def test_decontaminate_step_init(decontaminate_step_init):
    assert decontaminate_step_init.state == StepState.INITED


@pytest.fixture(scope="function")
def decontaminate_step_done(decontaminate_step_init):
    runner = BashRunner("bash", decontaminate_step.pipeline_dir)
    task_info = runner.submit_step(decontaminate_step)
    runner.wait_for_single_task(task_info["main_task"])
    return decontaminate_step_init


def test_decontaminate_step_done(decontaminate_step_done):
    assert decontaminate_step_done.state == StepState.DONE


def test_decontaminate_step_done_corpus_lines(decontaminate_step_done):
    # TODO(varisd): this can probably be generalized to all CorpusStep objects
    for dset in decontaminate_step_done.dataset_list:
        src_lines = count_lines(
            Path(
                decontaminate_step_done.output_dir,
                "{}.{}.gz".format(dset, decontaminate_step_done.src_lang)
            )
        )
        tgt_lines = count_lines(
            Path(
                decontaminate_step_done.output_dir,
                "{}.{}.gz".format(dset, decontaminate_step_done.tgt_lang)
            )
        )
        assert src_lines == tgt_lines
