import pytest

from sacrebleu.metrics import METRICS

from opuspocus.pipeline_steps import StepState
from opuspocus.pipeline_steps.evaluate import EvaluateStep
from opuspocus.runners.bash import BashRunner


@pytest.fixture(scope="function", params=EvaluateStep.AVAILABLE_METRICS)
def evaluate_step(request, raw_step_train_minimal):
    return EvaluateStep(
        step="evaluate",
        step_label="evaluate.test",
        pipeline_dir=raw_step_train_minimal.pipeline_dir,
        src_lang=raw_step_train_minimal.src_lang,
        tgt_lang=raw_step_train_minimal.tgt_lang,
        datasets=raw_step_train_minimal.dataset_list,
        translated_corpus_step=raw_step_train_minimal,
        reference_corpus_step=raw_step_train_minimal,
        metrics=[request.param],
    )


def test_evaluate_step_unknown_metric(raw_step_train_minimal):
    with pytest.raises(ValueError):
        step = EvaluateStep(
            step="evaluate",
            step_label="evaluate.test",
            pipeline_dir=raw_step_train_minimal.pipeline_dir,
            src_lang=raw_step_train_minimal.src_lang,
            tgt_lang=raw_step_train_minimal.tgt_lang,
            datasets=raw_step_train_minimal.dataset_list,
            translated_corpus_step=raw_step_train_minimal,
            reference_corpus_step=raw_step_train_minimal,
            metrics=["foo"]
        )


def test_evaluate_step_init(evaluate_step):
    evaluate_step.init_step()
    assert evaluate_step.state == StepState.INITED


def test_evaluate_step_run(evaluate_step):
    evaluate_step.init_step()
    runner = BashRunner("bash", evaluate_step.pipeline_dir)

    task_info = runner.run_step(evaluate_step)
    assert task_info is not None

    runner.wait_for_single_task(task_info.main_task)
    assert task_info.state == StepState.DONE
