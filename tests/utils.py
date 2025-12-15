from opuspocus import pipeline_steps
from opuspocus.runners import load_runner_from_directory
from opuspocus.utils import clean_dir


def teardown_step(step):
    """Helper function for step fixture cleanup."""
    if step.step_dir.exists():
        if step.is_running_or_submitted:
            runner = load_runner_from_directory(step.pipeline_dir)
            runner.stop_step(step)
        clean_dir(step.step_dir)
        step.step_dir.rmdir()
    if step.step_label in pipeline_steps.STEP_INSTANCE_REGISTRY:
        del pipeline_steps.STEP_INSTANCE_REGISTRY[step.step_label]


def teardown_pipeline(pipeline):
    """Helper function for pipeline fixture cleanup."""
    for step in pipeline.steps:
        teardown_step(step)
    if pipeline.pipeline_dir.exists():
        clean_dir(pipeline.pipeline_dir)
        pipeline.pipeline_dir.rmdir()
