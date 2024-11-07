from opuspocus import pipeline_steps
from opuspocus.utils import clean_dir


def teardown_step(step):
    """Helper function for step fixture cleanup."""
    if step.step_dir.exists():
        clean_dir(step.step_dir)
        step.step_dir.rmdir()
    if step.step_label in pipeline_steps.STEP_INSTANCE_REGISTRY:
        del pipeline_steps.STEP_INSTANCE_REGISTRY[step.step_label]


def teardown_pipeline(pipeline):
    """Helper function for pipeline fixture cleanup."""
    if pipeline.pipeline_dir.exists():
        clean_dir(pipeline.pipeline_dir)
        pipeline.pipeline_dir.rmdir()
    for s in pipeline.steps:
        if s.step_label in pipeline_steps.STEP_INSTANCE_REGISTRY:
            del pipeline_steps.STEP_INSTANCE_REGISTRY[s.step_label]
