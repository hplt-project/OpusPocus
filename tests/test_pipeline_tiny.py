from opuspocus.pipeline_steps import StepState


## DATA PREPROCESSING ##


def test_pipeline_preprocess_tiny_inited(pipeline_preprocess_tiny_inited):
    """Test mock dataset preprocessing pipeline initialization."""
    for step in pipeline_preprocess_tiny_inited.steps:
        assert step.state == StepState.INITED


def test_pipeline_preprocess_tiny_done(pipeline_preprocess_tiny_done):
    """Test whether all mock dataset pipeline steps finished successfully."""
    for step in pipeline_preprocess_tiny_done.steps:
        assert step.state == StepState.DONE


## MODEL TRAINING ##


def test_pipeline_train_tiny_inited(pipeline_train_tiny_inited):
    """Test mock training pipeline initialization."""
    for step in pipeline_train_tiny_inited.steps:
        assert step.state == StepState.INITED


def test_pipeline_train_tiny_done(pipeline_train_tiny_done):
    """Test whether all mock training pipeline steps finished successfully."""
    for step in pipeline_train_tiny_done.steps:
        assert step.state == StepState.DONE
