import importlib
from pathlib import Path
from typing import Callable

from .opuspocus_step import OpusPocusStep, StepState

__all__ = [
    "OpusPocusStep",
    "StepState",
]

STEP_REGISTRY = {}
STEP_INSTANCE_REGISTRY = {}
STEP_CLASS_NAMES = set()


def build_step(step: str, step_label: str, pipeline_dir: Path, **kwargs) -> OpusPocusStep:  # noqa: ANN003
    """Pipeline step builder function. Use this to create pipeline step
    objects.
    """
    if step_label is not None and step_label in STEP_INSTANCE_REGISTRY:
        return STEP_INSTANCE_REGISTRY[step_label]

    step_instance = get_step(step).build_step(step, step_label, pipeline_dir, **kwargs)

    # sanity check (TODO: make this test into a warning)
    if step_label is not None:
        assert step_label == step_instance.step_label

    STEP_INSTANCE_REGISTRY[step_instance.step_label] = step_instance
    return step_instance


def load_step(step_label: str, pipeline_dir: Path) -> OpusPocusStep:
    """Load an existing (initialized) pipeline step."""
    step_params = OpusPocusStep.load_parameters(step_label, pipeline_dir)

    step = step_params["step"]
    del step_params["step"]
    del step_params["step_label"]
    del step_params["pipeline_dir"]

    step_deps = OpusPocusStep.load_dependencies(step_label, pipeline_dir)
    for k, v in step_deps.items():
        step_params[k] = load_step(v, pipeline_dir)

    return build_step(step, step_label, pipeline_dir, **step_params)


def register_step(name: str) -> Callable:
    """
    New steps can be added to OpusPocus with the
    :func:`~opuspocus.opuspocus_steps.register_step` function decorator.

    Based on the Fairseq task/model/etc registrations
    (https://github.com/facebookresearch/fairseq)

    For example:

        @register_step('train_model')
        class TrainModelStep(OpusPocusStep):
            (...)

    .. note:: All pipeline steps must implement the :class:`OpusPocusStep` interface.
        Typically you will extend the base class directly, however, in case
        of corpus creating/modifying steps (cleaning, translation), you should
        extend the :class:`CorpusStep` instead since it provides additional
        corpus-related functionality.

    Args:
        name (str): the name of the pipeline step
    """

    def register_step_cls(cls: OpusPocusStep) -> OpusPocusStep:
        if name in STEP_REGISTRY:
            err_msg = f"Cannot register duplicate step ({name})"
            raise ValueError(err_msg)
        if not issubclass(cls, OpusPocusStep):
            err_msg = f"Pipeline step ({name}: {cls.__name__}) must extend OpusPocusStep"
            raise TypeError(err_msg)
        if cls.__name__ in STEP_CLASS_NAMES:
            err_msg = f"Cannot register pipeline step with duplicate class name ({cls.__name__})"
            raise ValueError(err_msg)
        STEP_REGISTRY[name] = cls
        STEP_CLASS_NAMES.add(cls.__name__)
        return cls

    return register_step_cls


def get_step(name: str) -> OpusPocusStep:
    return STEP_REGISTRY[name]


steps_dir = Path(__file__).parents[0]
for file in steps_dir.iterdir():
    if (
        not file.stem.startswith("_")
        and not file.stem.startswith(".")
        and file.name.endswith("py")
        and not file.is_dir()
    ):
        step_label = file.stem if file.name.endswith(".py") else file
        importlib.import_module("opuspocus.pipeline_steps." + str(step_label))
