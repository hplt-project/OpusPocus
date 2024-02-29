import argparse
import inspect
import importlib
from pathlib import Path

from .opuspocus_step import OpusPocusStep
from opuspocus.utils import update_args


STEP_REGISTRY = {}
STEP_INSTANCE_REGISTRY = {}
STEP_CLASS_NAMES = set()


def build_step(step, pipeline_dir, step_name: str = None, **kwargs):
    """Pipeline step builder function. Use this to create pipeline step
    objects.
    """
    if step_name is not None and step_name in STEP_INSTANCE_REGISTRY:
        return STEP_INSTANCE_REGISTRY[step_name]

    step_instance = STEP_REGISTRY[step].build_step(
        step, pipeline_dir, **kwargs
    )

    # sanity check (TODO: make this test into a warning)
    if step_name is not None:
        assert step_name == step_instance.step_name

    STEP_INSTANCE_REGISTRY[step_instance.step_name] = step_instance
    return step_instance


def load_step(step_name, args):
    """Load an existing (initialized) pipeline step."""
    step_params = OpusPocusStep.load_parameters(step_name, args.pipeline_dir)

    step = step_params['step']  
    del step_params['step']

    pipeline_dir = step_params['pipeline_dir']
    assert pipeline_dir == args.pipeline_dir
    del step_params['pipeline_dir']

    step_deps = OpusPocusStep.load_dependencies(step_name, args.pipeline_dir)
    for k, v in step_deps.items():
        step_params[k] = load_step(v, args)

    return build_step(step, pipeline_dir, step_name, **step_params)


def register_step(name):
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

    def register_step_cls(cls):
        if name in STEP_REGISTRY:
            raise ValueError(
                'Cannot register duplicate step ({})'.format(name)
            )
        if not issubclass(cls, OpusPocusStep):
            raise ValueError(
                'Pipeline step ({}: {}) must extend OpusPocusStep'
                .format(name, cls.__name__)
            )
        if cls.__name__ in STEP_CLASS_NAMES:
            raise ValueError(
                'Cannot register pipeline step with duplicate class name ({})'
                .format(cls.__name__)
            )
        STEP_REGISTRY[name] = cls
        STEP_CLASS_NAMES.add(cls.__name__)
        return cls

    return register_step_cls


def get_step(name):
    return STEP_REGISTRY[name]


steps_dir = Path(__file__).parents[0]
for file in steps_dir.iterdir():
    if (
        not file.stem.startswith('_')
        and not file.stem.startswith('.')
        and file.name.endswith('py')
        and not file.is_dir()
    ):
        step_name = (
            file.stem if file.name.endswith('.py')
            else file
        )
        importlib.import_module('opuspocus.pipeline_steps.' + str(step_name))
