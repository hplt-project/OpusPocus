from argparse import Namespace
import importlib
from pathlib import Path

from .opuspocus_runner import (
    OpusPocusRunner,
    TaskId,
    TaskInfo
)


RUNNER_REGISTRY = {}
RUNNER_CLASS_NAMES = set()


def build_runner(runner: str, pipeline_dir: Path, args: Namespace):
    """Runner builder function. Use this to create runner objects."""
    kwargs = {}
    for param in RUNNER_REGISTRY[runner].list_parameters():
        if param == 'runner' or param == 'pipeline_dir':
            continue
        kwargs[param] = getattr(args, param)

    return RUNNER_REGISTRY[runner].build_runner(runner, pipeline_dir, **kwargs)


def load_runner(pipeline_dir: Path):
    runner_params = OpusPocusRunner.load_parameters(pipeline_dir)

    runner = runner_params['runner']
    del runner_params['runner']
    del runner_params['pipeline_dir']

    return RUNNER_REGISTRY[runner].build_runner(
        runner, pipeline_dir, **runner_params
    )


def register_runner(name):
    """
    New runner can be added to OpusPocus with the
    :func:`~opuspocus.runners.register_runner` function decorator.

    Based on the Fairseq task/model/etc registrations
    (https://github.com/facebookresearch/fairseq)

    For example:

        @register_runner('slurm')
        class SlurmRunner(OpusPocusRunner):
            (...)

    .. note:: All runners must implement the :class:`OpusPocusRunner` interface.

    Args:
        name (str): the name of the runner
    """

    def register_runner_cls(cls):
        if name in RUNNER_REGISTRY:
            raise ValueError(
                'Cannot register duplicate runner ({})'.format(name)
            )
        if not issubclass(cls, OpusPocusRunner):
            raise ValueError(
                'Runner ({}: {}) must extend OpusPocusRunner'
                .format(name, cls.__name__)
            )
        if cls.__name__ in RUNNER_CLASS_NAMES:
            raise ValueError(
                'Cannot register runner with duplicate class name ({})'
                .format(cls.__name__)
            )
        RUNNER_REGISTRY[name] = cls
        RUNNER_CLASS_NAMES.add(cls.__name__)
        return cls

    return register_runner_cls


runners_dir = Path(__file__).parents[0]
for file in runners_dir.iterdir():
    if (
        not file.stem.startswith('_')
        and not file.stem.startswith('.')
        and file.name.endswith('py')
        and not file.is_dir()
    ):
        runner_name = (
            file.stem if file.name.endswith('.py')
            else file
        )
        importlib.import_module('opuspocus.runners.' + str(runner_name))
