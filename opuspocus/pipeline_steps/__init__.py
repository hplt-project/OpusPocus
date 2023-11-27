import argparse
import importlib
from pathlib import Path

from .opuspocus_step import OpusPocusStep
from opuspocus.utils import update_args


STEP_REGISTRY = {}
STEP_INSTANCE_REGISTRY = {}
STEP_CLASS_NAMES = set()

__all__ = [
    'CleanCorpusMonoStep',
    'CleanCorpusParaStep',
    'DecontaminateCorpusMonoStep',
    'DecontaminateCorpusParaStep',
    'GatherTrainStep',
    'GenerateVocabStep',
    'TrainModelStep',
]


def build_step(step, args, step_name: str = None, **kwargs):
    if step_name is not None and step_name in STEP_INSTANCE_REGISTRY:
        return STEP_INSTANCE_REGISTRY[step_name]

    step_instance = STEP_REGISTRY[step].build_step(
        step, args, **kwargs
    )

    # sanity check (TODO: make this test into a warning)
    if step_name is not None:
        assert step_name == step_instance.step_name

    STEP_INSTANCE_REGISTRY[step_instance.step_name] = step_instance
    return step_instance


def load_step(step_name, args):
    # recursively load and instantiate the dependencies first
    step_deps = OpusPocusStep.load_dependencies(step_name, args.pipeline_dir)
    step_deps_inst = {}
    for dep, dep_name in step_deps.items():
        dep_inst = load_step(dep_name, args)
        step_deps_inst[dep] = dep_inst

    # now we load the current step
    step_vars = OpusPocusStep.load_variables(step_name, args.pipeline_dir)
    step = step_vars['step']
    del step_vars['step']
    step_args = update_args(args, step_vars)
    return build_step(step, step_args, **step_deps_inst)


def register_step(name):
    """
    New steps can be added to OpusPocus with the
    :func:`~opuspocus.opuspocus_steps.register_step` function decorator.

    Inspired by Fairseq task/model/etc registrations

    For example:
        TODO

    Args:
        TODO
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

        # expose `step_parser` for sphinx
        if step_name in STEP_REGISTRY:
            parser = argparse.ArgumentParser(add_help=False)
            #group_step = parser.add_argument_group('Step name')
            # fmt: off
            #group_step.add_argument('--step', metavar=step_name,
            #                        help='Enable this step with: ``--step=' + task_name + '``')
            # fmt: on
            step_args_group = parser.add_argument_group('Additional command-line arguments')
            STEP_REGISTRY[step_name].add_args(step_args_group)
            globals()[step_name + '_parser'] = parser
