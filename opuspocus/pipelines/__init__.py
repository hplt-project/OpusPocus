import argparse
import importlib
from pathlib import Path

from .opuspocus_pipeline import OpusPocusPipeline


PIPELINE_REGISTRY = {}
PIPELINE_CLASS_NAMES = set()


def build_pipeline(pipeline, args, steps=None, targets=None):
    return PIPELINE_REGISTRY[pipeline].build_pipeline(
        pipeline, args, steps, targets
    )


def load_pipeline(args):
    steps, targets, vars_dict = OpusPocusPipeline.load_variables(args)
    return build_pipeline(vars_dict['pipeline'], args, steps, targets)


def register_pipeline(name):
    """
    New pipeline can be added to OpusPocus with the
    :func:`~opuspocus.pipelines.register_pipeline` function decorator.

    Inspired by Fairseq task/model/etc registrations

    For example:
        TODO

    Args:
        TODO
    """

    def register_pipeline_cls(cls):
        if name in PIPELINE_REGISTRY:
            raise ValueError(
                'Cannot register duplicate pipeline ({})'.format(name)
            )
        if not issubclass(cls, OpusPocusPipeline):
            raise ValueError(
                'Pipeline ({}: {}) must extend OpusPocusPipeline'
                .format(name, cls.__name__)
            )
        if cls.__name__ in PIPELINE_CLASS_NAMES:
            raise ValueError(
                'Cannot register pipeline with duplicate class name ({})'
                .format(cls.__name__)
            )
        PIPELINE_REGISTRY[name] = cls
        PIPELINE_CLASS_NAMES.add(cls.__name__)
        return cls

    return register_pipeline_cls


def get_pipeline(name):
    return PIPELINE_REGISTRY[name]


pipelines_dir = Path(__file__).parents[0]
for file in pipelines_dir.iterdir():
    if (
        not file.stem.startswith('_')
        and not file.stem.startswith('.')
        and file.name.endswith('py')
        and not file.is_dir()
    ):
        pipeline_name = (
            file.stem if file.name.endswith('.py')
            else file
        )
        importlib.import_module('opuspocus.pipelines.' + str(pipeline_name))

        # expose `pipeline_parser` for sphinx
        #if pipeline_name in PIPELINE_REGISTRY:
        #    parser = argparse.ArgumentParser(add_help=False)
        #    group_pipeline = parser.add_argument_group('Pipeline name')
        #    group_pipeline.add_argument(
        #        '--pipeline', metavar=pipeline_name,
        #        help='Enable this pipeline with: '
        #        '``--pipeline={}``'.format(pipeline_name)
        #    )
        #    pipeline_args_group = parser.add_argument_group('Additional command-line arguments')
        #    PIPELINE_REGISTRY[pipeline_name].add_args(pipeline_args_group)
        #    globals()[pipeline_name + '_parser'] = parser
