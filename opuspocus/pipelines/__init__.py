import argparse
import importlib
from pathlib import Path

from .opuspocus_pipeline import OpusPocusPipeline


PIPELINE_REGISTRY = {}
PIPELINE_CLASS_NAMES = set()


def build_pipeline(args):
    """Pipeline builder function. Use this to create pipeline objects."""

    return PIPELINE_REGISTRY[args.pipeline].build_pipeline(
        args.pipeline, args.pipeline_dir, args.pipeline_config, args
    )


def load_pipeline(args):
    """Load an existing (initialized) pipeline."""
    pipeline_dir = args.pipeline_dir
    variables_path = Path(pipeline_dir, OpusPocusPipeline.variables_file)
    config_path = Path(pipeline_dir, OpusPocusPipeline.config_file)

    variables_dict = yaml.safe_load(open(variables_path, 'r'))
    pipeline = variables_dict['pipeline']
    return PIPELINE_REGISTRY[pipeline].build_pipeline(
        pipeline, pipeline_dir, config_path, args
    )


def register_pipeline(name):
    """
    New pipeline can be added to OpusPocus with the
    :func:`~opuspocus.pipelines.register_pipeline` function decorator.

    Based on the Fairseq task/model/etc registrations
    (https://github.com/facebookresearch/fairseq)

    For example:

        @register_pipeline('simple')
        class SimplePipeline(OpusPocusPipeline):
            (...)

    .. note:: All pipelines must implement the :class:`OpusPocusPipeline` interface.

    Args:
        name (str): the name of the pipeline
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
