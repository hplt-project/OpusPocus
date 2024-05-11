#!/usr/bin/env python3
import argparse
import logging
import sys
from pathlib import Path

from opuspocus import pipelines
from opuspocus import runners
from opuspocus.utils import load_config_defaults, update_args

logger = logging.getLogger(__name__)


def check_pipeline_dir_exists(pipeline_dir) -> None:
    if pipeline_dir is None:
        raise ValueError('Missing "--pipeline-dir" option.')


def main_init(args, unparsed_args, parser):
    """Pipeline initialization sub-command.

    Builds the pipeline step objects and initializes the steps,
    creating the respective directories and saving the step parameters.
    """

    # Add pipeline args and parse again
    args = parse_init_args(args, unparsed_args, parser)

    logger.info('Building pipeline...')
    pipeline = pipelines.build_pipeline(args)
    logger.info('Initializing pipeline...')
    pipeline.init()
    logger.info('Pipeline initialized successfully.')


def main_run(args, *_):
    """Pipeline execution sub-command.

    Submits the pipeline steps, respecting their dependencies, using
    the specified runner (bash, slurm, ...).
    """
    check_pipeline_dir_exists(args.pipeline_dir)

    # Load the pipeline
    pipeline = pipelines.load_pipeline(args)

    # Initialize the runner
    runner = runners.build_runner(args.runner, args)

    # Run the pipeline
    runner.run_pipeline(pipeline, args)


def main_traceback(args, *_):
    """Pipeline structure analysis subcommand.

    Prints the simplified dependency graph of the pipeline steps
    with their current status.
    """
    check_pipeline_dir_exists(args.pipeline_dir)

    pipeline = pipelines.load_pipeline(args)
    pipeline.traceback(args.targets, args.full_trace)


def main_stop(args, *_):
    """TODO"""
    check_pipeline_dir_exists(args.pipeline_dir)

    # Load the pipeline
    pipeline = pipelines.load_pipeline(args)

    # Initialize the runner
    runner = runners.build_runner(args.runner, args)

    # Stop the pipeline execution
    runner.stop_pipeline(pipeline)


def main_list_commands(args, *_):
    print(
       'Error: No sub-command specified.\n\n'
       'Available commands:\n'
       '  init      Initialize the pipeline.\n'
       '  run       Execute the pipeline.\n'
       '  traceback Print the pipeline graph.\n'
       '', file=sys.stderr
    )
    sys.exit(1)


def create_args_parser():
    parser = argparse.ArgumentParser(
        description='OpusPocus NMT Pipeline Manager'
    )
    parser.set_defaults(fn=main_list_commands)
    parser.add_argument(
        '--log-level', choices=['info', 'debug'], default='info',
        help='Indicates current logging level.'
    )
    parser.add_argument(
        '--pipeline-dir', type=str, default=None,
        help='Pipeline root directory.'
    )
    subparsers = parser.add_subparsers(help='command', dest='command')

    # TODO: more arguments (?)

    # Pipeline Init
    parser_init = subparsers.add_parser('init')
    parser_init.add_argument(
        '--pipeline-config', type=str, default=None,
        help='Pipeline configuration YAML.'
    )
    parser_init.add_argument(
        '--pipeline', '-p', type=str, default='custom', metavar='PIPELINE',
        choices=pipelines.PIPELINE_REGISTRY.keys(),
        help='Training pippipeline_nameline type.'
    )
    parser_init.set_defaults(fn=main_init)

    # Pipeline Run
    parser_run = subparsers.add_parser('run')
    parser_run.add_argument(
        '--runner', type=str, required=True, metavar='RUNNER',
        choices=runners.RUNNER_REGISTRY.keys(),
        help='Pipeline step execution command.'
    )
    parser_run.add_argument(
        '--targets', type=str, nargs='+', default=None,
        help='TODO'
    )
    parser_run.set_defaults(fn=main_run)

    # Pipeline Stop
    parser_stop = subparsers.add_parser('stop')
    parser_stop.add_argument(
        '--runner', type=str, required=True, metavar='RUNNER',
        choices=runners.RUNNER_REGISTRY.keys(),
        help='Pipeline step cancellation command.'
    )

    # Pipeline Traceback
    parser_traceback = subparsers.add_parser('traceback')
    parser_traceback.add_argument(
        '--full-trace', action='store_true',
        help='Also print the parameters of the individual '
             'pipeline steps.'
    )
    parser_traceback.add_argument(
        '--targets', type=str, nargs='+', default=None,
        help='TODO'
    )
    parser_traceback.set_defaults(fn=main_traceback)

    return parser


def parse_init_args(args, unparsed_args, parser):
    pipelines.PIPELINE_REGISTRY[args.pipeline].add_args(parser)
    if hasattr(args, 'runner'):
        runners.RUNNER_REGISTRY[args.runner].add_args(runner)

    parser = load_config_defaults(parser, args.pipeline_config)

    # Parse second time to get pipeline options
    additional_args, _ = parser.parse_known_args(unparsed_args)

    return update_args(args, additional_args)

if __name__ == '__main__':
    parser = create_args_parser()

    # Parse the main command
    args, unparsed_args = parser.parse_known_args()

    # TODO: fix logging using a global logger
    logging.basicConfig(level=logging.INFO)
    if args.log_level == 'debug':
        logging.basicConfig(level=logging.DEBUG)

    args.fn(args, unparsed_args, parser)
