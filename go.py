#!/usr/bin/env python3
import argparse
import logging
import sys
from pathlib import Path

from opuspocus import pipelines
from opuspocus.utils import load_config_defaults, update_args

logger = logging.getLogger(__name__)


def main_init(args, unparsed_args, parser):
    '''Pipeline initialization sub-command.

    Builds the pipeline step objects and initializes the steps,
    creating the respective directories and saving the step parameters.
    '''

    # Add pipeline args and parse again
    args = parse_init_args(args, unparsed_args, parser)

    logger.info('Building pipeline...')
    pipeline = pipelines.build_pipeline(args.pipeline, args)
    logger.info('Initializing pipeline...')
    pipeline.init()
    logger.info('Pipeline initialized successfully.')

    # Dry-run
    if args.dry_run:
        # TODO: test 3rd party tools (?), anything else(?), touch files for
        # checking dependencies (?)
        pass


def main_run(args, *_):
    '''Pipeline execution sub-command.

    Submits the pipeline steps, respecting their dependencies, using
    the specified runner (bash, slurm, ...)
    '''
    print(args.pipeline_dir)
    pipeline = pipelines.load_pipeline(args)
    pipeline.run(args)


def main_traceback(args, *_):
    '''Pipeline structure analysis subcommand.

    Prints the simplified dependency graph of the pipeline steps
    with their current status.
    '''
    pipeline = pipelines.load_pipeline(args)
    pipeline.traceback(args.full_trace)


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
    parser = argparse.ArgumentParser(description='OpusPocus NMT Pipeline Manager')
    parser.set_defaults(fn=main_list_commands)
    parser.add_argument(
        '--log-level', choices=['info', 'debug'], default='info',
        help='Indicates current logging level.'
    )
    subparsers = parser.add_subparsers(help='command', dest='command')

    # TODO: more arguments (?)

    # Pipeline Init
    parser_init = subparsers.add_parser('init')
    parser_init.add_argument(
        '--pipeline-dir', type=str, required=True,
        help='Pipeline root directory.'
    )
    parser_init.add_argument(
        '--pipeline-config', type=str, default=None,
        help='Pipeline configuration YAML.'
    )
    from opuspocus.pipelines import PIPELINE_REGISTRY
    parser_init.add_argument(
        '--pipeline', '-p', type=str, default='simple', metavar='PIPELINE',
        choices=PIPELINE_REGISTRY.keys(),
        help='Training pipeline type.'
    )
    parser_init.set_defaults(fn=main_init)

    # Pipeline Run
    parser_run = subparsers.add_parser('run')
    parser_run.add_argument(
        '--pipeline-dir', type=str, required=True,
        help='Pipeline root directory.'
    )
    parser_run.add_argument(
        '--runner', choices=['sbatch'], default='sbatch',
        help='Pipeline step execution command.'
    )
    parser_run.add_argument(
        '--runner-opts', type=str, default=None,
        help='Additional options for the pipeline step execution.'
    )
    parser_run.set_defaults(fn=main_run)

    # Pipeline Traceback
    parser_traceback = subparsers.add_parser('traceback')
    parser_traceback.add_argument(
        '--pipeline-dir', type=str, required=True,
        help='Pipeline root directory.'
    )
    parser_traceback.add_argument(
        '--full-trace', action='store_true',
        help='Also print the parameters of the individual '
             'pipeline steps.'
    )
    parser_traceback.set_defaults(fn=main_traceback)

    return parser


def parse_init_args(args, unparsed_args, parser):
    from opuspocus.pipelines import PIPELINE_REGISTRY
    PIPELINE_REGISTRY[args.pipeline].add_args(parser)

    parser = load_config_defaults(parser, args.pipeline_config)

    # Parse second time to get pipeline options
    additional_args, _ = parser.parse_known_args(unparsed_args)

    return update_args(args, additional_args)

if __name__ == '__main__':
    parser = create_args_parser()

    # Parse the main command
    args, unparsed_args = parser.parse_known_args()

    logging.basicConfig(level=logging.INFO)
    if args.log_level == 'debug':
        logging.basicConfig(level=logging.DEBUG)

    args.fn(args, unparsed_args, parser)
