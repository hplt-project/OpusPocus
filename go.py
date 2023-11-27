import argparse
import sys
import yaml
from pathlib import Path

from opuspocus.pipelines import build_pipeline, load_pipeline
from opuspocus.utils import update_args


def main_init(args):
    pipeline = build_pipeline(args.pipeline, args)
    pipeline.init()

    # Dry-run
    if args.dry_run:
        # TODO: test 3rd party tools (?), anything else(?), touch files for
        # checking dependencies (?)
        pass


def main_run(args):
    pipeline = load_pipeline(args)
    pipeline.run(args)


def main_traceback(args):
    pipeline = load_pipeline(args)
    pipeline.traceback()


def main_list_commands(args):
    print(
       'Error: No command specified.\n\n'
       'Available commands:\n'
       '  init      initialize the pipeline\n'
       '  run       execute the pipeline\n'
       '  traceback print the pipeline graph\n'
       '', file=sys.stderr
    )
    sys.exit(1)


def create_parse_args():
    parser = argparse.ArgumentParser(description='TODO')
    parser.set_defaults(fn=main_list_commands)
    subparsers = parser.add_subparsers(help='command', dest='command')

    # TODO: more arguments (?)

    # Pipeline Init
    parser_init = subparsers.add_parser('init')
    parser_init.add_argument(
        '--config', type=str, default=None,
        help='Pipeline configuration JSON.'
    )
    parser_init.add_argument(
        '--pipeline-dir', type=str, required=True,
        help='TODO'
    )
    parser_init.add_argument(
        '--vars', type=str, default='{}',
        help='Variable overwrite.'
    )
    parser_init.add_argument(
        '--dry-run', action='store_true',
        help='TODO'
    )
    parser_init.set_defaults(fn=main_init)

    # Pipeline Run
    parser_run = subparsers.add_parser('run')
    parser_run.add_argument(
        '--pipeline-dir', type=str, required=True,
        help='TODO'
    )
    parser_run.add_argument(
        '--runner', choices=['sbatch'], defaults='sbatch',
        help='TODO'
    )
    parser_run.add_argument(
        '--runner-opts', type=str, default=None,
        help='TODO'
    )
    parser_run.add_argument(
        '--overwrite', action='store_true',
        help='TODO'
    )
    parser_run.set_defaults(
        '--rerun-failed', action='store_true',
        help='TODO'
    )
    parser_run.set_defaults(fn=main_run)

    # Pipeline Traceback
    parser_traceback = subparsers.add_parser('traceback')
    parser_traceback.add_argument(
        '--pipeline-dir', type=str, required=True,
        help='TODO'
    )
    parser_traceback.add_argument(
        '--full-trace', action='store_true',
        help='TODO'
    )
    parser_traceback.set_defaults(fn=main_traceback)

    return parser


def parse_args(parser):
    args = parser.parse_args()
    if args.command == 'init' and args.config is not None:
        config_path = Path(args.config).resolve()
        if not config_path.exists():
            raise ValueError('File {} not found.'.format(config_path))
        config = yaml.load(open(str(config_path), 'r'))
        delattr(args, 'config')
        return update_args(args, config)
    return args


if __name__ == '__main__':
    parser = create_args_parser()
    args = parse_args(parser)
    args.fn(args)
