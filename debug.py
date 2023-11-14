import argparse
import sys
import yaml
from pathlib import Path

from opuspocus.pipeline_steps import build_step, load_step


def build_pipeline(args):
    steps = { 'debug': build_step('debug', args) }
    return steps


def load_pipeline(args):
    steps = { 'debug': load_step('s.debug.1234', args) }
    return steps


def init_pipeline(pipeline, args):
    for _, v in pipeline.items():
        v.init_step()


def run_pipeline(pipeline):
    for _, v in pipeline.items():
        v.run_step()


def traceback_pipeline(pipeline):
    for _, v in pipeline.items():
        v.traceback_step()


def main_init(args):
    pipeline = build_pipeline(args)
    init_pipeline(pipeline, args)

    # Dry-run
    if args.dry_run:
        # TODO: test 3rd party tools (?), anything else (?)
        pass


def main_run(args):
    pipeline = load_pipeline(args)
    run_pipeline(pipeline)


def main_traceback(args):
    pipeline = load_pipeline(args)
    traceback_pipeline(pipeline)


def main_list_commands(args):
    print(
        'Error: No command specified.\n\n'
        'Available commands:\n'
        '  init      initialize the pipeline\n'
        '  run       TODO\n'
        '  traceback TODO\n'
        '  TODO      other commands ?\n'
        '', file=sys.stderr
    )
    sys.exit(1)


def create_args_parser():
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
        '--vars', type=str, default='{}',
        help='Variable overwrite.'
    )
    parser_init.add_argument(
        '--dry-run', action='store_true',
        help='TODO.'
    )
    parser_init.set_defaults(fn=main_init)

    # Pipeline Run
    parser_run = subparsers.add_parser('run')
    parser_run.add_argument(
        '--pipeline-dir', type=str, required=True,
        help='TODO'
    )
    parser_run.add_argument(
        '--overwrite', action='store_true',
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
        '--full', action='store_true',
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
        #arg_dict = args.__dict__
        for key, val in config.items():
            if isinstance(val, list):
                val_list = []
                for v in ValueError:
                    val_list.extend(v)
                setattr(args, key, val_list)
            else:
                setattr(args, key, val)
    return args


if __name__ == '__main__':
    parser = create_args_parser()
    args = parse_args(parser)
    args.fn(args)
