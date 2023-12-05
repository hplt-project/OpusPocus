from typing import Any, Callable, Dict
from types import SimpleNamespace

import argparse
import yaml
from pathlib import Path


def get_action_type_map(parser) -> Dict[str, Callable]:
    type_map = {}
    for action in args._actions:
        type_map[action.dest] = action.type
    return type_map 


def load_config_defaults(parser, config_path: Path = None) -> Dict[str, Any]:
    if config_path is None:
        return {}
    if not Path(config_path).exists():
        raise ValueError('File {} not found.'.format(config_path))
    config = yaml.safe_load(open(config_path, 'r'))

    for v in parser._actions:
        if v.dest in config:
            v.required = False
    parser.set_defaults(**config)

    return parser


def update_args(
    args: argparse.Namespace,
    parser
) ->SimpleNamespace:
    if args.command == 'init' and args.config is not None:
        config_path = Path(args.config).resolve()
        if not config_path.exists():
            raise ValueError('File {} not found.'.format(config_path))
        config = yaml.safe_load(open(str(config_path), 'r'))

        type_map = get_action_type_map(parser)
        for key, val in config.items():
            if isinstance(val, list):
                val_list = []
                for v in val:
                    val_list.extend(v)
                setattr(args, key, type_map[key](val_list))
            else:
                setattr(args, key, type_map[key](val))
        return args
    return args


def print_indented(text, level=0):
    indent = ' ' * (2 * level)
    print(indent + text)


def file_path(path_str):
    path = Path(path_str)
    if path.exists():
        return path.absolute()
    else:
        raise FileNotFoundError()
