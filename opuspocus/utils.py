from typing import Any, Callable, Dict

from argparse import Namespace
import yaml
from pathlib import Path


def get_action_type_map(parser) -> Dict[str, Callable]:
    type_map = {}
    for action in args._actions:
        type_map[action.dest] = action.type
    return type_map 


def load_config_defaults(parser, config_path: Path = None) -> Dict[str, Any]:
    '''Loads default values from a config file.'''
    if config_path is None:
        return parser
    if not Path(config_path).exists():
        raise ValueError('File {} not found.'.format(config_path))
    config = yaml.safe_load(open(config_path, 'r'))

    for v in parser._actions:
        if v.dest in config:
            v.required = False
    parser.set_defaults(**config)

    return parser


def update_args(orig_args: Namespace, updt_args: Namespace) -> Namespace:
    '''Updates an original Namespace using a provided update Namespace.'''

    orig_vars = vars(orig_args)
    updt_vars = vars(updt_args)
    for k in orig_vars.keys():
        if k in updt_vars:
            del updt_vars[k]
    return Namespace(**orig_vars, **updt_vars)


def print_indented(text, level=0):
    '''A function wrapper for indented printing (of traceback).'''
    indent = ' ' * (2 * level)
    print(indent + text)


def file_path(path_str):
    '''File_path type definition for argparse.'''
    path = Path(path_str)
    if path.exists():
        return path.absolute()
    else:
        raise FileNotFoundError(path)
