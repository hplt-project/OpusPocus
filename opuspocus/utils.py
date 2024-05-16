from typing import Any, Callable, Dict, List, Optional

import inspect
import json
import logging
import os
import yaml
from argparse import Namespace
from pathlib import Path

logger = logging.getLogger(__name__)


def get_action_type_map(parser) -> Dict[str, Callable]:
    type_map = {}
    for action in args._actions:
        type_map[action.dest] = action.type
    return type_map 


def load_config_defaults(parser, config_path: Path = None) -> Dict[str, Any]:
    """Loads default values from a config file."""
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
    """Update a give namespace values."""

    orig_vars = vars(orig_args)
    updt_vars = vars(updt_args)
    for k in orig_vars.keys():
        if k in updt_vars:
            del updt_vars[k]
    return Namespace(**orig_vars, **updt_vars)


def print_indented(text, level=0):
    """A function wrapper for indented printing (of traceback)."""
    indent = ' ' * (2 * level)
    print(indent + text)


def file_path(path_str):
    """A file_path type definition for argparse."""
    path = Path(path_str)
    if path.exists():
        return path.absolute()
    else:
        raise FileNotFoundError(path)


class RunnerResources(object):
    """Runner-agnostic resources object.

    TODO
    """

    def __init__(
        self,
        cpus: int = 1,
        gpus: int = 0,
        mem: str = '1g',
        partition: Optional[str] = None,
        account: Optional[str] = None,
    ):
        self.cpus = cpus
        self.gpus = gpus
        self.mem = mem
        self.partition = partition
        self.account = account

    @classmethod
    def list_parameters(cls) -> List[str]:
        """TODO"""
        return [
            param for param in inspect.signature(cls.__init__).parameters
            if param != 'self'
        ]

    def overwrite(self, resource_overwrite: 'RunnerResources') -> 'RunnerResources':
        params = {}
        for param in self.list_parameters():
            val = getattr(resource_overwrite, param)
            if val is None:
                val = getattr(self, param)
            params[param] = val
        return RunnerResources(**params)

    def to_json(self, json_path: Path) -> None:
        """Serialize the object (to JSON).

        TODO
        """
        json_dict = {
            param: getattr(self, param)
            for param in self.list_parameters()
        }
        json.dump(json_dict, open(json_path, 'w'), indent=2)

    @classmethod
    def from_json(cls, json_path: Path) -> 'RunnerResources':
        """TODO"""
        json_dict = json.load(open(json_path, 'r'))

        cls_params = cls.list_parameters()
        params = {}
        for k, v in json_dict.items():
            if k not in cls_params:
                logger.warn('Resource {} not supported. Ignoring'.format(k))
            params[k] = v
        return RunnerResources(**params)

    @classmethod
    def get_env_name(cls, name) -> str:
        """TODO"""
        assert name in cls.list_parameters()
        return 'OPUSPOCUS_{}'.format(name)

    def get_env_dict(self) -> Dict[str, str]:
        env_dict = {}
        for param in self.list_parameters():
            param_val = getattr(self, param)
            if param_val is not None:
                env_dict[self.get_env_name(param)] = str(param_val)
        return {**os.environ.copy(), **env_dict}
