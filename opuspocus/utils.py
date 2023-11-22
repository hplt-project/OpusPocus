from typing import Any, Dict
from types import SimpleNamespace


def update_args(
    args: SimpleNamespace,
    config: Dict[str, Any]
) ->SimpleNamespace:
    for key, val in config.items():
        if isinstance(val, list):
            val_list = []
            for v in val:
                val_list.extend(v)
            setattr(args, key, val_list)
        else:
            setattr(args, key, val)
    return args


def print_indented(text, level=0):
    indent = ' ' * (2 * level)
    print(indent + text)
