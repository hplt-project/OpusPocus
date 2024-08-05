from typing import Any, Callable, Dict, List
from argparse import Namespace
from pathlib import Path

import gzip
import inspect
import json
import logging
import os
import subprocess
import yaml

logger = logging.getLogger(__name__)


def get_open_fn(compressed: bool):
    if compressed:
        return gzip.open
    return open


def open_file(file: Path, mode: str):
    assert mode == "r" or mode == "w"
    open_fn = get_open_fn(compressed=(file.suffix == ".gz"))
    return open_fn(file, f"{mode}t")


def decompress_file(input_file: Path, output_file: Path) -> None:
    with gzip.open(input_file, "rt") as in_fh:
        with open(output_file, "wt") as out_fh:
            for line in in_fh:
                print(line, end="", file=out_fh)


def concat_files(
    input_files: List[Path], output_file: Path, compressed: bool = True
) -> None:
    open_fn = get_open_fn(compressed)
    with open_fn(output_file, "wt") as out_fh:
        for input_file in input_files:
            with open_fn(input_file, "rt") as in_fh:
                for line in in_fh:
                    print(line, end="", file=out_fh)


def paste_files(
    input_files: List[Path],
    output_file: Path,
    compressed: bool = True,
    delimiter: str = "\t",
) -> None:
    open_fn = get_open_fn(compressed)
    with open_fn(output_file, "wt") as out_fh:
        in_fhs = [open_fn(input_file, "rt") for input_file in input_files]
        for lines in zip(*in_fhs):
            lines = [line.rstrip("\n") for line in lines]
            print(delimiter.join(lines), end="\n", file=out_fh)


def cut_file(
    input_file: Path,
    output_files: List[Path],
    compressed: bool = True,
    delimiter: str = "\t",
) -> None:
    open_fn = get_open_fn(compressed)
    cut_filestream(
        input_stream=open_fn(input_file, "rt"),
        output_files=output_files,
        compressed=compressed,
        delimiter=delimiter,
    )


def cut_filestream(
    input_stream,
    output_files: List[Path],
    compressed: bool = True,
    delimiter: str = "\t",
) -> None:
    open_fn = get_open_fn(compressed)
    out_fhs = [open_fn(output_file, "wt") for output_file in output_files]
    for line in input_stream:
        for i, (col, fh) in enumerate(zip(line.split(delimiter), out_fhs)):
            if i == len(out_fhs) - 1:
                print(col, end="", file=fh)
            else:
                print(col, file=fh)


def save_filestream(
    input_stream,
    output_file: Path,
    compressed: bool = True,
) -> None:
    open_fn = get_open_fn(compressed)
    out_fh = open_fn(output_file, "wt")
    for line in input_stream:
        print(line, end="", file=out_fh)


def file_to_shards(
    file_path: Path,
    shard_dir: Path,
    shard_size: int,
    shard_index_pad_length: int = 4,
) -> List[str]:
    shard_list = []
    out_fh = None
    with open_file(file_path, "r") as in_fh:
        for i, line in enumerate(in_fh):
            if i % shard_size == 0:
                n = i // shard_size
                shard_filename = file_path.stem + file_path.suffix + f".{n}"
                shard_list.append(shard_filename)
                shard_file_path = Path(shard_dir, shard_filename)
                out_fh = open_file(shard_file_path, "w")
            print(line, end="", file=out_fh)
    return shard_list


def shards_to_file(
    shard_list: List[str],
    file_path: Path,
) -> None:
    with open_file(file_path, "w") as out_fh:
        for shard_file_path in shard_list:
            with open_file(shard_file_path, "r") as in_fh:
                for line in in_fh:
                    print(line, end="", file=out_fh)


def clean_dir(directory: Path, exclude: str = None) -> None:
    for file_path in directory.iterdir():
        filename = file_path.stem + file_path.suffix
        if exclude is not None and exclude == filename:
            continue
        try:
            if file_path.is_file() or file_path.is_symlink():
                file_path.unlink()
            elif file_path.is_dir():
                file_path.rmdir()
        except Exception as e:
            logger.error("Failed to delete %s. Reason: %s", file_path, e.message)


def count_lines(file_path: Path) -> int:
    with open_file(file_path, "r") as fh:
        return len(fh.readlines())


def subprocess_wait(proc: subprocess.Popen) -> None:
    rc = proc.wait()
    if rc:
        raise subprocess.SubprocessError(
            "Process {} exited with non-zero value.".format(proc.pid)
        )


def get_action_type_map(parser) -> Dict[str, Callable]:
    type_map = {}
    for action in parser._actions:
        type_map[action.dest] = action.type
    return type_map


def load_config_defaults(parser, config_path: Path = None) -> Dict[str, Any]:
    """Loads default values from a config file."""
    if config_path is None:
        return parser
    if not Path(config_path).exists():
        raise ValueError("File {} not found.".format(config_path))
    config = yaml.safe_load(open(config_path, "r"))

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
    indent = " " * (2 * level)
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
        mem: str = "1g",
    ):
        self.cpus = cpus
        self.gpus = gpus
        self.mem = mem

    @classmethod
    def list_parameters(cls) -> List[str]:
        """TODO"""
        return [
            param
            for param in inspect.signature(cls.__init__).parameters
            if param != "self"
        ]

    def overwrite(self, resource_dict: Dict[str, Any]) -> "RunnerResources":
        params = {}
        for param in self.list_parameters():
            val = getattr(self, param)
            if param in resource_dict:
                val = resource_dict[param]
            params[param] = val
        return RunnerResources(**params)

    def to_json(self, json_path: Path) -> None:
        """Serialize the object (to JSON).

        TODO
        """
        json_dict = {param: getattr(self, param) for param in self.list_parameters()}
        json.dump(json_dict, open(json_path, "w"), indent=2)

    @classmethod
    def from_json(cls, json_path: Path) -> "RunnerResources":
        """TODO"""
        json_dict = json.load(open(json_path, "r"))

        cls_params = cls.list_parameters()
        params = {}
        for k, v in json_dict.items():
            if k not in cls_params:
                logger.warn("Resource %s not supported. Ignoring", k)
            params[k] = v
        return RunnerResources(**params)

    @classmethod
    def get_env_name(cls, name) -> str:
        """TODO"""
        assert name in cls.list_parameters()
        return "OPUSPOCUS_{}".format(name)

    def get_env_dict(self) -> Dict[str, str]:
        env_dict = {}
        for param in self.list_parameters():
            param_val = getattr(self, param)
            if param_val is not None:
                env_dict[self.get_env_name(param)] = str(param_val)
        return {**os.environ.copy(), **env_dict}
