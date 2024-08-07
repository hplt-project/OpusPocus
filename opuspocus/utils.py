import gzip
import inspect
import json
import logging
import os
import subprocess
from argparse import Namespace
from pathlib import Path
from typing import Any, Callable, Dict, List

import yaml

logger = logging.getLogger(__name__)


def get_open_fn(compressed: bool):  # noqa: ANN201, FBT001
    if compressed:
        return gzip.open
    return open


def open_file(file: Path, mode: str):  # noqa: ANN201
    assert mode == "r" or mode == "w"  # noqa: PLR1714
    open_fn = get_open_fn(compressed=(file.suffix == ".gz"))
    return open_fn(file, f"{mode}t")


def file_line_index(file: Path) -> List[int]:
    offsets = []
    offset = 0
    for line in open_file(file, "r"):
        offsets.append(offset)
        offset += len(line)
    return offsets


def read_shard(
    file: Path, file_line_index: List[int], start: int, shard_size: int
) -> List[str]:
    assert shard_size > 0
    assert start >= 0
    lines = []
    with open_file(file, "r") as fh:
        fh.seek(file_line_index[start])
        for line in fh:
            lines.append(line)
            shard_size -= 1
            if shard_size == 0:
                break
    return lines


def decompress_file(input_file: Path, output_file: Path) -> None:
    with gzip.open(input_file, "rt") as in_fh:  # noqa: SIM117
        with open(output_file, "w") as out_fh:  # noqa: PTH123
            for line in in_fh:
                print(line, end="", file=out_fh)


def concat_files(input_files: List[Path], output_file: Path, compressed: bool = True) -> None:  # noqa: FBT001, FBT002
    open_fn = get_open_fn(compressed)
    with open_fn(output_file, "wt") as out_fh:
        for input_file in input_files:
            with open_fn(input_file, "rt") as in_fh:
                for line in in_fh:
                    print(line, end="", file=out_fh)


def paste_files(
    input_files: List[Path],
    output_file: Path,
    compressed: bool = True,  # noqa: FBT001, FBT002
    delimiter: str = "\t",
) -> None:
    open_fn = get_open_fn(compressed)
    with open_fn(output_file, "wt") as out_fh:
        in_fhs = [open_fn(input_file, "rt") for input_file in input_files]
        for lines in zip(*in_fhs):
            lines = [line.rstrip("\n") for line in lines]  # noqa: PLW2901
            print(delimiter.join(lines), end="\n", file=out_fh)


def cut_file(
    input_file: Path,
    output_files: List[Path],
    compressed: bool = True,  # noqa: FBT001, FBT002
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
    input_stream,  # noqa: ANN001
    output_files: List[Path],
    compressed: bool = True,  # noqa: FBT001, FBT002
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
    input_stream,  # noqa: ANN001
    output_file: Path,
    compressed: bool = True,  # noqa: FBT001, FBT002
) -> None:
    open_fn = get_open_fn(compressed)
    out_fh = open_fn(output_file, "wt")
    for line in input_stream:
        print(line, end="", file=out_fh)


def clean_dir(directory: Path, exclude: str = None) -> None:  # noqa: RUF013
    for file_path in directory.iterdir():
        filename = file_path.stem + file_path.suffix
        if exclude is not None and exclude == filename:
            continue
        try:
            if file_path.is_file() or file_path.is_symlink():
                file_path.unlink()
            elif file_path.is_dir():
                file_path.rmdir()
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to delete %s. Reason: %s", file_path, e.message)  # noqa: TRY400


def count_lines(file_path: Path) -> int:
    with open_file(file_path, "r") as fh:
        return len(fh.readlines())


def subprocess_wait(proc: subprocess.Popen) -> None:
    rc = proc.wait()
    if rc:
        raise subprocess.SubprocessError(f"Process {proc.pid} exited with non-zero value.")  # noqa: EM102, TRY003


def get_action_type_map(parser) -> Dict[str, Callable]:  # noqa: ANN001
    type_map = {}
    for action in parser._actions:  # noqa: SLF001
        type_map[action.dest] = action.type
    return type_map


def load_config_defaults(parser, config_path: Path = None) -> Dict[str, Any]:  # noqa: ANN001, RUF013
    """Loads default values from a config file."""
    if config_path is None:
        return parser
    if not Path(config_path).exists():
        raise ValueError(f"File {config_path} not found.")  # noqa: EM102, TRY003
    config = yaml.safe_load(open(config_path))  # noqa: PTH123, SIM115

    for v in parser._actions:  # noqa: SLF001
        if v.dest in config:
            v.required = False
    parser.set_defaults(**config)

    return parser


def update_args(orig_args: Namespace, updt_args: Namespace) -> Namespace:
    """Update a give namespace values."""

    orig_vars = vars(orig_args)
    updt_vars = vars(updt_args)
    for k in orig_vars.keys():  # noqa: SIM118
        if k in updt_vars:
            del updt_vars[k]
    return Namespace(**orig_vars, **updt_vars)


def print_indented(text, level=0):  # noqa: ANN001, ANN201
    """A function wrapper for indented printing (of traceback)."""
    indent = " " * (2 * level)
    print(indent + text)  # noqa: T201


def file_path(path_str):  # noqa: ANN001, ANN201
    """A file_path type definition for argparse."""
    path = Path(path_str)
    if path.exists():
        return path.absolute()
    else:  # noqa: RET505
        raise FileNotFoundError(path)


class RunnerResources:
    """Runner-agnostic resources object.

    TODO
    """

    def __init__(
        self,
        cpus: int = 1,
        gpus: int = 0,
        mem: str = "1g",
    ) -> None:
        self.cpus = cpus
        self.gpus = gpus
        self.mem = mem

    @classmethod
    def list_parameters(cls) -> List[str]:  # noqa: ANN102
        """TODO"""
        return [param for param in inspect.signature(cls.__init__).parameters if param != "self"]

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
        json.dump(json_dict, open(json_path, "w"), indent=2)  # noqa: PTH123, SIM115

    @classmethod
    def from_json(cls, json_path: Path) -> "RunnerResources":  # noqa: ANN102
        """TODO"""
        json_dict = json.load(open(json_path))  # noqa: PTH123, SIM115

        cls_params = cls.list_parameters()
        params = {}
        for k, v in json_dict.items():
            if k not in cls_params:
                logger.warning("Resource %s not supported. Ignoring", k)
            params[k] = v
        return RunnerResources(**params)

    @classmethod
    def get_env_name(cls, name) -> str:  # noqa: ANN001, ANN102
        """TODO"""
        assert name in cls.list_parameters()
        return f"OPUSPOCUS_{name}"

    def get_env_dict(self) -> Dict[str, str]:
        env_dict = {}
        for param in self.list_parameters():
            param_val = getattr(self, param)
            if param_val is not None:
                env_dict[self.get_env_name(param)] = str(param_val)
        return {**os.environ.copy(), **env_dict}
