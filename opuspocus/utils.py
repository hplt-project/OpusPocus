import gzip
import logging
import subprocess
import time
from pathlib import Path
from typing import Any, List, TextIO

from omegaconf import DictConfig, OmegaConf

logger = logging.getLogger(__name__)


def open_file(file: Path, mode: str) -> TextIO:
    """Return a correct file handle based on the file suffix."""
    assert mode in ("r", "w")
    if file.suffix == ".gz":
        return gzip.open(file, f"{mode}t")
    return file.open(f"{mode}t")


def file_line_index(file: Path) -> List[int]:
    """Return a list of beginning of line indices for a given file."""
    offsets = []
    offset = 0
    for line in open_file(file, "r"):
        offsets.append(offset)
        # Correctly measure the length of string with unicode characters
        offset += len(bytes(line, "utf-8"))
    return offsets


def read_shard(file: Path, file_line_index: List[int], start: int, shard_size: int) -> List[str]:
    """Read a subset of lines in a file using the line_index."""
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
    """Decompress a file."""
    with gzip.open(input_file, "rt") as in_fh, output_file.open("w") as out_fh:
        for line in in_fh:
            print(line, end="", file=out_fh)


def concat_files(input_files: List[Path], output_file: Path) -> None:
    """Concatenate files from a given list."""
    with open_file(output_file, "w") as out_fh:
        for input_file in input_files:
            with open_file(input_file, "r") as in_fh:
                for line in in_fh:
                    print(line, end="", file=out_fh)


def paste_files(
    input_files: List[Path],
    output_file: Path,
    delimiter: str = "\t",
) -> None:
    """A simplified Unix paste command."""
    with open_file(output_file, "w") as out_fh:
        in_fhs = [open_file(input_file, "r") for input_file in input_files]
        for lines in zip(*in_fhs):
            lines = [line.rstrip("\n") for line in lines]  # noqa: PLW2901
            print(delimiter.join(lines), end="\n", file=out_fh)


def cut_file(
    input_file: Path,
    output_files: List[Path],
    delimiter: str = "\t",
) -> None:
    """A simplified Unix cut command."""
    cut_filestream(
        input_stream=open_file(input_file, "r"),
        output_files=output_files,
        delimiter=delimiter,
    )


def cut_filestream(
    input_stream,  # noqa: ANN001
    output_files: List[Path],
    delimiter: str = "\t",
) -> None:
    """A simplified Unix cut for processing filestreams."""
    out_fhs = [open_file(output_file, "w") for output_file in output_files]
    for line in input_stream:
        for i, (col, fh) in enumerate(zip(line.split(delimiter), out_fhs)):
            if i == len(out_fhs) - 1:
                print(col, end="", file=fh)
            else:
                print(col, file=fh)


def save_filestream(
    input_stream,  # noqa: ANN001
    output_file: Path,
) -> None:
    """Save a filestream to a file."""
    out_fh = open_file(output_file, "w")
    for line in input_stream:
        print(line, end="", file=out_fh)


def clean_dir(directory: Path, exclude: str = None) -> None:  # noqa: RUF013
    """Recursively remove contents of a directory.

    Args:
        directory: location of the directory
        exclude: a filename to exclude from deletion
    """
    for file_path in directory.iterdir():
        filename = file_path.stem + file_path.suffix
        if exclude is not None and exclude == filename:
            continue
        try:
            if file_path.is_file() or file_path.is_symlink():
                file_path.unlink()
            elif file_path.is_dir():
                clean_dir(file_path, exclude=exclude)
                file_path.rmdir()
        except Exception as err:  # noqa: BLE001
            logger.error("Failed to delete %s. Reason: %s", file_path, err)  # noqa: TRY400


def count_lines(file_path: Path) -> int:
    """Return the number of lines in a text file."""
    with open_file(file_path, "r") as fh:
        return len(fh.readlines())


def subprocess_wait(proc: subprocess.Popen) -> None:
    """Wait until the subprocess finishes execution."""
    while proc.poll() is None:
        time.sleep(0.5)
    if proc.returncode:
        err_msg = f"Process {proc.pid} exited with non-zero value ({proc.returncode})."
        raise subprocess.SubprocessError(err_msg)


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


def flatten_dict_config(config: DictConfig, max_depth: int) -> DictConfig:
    """Flatten the nested config to a specified level.

    Args:
        config (DictConfig): config to be flattened
        max_level (int): maximum allowed depth of the flattened config (0 implies non-nested dictionary)
    """

    def flatten(config: DictConfig) -> DictConfig:
        new_config = DictConfig({})
        for key, value in config.items():
            if not isinstance(value, DictConfig):
                setattr(new_config, key, value)
            else:
                for k, v in flatten(value).items():
                    setattr(new_config, f"{key}.{k}", v)
        return new_config

    def nest(top_key: str, value: Any, keys: List[str]) -> DictConfig:  # noqa: ANN401
        if not keys:
            return DictConfig({top_key: value})
        return DictConfig({top_key: nest(keys[0], value, keys[1:])})

    config_flat = flatten(config)
    new_config = DictConfig({})
    for key, value in config_flat.items():
        key_split = key.split(".")
        nested_entry = nest(
            top_key=".".join(key_split[: len(key_split) - max_depth]),
            value=value,
            keys=key_split[len(key_split) - max_depth :],
        )
        new_config = OmegaConf.merge(new_config, nested_entry)
    return new_config
