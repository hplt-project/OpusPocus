import importlib
import logging
import sys
from pathlib import Path
from typing import Sequence

from omegaconf import DictConfig

from opuspocus.options import ERR_RETURN_CODE

CMD_MODULES = {}


def _print_usage() -> None:
    print(  # noqa: T201
        f"usage: {sys.argv[0]} " + "{" + ",".join(CMD_MODULES.keys()) + "} [options]",
    )


def parse_args(argv: Sequence[str]) -> DictConfig:
    """Call the correct subcommand parser, given the subcommand."""
    if not argv:
        _print_usage()
        sys.exit(ERR_RETURN_CODE)

    cmd = argv[0]
    if cmd not in CMD_MODULES:
        _print_usage()
        sys.exit(ERR_RETURN_CODE)

    try:
        args = CMD_MODULES[cmd].parse_args(argv[1:])
    except AttributeError as exc:
        _print_usage()
        err_msg = "Error parsing CLI arguments."
        raise AttributeError(err_msg) from exc
    assert not hasattr(args, "command")

    args.command = cmd
    return args


def main(argv: Sequence[str]) -> int:
    """Process the CLI arguments and call a specific CLI main method."""
    args = parse_args(argv)
    log_level = None
    if args.cli_options.log_level == "info":
        log_level = logging.INFO
    elif args.cli_options.log_level == "debug":
        log_level = logging.DEBUG
    else:
        err_msg = f"Unkown logging level: {log_level}."
        raise ValueError(err_msg)
    logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", level=log_level, datefmt="%Y-%m-%d %H:%M:%S")
    return CMD_MODULES[args.command].main(args)


cli_dir = Path(__file__).parents[0]
for file in cli_dir.iterdir():
    if (
        not file.stem.startswith("_")
        and not file.stem.startswith(".")
        and file.name.endswith("py")
        and not file.is_dir()
    ):
        cmd_name = file.stem if file.name.endswith(".py") else file

        # We import all the CLI modules and register them for later calls
        importlib.import_module(f"opuspocus_cli.{cmd_name}")
        CMD_MODULES[cmd_name] = sys.modules[f"opuspocus_cli.{cmd_name}"]
