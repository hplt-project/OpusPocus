import importlib
import logging
import sys
from argparse import Namespace
from pathlib import Path
from typing import Sequence


CMD_MODULES = {}


def _print_usage():
    print(
        "usage: {} ".format(sys.argv[0])
        + "{"
        + ",".join(CMD_MODULES.keys())
        + "} [options]",
    )


def parse_args(argv: Sequence[str]) -> Namespace:
    """Call the correct subcommand parser, given the subcommand."""
    if not argv:
        _print_usage()
        sys.exit(1)

    cmd = argv[0]
    if cmd not in CMD_MODULES:
        _print_usage()
        sys.exit(1)

    args = CMD_MODULES[cmd].parse_args(argv[1:])
    assert not hasattr(args, "command")

    setattr(args, "command", cmd)
    return args


def main(argv: Sequence[str]) -> int:
    """Process the CLI arguments and call a specific CLI main method."""
    args = parse_args(argv)
    if args.log_level == "info":
        logging.basicConfig(level=logging.INFO)
    elif args.log_level == "debug":
        logging.basicConfig(level=logging.DEBUG)
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
        importlib.import_module("opuspocus_cli.{}".format(cmd_name))
        CMD_MODULES[cmd_name] = sys.modules["opuspocus_cli.{}".format(cmd_name)]
