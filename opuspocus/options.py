import argparse
import logging
import sys
from typing import Any, Optional, Sequence

from omegaconf import DictConfig, OmegaConf

from opuspocus.pipelines import OpusPocusPipeline
from opuspocus.runners import RUNNER_REGISTRY
from opuspocus.utils import file_path

logger = logging.getLogger(__name__)

GENERAL_DESCRIPTION = "OpusPocus NLP Pipeline Manager"
NESTED_ATTR_LEN = 2


class OpusPocusParser(argparse.ArgumentParser):
    """Custom parser class, modifying default argparse error and help message handling."""

    def error(self, message: str) -> None:
        logger.error("Error: %s\n", message)
        self.print_help()
        sys.exit(2)

    def parse_args(
        self,
        args: Optional[Sequence[str]] = None,
        namespace: Optional[argparse.Namespace] = None,
    ) -> argparse.Namespace:
        if namespace is None and (args is None or not args):
            self.print_usage()
            sys.exit(1)
        return super().parse_args(args=args, namespace=namespace)


def _add_general_arguments(
    parser: argparse.ArgumentParser,
    *,
    pipeline_dir_required: bool = True,
) -> None:
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["info", "debug"],
        default="info",
        help="Current logging level.",
    )
    parser.add_argument("unparsed", nargs=argparse.REMAINDER)
    OpusPocusPipeline.add_args(parser, pipeline_dir_required=pipeline_dir_required)


def parse2config(parser: argparse.ArgumentParser, argv: Sequence[str]) -> DictConfig:
    """TODO"""
    args = parser.parse_args(argv)
    config = DictConfig({})
    config.unparsed = OmegaConf.from_cli(args.unparsed)

    def set_nested(config: DictConfig, name: str, value: Any) -> None:  # noqa: ANN401
        """TODO"""
        name_arr = name.split(".")
        assert len(name_arr) == NESTED_ATTR_LEN

        group = getattr(config, name_arr[0], DictConfig({}))
        setattr(group, name_arr[1], value)
        setattr(config, name_arr[0], group)

    config.cli_options = DictConfig({})
    for arg in vars(args):
        if arg == "unparsed":
            continue
        if "." in arg:
            set_nested(config, arg, getattr(args, arg))
        else:
            setattr(config.cli_options, arg, getattr(args, arg))

    return config


def parse_run_args(argv: Sequence[str]) -> DictConfig:
    parser = OpusPocusParser(description=f"{GENERAL_DESCRIPTION}: Pipeline Execution")
    _add_general_arguments(parser, pipeline_dir_required=False)

    parser.add_argument("--reinit", default=False, action="store_true", help="Re-initialize an existing pipeline.")
    parser.add_argument(
        "--reinit-failed",
        default=False,
        action="store_true",
        help="Re-initialize failed steps of an existing pipeline.",
    )
    parser.add_argument(
        "--stop-previous-run", default=False, action="store_true", help="Stop previous pipeline execution first."
    )
    parser.add_argument(
        "--resubmit-finished-subtasks", default=False, action="store_true", help="Re-run finished steps."
    )
    parser.add_argument("--pipeline-config", type=file_path, default=None, help="Pipeline configuration YAML file.")
    parser.add_argument(
        "--runner",
        type=str,
        choices=RUNNER_REGISTRY.keys(),
        dest="runner.runner",
        default=None,
        help="Runner used for pipeline execution manipulation {" + ",".join(RUNNER_REGISTRY.keys()) + "}",
    )

    args, _ = parser.parse_known_args(argv)
    RUNNER_REGISTRY[getattr(args, "runner.runner")].add_args(parser)

    return parse2config(parser, argv)


def parse_stop_args(argv: Sequence[str]) -> DictConfig:
    parser = OpusPocusParser(description=f"{GENERAL_DESCRIPTION}: Pipeline Termination")

    _add_general_arguments(parser, pipeline_dir_required=True)

    return parse2config(parser, argv)


def parse_status_args(argv: Sequence[str]) -> DictConfig:
    parser = OpusPocusParser(description=f"{GENERAL_DESCRIPTION}: Pipeline Step Status")

    _add_general_arguments(parser, pipeline_dir_required=True)

    return parse2config(parser, argv)


def parse_traceback_args(argv: Sequence[str]) -> DictConfig:
    parser = OpusPocusParser(description=f"{GENERAL_DESCRIPTION}: Pipeline Graph Traceback")

    _add_general_arguments(parser, pipeline_dir_required=True)
    parser.add_argument(
        "--verbose",
        default=False,
        action="store_true",
        help="Include additional step parameter information.",
    )

    return parse2config(parser, argv)
