import argparse
import logging
import sys
from typing import Optional, Sequence

from opuspocus.pipelines import OpusPocusPipeline
from opuspocus.runners import RUNNER_REGISTRY

logger = logging.getLogger(__name__)

GEN_DESCRIPTION = "OpusPocus NLP Pipeline Manager"


class OpusPocusParser(argparse.ArgumentParser):
    """Custom parser class, modifying default argparse error and help message
    handling.

    """

    def error(self, message: str) -> None:
        logger.error("Error: %s\n", message)
        self.print_help()
        sys.exit(2)

    def parse_args(
        self, args: Optional[Sequence[str]] = None, namespace: argparse.Namespace = None
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
    OpusPocusPipeline.add_args(parser, pipeline_dir_required=pipeline_dir_required)


def _add_runner_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--runner",
        type=str,
        metavar="RUNNER",
        required=True,
        choices=RUNNER_REGISTRY.keys(),
        help="Runner used for pipeline execution manipulation.",
    )
    parser.add_argument(
        "--targets",
        type=str,
        nargs="+",
        default=None,
        help="List of steps to be executed together with their dependencies.",
    )


def parse_init_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = OpusPocusParser(description="{}: Pipeline Initialization".format(GEN_DESCRIPTION))

    _add_general_arguments(parser, pipeline_dir_required=False)
    parser.add_argument(
        "--pipeline-config",
        type=str,
        default=None,
        help="Pipeline configuration YAML file.",
    )

    return parser.parse_args(argv)


def parse_run_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = OpusPocusParser(description="{}: Pipeline Execution".format(GEN_DESCRIPTION))

    _add_general_arguments(parser, pipeline_dir_required=True)
    _add_runner_arguments(parser)

    args, unparsed = parser.parse_known_args(argv)
    RUNNER_REGISTRY[args.runner].add_args(parser)

    return parser.parse_args(argv)


def parse_stop_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = OpusPocusParser(description="{}: Pipeline Termination".format(GEN_DESCRIPTION))

    _add_general_arguments(parser, pipeline_dir_required=True)
    _add_runner_arguments(parser)

    return parser.parse_args(argv)


def parse_status_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = OpusPocusParser(description="{}: Pipeline Step Status".format(GEN_DESCRIPTION))

    _add_general_arguments(parser, pipeline_dir_required=True)

    return parser.parse_args(argv)


def parse_traceback_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = OpusPocusParser(description="{}: Pipeline Graph Traceback".format(GEN_DESCRIPTION))

    _add_general_arguments(parser, pipeline_dir_required=True)

    parser.add_argument(
        "--targets",
        type=str,
        nargs="+",
        default=None,
        help="Pipeline targets from which to traceback.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Include additional step parameter information.",
    )

    return parser.parse_args(argv)
