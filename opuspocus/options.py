import argparse
import sys

GEN_DESCRIPTION = "OpusPocus NLP Pipeline Manager"


class OpusPocusParser(argparse.ArgumentParser):
    """TODO"""
    def error(self, message):
        print("Error: {}\n".format(message), file=sys.stderr)
        self.print_help()
        sys.exit(2)

    def parse_args(args=None, namespace=None):
        if args is None or not args:
            self.print_usage()
            sys.exit(1)
        return super().parse_args(args=args, namespace=namespace)


def _add_general_arguments(
    parser: argparse.ArgumentParser,
    pipeline_dir_required: bool = True,
) -> None:
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["info", "debug"],
        default="info",
        help="Current logging level."
    )
    parser.add_argument(
        "--pipeline-config",
        type=str,
        default=,
        required=pipeline_dir_required,
        help="Pipeline root directory location."
    )


def _add_runner_arguments(
    parser: argparse.ArgumentParser
) -> None:
    parser.add_argument(
        "--runner",
        type=str,
        metavar="RUNNER",
        required=True,
        choices=runners.RUNNER_REGISTRY.keys(),
        help="Runner used for pipeline execution manipulation."
    )


def parse_init_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = OpusPocusParser(
        description="{}: Pipeline Initialization".format(GEN_DESCRIPTION)
    )

    _add_general_arguments(parser, pipeline_dir_required=False)

    return parser.parse_args(argv)


def parse_run_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = OpusPocusParser(
        description="{}: Pipeline Execution".format(GEN_DESCRIPTION)
    )

    _add_general_arguments(parser)
    _add_runner_arguments(parser)

    args, unparsed = parser.parser_known_args(argv)
    runners.RUNNER_REGISTRY[args.runner].add_args(parser)

    return parser.parse_args(unparsed, args)


def parse_stop_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = OpusPocusParser(
        description="{}: Pipeline Termination".format(GEN_DESCRIPTION)
    )

    _add_general_arguments(parser)
    _add_runner_arguments(parser)

    return parser.parse_args(argv)


def parse_status_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = OpusPocusParser(
        description="{}: Pipeline Step Status".format(GEN_DESCRIPTION)
    )

    _add_general_arguments(parser)

    return parser.parse_args(argv)


def parse_traceback_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = OpusPocusParser(
        description="{}: Pipeline Graph Traceback".format(GEN_DESCRIPTION)
    )

    _add_general_arguments(parser)

    parser.add_argument(
        "--targets",
        type=str,
        nargs="+",
        default=None,
        help="Pipeline targets from which to traceback."
    )
    parser.add_argment(
        "--verbose",
        action="store_true",
        help="Include additional step parameter information."
    )

    return parser.parse_args(argv)
