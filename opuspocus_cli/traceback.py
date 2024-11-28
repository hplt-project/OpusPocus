#!/usr/bin/env python3
import sys
from argparse import Namespace
from typing import Sequence

from opuspocus.options import parse_traceback_args
from opuspocus.pipelines import load_pipeline


def parse_args(argv: Sequence[str]) -> Namespace:
    return parse_traceback_args(argv)


def main(args: Namespace) -> int:
    """Pipeline structure analysis command.

    Prints the simplified dependency graph of the pipeline steps
    with their current status.
    """
    pipeline = load_pipeline(args)
    pipeline.print_traceback(target_labels=args.targets)
    return 0


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    sys.exit(main(args))
