#!/usr/bin/env python3
from typing import Sequence

import sys
from argparse import Namespace

from opuspocus.options import create_traceback_parser
from opuspocus.pipelines import load_pipeline


def parse_args(argv: Sequence[str]) -> Namespace:
    parser = create_traceback_parser()
    return parser.parse_args(argv)


def main(args: Namespace) -> int:
    """Pipeline structure analysis command.

    Prints the simplified dependency graph of the pipeline steps 
    with their current status.
    """
    pipeline = load_pipeline(args)
    pipeline.traceback(args.targets)
    return 0


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    sys.exit(main(args))
