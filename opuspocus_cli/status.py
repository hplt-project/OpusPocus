#!/usr/bin/env python3
import sys
from argparse import Namespace
from typing import Sequence

from opuspocus.options import parse_status_args
from opuspocus.pipelines import load_pipeline


def parse_args(argv: Sequence[str]) -> Namespace:
    return parse_status_args(argv)


def main(args: Namespace) -> int:
    """Command that prints the current status of each pipeline step."""
    pipeline = load_pipeline(args)
    pipeline.print_status(pipeline.steps)
    return 0


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    sys.exit(main(args))
