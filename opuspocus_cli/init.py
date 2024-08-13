#!/usr/bin/env python3
import sys
from argparse import Namespace
from typing import Sequence

from opuspocus.options import parse_init_args
from opuspocus.pipelines import build_pipeline


def parse_args(argv: Sequence[str]) -> Namespace:
    return parse_init_args(argv)


def main(args: Namespace) -> int:
    """Pipeline initialization command.

    Prepares the pipeline directory structure for later execution.
    """
    pipeline = build_pipeline(args)
    pipeline.init()
    return 0


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    sys.exit(main(args))
