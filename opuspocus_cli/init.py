#!/usr/bin/env python3
from typing import Sequence

import sys
from argparse import Namespace

from opuspocus.options import create_init_parser
from opuspocus.pipelines import build_pipeline


def parse_args(argv: Sequence[str]) -> Namespace:
    parser = create_init_parser()
    return parser.parse_args(argv)


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
