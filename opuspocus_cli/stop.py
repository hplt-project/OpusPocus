#!/usr/bin/env python3
from typing import Sequence

import sys
from argparse import Namespace

from opuspocus.options import create_stop_parser
from opuspocus.pipelines import load_pipeline
from opuspocus.runners import build_runner


def parse_args(argv: Sequence[str]) -> Namespace:
    parser = create_stop_parser()
    return parser.parse_args(argv)


def main(args: Namespace) -> int:
    """Pipeline execution termination command.

    Each submitted or running pipeline step is terminated and its state
    is set to FAILED. Steps that already finished successfully are ignored.

    The runner command line argument must be identical to the runner used
    to execute the pipeline.
    """
    pipeline = load_pipeline(args)
    runner = runners.load_runner(args.pipeline_dir)
    runner.stop_pipeline(pipeline)
    return 0


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    sys.exit(main(args))
