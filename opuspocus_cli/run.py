#!/usr/bin/env python3
import sys
from argparse import Namespace
from typing import Sequence

from opuspocus.options import parse_run_args
from opuspocus.pipelines import load_pipeline
from opuspocus.runners import build_runner


def parse_args(argv: Sequence[str]) -> Namespace:
    return parse_run_args(argv)


def main(args: Namespace) -> int:
    """Pipeline execution command.

    Submits the pipeline steps, respecting their dependencies, as runner tasks
    using the specified runner (bash, slurm, ...) and executes the submitted
    runner tasks.
    """
    pipeline = load_pipeline(args)
    runner = build_runner(args.runner, args.pipeline_dir, args)
    runner.run_pipeline(pipeline, targets=args.targets)
    return 0


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    sys.exit(main(args))
