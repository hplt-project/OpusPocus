#!/usr/bin/env python3
from typing import Sequence

import sys
from argparse import Namespace

from opuspocus.options import create_run_parser
from opuspocus.pipelines import load_pipeline
from opuspocus.runners import build_runner
from opuspocus.utils import check_pipeline_dir_exists


def parse_args(argv: Sequence[str]) -> Namespace:
    parser = create_run_parser()
    return parser.parse_args(argv)


def main(args: Namespace) -> int:
    """Pipeline execution command.

    Submits the pipeline steps, respecting their dependencies, as runner tasks
    using the specified runner (bash, slurm, ...) and executes the submitted
    runner tasks.
    """
    pipeline = load_pipeline(args)
    runner = runners.build_runner(args.runner, args.pipeline_dir, args)
    runner.run_pipeline(pipeline, pipeline.get_targets(args.targets))
    return 0


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    sys.exit(main(args))
