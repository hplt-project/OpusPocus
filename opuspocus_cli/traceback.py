#!/usr/bin/env python3
import sys
from typing import Sequence

from omegaconf import DictConfig

from opuspocus.config import PipelineConfig
from opuspocus.options import parse_traceback_args
from opuspocus.pipelines import load_pipeline


def parse_args(argv: Sequence[str]) -> DictConfig:
    return parse_traceback_args(argv)


def main(args: DictConfig) -> int:
    """Pipeline structure analysis command.

    Prints the simplified dependency graph of the pipeline steps
    with their current status.
    """
    config = PipelineConfig.load_from_directory(args.pipeline.pipeline_dir, args)
    pipeline = load_pipeline(config)
    pipeline.print_traceback(target_labels=config.pipeline.targets)
    return 0


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    sys.exit(main(args))
