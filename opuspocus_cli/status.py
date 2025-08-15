#!/usr/bin/env python3
import sys
from omegaconf import DictConfig
from typing import Sequence

from opuspocus.config import PipelineConfig
from opuspocus.options import parse_status_args
from opuspocus.pipelines import load_pipeline


def parse_args(argv: Sequence[str]) -> DictConfig:
    return parse_status_args(argv)


def main(args: DictConfig) -> int:
    """Command that prints the current status of each pipeline step."""
    config = PipelineConfig.load_from_directory(args.pipeline.pipeline_dir, args)
    pipeline = load_pipeline(config)
    pipeline.print_status(pipeline.steps)
    return 0


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    sys.exit(main(args))
