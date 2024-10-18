#!/usr/bin/env python3
import logging
import sys
from argparse import Namespace
from pathlib import Path
from typing import Sequence

from opuspocus.options import parse_run_args
from opuspocus.pipelines import OpusPocusPipeline, PipelineConfig, PipelineState, build_pipeline, load_pipeline
from opuspocus.runners import build_runner

logger = logging.getLogger(__name__)


def parse_args(argv: Sequence[str]) -> Namespace:
    return parse_run_args(argv)


def main(args: Namespace) -> int:
    """Pipeline execution command.

    Submits the pipeline steps, respecting their dependencies, as runner tasks
    using the specified runner (bash, slurm, ...) and executes the submitted
    runner tasks.
    """
    # One of the options has to be provided
    if args.pipeline_config is None and args.pipeline_dir is None:
        logger.error(
            "--pipeline-config path with the pipeline configuration "
            "or a --pipeline-dir containing a valid pipeline must be provided."
        )
        sys.exit(2)

    # As a fallback, we can re-use config from the provided --pipeline-dir
    if args.pipeline_config is None:
        setattr(args, Path(args.pipeline_dir, OpusPocusPipeline.config_file))
        logger.info("No --pipeline-config was provided, reading pipeline configuration from %s", args.pipeline_config)

    # By default, we use the pipeline directory defined in the config file
    if args.pipeline_dir is None:
        config_file = PipelineConfig.load(args.pipeline_config)
        setattr(args, config_file["pipeline"]["pipeline_dir"])

    # First, we try to load a pipeline if it was previously saved
    try:
        pipeline = load_pipeline(args)
        logger.info("An existing pipeline located at %s was loaded.")
        # TODO(varisd): existing pipeline overwrite logic
    except Exception:  # noqa: BLE001
        pipeline = None

    # Initialization phase
    if pipeline is None:
        pipeline = build_pipeline(args)
        pipeline.init()
    elif pipeline.state == PipelineState.INIT_INCOMPLETE:
        logger.info("An existing pipeline's initialization is incomplete. Finishing initialization...")
        pipeline.init()
    elif args.reinit:
        if pipeline.state == PipelineState.RUNNING:
            logger.info("Re-initializing running pipeline. Stopping the previous run...")
            # TODO(varisd): add necessary logic (stopping pipeline without an explicit runner)
            raise NotImplementedError()
        pipeline.reinit()

    # Running phase
    runner = build_runner(args.runner, pipeline.pipeline_dir, args)
    runner.run_pipeline(pipeline, targets=pipeline.get_targets(args.targets), rerun_completed=args.rerun_completed)
    return 0


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    sys.exit(main(args))
