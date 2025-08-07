#!/usr/bin/env python3
import logging
import sys
import warnings
from argparse import Namespace
from pathlib import Path
from typing import Sequence

from opuspocus.config import PipelineConfig
from opuspocus.options import parse_run_args
from opuspocus.pipelines import OpusPocusPipeline, PipelineState, build_pipeline, load_pipeline
from opuspocus.runners import build_runner, load_runner

logger = logging.getLogger(__name__)


def parse_args(argv: Sequence[str]) -> Namespace:
    return parse_run_args(argv)


def init_pipeline(pipeline: OpusPocusPipeline, args: Namespace) -> OpusPocusPipeline:
    if pipeline is None:
        pipeline = build_pipeline(args)
        pipeline.init()
    elif pipeline.state == PipelineState.INIT_INCOMPLETE:
        logger.info("An existing pipeline's initialization is incomplete. Finishing initialization...")
        pipeline.init()
    elif args.reinit or args.reinit_failed:
        logger.info("Re-initializing the pipeline...")
        if pipeline.state in [PipelineState.RUNNING, PipelineState.SUBMITTED]:
            logger.info("Stopping the previous run...")
            prev_runner = load_runner(Namespace(**{"pipeline": Namespace(**{"pipeline_dir": pipeline.pipeline_dir})}))
            prev_runner.stop_pipeline(pipeline)
        pipeline.reinit(ignore_finished=args.reinit_failed)
    return pipeline


def main(args: Namespace) -> int:
    """Pipeline execution command.

    Submits the pipeline steps, respecting their dependencies, as runner tasks
    using the specified runner (bash, slurm, ...) and executes the submitted
    runner tasks.
    """
    # One of the options has to be provided
    if args.pipeline_config is None and getattr(args.pipeline, "pipeline_dir", None) is None:
        logger.error(
            "--pipeline-config path with the pipeline configuration "
            "or a --pipeline-dir containing a valid pipeline must be provided."
        )
        sys.exit(2)

    # As a fallback, we can re-use config from the provided --pipeline-dir
    if args.pipeline_config is None:
        args.pipeline_config = Path(getattr(args.pipeline, "pipeline_dir", None), OpusPocusPipeline._config_file)  # noqa: SLF001
        logger.info("No --pipeline-config was provided, reading pipeline configuration from %s", args.pipeline_config)

    # By default, we use the pipeline directory defined in the config file
    config = PipelineConfig.load(args.pipeline_config, cli_override=args)
    if getattr(args.pipeline, "pipeline_dir", None) is None:
        args.pipeline.pipeline_dir = Path(config.pipeline.pipeline_dir)

    # First, we try to load a pipeline if it was previously saved
    try:
        pipeline = load_pipeline(args)
        logger.info("An existing pipeline located at %s was loaded.", pipeline.pipeline_dir)
        # TODO(varisd): existing pipeline overwrite logic
    except Exception:  # noqa: BLE001
        pipeline = None

    ## Initialization phase ##
    pipeline = init_pipeline(pipeline, args)

    # The pipeline must be at least INITED at this point
    if pipeline.state is None or pipeline.state == PipelineState.INIT_INCOMPLETE:
        logger.error("Something went wrong with pipeline initialization (Current pipeline state: %s)", pipeline.state)
        sys.exit(2)

    ## Run phase ##
    # Get default runner value from the config if not provided via CLI
    if args.runner is None:
        args.runner.runner = config.runner.runner
    if args.pipeline is None:
        args.pipeline.target = config.pipeline.targets

    runner = build_runner(args)

    # Check the pipeline state and whether it requires --reinit or --rerun flags
    if args.stop_previous_run:
        stopped_states = [
            PipelineState.INIT_INCOMPLETE,
            PipelineState.INITED,
            PipelineState.DONE,
            PipelineState.FAILED,
            None,
        ]
        if pipeline.state in stopped_states:
            warnings.warn(
                f"Pipeline in the {pipeline.state} does not need to be stopped. "
                "Ignoring the --stop-previous-run option.",
                UserWarning,
                stacklevel=1,
            )
        else:
            prev_runner = load_runner(args)
            prev_runner.stop_pipeline(pipeline)
    elif pipeline.state in [PipelineState.SUBMITTED, PipelineState.RUNNING]:
        logger.error(
            "You are trying to run pipeline in the %s state. Use --stop-previous-run option to first stop "
            "the previous execution.",
            pipeline.state,
        )
        sys.exit(2)

    runner.run_pipeline(
        pipeline, target_labels=getattr(args.pipeline, "targets", None), resubmit_done=args.resubmit_finished_subtasks
    )
    return 0


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    sys.exit(main(args))
