#!/usr/bin/env python3
import logging
import sys
import warnings
from typing import Sequence

from omegaconf import DictConfig

from opuspocus.config import PipelineConfig
from opuspocus.options import ERR_RETURN_CODE, parse_run_args
from opuspocus.pipelines import OpusPocusPipeline, PipelineState, build_pipeline, load_pipeline
from opuspocus.runners import build_runner, load_runner

logger = logging.getLogger(__name__)


def parse_args(argv: Sequence[str]) -> DictConfig:
    return parse_run_args(argv)


def init_pipeline(pipeline: OpusPocusPipeline, config: PipelineConfig) -> OpusPocusPipeline:
    if pipeline is None:
        pipeline = build_pipeline(config)
        pipeline.init()
    elif pipeline.state == PipelineState.INIT_INCOMPLETE:
        logger.info("An existing pipeline's initialization is incomplete. Finishing initialization...")
        pipeline.init()
    elif config.cli_options.reinit or config.cli_options.reinit_failed:
        logger.info("Re-initializing the pipeline...")
        if pipeline.state in [PipelineState.RUNNING, PipelineState.SUBMITTED]:
            logger.info("Stopping the previous run...")
            prev_runner = load_runner(config)
            prev_runner.stop_pipeline(pipeline)
        pipeline.reinit(ignore_finished=config.cli_options.reinit_failed)
    return pipeline


def main(args: DictConfig) -> int:
    """Pipeline execution command.

    Submits the pipeline steps, respecting their dependencies, as runner tasks
    using the specified runner (bash, slurm, ...) and executes the submitted
    runner tasks.
    """
    # Load the config
    if args.cli_options.pipeline_config is not None:
        # Load the provided --config-file
        config = PipelineConfig.load(args.cli_options.pipeline_config, args)
    elif args.pipeline.pipeline_dir is not None:
        # If the --config-file was not provided, try to load the pipeline's config_file from the --pipeline-dir
        config = PipelineConfig.load_from_directory(args.pipeline.pipeline_dir, args)
        # Save the overwritten config file
        config.save_to_directory(args.pipeline.pipeline_dir)
        logger.info(
            "No --pipeline-config was provided, reading pipeline configuration from pipeline at %s",
            args.pipeline.pipeline_dir,
        )
    else:
        logger.error(
            "--pipeline-config path with the pipeline configuration "
            "or a --pipeline-dir containing a valid pipeline must be provided."
        )
        return ERR_RETURN_CODE

    # Not providing --pipeline-config but providing --pipeline-dir implies existing pipeline
    pipeline = None
    if args.cli_options.pipeline_config is None and args.pipeline.pipeline_dir is not None:
        pipeline = load_pipeline(config)
        logger.info("An existing pipeline located at %s was loaded.", pipeline.pipeline_dir)
        # TODO(varisd): existing pipeline overwrite logic

    ## Initialization phase ##
    pipeline = init_pipeline(pipeline, config)

    # The pipeline must be at least INITED at this point
    if pipeline.state is None or pipeline.state == PipelineState.INIT_INCOMPLETE:
        logger.error("Something went wrong with pipeline initialization (Current pipeline state: %s)", pipeline.state)
        return ERR_RETURN_CODE

    ## Run phase ##
    runner = build_runner(config)

    # Check the pipeline state and whether it requires --reinit or --rerun flags
    if config.cli_options.stop_previous_run:
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
            prev_runner = load_runner(config)
            prev_runner.stop_pipeline(pipeline)
    elif pipeline.state in [PipelineState.SUBMITTED, PipelineState.RUNNING]:
        logger.error(
            "You are trying to run pipeline in the %s state. Use --stop-previous-run option to first stop "
            "the previous execution.",
            pipeline.state,
        )
        return ERR_RETURN_CODE

    runner.run_pipeline(
        pipeline, target_labels=config.pipeline.targets, resubmit_done=config.cli_options.resubmit_finished_subtasks
    )
    return 0


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    sys.exit(main(args))
