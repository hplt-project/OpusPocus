#!/usr/bin/env python3
import logging
import sys
import warnings
from argparse import Namespace
from typing import Sequence

from opuspocus.options import parse_stop_args
from opuspocus.pipeline_steps import StepState
from opuspocus.pipelines import load_pipeline
from opuspocus.runners import load_runner

logger = logging.getLogger(__name__)


def parse_args(argv: Sequence[str]) -> Namespace:
    return parse_stop_args(argv)


def main(args: Namespace) -> int:
    """Pipeline execution termination command.

    Each submitted or running pipeline step is terminated and its state
    is set to FAILED. Steps that already finished successfully are ignored.

    The runner command line argument must be identical to the runner used
    to execute the pipeline.
    """
    try:
        pipeline = load_pipeline(args)
        logger.info("An existing pipeline located at %s was loaded.", pipeline.pipeline_dir)
    except Exception:  # noqa: BLE001
        warnings.warn(
            f"Pipeline located at {args.pipeline_dir} does not exist (perhaps an incorrect --pipeline-dir option?).",
            UserWarning,
            stacklevel=1,
        )
        return 0

    if pipeline.state not in [StepState.SUBMITTED, StepState.RUNNING]:
        warnings.warn(
            f"Trying to stop a pipeline in a {pipeline.state} state.",
            UserWarning,
            stacklevel=1,
        )

    if pipeline.state not in [StepState.INITED, StepState.INIT_INCOMPLETE]:
        runner = load_runner(args)
        logger.info("Stopping pipeline...")
        runner.stop_pipeline(pipeline)
    return 0


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    sys.exit(main(args))
