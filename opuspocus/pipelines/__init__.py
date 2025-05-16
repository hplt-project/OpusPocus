import argparse
import logging

from .exceptions import PipelineInitError
from .opuspocus_pipeline import OpusPocusPipeline, PipelineState, PipelineStateError

__all__ = ["OpusPocusPipeline", "PipelineInitError", "PipelineState", "PipelineStateError"]

logger = logging.getLogger(__name__)


def build_pipeline(args: argparse.Namespace) -> OpusPocusPipeline:
    logger.info("Building pipeline...")
    return OpusPocusPipeline.build_pipeline(args.pipeline_config, getattr(args.pipeline, "pipeline_dir", None))


def load_pipeline(args: argparse.Namespace) -> OpusPocusPipeline:
    logger.info("Loading pipeline...")
    return OpusPocusPipeline.load_pipeline(args.pipeline.pipeline_dir)
