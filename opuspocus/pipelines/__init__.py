import logging
from pathlib import Path

from opuspocus.config import PipelineConfig

from .exceptions import PipelineInitError
from .opuspocus_pipeline import OpusPocusPipeline, PipelineState, PipelineStateError

__all__ = ["OpusPocusPipeline", "PipelineInitError", "PipelineState", "PipelineStateError"]

logger = logging.getLogger(__name__)


def build_pipeline(config: PipelineConfig) -> OpusPocusPipeline:
    logger.info("Building pipeline...")
    return OpusPocusPipeline.build_pipeline(config)


def load_pipeline_from_directory(pipeline_dir: Path) -> OpusPocusPipeline:
    logger.info("Loading pipeline from directory %s", pipeline_dir)
    config = PipelineConfig.load_from_directory(pipeline_dir)
    return load_pipeline(config)


def load_pipeline(config: PipelineConfig) -> OpusPocusPipeline:
    logger.info("Loading pipeline...")
    return OpusPocusPipeline.load_pipeline(config)
