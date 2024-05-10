from typing import Optional

import argparse
from pathlib import Path
from opuspocus.pipelines import OpusPocusPipeline, register_pipeline


@register_pipeline('custom')
class CustomPipeline(OpusPocusPipeline):
    """TODO"""
    def __init__(
        self,
        pipeline: str,
        args: argparse.Namespace,
        pipeline_dir: Optional[Path] = None,
        pipeline_config_path: Optional[Path] = None
    ):
        super().__init__(pipeline, args, pipeline_dir, pipeline_config_path)
