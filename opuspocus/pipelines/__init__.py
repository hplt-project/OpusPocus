import argparse
import importlib
from pathlib import Path

from .opuspocus_pipeline import OpusPocusPipeline


def build_pipeline(args):
    return OpusPocusPipeline.build_pipeline(args.pipeline_config, args.pipeline_dir)


def load_pipeline(args):
    return OpusPocusPipeline.load_pipeline(args.pipeline_dir)


def add_args(parser):
    OpusPocusPipeline.add_args(parser)
