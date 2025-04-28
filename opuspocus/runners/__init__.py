import importlib
import logging
from argparse import Namespace
from pathlib import Path
from typing import Callable

from opuspocus.config import PipelineConfig
from opuspocus.pipelines import OpusPocusPipeline

from .opuspocus_runner import OpusPocusRunner, SubmissionInfo, TaskInfo

__all__ = [
    "OpusPocusRunner",
    "SubmissionInfo",
    "TaskInfo",
]

logger = logging.getLogger(__name__)

RUNNER_REGISTRY = {}
RUNNER_CLASS_NAMES = set()


def build_runner(args: Namespace) -> OpusPocusRunner:
    """Runner builder function. Use this to create runner objects."""
    runner = args.runner.runner
    assert runner is not None
    pipeline_dir = args.pipeline_dir
    assert pipeline_dir is not None

    logger.info("Building runner (%s) based on config in (%s)", runner, pipeline_dir)
    kwargs = {}

    config_path = Path(pipeline_dir, OpusPocusPipeline.get_config_file())
    if config_path.exists():
        config = PipelineConfig.load(Path(pipeline_dir, OpusPocusPipeline.get_config_file()))

        kwargs = dict(config.runner)
        del kwargs["runner"]

    # NOTE(varisd): the following override should become obsolete after reimplementing CLI config override support
    for param in RUNNER_REGISTRY[runner].list_parameters():
        if param in {"runner", "pipeline_dir"}:
            continue
        kwargs[param] = getattr(args, f"runner.{param}")

    return RUNNER_REGISTRY[runner].build_runner(runner, pipeline_dir, **kwargs)


def load_runner(args: Namespace) -> OpusPocusRunner:
    """Recreate a previously used runner. Required for pipeline execution
    updates, i.e. execution termination.
    """
    pipeline_dir = args.pipeline_dir
    assert pipeline_dir is not None

    runner_params = OpusPocusRunner.load_parameters(pipeline_dir)

    runner = runner_params["runner"]
    del runner_params["runner"]
    del runner_params["pipeline_dir"]

    logger.info("Loading runner (%s)...", runner)
    return RUNNER_REGISTRY[runner].build_runner(runner, pipeline_dir, **runner_params)


def register_runner(name: str) -> Callable:
    """
    New runner can be added to OpusPocus with the
    :func:`~opuspocus.runners.register_runner` function decorator.

    Based on the Fairseq task/model/etc registrations
    (https://github.com/facebookresearch/fairseq)

    For example:

        @register_runner('slurm')
        class SlurmRunner(OpusPocusRunner):
            (...)

    .. note:: All runners must implement the :class:`OpusPocusRunner` interface.

    Args:
        name (str): the name of the runner
    """

    def register_runner_cls(cls: OpusPocusRunner) -> OpusPocusRunner:
        if name in RUNNER_REGISTRY:
            err_msg = f"Cannot register duplicate runner ({name})"
            raise ValueError(err_msg)
        if not issubclass(cls, OpusPocusRunner):
            err_msg = f"Runner ({name}: {cls.__name__}) must extend OpusPocusRunner"
            raise TypeError(err_msg)
        if cls.__name__ in RUNNER_CLASS_NAMES:
            err_msg = f"Cannot register runner with duplicate class name ({cls.__name__})"
            raise ValueError(err_msg)
        RUNNER_REGISTRY[name] = cls
        RUNNER_CLASS_NAMES.add(cls.__name__)
        return cls

    return register_runner_cls


runners_dir = Path(__file__).parents[0]
for file in runners_dir.iterdir():
    if (
        not file.stem.startswith("_")
        and not file.stem.startswith(".")
        and file.name.endswith("py")
        and not file.is_dir()
    ):
        runner_name = file.stem if file.name.endswith(".py") else file
        importlib.import_module("opuspocus.runners." + str(runner_name))
