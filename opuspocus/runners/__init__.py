import importlib
import logging
from argparse import Namespace
from pathlib import Path

from .opuspocus_runner import OpusPocusRunner, TaskId, TaskInfo

__all__ = [
    "OpusPocusRunner",
    "TaskId",
    "TaskInfo",
]

logger = logging.getLogger(__name__)

RUNNER_REGISTRY = {}
RUNNER_CLASS_NAMES = set()


def build_runner(runner: str, pipeline_dir: Path, args: Namespace):
    """Runner builder function. Use this to create runner objects."""
    logger.info("Building runner (%s)...", runner)

    kwargs = {}
    for param in RUNNER_REGISTRY[runner].list_parameters():
        if param == "runner" or param == "pipeline_dir":
            continue
        kwargs[param] = getattr(args, param)

    return RUNNER_REGISTRY[runner].build_runner(runner, pipeline_dir, **kwargs)


def load_runner(pipeline_dir: Path):
    """Recreate a previously used runner. Required for pipeline execution
    updates, i.e. execution termination.

    """
    runner_params = OpusPocusRunner.load_parameters(pipeline_dir)

    runner = runner_params["runner"]
    del runner_params["runner"]
    del runner_params["pipeline_dir"]

    logger.info("Loading runner (%s)...", runner)
    return RUNNER_REGISTRY[runner].build_runner(runner, pipeline_dir, **runner_params)


def register_runner(name):
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

    def register_runner_cls(cls):
        if name in RUNNER_REGISTRY:
            raise ValueError(f"Cannot register duplicate runner ({name})")
        if not issubclass(cls, OpusPocusRunner):
            raise ValueError(f"Runner ({name}: {cls.__name__}) must extend OpusPocusRunner")
        if cls.__name__ in RUNNER_CLASS_NAMES:
            raise ValueError(f"Cannot register runner with duplicate class name ({cls.__name__})")
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
