import inspect
import logging
from argparse import ArgumentParser
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, get_type_hints

import yaml
from typing_extensions import TypedDict

from opuspocus.pipeline_steps import OpusPocusStep, StepState
from opuspocus.pipelines import OpusPocusPipeline
from opuspocus.utils import RunnerResources

logger = logging.getLogger(__name__)


class TaskId(TypedDict):
    file_path: str
    id: Any


class TaskInfo(TypedDict):
    runner: str
    main_task: TaskId
    subtasks: List[TaskId]


class OpusPocusRunner:
    """Base class for OpusPocus runners."""

    parameter_file = "runner.parameters"
    info_file = "runner.task_info"
    submitted_tasks: ClassVar[List[TaskId]] = []

    @staticmethod
    def add_args(parser: ArgumentParser) -> None:
        """Add runner-specific arguments to the parser."""

    def __init__(self, runner: str, pipeline_dir: Path, **kwargs) -> None:  # noqa: ANN003
        self.runner = runner
        self.pipeline_dir = pipeline_dir

        self.register_parameters(**kwargs)

    @classmethod
    def build_runner(cls: "OpusPocusRunner", runner: str, pipeline_dir: Path, **kwargs) -> "OpusPocusRunner":  # noqa: ANN003
        """Build a specified runner instance.

        Args:
            runner (str): TODO
            pipeline_dir (Path): TODO

        Returns:
            An instance of the specified runner class.
        """
        return cls(runner, pipeline_dir, **kwargs)

    @classmethod
    def list_parameters(cls: "OpusPocusRunner") -> List[str]:
        """TODO"""
        param_list = []
        for param in inspect.signature(cls.__init__).parameters:
            if param == "self":
                continue
            param_list.append(param)
        return param_list

    @classmethod
    def load_parameters(cls: "OpusPocusRunner", pipeline_dir: Path) -> Dict[str, Any]:
        """TODO"""
        params_path = Path(pipeline_dir, cls.parameter_file)
        logger.debug("Loading step variables from %s", params_path)

        with params_path.open("r") as fh:
            return yaml.safe_load(fh)

    def get_parameters_dict(self) -> Dict[str, Any]:
        """TODO"""
        param_dict = {}
        for param in self.list_parameters():
            p = getattr(self, param)
            if isinstance(p, Path):
                p = str(p)
            if isinstance(p, list) and isinstance(p[0], Path):
                p = [str(v) for v in p]
            param_dict[param] = p
        return param_dict

    def save_parameters(self) -> None:
        """TODO"""
        with Path(self.pipeline_dir, self.parameter_file).open("w") as fh:
            yaml.dump(self.get_parameters_dict(), fh)

    def register_parameters(self, **kwargs) -> None:  # noqa: ANN003
        """TODO"""
        type_hints = get_type_hints(self.__init__)
        logger.debug("Class type hints: %s", type_hints)

        for param, val in kwargs.items():
            v = val
            if type_hints[param] == Path and val is not None:
                v = Path(val)
            if type_hints[param] == List[Path]:
                v = [Path(v) for v in val]
            setattr(self, param, v)

    def stop_pipeline(self, pipeline: OpusPocusPipeline) -> None:
        """TODO"""
        for step in pipeline.steps:
            if not step.is_running_or_submitted:
                continue
            task_info = self.load_task_info(step)
            if task_info is None:
                t_info_runner = task_info["runner"]
                err_msg = (
                    f"Step {step.step_label} cannot be cancelled using {self.runner} runner because it "
                    f"was submitted by a different runner type ({t_info_runner})."
                )
                raise ValueError(err_msg)
            logger.info("Stopping %s. Setting state to FAILED.", step.step_label)

            task_id = task_info["main_task"]
            logger.debug("Stopping task %i.", task_id)
            self.cancel_task(task_id)

    def run_pipeline(
        self,
        pipeline: OpusPocusPipeline,
        targets: List[str],
        *,
        keep_finished: bool = False,
    ) -> None:
        """TODO"""
        logger.info("Submitting pipeline tasks...")

        self.save_parameters()
        self.submitted_tasks = []
        for step in pipeline.get_targets(targets):
            self.submit_step(step, keep_finished=keep_finished)

        self.run()
        logger.info("Pipeline tasks submitted successfully.")

    def run(self) -> None:
        """TODO"""
        pass

    def submit_step(self, step: OpusPocusStep, *, keep_finished: bool = False) -> Optional[TaskInfo]:
        """TODO"""
        if step.is_running_or_submitted:
            task_info = self.load_task_info(step)
            if task_info["runner"] != self.runner:
                t_info_runner = task_info["runner"]
                err_msg = (
                    f"Step {step.step_label} cannot be submitted because it is currently {step.state} using a "
                    f"different runner ({t_info_runner})."
                )
                raise ValueError(err_msg)
            return task_info
        if step.has_state(StepState.DONE):
            logger.info("Step %s has already finished. Skipping...", step.step_label)
            return None
        if step.has_state(StepState.FAILED):
            step.clean_directories(keep_finished=keep_finished)
            logger.info(
                "Step %s has previously failed. Resubmitting...",
                step.step_label,
            )
        elif not step.has_state(StepState.INITED):
            err_msg = f"Cannot run step {step.step_label}. Step is not in INITED state."
            raise ValueError(err_msg)

        # Recursively submit step dependencies first
        dep_task_info_list = []
        for dep in step.dependencies.values():
            if dep is None:
                continue
            dep_task_info = self.submit_step(dep)
            if dep_task_info is not None:
                dep_task_info_list.append(dep_task_info)

        cmd_path = Path(step.step_dir, step.command_file)

        # Submit the main step task (which eventually can submit subtasks)
        logger.info("[%s] Submitting main step task.", step.step_label)

        # We set the state to SUBMITTED befor actual submit to avoid possible
        # race conditions
        step.state = StepState.SUBMITTED
        try:
            task_id = self.submit_task(
                cmd_path=cmd_path,
                target_file=None,
                dependencies=[dep["main_task"] for dep in dep_task_info_list],
                step_resources=self.get_resources(step),
                stdout_file=Path(step.log_dir, f"{self.runner}.main.out"),
                stderr_file=Path(step.log_dir, f"{self.runner}.main.err"),
            )
        except Exception:
            step.state = StepState.FAILED
            raise

        task_info = TaskInfo(runner=self.runner, main_task=task_id, subtasks=[])
        self.save_task_info(step, task_info)

        self.submitted_tasks.append(task_info)
        return task_info

    def submit_task(
        self,
        cmd_path: Path,
        target_file: Optional[Path] = None,
        dependencies: Optional[List[TaskId]] = None,
        step_resources: Optional[RunnerResources] = None,
        stdout_file: Optional[Path] = None,
        stderr_file: Optional[Path] = None,
    ) -> TaskId:
        """TODO"""
        raise NotImplementedError()

    def update_dependants(self, task_id: TaskId) -> None:
        raise NotImplementedError()

    def cancel_task(self, task_id: TaskId) -> None:
        """TODO"""
        raise NotImplementedError()

    def wait_for_tasks(self, task_ids: Optional[List[TaskId]] = None) -> None:
        # Wait for all tasks by default
        if task_ids is None:
            task_ids = [t["main_task"] for t in self.submitted_tasks]
        for task_id in task_ids:
            self.wait_for_single_task(task_id)

    def wait_for_single_task(self, task_id: TaskId) -> None:
        raise NotImplementedError()

    def is_task_running(self, task_id: TaskInfo) -> bool:
        """TODO"""
        raise NotImplementedError()

    def remove_task_file(self, task_id: TaskId) -> bool:
        """TODO"""
        file_path = Path(task_id["file_path"])
        if file_path.exists():
            file_path.unlink()

    def save_task_info(self, step: OpusPocusStep, task_info: TaskInfo) -> None:
        """TODO"""
        with Path(step.step_dir, self.info_file).open("w") as fh:
            yaml.dump(task_info, fh)

    def load_task_info(self, step: OpusPocusStep) -> Optional[TaskInfo]:
        """TODO"""
        with Path(step.step_dir, self.info_file).open("r") as fh:
            return yaml.safe_load(fh)

    def get_resources(self, step: OpusPocusStep) -> RunnerResources:
        """TODO"""
        # TODO: expand the logic here
        return step.default_resources

    def __eq__(self, other: "OpusPocusRunner") -> bool:
        """Object comparison logic."""
        return all(getattr(self, param, None) == getattr(other, param, None) for param in self.list_parameters())
