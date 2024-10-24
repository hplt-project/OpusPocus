import inspect
import logging
import signal
from argparse import ArgumentParser
from pathlib import Path
from typing import Any, Dict, List, Optional, get_type_hints

import yaml
from typing_extensions import TypedDict

from opuspocus.pipeline_steps import OpusPocusStep, StepState
from opuspocus.pipelines import OpusPocusPipeline
from opuspocus.utils import RunnerResources

logger = logging.getLogger(__name__)

SLEEP_TIME = 0.5


class TaskInfo(TypedDict):
    file_path: str
    id: Any


class SubmissionInfo(TypedDict):
    runner: str
    main_task: TaskInfo
    subtasks: List[TaskInfo]


class OpusPocusRunner:
    """Base class for OpusPocus runners."""

    parameter_file = "runner.parameters"
    info_file = "runner.step_info"

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
        logger.debug("[OpusPocusRunner] Loading step variables from %s", params_path)

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
        logger.debug("[%s] Class type hints: %s", self.runner, type_hints)

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
            sub_info = self.load_submission_info(step)
            sub_runner = sub_info["runner"]
            if sub_runner != self.runner:
                err_msg = (
                    f"Step {step.step_label} cannot be cancelled using {self.runner} runner because it "
                    f"was submitted by a different runner type ({sub_runner})."
                )
                raise ValueError(err_msg)

            logger.info("[%s] Stopping step %s and setting the step state to FAILED.", self.runner, step.step_label)
            task_info = sub_info["main_task"]

            # TODO(varisd): the main task should take care of cancelling / cleaning up its subtasks after receiving a
            #   SIGTERM/SIGUSR1 signal, however, we should probably take care of situations where the main task dies
            #   before finishing the cleanup
            self.cancel_task(task_info)
            step.state = StepState.FAILED

    def run_pipeline(
        self,
        pipeline: OpusPocusPipeline,
        targets: Optional[List[OpusPocusStep]] = None,
        *,
        resubmit_done: bool = False,
    ) -> None:
        """TODO"""
        self.save_parameters()
        for step in pipeline.get_targets(targets):
            self.submit_step(step, keep_finished=(not resubmit_done))
        self.run()
        logger.info("[%s] Pipeline tasks submitted successfully.", self.runner)

    def run(self) -> None:
        """TODO"""
        pass

    def submit_step(self, step: OpusPocusStep, *, keep_finished: bool = False) -> Optional[SubmissionInfo]:
        """TODO"""
        if step.is_running_or_submitted:
            sub_info = self.load_submission_info(step)
            sub_runner = sub_info["runner"]
            if sub_runner != self.runner:
                err_msg = (
                    f"Step {step.step_label} cannot be submitted because it is currently {step.state} "
                    f"using a different runner ({sub_runner})."
                )
                raise ValueError(err_msg)
            return sub_info
        if step.has_state(StepState.DONE):
            logger.info("[%s] Step %s has already finished. Skipping...", self.runner, step.step_label)
            return None
        if step.has_state(StepState.FAILED):
            step.clean_directories(keep_finished=keep_finished)
            logger.info(
                "[%s] Step %s is in FAILED state. Resubmitting...",
                self.runner,
                step.step_label,
            )
        elif not step.has_state(StepState.INITED):
            err_msg = f"Cannot run step {step.step_label}. Step is not in INITED state."
            raise ValueError(err_msg)

        # Recursively submit step dependencies first
        dep_sub_info_list = []
        for dep in step.dependencies.values():
            if dep is None:
                continue
            dep_sub_info = self.submit_step(dep)
            if dep_sub_info is not None:
                dep_sub_info_list.append(dep_sub_info)

        cmd_path = Path(step.step_dir, step.command_file)

        # Submit the main step task which then is responsible of submitting its subtasks
        logger.info("[%s] Submitting '%s' main step task.", self.runner, step.step_label)

        # NOTE(varisd): we set the state to SUBMITTED before actual submission to avoid possible race conditions
        step.state = StepState.SUBMITTED
        try:
            timestamp = time.time()
            task_info = self.submit_task(
                cmd_path=cmd_path,
                target_file=None,
                dependencies=[dep["main_task"] for dep in dep_sub_info_list],
                step_resources=self.get_resources(step),
                stdout_file=Path(step.log_dir, f"{self.runner}.main.{timestamp}.out"),
                stderr_file=Path(step.log_dir, f"{self.runner}.main.{timestamp}.err"),
            )
        except Exception as err:
            step.state = StepState.FAILED
            logger.exception("Task submission in runner.submit_step raised the following error:\n%s", err.message)
            raise

        sub_info = SubmissionInfo(runner=self.runner, main_task=task_info, subtasks=[])
        self.save_submission_info(step, sub_info)
        return sub_info

    def resubmit_step(self, step: OpusPocusStep, *, keep_finished: bool = False) -> SubmissionInfo:
        """TODO"""
        sub_info = self.load_submission_info(step)
        if keep_finished:
            self.send_signal(sub_info["main_task"], signal.SIGUSR1)
        else:
            self.send_signal(sub_info["main_task"], signal.SIGUSR2)
        while step.state != StepState.RUNNING:
            time.sleep(SLEEP_TIME)
        return self.load_submission_info(step)

    def update_dependants(
        self,
        step: OpusPocusStep,
        remove_task_list: Optional[List[TaskInfo]] = None,
        add_task_list: Optional[List[TaskInfo]] = None,
    ) -> None:
        """TODO"""
        raise NotImplementedError()

    def submit_task(
        self,
        cmd_path: Path,
        target_file: Optional[Path] = None,
        dependencies: Optional[List[TaskInfo]] = None,
        step_resources: Optional[RunnerResources] = None,
        stdout_file: Optional[Path] = None,
        stderr_file: Optional[Path] = None,
    ) -> TaskInfo:
        """TODO"""
        raise NotImplementedError()

    def send_signal(self, task_info: TaskInfo, signal: int) -> None:
        """TODO"""
        raise NotImplementedError()

    def cancel_task(self, task_info: TaskInfo, signal: int = signal.SIGTERM) -> None:
        """TODO"""
        self.send_signal(task_info, signal.SIGTERM)

    def wait_for_tasks(
        self, task_info_list: Optional[List[TaskInfo]] = None, *, ignore_returncode: bool = False
    ) -> None:
        for task_info in task_info_list:
            self.wait_for_single_task(task_info, ignore_returncode=ignore_returncode)

    def wait_for_single_task(self, task_info: TaskInfo, *, ignore_returncode: bool = False) -> None:
        raise NotImplementedError()

    def is_task_running(self, task_info: TaskInfo) -> bool:
        raise NotImplementedError()

    def save_submission_info(self, step: OpusPocusStep, sub_info: SubmissionInfo) -> None:
        """TODO"""
        with Path(step.step_dir, self.info_file).open("w") as fh:
            yaml.dump(sub_info, fh)

    def load_submission_info(self, step: OpusPocusStep) -> Optional[SubmissionInfo]:
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
