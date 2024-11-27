import logging
import signal
import time
from argparse import ArgumentParser
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from attrs import asdict, define, field, fields, validators
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


@define(kw_only=True)
class OpusPocusRunner:
    """Base class for OpusPocus runners."""

    runner: str = field(validator=validators.instance_of(str))
    pipeline_dir: Path = field(converter=Path)

    _parameter_filename = "runner.parameters"
    _info_filename = "runner.step_info"

    @staticmethod
    def add_args(parser: ArgumentParser) -> None:
        """Add runner-specific arguments to the parser."""
        pass

    @classmethod
    def build_runner(cls: "OpusPocusRunner", runner: str, pipeline_dir: Path, **kwargs) -> "OpusPocusRunner":  # noqa: ANN003
        """Build a specified runner instance.

        Args:
            runner (str): runner class name in the runner registry
            pipeline_dir (Path): path to the pipeline directory
            **kwargs: additional parameters for the specific runner class implementation

        Returns:
            An instance of the specified runner class.
        """
        return cls(runner, pipeline_dir, **kwargs)

    @classmethod
    def list_parameters(cls: "OpusPocusRunner") -> List[str]:
        """Return a list of arguments required for runner initialization.

        Parameter list used mainly during runner instance saving/loading.

        Returns:
            List of runner parameters.
        """
        param_list = []
        for p in fields(cls):
            if p.name.startswith("_"):
                continue
            param_list.append(p)
        return param_list

    @classmethod
    def load_parameters(cls: "OpusPocusRunner", pipeline_dir: Path) -> Dict[str, Any]:
        """Load the previously initialized runner instance parameters.

        Args:
            pipeline_dir (Path): path to the pipeline directory

        Returns:
            Dict containing key-value paris for the runner instance initialization.
        """
        params_path = Path(pipeline_dir, cls._parameter_filename)
        logger.debug("[OpusPocusRunner] Loading step variables from %s", params_path)

        with params_path.open("r") as fh:
            return yaml.safe_load(fh)

    def get_parameters_dict(self) -> Dict[str, Any]:
        """Serialize runner parameters.

        Returns:
            Dict containing key-value pairs for the runner instance initialization.
        """
        param_dict = {}
        for attr, value in asdict(self, filter=lambda attr, _: not attr.name.startswith("_")):
            if isinstance(value, Path):
                param_dict[attr] = str(value)
            elif isinstance(value, (list, tuple)) and any(isinstance(v, Path) for v in value):
                param_dict[attr] = [str(v) for v in value]
            else:
                param_dict[attr] = value
        return param_dict

    def save_parameters(self) -> None:
        """Save the runner instance parameters."""
        with Path(self.pipeline_dir, self._parameter_filename).open("w") as fh:
            yaml.dump(self.get_parameters_dict(), fh)

    def stop_pipeline(self, pipeline: OpusPocusPipeline) -> None:
        """Stop a running pipeline execution."""
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
        target_labels: Optional[List[str]] = None,
        *,
        resubmit_finished_subtasks: bool = False,
    ) -> None:
        """Submit and execute pipeline steps with labels in target_labels and their dependencies.

        Args:
            pipeline (OpusPocusPipeline): pipeline to execute
            target_labels (List[str]): list of step labels to execute (implying execution of their dependencies)
            resubmit_done (bool): should we resubmit finished subtasks of a failed task
        """
        self.save_parameters()
        for step in pipeline.get_targets(target_labels):
            self.submit_step(step, resubmit_finished_subtasks=resubmit_finished_subtasks)
        self.run()
        logger.info("[%s] Pipeline tasks submitted successfully.", self.runner)

    def submit_step(self, step: OpusPocusStep, *, resubmit_finished_subtasks: bool = True) -> Optional[SubmissionInfo]:
        """Submit a pipeline step for execution.

        First, check the current step state to avoid resubmission of already SUBMITTED/RUNNING/DONE
        step.
        For FAILED tasks, first clean up the work directories (output, temp) and remove already finished outputs
        if resubmit_finished_subtasks is set to True.
        Afterwards, submit the step's main_task using the specific runner's submit_task method implementation and
        save the information about the main_task submission.

        Args:
            step (OpusPocusStep): step to submit
            resubmit_finished_subtasks (bool): resubmit finished subtasks of a failed (partially done) task

        Returns:
            SubmissionInfo containing the submission ID of the main_task.
        """
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
            step.clean_directories(resubmit_finished_command_targets=resubmit_finished_subtasks)
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

        # Submit the main step task which then is responsible of submitting its subtasks
        logger.info("[%s] Submitting '%s' main step task.", self.runner, step.step_label)

        # NOTE(varisd): we set the state to SUBMITTED before actual submission to avoid possible race conditions
        step.state = StepState.SUBMITTED
        try:
            timestamp = time.time()
            task_info = self.submit_task(
                cmd_path=step.cmd_path,
                target_file=None,
                dependencies=[dep["main_task"] for dep in dep_sub_info_list],
                step_resources=self.get_resources(step),
                stdout_file=Path(step.log_dir, f"{self.runner}.main.{timestamp}.out"),
                stderr_file=Path(step.log_dir, f"{self.runner}.main.{timestamp}.err"),
            )
        except Exception:
            step.state = StepState.FAILED
            logger.exception("Task submission in runner.submit_step raised an Exception.")
            raise

        sub_info = SubmissionInfo(runner=self.runner, main_task=task_info, subtasks=[])
        self.save_submission_info(step, sub_info)
        return sub_info

    def resubmit_step(self, step: OpusPocusStep, *, resubmit_finished_subtasks: bool = True) -> SubmissionInfo:
        """Resubmit a currently running step execution.

        This is a wrapper that first cancels a current step execution and then resubmits that steps main_task.
        All of this is implemented via the signal-handling within the OpusPocusStep.run_main_task method.

        Args:
            step (OpusPocusStep): running step to resubmit
            resubmit_finished_subtasks (bool): resubmit step's subtasks that have alredy finished execution (delete
                their output files)

        Returns:
            SubmissionInfo containing the submission ID of the main_task.
        """
        sub_info = self.load_submission_info(step)
        if not resubmit_finished_subtasks:
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
        """Update tasks that have the provided step as a dependency.

        Each dependant (task with the `step` as a dependency) is updated using the provided task lists.
        This method is aimed at updating a running pipeline a pipeline step is resubmitted. After submitting new
        main_task, the previous dependencies should be updated to have the new task replace the old task in the
        dependency list.

        Args:
            step (OpusPocusStep): step that is the dependency of the updated tasks
            remove_task_list (List[SlurmTaskInfo]): tasks to remove from the dependencies
            add_task_list (List[SlurmTaskInfo]): tasks to add to the dependencies
        """
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
        """A runner specific code for submitting step's tasks.

        Args:
            cmd_path (Path): location of the step's command to execute
            target_file (Path): target_file to create by a subtask (if not None)
            dependencies (List[TaskInfo]): list of task information about the running dependencies
            step_resources (RunnerResources): resources to allocate for the task
            stdout_file (Path): location of the log file for task's stdout
            stderr_file (Path): location of the log file for task's stderr

        Returns:
            TaskInfo containing the information about the submitted task.
        """
        raise NotImplementedError()

    def send_signal(self, task_info: TaskInfo, signal: int) -> None:
        """A runner specific code for sending signals to SUBMITTED/RUNNING tasks.

        Args:
            task_info (TaskInfo): specification of the task receiving the signal
            signal (int): signal to sent
        """
        raise NotImplementedError()

    def cancel_task(self, task_info: TaskInfo) -> None:
        """Cancel given task info (send a SIGTERM signal).

        Args:
            task_info (TaskInfo): specification of the task to cancel
        """
        self.send_signal(task_info, signal.SIGTERM)

    def wait_for_tasks(
        self, task_info_list: Optional[List[TaskInfo]] = None, *, ignore_returncode: bool = False
    ) -> None:
        """Wait for the list of tasks to finish execution.

        Args:
            task_info_list (List[TaskInfo]): list of the task-specific information for the given tasks
            ignore_returncode (bool): ignore the finished tasks' return code
        """
        for task_info in task_info_list:
            self.wait_for_single_task(task_info, ignore_returncode=ignore_returncode)

    def wait_for_single_task(self, task_info: TaskInfo, *, ignore_returncode: bool = False) -> None:
        """Wait for the task to finish execution. A runner-specific code.

        Args:
            task_info (TaskInfo): task-specific information
            ignore_returncode (bool): ignore the finished task's return code
        """
        raise NotImplementedError()

    def is_task_running(self, task_info: TaskInfo) -> bool:
        """Check whether a task is currently running.

        Args:
            task_info (TaskInfo): task-specific information

        Returns:
            True if the task is currenly running.
        """
        raise NotImplementedError()

    def save_submission_info(self, step: OpusPocusStep, sub_info: SubmissionInfo) -> None:
        """Save the submitted step's submission information.

        The information about the execution submission is saved in the given step's directory.
        This can be later used in later OpusPocus calls to manipulate a running pipeline (pipeline stopping,
        resubmission, etc.).

        Args:
            step (OpusPocusStep): step connected to the given submission info
            sub_info (SubmissionInfo): submission information for the given step execution submission
        """
        with Path(step.step_dir, self._info_filename).open("w") as fh:
            yaml.dump(sub_info, fh)

    def load_submission_info(self, step: OpusPocusStep) -> Optional[SubmissionInfo]:
        """Load the submission information for a given pipeline step.

        Args:
            step (OpusPocusStep): pipelinene step

        Returns:
            Information about the step execution.
        """
        with Path(step.step_dir, self._info_filename).open("r") as fh:
            return yaml.safe_load(fh)

    def get_resources(self, step: OpusPocusStep) -> RunnerResources:
        """Get default runner resources."""
        # TODO: expand the logic here
        return step.default_resources
