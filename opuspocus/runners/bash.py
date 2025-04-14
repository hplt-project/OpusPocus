import logging
import signal
import subprocess
import sys
import time
from argparse import ArgumentParser
from pathlib import Path
from typing import List, Optional

from attrs import define, field, validators
from psutil import NoSuchProcess, Process, wait_procs

from opuspocus.runners import OpusPocusRunner, TaskInfo, register_runner
from opuspocus.utils import RunnerResources, subprocess_wait

logger = logging.getLogger(__name__)

SLEEP_TIME = 0.5


class BashTaskInfo(TaskInfo):
    id: int  # process ID


@register_runner("bash")
@define(kw_only=True)
class BashRunner(OpusPocusRunner):
    """Class implementing task execution using bash."""

    run_tasks_in_parallel: bool = field(validator=validators.instance_of(bool), default=False)

    _submit_wrapper = "scripts/bash_runner_submit.py"

    @staticmethod
    def add_args(parser: ArgumentParser) -> None:
        """Add runner-specific arguments to the parser."""
        OpusPocusRunner.add_args(parser)
        OpusPocusRunner.add_runner_argument(
            parser,
            "run_tasks_in_parallel",
            default=False,
            action="store_true",
            help="Submit tasks as processes running in parallel wherever possible.",
        )

    def submit_task(
        self,
        cmd_path: Path,
        target_file: Optional[Path] = None,
        dependencies: Optional[List[BashTaskInfo]] = None,
        step_resources: Optional[RunnerResources] = None,
        stdout_file: Optional[Path] = None,
        stderr_file: Optional[Path] = None,
    ) -> BashTaskInfo:
        """Submits the task using the `bash` command.

        Submits the command via the _submit_wrapper that handles waiting for the task dependencies to finish.
        This logic is moved to the wrapper conserve OpusPocus's runner "execute and terminate" policy.
        OpusPocus exectus the requested command (run, stop, etc.) and then the OpusPocus process terminates.
        Any scheduling, task monitoring is handled outside of the OpusPocus process (in this case using
        the task's specific wrapper).

        Args:
            cmd_path (Path): location of the step's command to be executed
            target_file (Path): target_file to be created by a subtask (if not None)
            dependencies (List[BashTaskInfo]): list of task information about the running dependencies
            step_resources (RunnerResources): resources to be allocated for the task
            stdout_file (Path): location of the log file for task's stdout
            stderr_file (Path): location of the log file for task's stderr

        Returns:
            BashTaskInfo with the process ID and the related target_file in case of subtask execution.
        """
        dependencies_str = ""
        if dependencies is not None:
            dependencies_str = " ".join([str(dep["id"]) for dep in dependencies])
        env_dict = step_resources.get_env_dict()

        stdout = sys.stdout
        if stdout_file is not None:
            stdout = stdout_file.open("w")
        stderr = sys.stderr
        if stderr_file is not None:
            stderr = stderr_file.open("w")

        # Subtasks do not have dependencies - no need for the wrapper
        if target_file is not None:
            proc = subprocess.Popen(
                [str(cmd_path), str(target_file)],
                stdout=stdout,
                stderr=stderr,
                shell=False,
                env=env_dict,
            )
        else:
            proc = subprocess.Popen(
                [
                    self._submit_wrapper,
                    str(cmd_path),
                    dependencies_str,
                ],
                start_new_session=True,
                stdout=stdout,
                stderr=stderr,
                shell=False,
                env=env_dict,
            )
        t_file_str = None
        if target_file is not None:
            t_file_str = str(target_file)
        task_info = BashTaskInfo(file_path=t_file_str, id=proc.pid)
        logger.info("Submitted command: '%s %s', pid: %i", cmd_path, t_file_str, proc.pid)
        time.sleep(SLEEP_TIME)  # We do not want to start proccess too quickly

        # If executing serially, we wait for each process to finish before submitting next
        if target_file is not None and not self.run_tasks_in_parallel:
            logger.debug("Waiting for process %i to finish...", proc.pid)
            subprocess_wait(proc)
        return task_info

    def send_signal(self, task_info: BashTaskInfo, signal: int = signal.SIGTERM) -> None:
        """Send the signal to the process with ID from the task_info.

        Args:
            task_info (BashTaskInfo): task info containing the PID of the task's process
            signal (int): signal to send
        """
        proc = self._get_process(task_info)
        logger.debug("Signal %i was sent to process %i.", signal, task_info["id"])
        if proc is not None:
            proc.send_signal(signal)

    def wait_for_single_task(self, task_info: BashTaskInfo, *, ignore_returncode: bool = False) -> None:
        """Wait for the task's process to finish.

        Args:
            task_info (BashTaskInfo): task info containing the PID of the task's process
            ignore_returncode (bool): ignore the return code of the finished process execution
        """
        pid = task_info["id"]
        proc = self._get_process(task_info)
        if proc is None:
            return
        gone, _ = wait_procs([proc])
        if ignore_returncode:
            return
        for p in gone:
            if p.returncode:
                if task_info["file_path"] is not None:
                    file_path = Path(task_info["file_path"])
                    if file_path.exists():
                        file_path.unlink()
                err_msg = f"Process {pid} exited with non-zero value."
                raise subprocess.SubprocessError(err_msg)

    def is_task_running(self, task_info: BashTaskInfo) -> bool:
        """Check whether the task's process is currently running.

        Args:
            task_info (BashTaskInfo): task info containing the PID of the task's process

        Returns:
            True if the process is currently running.
        """
        return self._get_process(task_info) is not None

    def _get_process(self, task_info: BashTaskInfo) -> Optional[Process]:
        """Get the process given the process ID.

        Args:
            task_info (BashTaskInfo): task info containing the PID of the task's process
        """
        try:
            proc = Process(task_info["id"])
        except NoSuchProcess:
            logger.debug("Process with pid=%i does not exist. Ignoring...", task_info["id"])
            return None
        return proc
