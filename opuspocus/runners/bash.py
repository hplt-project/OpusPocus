from typing import List, Optional

import logging
import subprocess
import sys
from pathlib import Path
from psutil import NoSuchProcess, Process, wait_procs

from opuspocus.runners import OpusPocusRunner, TaskId, register_runner
from opuspocus.utils import RunnerResources, subprocess_wait

logger = logging.getLogger(__name__)

SLEEP_TIME = 0.1


@register_runner("bash")
class BashRunner(OpusPocusRunner):
    """TODO"""

    submit_wrapper = "scripts/bash_runner_submit.py"

    @staticmethod
    def add_args(parser):
        OpusPocusRunner.add_args(parser)
        parser.add_argument(
            "--run-subtasks-in-parallel",
            action="store_true",
            default=False,
            help="TODO",
        )

    def __init__(
        self,
        runner: str,
        pipeline_dir: Path,
        run_subtasks_in_parallel: bool = False,
    ):
        super().__init__(
            runner=runner,
            pipeline_dir=pipeline_dir,
            run_subtasks_in_parallel=run_subtasks_in_parallel,
        )

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
        dependencies_str = ""
        if dependencies is not None:
            dependencies_str = " ".join([str(dep["id"]) for dep in dependencies])
        env_dict = step_resources.get_env_dict()

        stdout = sys.stdout
        if stdout_file is not None:
            stdout = open(stdout_file, "w")
        stderr = sys.stderr
        if stderr_file is not None:
            stderr = open(stderr_file, "w")

        # Subtasks do not have dependencies, no need for the wrapper
        if target_file is not None:
            proc = subprocess.Popen(
                [str(cmd_path), str(target_file)],
                stdout=stdout,
                stderr=stderr,
                shell=False,
                env=env_dict,
            )
            if not self.run_subtasks_in_parallel:
                logger.debug("Waiting for process to finish...")
                subprocess_wait(proc)
        else:
            if not self.run_subtasks_in_parallel:
                dependencies_str = " ".join(
                    str(task["main_task"]["id"]) for task in self.submitted_tasks
                )
            proc = subprocess.Popen(
                [
                    self.submit_wrapper,
                    str(cmd_path),
                    dependencies_str,
                ],
                start_new_session=True,
                stdout=stdout,
                stderr=stderr,
                shell=False,
                env=env_dict,
            )

        return TaskId(file_path=str(target_file), id=proc.pid)

    def update_dependants(self, task_id: TaskId) -> None:
        return NotImplementedError()

    def cancel_task(self, task_id: TaskId) -> None:
        """TODO"""
        proc = self._get_process(task_id["id"])
        if proc is not None:
            proc.terminate()

    def wait_for_task(self, task_id: TaskId) -> None:
        proc = self._get_process(task_id)
        if proc is None:
            return
        gone, _ = wait_procs([proc])
        for p in gone:
            if p.returncode:
                self.remove_task_file(task_id)
                raise subprocess.SubprocessError(
                    "Process {} exited with non-zero " "value.".format(task_id["id"])
                )

    def is_task_running(self, task_id: TaskId) -> bool:
        """TODO"""
        proc = self._get_process(task_id)
        return proc is not None

    def _get_process(self, task_id: TaskId) -> Optional[Process]:
        """TODO"""
        try:
            proc = Process(task_id["id"])
        except Exception:
            logger.debug(
                "Process with pid={} does not exist. Ignoring...".format(task_id)
            )
            return None
        return proc
