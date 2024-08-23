import logging
import subprocess
import sys
import time
from argparse import ArgumentParser
from pathlib import Path
from typing import List, Optional

from psutil import NoSuchProcess, Process, wait_procs

from opuspocus.runners import OpusPocusRunner, TaskInfo, register_runner
from opuspocus.utils import RunnerResources, subprocess_wait

logger = logging.getLogger(__name__)

SLEEP_TIME = 0.5


class BashTaskInfo(TaskInfo):
    id: int


@register_runner("bash")
class BashRunner(OpusPocusRunner):
    """TODO"""

    submit_wrapper = "scripts/bash_runner_submit.py"

    @staticmethod
    def add_args(parser: ArgumentParser) -> None:
        OpusPocusRunner.add_args(parser)
        parser.add_argument(
            "--run-tasks-in-parallel",
            action="store_true",
            default=False,
            help="TODO",
        )

    def __init__(
        self,
        runner: str,
        pipeline_dir: Path,
        *,
        run_tasks_in_parallel: bool = False,
    ) -> None:
        super().__init__(
            runner=runner,
            pipeline_dir=pipeline_dir,
            run_tasks_in_parallel=run_tasks_in_parallel,
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
        """TODO"""
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
            task_info = BashTaskInfo(file_path=str(target_file), id=proc.pid)
        else:
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
            task_info = BashTaskInfo(file_path=None, id=proc.pid)
        time.sleep(SLEEP_TIME)  # We do not want to start proccess too quickly
        # If executing serially, we wait for each process to finish before submitting next
        if target_file is not None and not self.run_tasks_in_parallel:
            logger.info("Waiting for process to finish...")
            subprocess_wait(proc)
        return task_info

    def cancel_task(self, task_info: BashTaskInfo) -> None:
        """TODO"""
        proc = self._get_process(task_info)
        if proc is not None:
            proc.terminate()

    def wait_for_single_task(self, task_info: BashTaskInfo) -> None:
        pid = task_info["id"]
        proc = self._get_process(task_info)
        if proc is None:
            return
        gone, _ = wait_procs([proc])
        for p in gone:
            if p.returncode:
                Path(task_info["file_path"]).unlink()
                err_msg = f"Process {pid} exited with non-zero value."
                raise subprocess.SubprocessError(err_msg)

    def is_task_running(self, task_info: BashTaskInfo) -> bool:
        """TODO"""
        proc = self._get_process(task_info)
        return proc is not None

    def _get_process(self, task_info: BashTaskInfo) -> Optional[Process]:
        """TODO"""
        try:
            proc = Process(task_info["id"])
        except NoSuchProcess:
            logger.debug("Process with pid=%i does not exist. Ignoring...", task_info["id"])
            return None
        return proc
