from typing import Any, Dict, List, Optional

import argparse
import logging
import subprocess
import sys
from pathlib import Path
from psutil import Process, wait_procs
import time

from opuspocus.runners import (
    OpusPocusRunner,
    TaskId,
    TaskInfo,
    register_runner
)
from opuspocus.utils import RunnerResources, subprocess_wait

logger = logging.getLogger(__name__)

SLEEP_TIME = 0.1


@register_runner('bash')
class BashRunner(OpusPocusRunner):
    """TODO"""
    submit_wrapper = 'scripts/bash_runner_submit.py'

    @staticmethod
    def add_args(parser):
        OpusPocusRunner.add_args(parser)
        parser.add_argument(
            '--run-subtasks-in-parallel', action='store_true', default=False,
            help='TODO'
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
            run_subtasks_in_parallel=run_subtasks_in_parallel
        )

    def submit_task(
        self,
        cmd_path: Path,
        target_file: Optional[Path] = None,
        dependencies: Optional[List[TaskId]] = None,
        step_resources: Optional[RunnerResources] = None,
        stdout=sys.stdout,
        stderr=sys.stderr
    ) -> TaskId:
        """TODO"""
        dependencies_str = ''
        if dependencies is not None:
            dependencies_str = ' '.join(
                [str(dep['id']) for dep in dependencies]
            )
        env_dict = step_resources.get_env_dict()

        # Subtasks do not have dependencies, no need for the wrapper
        if target_file is not None:
            proc = subprocess.Popen(
                [str(cmd_path), str(target_file)],
                start_new_session=False,
                stdout=stdout,
                stderr=stderr,
                shell=False,
                env=env_dict
            )
            if not self.run_subtasks_in_parallel:
                logger.debug('Waiting for process to finish...')
                subprocess_wait(proc)
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
                env=env_dict
            )

        return TaskId(filename=str(target_file), id=proc.pid)

    def get_process(self, task_id: TaskId) -> Optional[Process]:
        """TODO"""
        try:
            proc = Process(task_id['id'])
        except:
            logger.debug(
                'Process with pid={} does not exist. Ignoring...'
                .format(task_id)
            )
            return None
        return proc

    def cancel_task(self, task_id: TaskId) -> None:
        """TODO"""
        proc = self.get_process(task_id['id'])
        p.terminate()

    def wait_for_task(self, task_id: TaskId) -> None:
        proc = self.get_process(task_id)
        if proc is None:
            return
        gone, _ = wait_procs([proc])
        for p in gone:
            if p.returncode:
                raise subprocess.SubprocessError(
                    'Process {} exited with non-zero '
                    'value.'.format(task_id['id'])
                )

    def is_task_running(self, task_id: TaskId) -> bool:
        """TODO"""
        proc = self.get_process(task_id)
        return (proc is not None)
