from typing import Any, Dict, List, Optional

import argparse
import logging
from pathlib import Path
from psutil import Process
import time

from opuspocus.runners import (
    OpusPocusRunner,
    TaskId,
    RunnerResources,
    register_runner
)


SLEEP_TIME = 0.1

logger = logging.getLogger(__name__)


@register_runner('bash')
class BashRunner(OpusPocusRunner):
    """TODO"""
    submit_wrapper = 'scripts/bash_runner_submit.py'

    def __init__(
        self,
        name: str,
        args: argparse.Namespace,
    ):
        super().__init__(name, args)

    def _submit_step(
        cmd_path: Path,
        file_list: Optional[List[str]] = None,
        dependencies: Optional[List[TaskId]] = None,
        step_resources: Optional[RunnerResources] = None,
        stdout: Optional[Path] = None,
        stderr: Optional[Path] = None
    ) -> List[TaskId]:
        dependencies_str = ''
        if dependencies is not None:
            dependencies_str = ' '.join([str(dep) for dep in dependencies])

        # Each task is started as a new process that waits on the depencencies
        # (processes) in a for loop, checks their status after they finish and
        # executes the task command
        # TODO: what happens when we submit hundreds/thousands such ``waiting''
        # processes?
        # TODO: implement a proper ``scheduler'' for running on a single machine
        if file_list:
            outputs = []
            for file in file_list:
                proc = subprocess.Popen(
                    [self.submit_wrapper, cmd_path, dependencies_str, file]
                    start_new_session=True,
                    stdout=stdout,
                    stderr=stderr,
                    shell=False
                )
                outputs.append({'pid': proc.pid})
                time.time(SLEEP_TIME)  # Sleep to not overload the process manager
            return outputs

        proc = subprocess.Popen(
            [self.submit_wrapper, cmd_path, dependencies_str]
            start_new_session=True,
            stdout=stdout,
            stderr=stderr,
            shell=False
        )
        return [{'pid': proc.pid}]

    def cancel(task_id: TaskId) -> None:
        try:
            proc = Process(task_id['pid'])
            p.terminate()
        except:
            logger.warn('Process {} does not exist. Ignoring.'.format(task_id))

    def task_id_to_string(self, task_id: TaskId) -> str:
        return str(task_id['pid'])

    def string_to_task_id(self, id_str: str) -> TaskId:
        return {'pid': int(id_str)}
