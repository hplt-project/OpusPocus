from typing import Any, Dict, List, Optional

import argparse
import logging
import subprocess
import sys
from pathlib import Path
from psutil import Process
import time

from opuspocus.runners import (
    OpusPocusRunner,
    TaskId,
    register_runner
)
from opuspocus.utils import RunnerResources


SLEEP_TIME = 0.1

logger = logging.getLogger(__name__)


@register_runner('bash')
class BashRunner(OpusPocusRunner):
    """TODO"""
    submit_wrapper = 'scripts/bash_runner_submit.py'

    def __init__(
        self,
        runner: str,
        pipeline_dir: Path
    ):
        super().__init__(runner, pipeline_dir)

    def _submit_step(
        self,
        cmd_path: Path,
        file_list: Optional[List[str]] = None,
        dependencies: Optional[List[TaskId]] = None,
        step_resources: Optional[RunnerResources] = None,
        stdout=sys.stdout,
        stderr=sys.stderr
    ) -> List[TaskId]:
        dependencies_str = ''
        if dependencies is not None:
            dependencies_str = ' '.join([
                self.task_id_to_string(dep)
                for dep in dependencies
            ])
        env_dict = step_resources.get_env_dict()

        # Each task is started as a new process that waits on the depencencies
        # (processes) in a for loop, checks their status after they finish and
        # executes the task command
        # TODO: what happens when we submit hundreds/thousands such ``waiting''
        # processes?
        # TODO: implement a proper ``scheduler'' for running on a single machine
        if file_list is not None:
            outputs = []
            for file in file_list:
                dep_str = dependencies_str
                if outputs:
                    dep_str = '{} {}'.format(dep_str, outputs[-1]['pid'])
                proc = subprocess.Popen(
                    [
                        self.submit_wrapper,
                        cmd_path,
                        dep_str,
                        file
                    ],
                    start_new_session=True,
                    stdout=stdout,
                    stderr=stderr,
                    shell=False,
                    env=env_dict
                )
                outputs.append({'pid': proc.pid})
                time.sleep(SLEEP_TIME)  # Sleep to not overload the process manager
            return outputs

        proc = subprocess.Popen(
            [self.submit_wrapper, cmd_path, dependencies_str],
            start_new_session=True,
            stdout=stdout,
            stderr=stderr,
            shell=False,
            env=env_dict
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
