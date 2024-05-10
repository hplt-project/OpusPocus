from typing import Any, Dict, List, Optional

import argparse
from pathlib import Path

from opuspocus.runners import (
    OpusPocusRunner,
    TaskId,
    RunnerResources,
    register_runner
)

SLEEP_TIME = 0.1


@register_runner('slurm')
class SlurmRunner(OpusPocusRunner):
    """TODO"""

    @staticmethod
    def add_args(parser):
        """Add runner-specific arguments to the parser."""
        pass

    def __init__(
        self,
        name: str,
        args: argparse.Namespace,
    ):
        super().__init__(name, args)

    def submit(
        cmd_path: Path,
        file_list: Optional[List[str]] = None,
        dependencies: Optional[List[TaskId]] = None,
        step_resources: Optional[RunnerResources] = None
    ) -> List[TaskId]:
        dep_jids = []
        if dependencies:
            dep_jids = [dep['jid'] for dep in dependencies]

        # TODO: can we replace this with a proper Python API?
        cmd = ['sbatch', '--parsable']

        if dep_jids:
            cmd.append('--dependency')
            cmd.append(
                ','.join(['afterok:{}'.format(dep) for dep in dep_jids])
            )

        cmd += self.convert_resources(step_resources)
        cmd += self.add_environment_variables(resources)

        cmd.append(str(cmd_path))
        if file_list is not None:
            new_dependencies = []
            for f in file_list:
                proc = subprocess.Popen(
                    cmd + [f],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    shell=False
                )
                new_dependencies.append(
                    { 'jid': int(proc.stdout.readline()) }
                )
                # small delay to avoid overwhelming the scheduler
                sys.sleep(SLEEP_TIME)
            return new_dependencies

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False
        )
        return [{'jid': int(proc.stdout.readline())}]

    def cancel(task_id: TaskId) -> None:
        proc = subprocess.Popen(
            ['scancel', str(task_id['jid'])], shell=False
        )

    def convert_resources(resources: RunnerResources) -> List[str]:
        converted = []
        if resources.cpus is not None:
            converted += ['--cpus', str(resources.cpus)]

        if resources.gpus is not None:
            converted += ['--gpus', str(resources.gpus)]

        if resources.mem is not None:
            converted += ['--mem', str(resources.mem)]

        if resources.account is not None:
            converted += ['--account', str(resources.account)]

        if resources.partition is not None:
            converted += ['--partition', str(resources.partition)]

        return converted

    def add_environment_variables(resources: RunnerResources) -> List[str]:
        # TODO finish this
        pass

    def task_id_to_string(self, task_id: TaskId) -> str:
        return str(task_id['jid'])

    def string_to_task_id(self, id_str: str) -> TaskId:
        return {'jid': int(id_str)}
