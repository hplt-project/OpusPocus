from typing import Any, Dict, List, Optional

import argparse
from pathlib import Path

from opuspocus.runners import (
    OpusPocusRunner,
    RunnerOutput,
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
        args: argparse.Namespace,
    ):
        super().__init__(args)

    def submit(
        cmd_path: Path,
        file_list: Optional[List[str]] = None,
        dependencies: Optional[List[SlurmOutput]] = None,
        step_resources: Optional[RunnerResources] = None
    ) -> List[SlurmOutput]:
        if dependencies:
            dependencies = [dep.jid for dep in dependencies]

        # TODO: can we replace this with a proper Python API?
        cmd = ['sbatch', '--parsable']

        if dependencies:
            cmd.append('--dependency')
            cmd.append(
                ','.join(['afterok:{}'.format(dep) for dep in dependencies])
            )

        resources = self.global_resources.overwrite(step_resources)
        cmd += self.convert_resources(resources)
        cmd += self.add_environment_variables(resources)

        cmd.append(str(cmd_path))
        if file_list is not None:
            new_dependencies = []
            for f in file_list
                proc = subprocess.Popen(
                    cmd + [f],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    shell=False
                )
                new_dependencies.append(
                    SlurmOutput(int(proc.stdout.readline()))
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
        return [SlurmOutput(int(proc.stdout.readline()))]

    def run():
        pass

    def convert_resources(resources: StepResources) -> List[str]:
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

    def add_environment_variables(resources: StepResources) -> List[str]:
        # TODO finish this
        pass


class SlurmOutput(RunnerOutput):
    """TODO"""

    def __init__(self, job_id: int):
        self.jid = job_id

    def __str__(self) -> str:
        return str(self.jid)
