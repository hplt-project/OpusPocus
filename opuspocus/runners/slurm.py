from typing import List, Optional

import logging
import subprocess
import sys
import time
from pathlib import Path

from opuspocus.runners import OpusPocusRunner, TaskId, register_runner
from opuspocus.utils import RunnerResources

logger = logging.getLogger(__name__)

SLEEP_TIME = 0.5
WAIT_TIME = 30


@register_runner("slurm")
class SlurmRunner(OpusPocusRunner):
    """TODO"""

    @staticmethod
    def add_args(parser):
        """Add runner-specific arguments to the parser."""
        OpusPocusRunner.add_args(parser)
        parser.add_argument(
            "--slurm_other_options", type=str, default=None, help="TODO"
        )

    def __init__(
        self,
        runner: str,
        pipeline_dir: Path,
        slurm_other_options: str,
    ):
        super().__init__(
            runner=runner,
            pipeline_dir=pipeline_dir,
            slurm_other_options=slurm_other_options,
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
        dep_jids = []
        if dependencies:
            dep_jids = [dep["jid"] for dep in dependencies]

        # TODO: can we replace this with a proper Python API?
        cmd = ["sbatch", "--parsable"]

        if dep_jids:
            cmd.append("--dependency")
            cmd.append(",".join(["afterok:{}".format(dep) for dep in dep_jids]))

        cmd += self._convert_resources(step_resources)
        cmd += self._add_environment_variables(resources)

        jobname = "{}.{}.{}".format(
            runner, pipeline_dir.stem + pipeline_dir.suffix, target_file.stem
        )
        cmd += ["--jobname", jobname]
        cmd += ["--signal", "TERM@10:00"]  # send SIGTERM 10m before time-limit

        if stdout_file is not None:
            cmd += ["-o", stdout_file]
        if stderr_file is not None:
            cmd += ["-e", stderr_file]
        if self.slurm_other_options is not None:
            cmd += [self.slurm_other_options]

        if target_file is not None:
            cmd += [str(cmd_path), str(target_file)]
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False
            )
        else:
            cmd += [str(cmd_path)]
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
            )
        time.sleep(SLEEP_TIME)
        jid = int(proc.stdout.readline())
        return TaskId(filename=str(target_file), id=jid)

    def update_dependants(self, task_id: TaskId) -> None:
        raise NotImplementedError()

    def cancel_task(self, task_id: TaskId) -> None:
        """TODO"""
        proc = subprocess.Popen(["scancel", str(task_id["id"])], shell=False)
        rc = proc.wait()
        if rc:
            raise ("Failed to cancel SLURM task {}".format(task_id["id"]))

    def wait_for_task(self, task_id: TaskId) -> None:
        """TODO"""
        while self.is_task_running(task_id):
            sys.sleep(WAIT_TIME)

    def is_task_running(self, task_id: TaskId) -> bool:
        """TODO"""
        proc = subprocess.Popen(["squeue", "-j", task_id["id"]], shell=False)
        rc = proc.wait()
        if rc:
            return False
        return True

    def _convert_resources(resources: RunnerResources) -> List[str]:
        converted = []
        if resources.cpus is not None:
            converted += ["--cpus", str(resources.cpus)]

        if resources.gpus is not None:
            converted += ["--gpus", str(resources.gpus)]

        if resources.mem is not None:
            converted += ["--mem", str(resources.mem)]

        return converted

    def _add_environment_variables(resources: RunnerResources) -> List[str]:
        # TODO finish this
        return [
            "--export={}".format(
                ",".join(
                    [
                        "{}='{}'".format(k, str(v))
                        for k, v in resources.get_env_dict().items()
                    ]
                )
            )
        ]
