import logging
import re
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional

from opuspocus.pipeline_steps import OpusPocusStep
from opuspocus.pipelines import OpusPocusPipeline
from opuspocus.runners import OpusPocusRunner, TaskInfo, register_runner
from opuspocus.utils import RunnerResources, subprocess_wait

logger = logging.getLogger(__name__)

SLEEP_TIME = 2


class SlurmTaskInfo(TaskInfo):
    id: int


@register_runner("slurm")
class SlurmRunner(OpusPocusRunner):
    """TODO"""

    @staticmethod
    def add_args(parser):  # noqa: ANN001, ANN205
        """Add runner-specific arguments to the parser."""
        OpusPocusRunner.add_args(parser)
        parser.add_argument(
            "--slurm-other-options", type=str, metavar="RUNNER", default=None, help="Additional Slurm CLI options."
        )

    def __init__(
        self,
        runner: str,
        pipeline_dir: Path,
        slurm_other_options: str,
    ) -> None:
        super().__init__(
            runner=runner,
            pipeline_dir=pipeline_dir,
            slurm_other_options=slurm_other_options,
        )

    def submit_task(
        self,
        cmd_path: Path,
        target_file: Optional[Path] = None,
        dependencies: Optional[List[SlurmTaskInfo]] = None,
        step_resources: Optional[RunnerResources] = None,
        stdout_file: Optional[Path] = None,
        stderr_file: Optional[Path] = None,
    ) -> SlurmTaskInfo:
        dependencies_ids = []
        if dependencies is not None:
            dependencies_ids = [dep["id"] for dep in dependencies]

        # TODO: can we replace this with a proper Python API?
        cmd = ["sbatch"]

        if dependencies_ids:
            cmd.append("--dependency")
            cmd.append(",".join([f"afterok:{dep!s}" for dep in dependencies_ids]))

        cmd += self._convert_resources(step_resources)

        jobname = f"{self.runner}.{self.pipeline_dir.stem}{self.pipeline_dir.suffix}"
        if target_file is not None:
            jobname += f".{target_file.stem}"
        cmd += ["--job-name", jobname]
        cmd += ["--time", "60"]
        cmd += ["--signal", "10@600"]  # send SIGTERM 10m before time-limit

        if stdout_file is not None:
            cmd += ["-o", str(stdout_file)]
        if stderr_file is not None:
            cmd += ["-e", str(stderr_file)]
        if self.slurm_other_options is not None:
            cmd += [self.slurm_other_options.split(",")]

        t_file_str = None
        if target_file is not None:
            t_file_str = str(target_file)

        if target_file is not None:
            cmd += [str(cmd_path), str(target_file)]
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=sys.stderr, shell=False, env=step_resources.get_env_dict()
            )
        else:
            cmd += [str(cmd_path)]
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=sys.stderr, shell=False, env=step_resources.get_env_dict()
            )
        logger.info("Submitted sbatch command: %s", " ".join(cmd))

        subprocess_wait(proc)
        cmd_out = proc.stdout.readline().decode().strip("\n")
        jid = int(cmd_out.split(" ")[-1])

        task_info = SlurmTaskInfo(file_path=t_file_str, id=jid)
        logger.info("sbatch command jobid: %i", jid)
        logger.debug("sbatch output: '%s'", cmd_out)

        return task_info

    def update_dependants(
        self,
        step: OpusPocusStep,
        remove_task_list: Optional[List[SlurmTaskInfo]] = None,
        add_task_list: Optional[List[SlurmTaskInfo]] = None,
    ) -> None:
        if remove_task_list is None:
            remove_task_list = []
        if add_task_list is None:
            add_task_list = []
        pipeline = OpusPocusPipeline.load_pipeline(self.pipeline_dir)
        dependant_list = pipeline.get_dependants(step)
        dependant_sub_info_list = [self.load_submission_info(dep) for dep in dependant_list]

        for sub_info in dependant_sub_info_list:
            jid = sub_info["main_task"]["id"]
            dependency_list = self._get_slurm_dependencies(
                sub_info["main_task"], exclude_ids=[t_info["id"] for t_info in remove_task_list]
            )
            dependency_list_str = ",".join([f"afterok:{t_info['id']}" for t_info in add_task_list])
            if dependency_list:
                dependency_list_str += "," + ",".join(dependency_list)
            time.sleep(SLEEP_TIME)
            cmd = ["scontrol", "update", f"jobid={jid}", f"Dependency={dependency_list_str}"]
            logger.info("Executing command: '%s'", " ".join(cmd))
            proc = subprocess.Popen(
                cmd,
                stdout=sys.stdout,
                stderr=sys.stderr,
                shell=False,
            )
            subprocess_wait(proc)

    def send_signal(self, task_info: SlurmTaskInfo, signal: int = signal.SIGTERM) -> None:
        """TODO"""
        time.sleep(SLEEP_TIME)
        status = self._get_job_status(task_info)
        logger.debug("Sending signal to job %s with '%s' status...", task_info["id"], status)
        if status == "FAILED":
            return
        if status == "PENDING":
            proc = subprocess.Popen(
                ["scancel", str(task_info["id"])], stdout=sys.stdout, stderr=sys.stderr, shell=False
            )
        else:
            cmd = ["scancel", "-b", f"--signal={signal:d}", str(task_info["id"])]
            proc = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr, shell=False)
            logger.debug(
                "Signal %i was sent to Slurm job %i using the following command: '%s'.\n"
                "Submitted scancel process pid %i.",
                signal,
                task_info["id"],
                " ".join(cmd),
                proc.pid,
            )
        subprocess_wait(proc)

    def wait_for_single_task(self, task_info: SlurmTaskInfo, *, ignore_returncode: bool = False) -> None:
        """TODO"""
        time.sleep(SLEEP_TIME)  # NOTE(varisd): workaround to give sbatch time to properly submit the job
        while self.is_task_running(task_info):
            time.sleep(SLEEP_TIME)
        if ignore_returncode:
            return
        status = self._get_job_status(task_info)
        if status != "COMPLETED":
            if task_info["file_path"] is not None:
                file_path = Path(task_info["file_path"])
                if file_path.exists():
                    file_path.unlink()
            jid = task_info["id"]
            err_msg = f"Slurm Job {jid} finished execution in state {status}."
            raise subprocess.SubprocessError(err_msg)

    def is_task_running(self, task_info: SlurmTaskInfo) -> bool:
        """TODO"""
        return self._get_job_status(task_info) in {"PENDING", "RUNNING"}

    def _get_sacct_info(self, task_info: SlurmTaskInfo) -> List[str]:
        jid = task_info["id"]
        proc = subprocess.Popen(
            ["sacct", "-j", f"{jid}", "--brief", "-p"],
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
            shell=False,
        )
        subprocess_wait(proc)
        return [line.decode() for line in proc.stdout.readlines()]

    def _get_slurm_dependencies(self, task_info: SlurmTaskInfo, exclude_ids: Optional[List[int]] = None) -> List[int]:
        if exclude_ids is None:
            exclude_ids = []
        jid = task_info["id"]
        proc = subprocess.Popen(
            ["squeue", "-j", f"{jid}", "--format", "%E"],
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
            shell=False,
        )
        subprocess_wait(proc)
        cmd_out = [line.decode() for line in proc.stdout.readlines()][-1]
        re.sub("([^(]*)", "", cmd_out)
        return [dep_id for dep_id in cmd_out.split(",") if dep_id not in exclude_ids]

    def _get_job_status(self, task_info: SlurmTaskInfo) -> str:
        cmd_out = self._get_sacct_info(task_info)
        jid = str(task_info["id"])
        for line in cmd_out:
            line_list = line.split("|")
            if line_list[0] == jid:
                return line_list[1]
        err_msg = f"[{self.runner}._get_job_status] Sacct could not retrieve job {jid}. Command output:\n{cmd_out}"
        raise subprocess.SubprocessError(err_msg)

    def _convert_resources(self, resources: RunnerResources) -> List[str]:
        converted = []
        if resources.cpus is not None:
            converted += ["--cpus-per-task", str(resources.cpus)]

        if resources.gpus is not None:
            converted += ["--gpus", str(resources.gpus)]

        if resources.mem is not None:
            converted += ["--mem", str(resources.mem)]

        return converted
