import subprocess
from pathlib import Path
from typing import Dict, List, Optional

import hyperqueue

from opuspocus.runners import OpusPocusRunner, TaskId, register_runner
from opuspocus.utils import RunnerResources, file_path, subprocess_wait


@register_runner("hyperqueue")
class HyperqueueRunner(OpusPocusRunner):
    """TODO"""

    @staticmethod
    def add_args(parser):  # noqa: ANN001, ANN205
        OpusPocusRunner.add_args(parser)
        parser.add_argument(
            "--hq-server-dir",
            type=file_path,
            default="opuspocus_hq_server",
            help="TODO",
        )
        parser.add_argument("--hq-path", type=file_path, default="hyperqueue/bin/hq", help="TODO")
        parser.add_argument("--hq-scheduler", type=str, choices=["slurm"], default="slurm", help="TODO")
        parser.add_argument("--hq-alloc-time-limit", type=str, default="24h", help="TODO")
        parser.add_argument("--hq-alloc-backlog", type=int, default=1, help="TODO")
        parser.add_argument("--hq-alloc-range-cpus", type=str, default="0,1", help="TODO")
        parser.add_argument("--hq-alloc-range-gpus", type=str, default=None, help="TODO")
        parser.add_argument("--hq-max-worker-count", type=int, default=1, help="TODO")

    def __init__(
        self,
        runner: str,
        pipeline_dir: Path,
        hq_server_dir: Path = "opuspocus_hq_server",
        hq_path: Path = "./hq",
        hq_scheduler: str = "slurm",
        hq_alloc_time_limit: str = "24h",
        hq_alloc_backlog: int = 1,
        hq_alloc_range_cpus: str = "0,1",
        hq_alloc_range_gpus: Optional[str] = None,
        hq_max_worker_count: int = 1,
    ) -> None:
        hq_alloc_range_cpus = [int(n) for n in hq_alloc_range_cpus.split(",")]
        if hq_alloc_range_gpus is not None:
            hq_alloc_range_gpus = [int(n) for n in hq_alloc_range_gpus.split(",")]
        super().__init__(
            runner=runner,
            pipeline_dir=pipeline_dir,
            hq_server_dir=hq_server_dir,
            hq_path=hq_path,
            hq_scheduler=hq_scheduler,
            hq_alloc_time_limit=hq_alloc_time_limit,
            hq_alloc_backlog=hq_alloc_backlog,
            hq_alloc_range_cpus=hq_alloc_range_cpus,
            hq_alloc_range_gpus=hq_alloc_range_gpus,
            hq_max_worker_count=hq_max_worker_count,
        )
        # TODO: Better typing and value checking
        assert len(self.hq_alloc_range_cpus) == 2  # noqa: PLR2004
        if self.hq_alloc_range_gpus is not None:
            assert len(self.hq_alloc_range_gpus) == 2  # noqa: PLR2004

        # Start the HQ server (if not running)
        # TODO: replace the launcher script with a better alternative
        proc = subprocess.run(
            [
                "scripts/launch_hq_server.sh",
                str(self.hq_server_dir),
            ],
            shell=False,
            check=False,
        )
        subprocess_wait(proc)

        # Create the client
        self.client = hyperqueue.Client(self.hq_server_dir)

        # Initialize the job
        self.job = hyperqueue.Job()

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
        dep_tasks = []
        if dependencies:
            dep_tasks = [dep["hq_task"] for dep in dependencies]

        # Prepare the ENV
        env_dict = step_resources.get_env_dict()

        # Prepare the resource request
        res_request = hyperqueue.ResourceRequest(
            cpus=(step_resources.cpus if step_resources.cpus is not None else 1),
            resources=self._convert_resources(step_resources),
        )

        jobname = "XXX"  # TODO: fix this

        if target_file is not None:
            prog = self.job.program(
                [str(cmd_path), str(target_file)],
                env=env_dict,
                resources=res_request,
                name=jobname,
                stdout=(str(stdout_file) if stdout_file is not None else None),
                stderr=(str(stderr_file) if stderr_file is not None else None),
                deps=dep_tasks,
            )
        else:
            prog = self.job.program(
                [str(cmd_path)],
                env=env_dict,
                resources=res_request,
                name=jobname,
                stdout=(str(stdout_file) if stdout_file is not None else None),
                stderr=(str(stderr_file) if stderr_file is not None else None),
                deps=dep_tasks,
            )

        return TaskId(filename=str(target_file), id=prog.task_id)

    def update_dependants(self, task_id: TaskId) -> None:
        raise NotImplementedError()

    def cancel_task(self, task_id: TaskId) -> None:
        # TODO: Based on this implementation, we also need to adjust the
        # task_id saving/loading methods
        raise NotImplementedError()

    def wait_for_single_task(self, task_id: TaskId) -> None:
        raise NotImplementedError()

    def is_task_running(self, task_id: TaskId) -> bool:
        raise NotImplementedError()

    def run(self) -> None:
        """TODO"""
        # Add an automatic allocation queue
        # TODO: ideally, the allocation queue should be removed after
        #       the pipeline execution ends
        # TODO: separate queue CPU and GPU computation queues?
        hq_cmd = [
            self.hq_path,
            "alloc",
            "add",
            self.hq_scheduler,
            f"--server-dir={self.hq_server_dir}",
            f"--time-limit={self.hq_alloc_time_limit}",
            f"--backlog={self.hq_alloc_backlog}",
            f"--max-worker-count={self.hq_max_worker_count}",
        ]
        hq_cmd += ["--resource", f"cpus=range(0,{self.hq_alloc_range_cpus})"]

        if self.hq_alloc_range_gpus is not None:
            hq_cmd += ["--resource", f"gpus=range({self.hq_alloc_range_gpus})"]

        hq_cmd += ["--"]
        if self.partition is not None:
            hq_cmd += [f"--partition={self.partition}"]

        if self.account is not None:
            hq_cmd += [f"--account={self.account}"]

        subprocess.run(hq_cmd, check=False)

        # TODO: info about the alloc queue
        self.client.submit(self.job)

    def _convert_resources(self, resources: RunnerResources) -> Dict[str, str]:
        res_dict = {}
        if resources.cpus is not None:
            res_dict["cpus"] = resources.cpus

        if resources.gpus is not None:
            res_dict["gpus"] = resources.gpus

        if resources.mem is not None:
            res_dict["mem"] = self.convert_memory(resources.mem)

    def _convert_memory(self, mem: str) -> int:
        unit = mem[-1]
        if unit == "g" or unit == "G":  # noqa: PLR1714
            return int(mem[:-1] * 1024**3)
        if unit == "m" or unit == "M":  # noqa: PLR1714
            return int(mem[:-1] * 1024**2)
        if unit == "k" or unit == "K":  # noqa: PLR1714
            return int(mem[:-1] * 1024)
        raise ValueError(f"Unknown unit of memory ({unit}).")  # noqa: EM102
