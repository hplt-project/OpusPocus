from typing import Dict, List, Optional

import argparse
import hyperqueue
import subprocess
from pathlib import Path

from opuspocus.runners import (
    OpusPocusRunner,
    TaskId,
    register_runner
)
from opuspocus.utils import file_path, RunnerResources


@register_runner('hyperqueue')
class HyperqueueRunner(OpusPocusRunner):
    """TODO"""

    @staticmethod
    def add_args(parser):
        """Add runner-specific arguments to the parser."""
        parser.add_argument(
            '--hq-server-dir', type=file_path, default='opuspocus_hq_server',
            help='TODO'
        )
        parser.add_argument(
            '--hq-path', type=file_path, default='hyperqueue/hq',
            help='TODO'
        )
        parser.add_argument(
            '--hq-scheduler', type=str, choices=['slurm'], default='slurm',
            help='TODO'
        )
        parser.add_argument(
            '--hq-alloc-time-limit', type=str, default='24h',
            help='TODO'
        )
        parser.add_argument(
            '--hq-alloc-backlog', type=int, default=1,
            help='TODO'
        )
        parser.add_argument(
            '--hq-alloc-range-cpus', type=str, default='0,1',
            help='TODO'
        )
        parser.add_argument(
            '--hq-alloc-range-gpus', type=str, default=None,
            help='TODO'
        )
        parser.add_argument(
            '--hq-max-worker-count', type=int, default=1,
            help='TODO'
        )


    def __init__(
        self,
        runner: str,
        args: argparse.Namespace,
    ):
        super().__init__(runner, args)

        # TODO: args values checking
        self.hq_server_dir = args.hq_server_dir
        self.hq_path = args.hq_path
        self.hq_scheduler = args.hq_scheduler

        self.hq_alloc_time_limit = args.hq_alloc_time_limit
        self.hq_alloc_backlog = args.hq_alloc_backlog

        self.hq_alloc_range_cpus = args.hq_alloc_range_cpus.split(',')
        assert len(self.hq_alloc_range_cpus) == 2

        self.hq_alloc_range_gpus = None
        if args.hq_alloc_range_gpus is not None:
            self.hq_alloc_range_cpus = args.hq_alloc_range_gpus.split(',')
            assert len(self.hq_alloc_range_gpus) == 2

        self.hq_max_worker_count = args.hq_max_worker_count

        # Start the HQ server (if not running)
        # TODO: replace the launcher script with a better alternative
        subprocess.run(
            [
                'scripts/launch_hq_server.sh',
                str(self.hq_server_dir),
            ],
            shell=False
        )

        # Create the client
        self.client = hyperqueue.Client(self.hq_server_dir)

        # Initialize the job
        self.job = hyperqueue.Job()

    def _submit_step(
        cmd_path: Path,
        file_list: Optional[List[str]] = None,
        dependencies: Optional[List[TaskId]] = None,
        step_resources: Optional[RunnerResources] = None
    ) -> List[TaskId]:
        """TODO"""
        dep_tasks = []
        if dependencies:
            dep_tasks = [dep['hq_task'] for dep in dependencies]

        # Prepare the ENV
        env_dict = resources.get_env_dict()

        # Prepare the resource request
        res_request = hyperqueue.ResourceRequest(
            cpus=(step_resources.cpus if step_resources.cpus is not None else 1),
            resources=self.convert_resources(step_resources)
        )

        if file_list is not None:
            return [
                {
                    'task_id' : self.job.program(
                        [cmd_path, p],
                        env=env_dict,
                        resources=res_request,
                        name='{}.{}'.format(job_name, p),
                        stdout='{}/logs/{}.hq.log'.format(job_dir, p),
                        stderr='{}/logs/{}.hq.log'.format(job_dir, p),
                        deps=dep_tasks
                    ).task_id
                } for p in param_list
            ]
        return [{
            'task_id': self.job.program(
                [cmd_path],
                env=env_dict,
                resources=res_request,
                name='{}'.format(job_name),
                stdout='{}/logs/hq.log'.format(job_dir),
                stderr='{}/logs/hq.log'.format(job_dir),
                deps=dep_tasks
            ).task_id
        }]

    def cancel(task_id: TaskId) -> None:
        # TODO: Based on this implementation, we also need to adjust the
        # task_id saving/loading methods
        raise NotImplementedError()

    def run():
        """TODO"""
        # Add an automatic allocation queue
        # TODO: ideally, the allocation queue should be removed after
        #       the pipeline execution ends
        # TODO: separate queue CPU and GPU computation queues?
        hq_cmd = [
            self.hq_path,
            'alloc', 'add', self.hq_scheduler,
            '--server-dir={}'.format(self.hq_server_dir),
            '--time-limit={}'.format(self.hq_alloc_time_limit),
            '--backlog={}'.format(self.hq_alloc_backlog),
            '--max-worker-count={}'.format(self.hq_max_worker_count)
        ]
        hq_cmd += [
            '--resource',
            'cpus=range(0,{})'.format(self.hq_alloc_range_cpus)
        ]

        if self.hq_alloc_range_gpus is not None:
            hq_cmd += [
                '--resource',
                'gpus=range({})'.format(self.hq_alloc_range_gpus)
            ]

        hq_cmd += ['--']
        if self.partition is not None:
            hq_cmd += ['--partition={}'.format(self.partition)]

        if self.account is not None:
            hq_cmd += ['--account={}'.format(self.account)]

        subprocess.run(hq_cmd)

        # TODO: info about the alloc queue
        client.submit(self.job)

    def convert_resources(resources: RunnerResources) -> Dict[str, str]:
        res_dict = {}
        if resources.cpus is not None:
            res_dict['cpus'] = resources.cpus

        if resources.gpus is not None:
            res_dict['gpus'] = resources.gpus

        if resources.mem is not None:
            res_dict['mem'] = self.convert_memory(resources.mem)

    def convert_memory(self, mem: str) -> int:
        unit = mem[-1]
        if unit == 'g' or unit == 'G':
            return int(mem[:-1] * 1024 ** 3)
        if unit == 'm' or unit == 'M':
            return int(mem[:-1] * 1024 ** 2)
        if unit == 'k' or unit == 'K':
            return int(mem[:-1] * 1024)
        raise ValueError('Unknown unit of memory ({}).'.format(unit))

    def task_id_to_string(self, task_id: TaskId) -> str:
        tid = task_id['task_id']
        jid = task_id['job_id']
        if jid is not None:
            return '{},{}'.format(jid, tid)
        return 'X,{}'.format(task_id)

    def string_to_task_id(self, id_str: str) -> TaskId:
        jid, tid = id_str.split(',')
        return {'task_id': tid, 'job_id': jid}
