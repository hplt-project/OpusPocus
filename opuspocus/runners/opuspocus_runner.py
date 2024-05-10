from typing import Any, Dict, List, Optional

import argparse
import inspect
import json
from pathlib import Path

from opuspocus.pipeline_steps import OpusPocusStep
from opuspocus.pipelines import OpusPocusPipeline

TaskId = Dict[str, Any]


class RunnerResources(object):
    """Runner-agnostic resources object.

    TODO
    """

    def __init__(
        self,
        cpus: int = 1,
        gpus: Optional[int] = None,
        mem: Optional[str] = None,
        partition: Optional[str] = None,
        account: Optional[str] = None,
    ):
        self.cpus = cpus
        self.gpus = gpus
        self.mem = mem
        self.partition = partition
        self.account = account

    @classmethod
    def list_parameters(cls) -> List[str]:
        """TODO"""
        return [
            param for param in inspect.signature(cls.__init__).parameters
            if param != 'self'
        ]

    def overwrite(self, resource_overwrite: 'RunnerResources') -> 'RunnerResources':
        params = {}
        for param in self.list_parameters():
            val = getattr(resource_overwrite, param)
            if val is None:
                val = getattr(self, param)
            params[param] = val
        return RunnerResources(**params)

    def to_json(self, json_path: Path) -> None:
        """Serialize the object (to JSON).

        TODO
        """
        json_dict = {
            param: getattr(self, param)
            for param in self.list_parameters()
        }
        json.dump(json_dict, open(json_path, 'w'), indent=2)

    @classmethod
    def from_json(cls, json_path: Path) -> 'RunnerResources':
        """TODO"""
        json_dict = json.load(open(json_path, 'r'))

        cls_params = cls.list_parameters()
        params = {}
        for k, v in json_dict.items():
            if k not in cls_params:
                logger.warn('Resource {} not supported. Ignoring'.format(k))
            params[k] = v
        return RunnerResources(**params)

    @classmethod
    def get_env_name(cls, name) -> str:
        """TODO"""
        assert name in self.list_parameters()
        return 'OPUSPOCUS_{}'.format(name)

    def get_env_dict(self) -> Dict[str, str]:
        env_dict = {}
        for param in self.list_parameters():
            param_val = getattr(self, param)
            if param_val is not None:
                env_dict[self.get_env_name(param)] = str(param_val)
        return env_dict


class OpusPocusRunner(object):
    """Base class for OpusPocus runners."""
    jobid_file = 'runner.jobid'

    @staticmethod
    def add_args(parser):
        """Add runner-specific arguments to the parser."""
        parser.add_argument(
            '--gpus', type=int, default=None,
            help='TODO'
        )
        parser.add_argument(
            '--cpus', type=int, default=1,
            help='TODO'
        )
        parser.add_argument(
            '--mem', type=str, default='1g',
            help='TODO'
        )
        parser.add_argument(
            '--partition', type=str, default=None,
            help='TODO'
        )
        parser.add_argument(
            '--account', type=str, default=None,
            help='TODO'
        )

    def __init__(
        self,
        name: str,
        args: argparse.Namespace,
    ):
        self.global_resources = RunnerResources(
            cpus = args.cpus,
            gpus = args.gpus,
            mem = args.mem,
            partition = args.partition,
            account = args.account
        )
        self.name = name

    @classmethod
    def build_runner(
        cls,
        args: argparse.Namespace,
    ) -> 'OpusPocusRunner':
        """Build a specified runner instance.

        Args:
            args (argparse.Namespace): parsed command-line arguments

        Returns:
            An instance of the specified runner class.
        """
        return cls(name, args)

    def stop_pipeline(
        self,
        pipeline: OpusPocusPipeline
    ) -> None:
        for step in pipeline.steps:
            if not step.is_running_or_submitted:
                continue
            task_ids = self.load_task_ids(step)
            if task_ids is None:
                logger.error(
                    'Step {} cannot be cancelled using {} runner because it '
                    'was submitted by a different runner type.'
                    .format(step.step_label, self.name)
                )
            for task_id in task_ids:
                logger.info(
                    'Stopping {}. Setting state to FAILED.'
                    .format(step.step_label)
                )
                self.cancel(task_id)
            step.set_state('FAILED')

    def run_pipeline(
        self,
        pipeline: OpusPocusPipeline,
        args: argparse.Namespace,
    ) -> None:
        for step in pipeline.targets:
            self.submit_step(step)
        self.run()

    def run():
        """TODO"""
        pass

    def submit_step(self, step: OpusPocusStep) -> Optional[List[TaskId]]:
        if step.has_state('RUNNING') or step.has_state('SUBMITTED'):
            task_ids = self.load_task_ids(step)
            if task_ids is None:
                logger.error(
                    'Step {} cannot be submitted because it is currently '
                    '{} using a different runner.'
                    .format(step.step_label, step.state)
                )
            return task_ids
        elif step.has_state('DONE'):
            logger.info(
                'Step {} has already finished. Skipping...'
                .format(step.step_label)
            )
            return None
        elif step.has_state('FAILED'):
            return self.resubmit_step(step)
        elif not step.has_state('INITED'):
            raise ValueError(
                'Cannot run step {}. Not in INITED state.'
                .format(step.step_label)
            )

        # recursively run step dependencies first
        dependencies_task_ids = []
        for dep in step.dependencies.values():
            if dep is None:
                continue
            dep_task_ids = self.submit_step(dep)
            if dep_task_ids is not None:
                dependencies_task_ids += dep_task_ids

        cmd_path = Path(step.step_dir, step.command_file)
        task_ids = self._submit_step(
            cmd_path,
            step.get_file_list(),
            dependencies_task_ids,
            self.get_resources(step)
        )
        self.save_task_ids(step, task_ids)
        step.set_state('SUBMITTED')

        return task_ids

    def resubmit_step(self, step: OpusPocusStep) -> Optional[List[TaskId]]:
        # TODO: add more logic here
        step.step_state('INITED')
        return self.submit_step(step)

    def _submit_step(
        cmd_path: str,
        file_list: Optional[List[str]] = None,
        dependencies: Optional[List[TaskId]] = None,
        step_resources: Optional[RunnerResources] = None,
        stdout: Optional[Path] = None,
        stderr: Optional[Path] = None
    ) -> List[TaskId]:
        """TODO"""
        raise NotImplementedError()

    def cancel(task_id: TaskId) -> None:
        raise NotImplementedError()

    def task_id_to_string(self, task_id: TaskId) -> str:
        raise NotImplementedError()

    def string_to_task_id(self, id_str: str) -> TaskId:
        raise NotImplementedError()

    def save_task_ids(
        self,
        step: OpusPocusStep,
        task_ids: List[TaskId]
    ) -> None:
        ids_str = ';'.join(
            [self.task_id_to_string(task_id) for task_id in task_ids]
        )
        logger.debug(
            'Saving Taks Ids ({}) for {} step.'.format(ids_str, step.step_label)
        )
        yaml.dump(
            {self.name : ids_str},
            open(Path(step.step_dir, self.jobid_file), 'w')
        )

    def load_task_ids(self, step: OpusPocusStep) -> Optional[List[TaskId]]:
        ids_dict = yaml.safe_load(open(step.step_dir, self.jobid_file), 'r')
        if self.name not in ids_dict:
            return None
        task_ids = [
            self.string_to_task_id(id_str)
            for id_str in ids_dict[self.name].split(';')
        ]
        return task_ids

    def get_resources(self, step: OpusPocusStep) -> RunnerResources:
        # TODO: expand the logic here
        return step.default_resources.overwrite(self.global_resources)
