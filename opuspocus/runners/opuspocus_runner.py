from typing import Any, Dict, List, Optional

from argparse import Namespace
import inspect
import logging
import sys
import yaml
from pathlib import Path

from opuspocus.pipeline_steps import OpusPocusStep, StepState
from opuspocus.pipelines import OpusPocusPipeline
from opuspocus.utils import RunnerResources

logger = logging.getLogger(__name__)

TaskId = Dict[str, Any]


class OpusPocusRunner(object):
    """Base class for OpusPocus runners."""
    parameter_file = 'runner.parameters'
    jobid_file = 'runner.jobid'

    @staticmethod
    def add_args(parser):
        """Add runner-specific arguments to the parser."""
        pass

    def __init__(
        self,
        runner: str,
        pipeline_dir: Path,
        **kwargs
    ):
        self.runner = runner
        self.pipeline_dir = pipeline_dir

        # TODO: properly include global resource request override
        self.global_resources = RunnerResources()

    @classmethod
    def build_runner(
        cls,
        runner: str,
        pipeline_dir: Path,
        **kwargs
    ) -> 'OpusPocusRunner':
        """Build a specified runner instance.

        Args:
            runner (str): TODO
            pipeline_dir (Path): TODO
            args (Namespace): parsed command-line arguments

        Returns:
            An instance of the specified runner class.
        """
        return cls(runner, pipeline_dir, **kwargs)

    @classmethod
    def list_parameters(cls) -> List[str]:
        """TODO"""
        param_list = []
        for param in inspect.signature(cls.__init__).parameters:
            if param == 'self':
                continue
            param_list.append(param)
        return param_list

    @classmethod
    def load_parameters(
        cls,
        pipeline_dir: Path
    ) -> Dict[str, Any]:
        """TODO"""
        params_path = Path(pipeline_dir, cls.parameter_file)
        logger.debug('Loading step variables from {}'.format(params_path))

        params_dict = yaml.safe_load(open(params_path, 'r'))
        return params_dict

    def get_parameters_dict(self) -> Dict[str, Any]:
        """TODO"""
        param_dict = {}
        for param in self.list_parameters():
            p = getattr(self, param)
            if isinstance(p, Path):
                p = str(p)
            if isinstance(p, list) and isinstance(p[0], Path):
                p = [str(v) for v in p]
            param_dict[param] = p
        return param_dict

    def save_parameters(self) -> None:
        """TODO"""
        yaml.dump(
            self.get_parameters_dict(),
            open(Path(self.pipeline_dir, self.parameter_file), 'w')
        )

    def stop_pipeline(
        self,
        pipeline: OpusPocusPipeline
    ) -> None:
        for step in pipeline.steps:
            if not step.is_running_or_submitted:
                continue
            task_ids = self.load_task_ids(step)
            if task_ids is None:
                raise ValueError(
                    'Step {} cannot be cancelled using {} runner because it '
                    'was submitted by a different runner type.'
                    .format(step.step_label, self.runner)
                )
            for task_id in task_ids:
                logger.info(
                    'Stopping {}. Setting state to FAILED.'
                    .format(step.step_label)
                )
                self.cancel(task_id)
            step.set_state(StepState.FAILED)

    def run_pipeline(
        self,
        pipeline: OpusPocusPipeline,
        targets: List[str],
    ) -> None:
        self.save_parameters()
        for step in pipeline.get_targets(targets):
            self.submit_step(step)
        self.run()

    def run(self) -> None:
        """TODO"""
        pass

    def submit_step(self, step: OpusPocusStep) -> Optional[List[TaskId]]:
        if step.is_running_or_submitted():
            task_ids = self.load_task_ids(step)
            if task_ids is None:
                raise ValueError(
                    'Step {} cannot be submitted because it is currently '
                    '{} using a different runner.'
                    .format(step.step_label, step.state)
                )
            return task_ids
        elif step.has_state(StepState.DONE):
            logger.info(
                'Step {} has already finished. Skipping...'
                .format(step.step_label)
            )
            return None
        elif step.has_state(StepState.FAILED):
            return self.resubmit_step(step)
        elif not step.has_state(StepState.INITED):
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
            cmd_path=cmd_path,
            file_list=step.get_input_file_list(),
            dependencies=dependencies_task_ids,
            step_resources=self.get_resources(step),
            stdout=open(Path(step.log_dir, '{}.out'.format(self.runner)), 'w'),
            stderr=open(Path(step.log_dir, '{}.err'.format(self.runner)), 'w'),
        )
        self.save_task_ids(step, task_ids)
        step.set_state(StepState.SUBMITTED)

        return task_ids

    def resubmit_step(self, step: OpusPocusStep) -> Optional[List[TaskId]]:
        # Cancel the original job
        if step.is_running_or_submitted:
            task_ids = self.load_task_ids(step)
            if task_ids is None:
                raise ValueError(
                    'Step {} cannot be cancelled using {} runner because it '
                    'was submitted by a different runner type.'
                    .format(step.step_label, self.runner)
                )
            for task_id in task_ids:
                logger.info(
                    'Stopping {}. Setting state to FAILED.'
                    .format(step.step_label)
                )
                self.cancel(task_id)
        step.step_state(StepState.INITED)

        # Submit the job again
        return self.submit_step(step)

    def _submit_step(
        self,
        cmd_path: Path,
        file_list: Optional[List[Path]] = None,
        dependencies: Optional[List[TaskId]] = None,
        step_resources: Optional[RunnerResources] = None,
        stdout=sys.stdout,
        stderr=sys.stderr
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
            {self.runner : ids_str},
            open(Path(step.step_dir, self.jobid_file), 'w')
        )

    def load_task_ids(self, step: OpusPocusStep) -> Optional[List[TaskId]]:
        ids_dict = yaml.safe_load(
            open(Path(step.step_dir, self.jobid_file), 'r')
        )
        if self.runner not in ids_dict:
            return None
        task_ids = [
            self.string_to_task_id(id_str)
            for id_str in ids_dict[self.runner].split(';')
        ]
        return task_ids

    def get_resources(self, step: OpusPocusStep) -> RunnerResources:
        # TODO: expand the logic here
        return step.default_resources.overwrite(self.global_resources)
