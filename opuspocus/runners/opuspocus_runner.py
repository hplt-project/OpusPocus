from typing import Any, Dict, List, Optional, get_type_hints
from typing_extensions import TypedDict

from argparse import Namespace
from pathlib import Path
import inspect
import json
import logging
import sys
import yaml

from opuspocus.pipeline_steps import OpusPocusStep, StepState
from opuspocus.pipelines import OpusPocusPipeline
from opuspocus.utils import RunnerResources

logger = logging.getLogger(__name__)

TaskId = TypedDict(
    'TaskId',
    {
        'filename': str,
        'id': Any
    }
)
TaskInfo = TypedDict(
    'TaskInfo',
    {
        'runner': str,
        'main_task': TaskId,
        'subtasks': List[TaskId]
    }
)


class OpusPocusRunner(object):
    """Base class for OpusPocus runners."""
    parameter_file = 'runner.parameters'
    info_file = 'runner.task_info'
    submitted_tasks = []

    @staticmethod
    def add_args(parser):
        """Add runner-specific arguments to the parser."""

    def __init__(
        self,
        runner: str,
        pipeline_dir: Path,
        **kwargs
    ):
        self.runner = runner
        self.pipeline_dir = pipeline_dir

        self.register_parameters(**kwargs)

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

    def register_parameters(self, **kwargs) -> None:
        """TODO"""
        type_hints = get_type_hints(self.__init__)
        logger.debug('Class type hints: {}'.format(type_hints))

        for param, val in kwargs.items():
            if type_hints[param] == Path and val is not None:
                val = Path(val)
            if type_hints[param] == List[Path]:
                val = [Path(v) for v in val]
            setattr(self, param, val)

    def stop_pipeline(
        self,
        pipeline: OpusPocusPipeline
    ) -> None:
        """TODO"""
        for step in pipeline.steps:
            if not step.is_running_or_submitted:
                continue
            task_info = self.load_task_info(step)
            if task_info is None:
                raise ValueError(
                    'Step {} cannot be cancelled using {} runner because it '
                    'was submitted by a different runner type ({}).'
                    .format(step.step_label, self.runner, task_info['runner'])
                )
            logger.info(
                'Stopping {}. Setting state to FAILED.'.format(step.step_label)
            )

            for task_id in task_info['subtasks'] + [task_info['main_task']]:
                logger.debug('Stopping task {}.'.format(task_id))
                self.cancel_task(task_id)
            step.set_state(StepState.FAILED)

    def run_pipeline(
        self,
        pipeline: OpusPocusPipeline,
        targets: List[str],
    ) -> None:
        """TODO"""
        self.save_parameters()
        self.submitted_tasks = []
        for step in pipeline.get_targets(targets):
            self.submit_step(step)
        self.run()

    def run(self) -> None:
        """TODO"""
        pass

    def submit_step(self, step: OpusPocusStep) -> Optional[TaskInfo]:
        """TODO"""
        if step.is_running_or_submitted:
            task_info = self.load_task_info(step)
            if task_info is None:
                raise ValueError(
                    'Step {} cannot be submitted because it is currently '
                    '{} using a different runner ({}).'
                    .format(step.step_label, step.state, task_info['runner'])
                )
            return task_info
        elif step.has_state(StepState.DONE):
            logger.info(
                'Step {} has already finished. Skipping...'
                .format(step.step_label)
            )
            return None
        elif step.has_state(StepState.FAILED):
            step.clean_directories()
            logger.info(
                'Step {} has previously failed. '
                'Removing previous outputs and resubmitting...'
            )
        elif not step.has_state(StepState.INITED):
            raise ValueError(
                'Cannot run step {}. Step is not in INITED state.'
                .format(step.step_label)
            )

        # Recursively submit step dependencies first
        dep_task_info_list = []
        for dep in step.dependencies.values():
            if dep is None:
                continue
            dep_task_info = self.submit_step(dep)
            if dep_task_info is not None:
                dep_task_info_list.append(dep_task_info)

        cmd_path = Path(step.step_dir, step.command_file)

        # Submit the main step task (which eventually can submit subtasks)
        logger.info('[{}] Submitting main step task.'.format(step.step_label))
        task_id = self.submit_task(
            cmd_path=cmd_path,
            target_file=None,
            dependencies=[dep['main_task'] for dep in dep_task_info_list],
            step_resources=self.get_resources(step),
            stdout_file=Path(step.log_dir, '{}.out'.format(self.runner)),
            stderr_file=Path(step.log_dir, '{}.err'.format(self.runner)),
        )
        task_info = TaskInfo(
            runner=self.runner,
            main_task=task_id,
            subtasks=[]
        )
        self.save_task_info(step, task_info)
        step.set_state(StepState.SUBMITTED)

        self.submitted_tasks.append(task_info)
        return task_info

    def resubmit_step(self, step: OpusPocusStep) -> Optional[TaskId]:
        """TODO"""
        # Cancel the original job if running
        if step.is_running_or_submitted:
            task_info = self.load_task_info(step)
            if task_info is None:
                raise ValueError(
                    'Step {} cannot be cancelled using {} runner because it '
                    'was submitted by a different runner type ({}).'
                    .format(step.step_label, self.runner, task_info['runner'])
                )

            logger.info(
                'Stopping {}. Setting state to FAILED.'
                .format(step.step_label)
            )
            step.set_state(StepState.FAILED)

            task_ids = task_info['subtasks'] + [task_info['main_task']]
            for task_id in task_ids:
                self.cancel(task_id)

        # Submit the job again
        task_id = self.submit_step(step)
        self.update_dependants(task_id)

        return task_id

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
        raise NotImplementedError()

    def update_dependants(self, task_id: TaskId) -> None:
        raise NotImplementedError()

    def cancel_task(task_id: TaskId) -> None:
        """TODO"""
        raise NotImplementedError()

    def wait_for_all_tasks(self, task_ids: List[TaskId]) -> None:
        for task_id in task_ids:
            self.wait_for_task(task_id)

    def wait_for_task(self, task_id: TaskId) -> None:
        raise NotImplementedError()

    def is_task_running(self, task_id: TaskInfo) -> bool:
        """TODO"""
        raise NotImplementedError()

    def save_task_info(
        self,
        step: OpusPocusStep,
        task_info: TaskInfo
    ) -> None:
        """TODO"""
        yaml.dump(task_info, open(Path(step.step_dir, self.info_file), 'w'))

    def load_task_info(self, step: OpusPocusStep) -> Optional[TaskInfo]:
        """TODO"""
        task_info = yaml.safe_load(
            open(Path(step.step_dir, self.info_file), 'r')
        )
        if task_info['runner'] != self.runner:
            return None
        return task_info

    def get_resources(self, step: OpusPocusStep) -> RunnerResources:
        """TODO"""
        # TODO: expand the logic here
        return step.default_resources