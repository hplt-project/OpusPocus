from typing import Any, Dict, List

import argparse
import logging
import yaml
from pathlib import Path

from opuspocus.pipeline_steps import load_step

logger = logging.getLogger(__name__)


class OpusPocusPipeline(object):
    '''Base class for OpusPocus pipelines.'''

    step_file = 'pipeline.steps'
    target_file = 'pipeline.targets'
    variables_file = 'pipeline.variables'

    @staticmethod
    def add_args(parser):
        '''Add pipeline-specific arguments to the parser.'''
        pass

    def __init__(
        self,
        pipeline: str,
        args: argparse.Namespace,
        steps = None,
        targets = None,
    ):
        self.pipeline = pipeline
        self.pipeline_dir = args.pipeline_dir

        if steps is not None or targets is not None:
            self.steps = steps
            self.targets = targets
        else:
            assert steps is None and targets is None
            self.steps, self.targets = self.build_pipeline_graph(args)

    @classmethod
    def build_pipeline(
        cls,
        pipeline: str,
        args: argparse.Namespace,
        steps = None,
        targets = None
    ):
        """Build a specified pipeline instance.

        Args:
            pipeline (str): pipeline class name in the register
            args (argparse.Namespace): parsed command-line arguments
            steps: pipeline steps (used for loading)
            targets: pipeline targets (used for loading)

        Retuns:
            An instance of the specified pipeline class.
        """
        return cls(pipeline, args, steps, targets)

    @classmethod
    def load_variables(cls, args: argparse.Namespace):
        """Load the parameters of an existing pipeline.

        Args:
            args (argparse.Namespace): parsed command-line arguments

        Returns:
            A tuple of pipeline step DAG (Dict), pipeline targets (List)
            and pipeline parameters (Dict).
        """

        pipeline_dir = args.pipeline_dir

        step_path = Path(pipeline_dir, cls.step_file)
        logger.debug('Loading pipeline steps from {}'.format(step_path))
        step_names = yaml.safe_load(open(step_path, 'r'))

        steps = {}
        for pipeline_key, step_name in step_names.items():
            steps[pipeline_key] = load_step(step_name, args)

        target_path = Path(pipeline_dir, cls.target_file)
        logger.debug('Loading pipeline targets from {}'.format(target_path))
        target_keys = yaml.safe_load(open(target_path, 'r'))

        targets = []
        for step_name in target_keys:
            targets.append(load_step(step_name, args))

        vars_path = Path(pipeline_dir, cls.variables_file)
        logger.debug('Loading pipeline variables from {}'.format(vars_path))
        vars_dict = yaml.safe_load(open(vars_path, 'r'))

        return steps, targets, vars_dict

    def save_pipeline(self):
        """Save the pipeline information.

        Saves the dependency structure of the pipeline steps, pipeline target
        steps and pipeline parameters in their respective YAML files.
        """
        step_names = {key: step.step_name for key, step in self.steps.items()}
        yaml.dump(
            step_names,
            open(Path(self.pipeline_dir, self.step_file), 'w')
        )

        target_names = [step.step_name for step in self.targets]
        yaml.dump(
            target_names,
            open(Path(self.pipeline_dir, self.target_file), 'w')
        )

        yaml.dump(
            self.get_variables(),
            open(Path(self.pipeline_dir, self.variables_file), 'w')
        )

    def get_variables(self) -> Dict[str, Any]:
        """Infer the pipeline parameters from the instantiated pipeline object.

        """
        vars_dict = {}
        for k,v in self.__dict__.items():
            if '__' in k:
                continue
            if k == 'steps':
                continue
            if k == 'targets':
                continue
            if isinstance(v, Path):
                v = str(v)
            vars_dict[k] = v
        return vars_dict


    def build_pipeline_graph(self, args: argparse.Namespace):
        """Abstract pipeline construction method.

        Each pipeline class implementation is required to specify the process
        of instantiating the individual pipeline steps and the dependencies
        between these steps.
        """
        raise NotImplementedError()

    def init(self):
        """Initialize the pipeline."""
        for _, v in self.steps.items():
            v.init_step()
        self.save_pipeline()

    def run(self, args):
        """Execute the pipeline."""
        for _, v in self.steps.items():
            v.run_step(args)

    def traceback(self, full: bool = False):
        """Print the pipeline structure and status of the individual steps."""
        for i, v in enumerate(self.targets):
            print('Target {}: {}'.format(i, v.step_name))
            v.traceback_step(level=0, full=full)
            print('')
