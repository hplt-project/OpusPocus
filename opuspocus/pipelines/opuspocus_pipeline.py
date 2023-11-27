from typing import Dict, List

import argparse
import logging
import yaml
from pathlib import Path


logger = logging.getLogger(__name__)
STEP_T = Dict[str, OpusPocusStep]
TARGET_T = List[OpusPocusStep]

class OpusPocusPipeline(object):
    step_file = 'pipeline.steps'
    target_file = 'pipeline.targets'
    variables_file = 'pipeline.variables'

    @staticmethod
    def add_args(parser):
        pass

    def __init__(
        self,
        pipeline: str,
        args: argparse.Namespace,
        steps: STEP_T = None,
        targets: TARGET_T = None,
    ):
        self.pipeline = pipeline
        self.pipeline_dir = args.pipeline_dir

        if steps is not None or targets is not None:
            self.steps = steps
            self.targets = targets
        else:
            assert steps is None and targets is None
            self.steps, self.targets = self.build_pipeline_graph(args, targets)

    @classmethod
    def build_pipeline(
        cls, pipeline: str, args: argparse.Namespace
    ) -> OpusPocusPipeline:
        """Build a specified pipeline instance.

        Args:
            args (argparse.Namespace): parsed command-line arguments
        """
        return cls(pipeline, args)

    @classmethod
    def load_variables(
        cls, args: argparse.Namespace
    ) -> Tuple[STATE_T, TARGET_T, Dict[str, Any]]:
        pipeline_dir = args.pipeline_dir

        step_path = Path(pipeline_dir, cls.step_file)
        logger.debug('Loading pipeline steps from {}'.format(step_path))
        step_names = yaml.load(open(step_path, 'r'))

        steps = {}
        for pipeline_key, step_name in steps_names.items():
            steps[k] = build_step(step_name, args)

        target_path = Path(pipeline_dir, cls.target_file)
        logger.debug('Loading pipeline targets from {}'.format(target_path))
        target_keys = yaml.load(open(target_path, 'r'))

        targets = []
        for k in target_keys:
            targets.append(steps[k])

        vars_path = Path(pipeline_dir, cls.variables_file)
        logger.debug('Loading pipeline variables from {}'.format(vars_path))
        vars_dict = yaml.load(open(vars_path, 'r'))

        return steps, targets, vars_dict

    def save_pipeline(self):
        step_names = {key, step.step_name for key, step in self.steps.items()}
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


    def build_pipeline_graph(self) -> Tuple[STATE_T, TARGET_T]:
        raise NotImplementedError()

    def init(self):
        for _, v in self.steps():
            v.init_step()
        self.save_pipeline()

    def run(self, args):
        for _, v in self.steps():
            v.run_step(args)

    def traceback(self):
        for i, v in enumerate(self.targets()):
            print('Target {}: {}'.format(i, v.step_name))
            v.traceback_step(level=0)
            print('')
