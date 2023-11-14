from typing import Dict

import logging
import yaml
from pathlib import Path


STEP_STATES = ['INITED', 'RUNNING', 'FAILED', 'DONE']

logger = logging.getLogger(__name__)


def get_hash():
    # TODO: proper hashing
    return 'debug'


# TODO: instead of step parameter in __init__,
# define a abstract step class attribute that is then extracted
# to create the step->DerivedClassStep mapping for the create_step
# method


class OpusPocusStep(object):
    command_file = 'step.command'
    dependencies_file = 'step.dependencies'
    jobid_file = 'step.jobid'
    state_file = 'step.state'
    variables_file = 'step.variables'

    @staticmethod
    def add_args(parser):
        pass

    def __init__(
        self,
        step,
        pipeline_dir,
        **kwargs
    ):
        self.step = step
        self.pipeline_dir = pipeline_dir
        for k, v in kwargs:
            self.__dict__[k] = v

        if not hasattr(self, 'pipeline_dir'):
            self.pipeline_dir = Path(self.pipeline_dir)

        if not hasattr(self, 'step_dir'):
            self.step_dir = Path(self.pipeline_dir, self.step_name)

        # init dependencies
        #self.dependencies = self.build_dependencies(args)

        self.state = self.load_state()
        if self.state is not None:
            # Implies the existence of the step_dir
            self.load_state()

    @classmethod
    def build_step(cls, step, pipeline_dir, **kwargs):
        """Build a specified step instance.

        Args:
            args (argparse.Namespace): parsed command-line arguments
        """
        return cls(step, pipeline_dir, **kwargs)

    @classmethod
    def load_variables(cls, step_name, pipeline_dir):
        """Load existing step."""

        vars_path = Path(pipeline_dir, step_name, cls.variables_file)
        logger.debug('Loading variables from {}'.format(vars_path))
        return yaml.load(open(vars_path, 'r'))

    def build_dependencies(self):
        raise NotImplementedError()

    @property
    def step_name(self):
        raise NotImplementedError()

    def init_step(self):
        if self.state is not None:
            if self.has_state('INITED'):
                logger.info('Step already initialized. Skipping...')
                return
            else:
                raise ValueError(
                    'Trying to initialize step in a {} state.'.format(self.state)
                )

        self.init_dependencies()
        self.create_step_dir()
        self.save_variables()
        self.create_command()

        # initialize state
        logger.info('[{}.init] Step Initialized.'.format(self.step))
        self.set_state('INITED')

    def init_dependencies(self):
        # TODO: improve the dependency representation and implement a
        # child-agnostic init deps method
        logger.warn('init_dependencies() method is not currently implemented.')
        pass
        #raise NotImplementedError()

    def save_dependencies(self):
        logger.warn('save_dependencies() method is not currently implemented.')
        pass

    def load_dependencies(self):
        logger.warn('load_dependencies() method is not currently implemented.')
        pass

    def create_step_dir(self):
        # create step dir
        logger.debug('Creating step dir.')
        if self.step_dir.is_dir():
            raise ValueError(
                'Cannot create {}. Directory already exists.'
                .format(self.step_dir)
            )
        self.step_dir.mkdir(parents=True)

    def save_variables(self):
        # TODO: do we use a different way for saving?
        logger.debug('Saving step variables.')
        vars_dict = {}
        for k, v in self.__dict__.items():
            # TODO: other variable exceptions
            if "__" in k:
                continue
            if k == "state":
                continue
            if isinstance(v, OpusPocusStep):
                # do not save dependency objects
                continue
            if isinstance(v, Path):
                v = str(v)
            vars_dict[k] = v
        yaml.dump(vars_dict, open(Path(self.step_dir, self.variables_file), 'w'))

    def create_command(self):
        # TODO: add start-end command boilerplate, slurm-related (or other)
        cmd_path = Path(self.step_dir, self.command_file)
        if cmd_path.exists():
            raise ValueError('File {} already exists.'.format(cmd_path))

        logger.debug('Creating step command.')
        print(self.get_command_str(), file=open(cmd_path, 'w'))

    def get_command_str(self):
        raise NotImplementedError()

    def run_step(cls):
        # TODO: logic for rerunning/overriding failed/running steps
        if not self.has_state('INITED'):
            raise ValueError(
                'Cannot run step. Not in INITED state.'.format(self.step)
            )
        import subprocess
        cmd_path = Path(self.step_dir, self.command_file)
        subprocess.run(['bash', self.cmd_path])

    def traceback_step(cls):
        pass

    def init_dependencies(self):
        pass

    def load_state(self):
        state_file = Path(self.step_dir, self.state_file)
        if state_file.exists():
            state = open(Path(self.step_dir, self.state_file), "r").readline().strip()
            assert state in STEP_STATES
            return state
        return None

    def set_state(self, state: str):
        """Whenever we change the Step state we also need to update the state
        file for the purpose of recovery failure."""
        assert state in STEP_STATES
        logger.debug("Old state: {} -> New state: {}".format(self.state, state))
        self.state = state
        print(state, file=open(Path(self.step_dir, self.state_file), 'w'))

    def has_state(self, state: str):
        if self.state is not None and self.state == state:
            return True
        return False
