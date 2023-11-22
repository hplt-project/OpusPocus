from typing import Dict

import logging
import yaml
from pathlib import Path

from utils import print_indented
from command_utils import build_subprocess


STEP_STATES = ['INITED', 'RUNNING', 'FAILED', 'DONE']

logger = logging.getLogger(__name__)


def get_hash():
    # TODO: proper hashing
    return 'debug'


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
        args,
        **kwargs
    ):
        """
        TODO: the derived classes should add attributes+logic for optional
        step dependencies.
        TODO: can we do it in a smarter way?
        """
        self.step = step
        self.pipeline_dir = args.pipeline_dir
        self.dependencies = {}
        self.state = self.load_state()

    @classmethod
    def build_step(cls, step, args, **kwargs):
        """Build a specified step instance.

        Args:
            args (argparse.Namespace): parsed command-line arguments
        """
        return cls(step, args, **kwargs)

    @classmethod
    def load_variables(cls, step_name, pipeline_dir):
        """Load existing step."""
        vars_path = Path(pipeline_dir, step_name, cls.variables_file)
        logger.debug('Loading variables from {}'.format(vars_path))
        return yaml.load(open(vars_path, 'r'))

    @classmethod
    def load_dependencies(cls, step_name, pipeline_dir):
        """Load step dependecies (directories)."""
        deps_path = Path(pipeline_dir, step_name, cls.dependencies_file)
        logger.debug('Loading dependencies from {}'.format(deps_path))
        return yaml.load(open(deps_path, 'r'))

    @property
    def step_name(self):
        """
        We can have multiple instances of a step with different
        parametrization. Must be implemented by the derived step classes.
        """
        raise NotImplementedError()

    @property
    def step_dir(self):
        return Path(self.pipeline_dir, self.step_name)

    def init_step(self):
        if self.state is not None:
            if self.has_state('INITED'):
                logger.info('Step already initialized. Skipping...')
                return
            else:
                raise ValueError(
                    'Trying to initialize step in a {} state.'.format(self.state)
                )

        self.create_step_dir()
        self.init_dependencies()
        #self._init_step()  # copying/linking files/dirs from dependencies
        self.save_variables()
        self.save_dependencies()
        self.create_command()

        # initialize state
        logger.info('[{}.init] Step Initialized.'.format(self.step))
        self.set_state('INITED')

    #def _add_dependency(self, key: str, val: OpusPocusStep):
    #    if key in self.dependencies:
    #        raise ValueError(
    #            'Duplicate dependency {}: {}'.format(key, val.__name__)
    #        )
    #    self.dependencies[key] = val

    def init_dependencies(self):
        # TODO: improve the dependency representation and implement a
        # child-agnostic init deps method
        for dep in self.dependencies.values():
            if not dep.has_state('INITED'):
                dep.init_step()

    def save_dependencies(self):
        deps_dict = {k: v.step_name for k, v in self.dependencies.items()}
        yaml.dump(
            deps_dict,
            open(Path(self.step_dir, self.dependencies_file), 'w')
        )

    def create_step_dir(self):
        # create step dir
        logger.debug('Creating step dir.')
        if self.step_dir.is_dir():
            raise ValueError(
                'Cannot create {}. Directory already exists.'
                .format(self.step_dir)
            )
        self.step_dir.mkdir(parents=True)

    def get_variables(self):
        vars_dict = {}
        for k, v in self.__dict__.items():
            # TODO: other variable exceptions
            if "__" in k:
                continue
            if k == "state":
                continue
            if k == "dependencies":
                continue
            #if isinstance(v, OpusPocusStep):
                # do not save dependency objects
            #    continue
            if isinstance(v, Path):
                v = str(v)
            vars_dict[k] = v
        return vars_dict

    def save_variables(self):
        # TODO: do we use a different way for saving?
        logger.debug('Saving step variables.')
        yaml.dump(
            self.get_variables(),
            open(Path(self.step_dir, self.variables_file), 'w')
        )

    def create_command(self):
        # TODO: add start-end command boilerplate, slurm-related (or other)

        cmd_path = Path(self.step_dir, self.command_file)
        if cmd_path.exists():
            raise ValueError('File {} already exists.'.format(cmd_path))

        logger.debug('Creating step command.')
        print(self.get_command_str(), file=open(cmd_path, 'w'))

    def get_command_str(self):
        raise NotImplementedError()

    def run_step(self, args):
        # TODO: logic for rerunning/overriding failed/running steps
        if self.has_state('RUNNING'):
            jobid = open(Path(self._step_dir, self.jobid_file), 'r').readline().strip()
            return jobid
        elif self.has_state('DONE'):
            logger.info(
                'Step {} already finished. Skipping...'.format(self.stepstep_name)
            )
            return None
        elif self.has_state('FAILED'):
            return self.retry_step(args)
        elif (
            not self.has_state('INITED')
        ):
            raise ValueError(
                'Cannot run step. Not in INITED state.'.format(self.step)
            )
        # TODO: add rerun option for FAILED jobs

        jid_deps = []
        for dep in self.dependencies.values():
            jid = dep.run_step(args)
            if jid is not None:
                jid_deps.append(jid)

        cmd_path = Path(self.step_dir, self.command_file)
        sub = build_subprocess(cmd_path, args, jid_deps=jid_deps)

        logger.info('Submitted {} job {}'.format(args.runner, sub['jobid']))
        print(sub['jobid'], file=open(Path(self.step_dir, self.jobid_file), 'w'))

        return sub['jobid']

    def retry_step(self, args):
        """
        Try to recover from a failed state.

        Default behavior is to change state and just rerun.
        Derived class should override this method if necessary.
        """
        self.set_state('INITED')
        sef.run_state(args)

    def traceback_step(cls, level=0):
        print_indented('+ {}'.format(step_name), level)
        for k, v in self.get_variables():
            print_indented('| {} = {}'.format(k, v))
        for name, dep in self.dependencies.values():
            print(print_indented('--| {}'.format(name)))
            dep.traceback_step(level + 1)


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
