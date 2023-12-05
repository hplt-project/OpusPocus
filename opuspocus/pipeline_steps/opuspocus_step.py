from typing import Any, Dict, List, Optional, get_type_hints

import argparse
import inspect
import logging
import yaml
from pathlib import Path

from opuspocus.command_utils import build_subprocess
from opuspocus.utils import print_indented


STEP_STATES = ['INITED', 'RUNNING', 'FAILED', 'DONE']

logger = logging.getLogger(__name__)


def get_hash():
    # TODO: proper hashing
    return 'debug'


class OpusPocusStep(object):
    command_file = 'step.command'
    dependency_file = 'step.dependencies'
    jobid_file = 'step.jobid'
    state_file = 'step.state'
    parameter_file = 'step.parameters'

    def __init__(
        self,
        step: str,
        pipeline_dir: Path,
        suffix: str = None,
        **kwargs
    ):
        """
        TODO: the derived classes should add attributes+logic for optional
        step dependencies.
        TODO: can we do it in a smarter way?
        """
        self.step = step
        self.pipeline_dir = pipeline_dir
        self.suffix = suffix
        self.register_parameters(**kwargs)
        self.state = self.load_state()

    @classmethod
    def build_step(cls, step: str, pipeline_dir: Path, **kwargs):
        """Build a specified step instance.

        Args:
            args (argparse.Namespace): parsed command-line arguments
        """
        return cls(step, pipeline_dir, **kwargs)

    @classmethod
    def list_parameters(cls) -> List[str]:
        """
        Return a list of arguments/required for initialization excluding
        the dependencies.
        """
        param_list = []
        for param in inspect.signature(cls.__init__).parameters:
            if param == 'self':
                continue
            if '_step' in param:
                continue
            param_list.append(param)
        return param_list

    @classmethod
    def load_parameters(
        cls, step_name: str, pipeline_dir: Path
    ) -> Dict[str, Any]:
        """Load existing step."""
        vars_path = Path(pipeline_dir, step_name, cls.parameter_file)
        logger.debug('Loading step variables from {}'.format(vars_path))
        return yaml.safe_load(open(vars_path, 'r'))

    def save_parameters(self):
        logger.debug('Saving step variables.')
        param_dict ={}
        for param in self.list_parameters():
            p = getattr(self, param)
            if isinstance(p, Path):
                p = str(p)
            if isinstance(p, list) and isinstance(p[0], Path):
                p = [str(v) for v in p]
            param_dict[param] = p
        yaml.dump(
            param_dict,
            open(Path(self.step_dir, self.parameter_file), 'w')
        )

    
    def register_parameters(self, **kwargs):
        """
        Pre-described setting of the class attributes that are set using
        the __init__ method parameters.

        We make a distinction between the regular attributes and the step
        dependencies (indicated by the '_step' suffix).

        This strict parameter registration enables 
        """
        type_hints = get_type_hints(self.__init__)

        self.dependencies = {}
        for param, val in kwargs.items():
            if '_step' in param:
                self.dependencies[param] = val
            else:
                if type_hints[param] == Path:
                    val = Path(val)
                if type_hints[param] == List[Path]:
                    val = [Path(v) for v in val]
                setattr(self, param, val)

    @classmethod
    def load_dependencies(
        cls, step_name: str, pipeline_dir: Path
    ) -> Dict[str, str]:
        """Load step dependecies (directories)."""
        deps_path = Path(pipeline_dir, step_name, cls.dependency_file)
        logger.debug('Loading dependencies from {}'.format(deps_path))
        return yaml.safe_load(open(deps_path, 'r'))

    def save_dependencies(self):
        deps_dict = {
            k: v.step_name 
            for k, v in self.dependencies.items()
            if v is not None
        }
        yaml.dump(
            deps_dict,
            open(Path(self.step_dir, self.dependency_file), 'w')
        )

    @property
    def step_name(self) -> str:
        """
        We can have multiple instances of a step with different
        parametrization. Must be implemented by the derived step classes.
        """
        raise NotImplementedError()

    @property
    def step_dir(self) -> Path:
        return Path(self.pipeline_dir, self.step_name)

    @property
    def output_dir(self) -> Path:
        return Path(self.step_dir, 'output')

    @property
    def log_dir(self) -> Path:
        return Path(self.step_dir, 'logs')

    def init_step(self):
        self.state = self.load_state()
        if self.state is not None:
            if self.has_state('INITED'):
                logger.info('Step already initialized. Skipping...')
                return
            else:
                raise ValueError(
                    'Trying to initialize step in a {} state.'.format(self.state)
                )

        self.create_directories()
        self.init_dependencies()
        self.save_parameters()
        self.save_dependencies()
        self.create_command()

        # initialize state
        logger.info('[{}.init] Step Initialized.'.format(self.step))
        self.set_state('INITED')

    def init_dependencies(self):
        # TODO: improve the dependency representation and implement a
        # child-agnostic init deps method
        for dep in self.dependencies.values():
            if dep is None:
                continue
            if not dep.has_state('INITED'):
                dep.init_step()

    def create_directories(self):
        # create step dir
        logger.debug('Creating step dir.')
        if self.step_dir.is_dir():
            raise ValueError(
                'Cannot create {}. Directory already exists.'
                .format(self.step_dir)
            )
        for d in [self.step_dir, self.log_dir, self.output_dir]:
            d.mkdir(parents=True)

    def create_command(self):
        # TODO: add start-end command boilerplate, slurm-related (or other)
        cmd_path = Path(self.step_dir, self.command_file)
        if cmd_path.exists():
            raise ValueError('File {} already exists.'.format(cmd_path))

        logger.debug('Creating step command.')
        print(self.get_command_str(), file=open(cmd_path, 'w'))

    def get_command_str(self) -> str:
        raise NotImplementedError()

    def run_step(self, args: argparse.Namespace) -> int:
        # TODO: return type (int or str?)
        # TODO: logic for rerunning/overriding failed/running steps
        if self.has_state('RUNNING'):
            jobid = open(Path(self.step_dir, self.jobid_file), 'r').readline().strip()
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
            if dep is None:
                continue
            jid = dep.run_step(args)
            if jid is not None:
                jid_deps.append(jid)

        cmd_path = Path(self.step_dir, self.command_file)
        sub = build_subprocess(cmd_path, args, jid_deps=jid_deps)

        logger.info('Submitted {} job {}'.format(args.runner, sub['jobid']))
        print(sub['jobid'], file=open(Path(self.step_dir, self.jobid_file), 'w'))
        self.set_state('RUNNING')

        return sub['jobid']

    def retry_step(self, args: argparse.Namespace):
        """
        Try to recover from a failed state.

        Default behavior is to change state and just rerun.
        Derived class should override this method if necessary.
        """
        self.set_state('INITED')
        self.run_step(args)

    def traceback_step(self, level: int = 0):
        assert level >= 0
        print_indented('+ {}: {}'.format(self.step_name, self.state), level)
        for param in self.list_parameters():
            print_indented('|-- {} = {}'.format(param, getattr(self, param)))
        for name, dep in self.dependencies.values():
            print_indented('└-+ {}'.format(name), level)
            if dep is None:
                print_indented()
            dep.traceback_step(level + 1)


    def load_state(self) -> Optional[str]:
        state_file = Path(self.step_dir, self.state_file)
        if state_file.exists():
            state = open(Path(self.step_dir, self.state_file), 'r').readline().strip()
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

    def has_state(self, state: str) -> bool:
        if self.state is not None and self.state == state:
            return True
        return False
