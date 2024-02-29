from typing import Any, Dict, List, Optional, get_type_hints

import argparse
import inspect
import logging
import yaml
from pathlib import Path

from opuspocus.command_utils import build_subprocess
from opuspocus.utils import print_indented


STEP_STATES = ['INITED', 'RUNNING', 'FAILED', 'DONE', 'INIT_INCOMPLETE']

logger = logging.getLogger(__name__)


class OpusPocusStep(object):
    """Base class for OpusPocus pipeline steps."""

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
        """Set the step parameters based on the parameters passed during
        step object initialization.

        Each step contains a set of step parameters and step dependencies
        that define a unique step instance.

        Any class inheriting from this class is required to avoid setting
        class instance attributes. If needed, the shortcuts to additional
        derived object attributes should be defined via @property class
        methods.

        TODO: implement strict restriction on setting object attributes
        the derived classes
        """
        self.step = step
        self.pipeline_dir = pipeline_dir
        self.suffix = suffix
        self.register_parameters(**kwargs)
        self.state = self.load_state()

    @classmethod
    def build_step(cls, step: str, pipeline_dir: Path, **kwargs) -> 'OpusPocusStep':
        """Build a specified step instance.

        Args:
            step (str): step class name in the step class registry
            pipeline_dir (Path): path to the pipeline directory
            **kwargs: additional parameters for the derivedclass

        Returns:
            An instance of the specified pipeline class.
        """
        try:
            cls_inst = cls(step, pipeline_dir, **kwargs)
        except TypeError as err:
            logger.error('Error occured while building step {}.'.format(step))
            raise err
        return cls_inst

    @classmethod
    def list_parameters(cls) -> List[str]:
        """Return a list of arguments/required for initialization while excluding
        the dependencies.

        These parameter lists are used during step instance saving/loading.
        Step dependencies are handled differently (by saving/loading
        their respective dep.step_name properties).
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
        """Load the previously initialized step instance parameters."""
        vars_path = Path(pipeline_dir, step_name, cls.parameter_file)
        logger.debug('Loading step variables from {}'.format(vars_path))
        return yaml.safe_load(open(vars_path, 'r'))

    def save_parameters(self) -> None:
        """Save the step instance parameters."""
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

    def register_parameters(self, **kwargs) -> None:
        """Class agnostic registration of the step instance parameters.

        Each step inheriting from the abstract class has a set of attributes
        and dependencies reflected by the parameters of its respective
        __init__ method.

        We make a distinction between the standard attributes and the step
        dependencies (indicated by the '_step' suffix).

        Use the @property method decorator to define object attributes that
        are not direct step parameters, i.e. direct access to the attributes
        of the step dependencies. 
        """
        type_hints = get_type_hints(self.__init__)
        logger.debug('Class type hints: {}'.format(type_hints))

        self.dependencies = {}
        for param, val in kwargs.items():
            if '_step' in param:
                self.dependencies[param] = val
            else:
                if type_hints[param] == Path and val is not None:
                    val = Path(val)
                if type_hints[param] == List[Path]:
                    val = [Path(v) for v in val]
                setattr(self, param, val)

    @classmethod
    def load_dependencies(
        cls, step_name: str, pipeline_dir: Path
    ) -> Dict[str, str]:
        """Load step dependecies based on their unique step_name values."""
        deps_path = Path(pipeline_dir, step_name, cls.dependency_file)
        logger.debug('Loading dependencies from {}'.format(deps_path))
        return yaml.safe_load(open(deps_path, 'r'))

    def save_dependencies(self) -> None:
        """Save the step dependencies using their unique step_name values."""
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
        """The unique step-instance identifier.

        Each derived step class is required to implement its own way to create
        its step name based on its step parameters.
        As a result, a pipeline should be able to instantiate multiple step
        instances, i.e. en-to-fr and fr-to-en tranlsation, multiple monolingual
        cleaning, decontaminating steps, etc.
        """
        raise NotImplementedError()

    @property
    def step_dir(self) -> Path:
        """Location of the step directory."""
        return Path(self.pipeline_dir, self.step_name)

    @property
    def output_dir(self) -> Path:
        """Location of the step output directory."""
        return Path(self.step_dir, 'output')

    @property
    def log_dir(self) -> Path:
        """Location of the step log directory."""
        return Path(self.step_dir, 'logs')

    def init_step(self) -> None:
        """Step initialization method.

        If the step.state is not initialized, the following initialization
        steps are executed:
        1. create the step directory structure
        2. (recursively) initialize the dependencies
        3. save the step parameters and dependency information
        4. create the step command
        5. set set.state to INITED
        """
        self.state = self.load_state()
        if self.state is not None:
             if self.has_state('INITED'):
                logger.info('Step already initialized. Skipping...')
                return
            else:
                raise ValueError(
                    'Trying to initialize step in a {} state.'.format(self.state)
                )
        # Set state to incomplete until finished initializing.
        self.create_directories()
        self.set_state('INIT_INCOMPLETE')

        self.init_dependencies()
        self.save_parameters()
        self.save_dependencies()
        self.create_command()

        # initialize state
        logger.info('[{}.init] Step Initialized.'.format(self.step))
        self.set_state('INITED')

    def init_dependencies(self) -> None:
        """Recursively call the init_step method of the step dependencies.

        Some steps can be dependants of multiple steps, we skip steps that
        are already initialized.
        """
        for dep in self.dependencies.values():
            if dep is None:
                continue
            if not dep.has_state('INITED'):
                dep.init_step()

    def create_directories(self) -> None:
        """Create the internal step directory structure."""
        # create step dir
        logger.debug('Creating step dir.')
        if self.step_dir.is_dir():
            raise FileExistsError(
                'Cannot create {}. Directory already exists.'
                .format(self.step_dir)
            )
        for d in [self.step_dir, self.log_dir, self.output_dir]:
            d.mkdir(parents=True)

    def create_command(self) -> None:
        """Save the string composed using into the compose_command method.

        This method only handles writing command into a file. Command creation
        is currently handled by the compose_command() method.
        """
        cmd_path = Path(self.step_dir, self.command_file)
        if cmd_path.exists():
            raise FileExistsError('File {} already exists.'.format(cmd_path))

        logger.debug('Creating step command.')
        print(self.compose_command(), file=open(cmd_path, 'w'))

    def run_step(self, args: argparse.Namespace) -> int:
        """Execute the step command.

        The method checks whether the step is in eligible state (INITED or
        FAILED, in case of retry), calls the run_step method of its
        dependencies recursively and executes the step.command script
        using the provided --runner.

        Returns:
            int indicating the job_id, process_id or other identification
            of the executed step.
        """
        # TODO: return type (int or str?)
        # TODO: logic for rerunning/overriding failed/running steps
        if self.has_state('RUNNING'):
            jobid = open(Path(self.step_dir, self.jobid_file), 'r').readline().strip()
            return jobid
        elif self.has_state('DONE'):
            logger.info(
                'Step {} already finished. Skipping...'.format(self.step_name)
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
        """Try to recover from a failed state.

        Default behavior is to change state and just rerun.
        Derived class should override this method if necessary.
        """
        self.set_state('INITED')
        self.run_step(args)

    def traceback_step(self, level: int = 0, full: bool = False) -> None:
        """Print the information about the step state and variables.

        If the step has any dependencies, call their respective traceback_step
        methods.
        """
        assert level >= 0
        print_indented('+ {}: {}'.format(self.step_name, self.state), level)
        if full:
            for param in self.list_parameters():
                print_indented('|-- {} = {}'.format(param, getattr(self, param)), level)
        for name, dep in self.dependencies.items():
            print_indented('└-+ {}'.format(name), level)
            if dep is None:
                print_indented('+ None', level + 1)
                continue
            dep.traceback_step(level + 1, full)

    def load_state(self) -> Optional[str]:
        """Load the current state of a step."""
        state_file = Path(self.step_dir, self.state_file)
        if state_file.exists():
            state = open(Path(self.step_dir, self.state_file), 'r').readline().strip()
            assert state in STEP_STATES
            return state
        return None

    def set_state(self, state: str) -> None:
        """Change the state of a step and save it into step.state file."""
        assert state in STEP_STATES
        if state == self.state:
            logger.warn('The new step state is identical to the old one.')

        logger.debug('Old state: {} -> New state: {}'.format(self.state, state))
        self.state = state
        print(state, file=open(Path(self.step_dir, self.state_file), 'w'))

    def has_state(self, state: str) -> bool:
        """Check whether the step is in a specific state."""
        if self.state is not None and self.state == state:
            return True
        return False

    def compose_command(self) -> str:
        """Compose the step command.

        We define a general step.command structure here to reduce code
        duplication. The respective parts can be overwritten/reused if
        necessary.

        More fine-grained structure should be defined through cmd_body_str
        method.
        """
        return """{cmd_header}
{cmd_vars}
{cmd_traps}
{cmd_body}
{cmd_exit}
""".format(
            cmd_header=self._cmd_header_str(),
            cmd_vars=self._cmd_vars_str(),
            cmd_traps=self._cmd_traps_str(),
            cmd_body=self._cmd_body_str(),
            cmd_exit=self._cmd_exit_str()
        )

    def _cmd_header_str(
        self,
        n_nodes: int = 1,
        n_tasks: int = 1,
        n_cpus: int = 1,
        n_gpus: int = None,
        mem: int = 1,
    ) -> str:
        """Produces scripts header code.

        Should contain stuff like shebang, sbatch-related defaults, etc.
        """
        sbatch_gpu = ''
        if n_gpus is not None:
            sbatch_gpu = '\n#SBATCH --gpus-per-node={}'.format(n_gpus)

        return """#!/usr/bin/env bash
#SBATCH --job-name={jobname}
#SBATCH --nodes={n_nodes}
#SBATCH --ntasks={n_tasks}
#SBATCH --cpus-per-task={n_cpus}{sbatch_gpu}
#SBATCH --mem={mem}G
#SBATCH -o {logdir}/slurm.%j.log
#SBATCH -e {logdir}/slurm.%j.log
set -euo pipefail
        """.format(
            jobname=self.step,
            n_nodes=n_nodes,
            n_tasks=n_tasks,
            n_cpus=n_cpus,
            mem=mem,
            logdir=self.log_dir,
            sbatch_gpu=sbatch_gpu,
        )

    def _cmd_vars_str(self) -> str:
        """Produces code with variable definitions.

        To increase readability and simplify the Python string replacements
        (using the step parameters),
        script variables should be defined at this place. Later parts should
        use the variables defined here.
        """
        raise NotImplementedError()

    def _cmd_traps_str(self) -> str:
        """
        Produces code that can catch exceptions, exectue cleanup
        (and recover from them).

        Mainly to define behavior at a (un)successful execution of the script,
        i.e. setting the DONE/FAILED step.state at the end of execution.
        """

        return """cleanup() {{
    exit_code=$?
    if [[ $exit_code -gt 0 ]]; then
        exit $exit_code
    fi
    echo DONE > {state_file}
    exit 0
}}

err_cleanup() {{
    exit_code=$?
    # Set the step state and exit
    echo FAILED > {state_file}
    exit $exit_code
}}

trap err_cleanup ERR
trap cleanup EXIT
        """.format(state_file=Path(self.step_dir, self.state_file))

    def _cmd_body_str(self) -> str:
        """Get the step specific code.

        This method must be overridden by the derived classes.
        """
        raise NotImplementedError()

    def _cmd_exit_str(self) -> str:
        """Code executed at the end of the scripts."""

        return """# Explicitly exit with a non-zero status
exit 0
"""
