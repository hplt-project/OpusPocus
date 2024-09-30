import enum
import inspect
import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, get_type_hints

import yaml

from opuspocus.utils import RunnerResources, clean_dir, print_indented

logger = logging.getLogger(__name__)


class StepState(str, enum.Enum):
    INIT_INCOMPLETE = "INIT_INCOMPLETE"
    FAILED = "FAILED"
    INITED = "INITED"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    DONE = "DONE"


class OpusPocusStep:
    """Base class for OpusPocus pipeline steps."""

    command_file = "step.command"
    dependency_file = "step.dependencies"
    state_file = "step.state"
    parameter_file = "step.parameters"

    def __init__(self, step: str, step_label: str, pipeline_dir: Path, **kwargs) -> None:  # noqa: ANN003
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
        self.step_label = step_label
        self.pipeline_dir = pipeline_dir

        if pipeline_dir is None:
            err_msg = (
                f"{step_label}.pipeline_dir was not specified. Use --pipeline-dir "
                "option to set global pipeline_dir or set the pipeline_dir "
                "for the step using the config file."
            )
            raise ValueError(err_msg)

        self.register_parameters(**kwargs)

    @classmethod
    def build_step(cls: "OpusPocusStep", step: str, step_label: str, pipeline_dir: Path, **kwargs) -> "OpusPocusStep":  # noqa: ANN003
        """Build a specified step instance.

        Args:
            step (str): step class name in the step class registry
            pipeline_dir (Path): path to the pipeline directory
            **kwargs: additional parameters for the derivedclass

        Returns:
            An instance of the specified pipeline class.
        """
        try:
            cls_inst = cls(step, step_label, pipeline_dir, **kwargs)
        except TypeError:
            sig = inspect.signature(cls.__init__)
            logger.exception(
                "Error occured while building step %s (%s).\nStep Signature:\n%s\n",
                step_label,
                step,
                "\n".join([f"\t{sig.parameters[x]}" for x in sig.parameters]),
            )
            raise
        return cls_inst

    @classmethod
    def list_parameters(cls: "OpusPocusStep", *, exclude_dependencies: bool = True) -> List[str]:
        """Return a list of arguments/required for initialization

        Args:
            exclude_dependencies (bool): exlude the step dependencies
            parameters

        These parameter lists are used during step instance saving/loading.
        Step dependencies are handled differently (by saving/loading
        their respective dep.step_label properties).
        """
        param_list = []
        for param in inspect.signature(cls.__init__).parameters:
            if param == "self":
                continue
            if "_step" in param and exclude_dependencies:
                continue
            param_list.append(param)
        return param_list

    @classmethod
    def load_parameters(
        cls: "OpusPocusStep",
        step_label: str,
        pipeline_dir: Path,
    ) -> Dict[str, Any]:
        """Load the previously initialized step instance parameters."""
        params_path = Path(pipeline_dir, step_label, cls.parameter_file)
        logger.debug("[%s] Loading step variables from %s", step_label, params_path)

        with params_path.open("r") as fh:
            # TODO(varisd): check for missing/unknown parameters
            return yaml.safe_load(fh)

    def get_parameters_dict(self, *, exclude_dependencies: bool = True) -> Dict[str, Any]:
        """Serialize the step parameters"""
        param_dict = {}
        for param in self.list_parameters(exclude_dependencies=exclude_dependencies):
            if "_step" in param:
                if param in self.dependencies and self.dependencies[param] is not None:
                    p = self.dependencies[param].step_label
                else:
                    p = None
            else:
                p = getattr(self, param)
                if isinstance(p, Path):
                    p = str(p)
                if isinstance(p, (list, tuple)) and isinstance(p[0], Path):
                    p = [str(v) for v in p]
            param_dict[param] = p
        return param_dict

    def save_parameters(self) -> None:
        """Save the step instance parameters."""
        with Path(self.step_dir, self.parameter_file).open("w") as fh:
            yaml.dump(self.get_parameters_dict(), fh)

    def register_parameters(self, **kwargs) -> None:  # noqa: ANN003
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
        logger.debug("[%s] Class type hints: %s", self.step_label, type_hints)

        self.dependencies = {}
        for param, val in kwargs.items():
            if "_step" in param:
                self.dependencies[param] = val
            else:
                v = val
                if type_hints[param] == Path and val is not None:
                    v = Path(val)
                if type_hints[param] == List[Path]:
                    v = [Path(v) for v in val]
                setattr(self, param, v)

    @classmethod
    def load_dependencies(cls: "OpusPocusStep", step_label: str, pipeline_dir: Path) -> Dict[str, str]:
        """Load step dependecies based on their unique step_label values."""
        deps_path = Path(pipeline_dir, step_label, cls.dependency_file)
        logger.debug("Loading dependencies from %s", deps_path)
        with deps_path.open("r") as fh:
            return yaml.safe_load(fh)

    def save_dependencies(self) -> None:
        """Save the step dependencies using their unique step_label values."""
        deps_dict = {k: v.step_label for k, v in self.dependencies.items() if v is not None}
        with Path(self.step_dir, self.dependency_file).open("w") as fh:
            yaml.dump(deps_dict, fh)

    @property
    def step_dir(self) -> Path:
        """Location of the step directory."""
        return Path(self.pipeline_dir, self.step_label)

    @property
    def output_dir(self) -> Path:
        """Location of the step output directory."""
        return Path(self.step_dir, "output")

    @property
    def log_dir(self) -> Path:
        """Location of the step log directory."""
        return Path(self.step_dir, "logs")

    @property
    def tmp_dir(self) -> Path:
        """Location of the step temp directory.

        The contents of this directory get deleted after a successfull step
        completuion.
        """
        return Path(self.step_dir, "temp")

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
        if self.state is not None:
            if self.has_state(StepState.INITED):
                logger.info("Step already initialized. Skipping...")
                return
            err_msg = f"Trying to initialize step in a {self.state} state."
            raise ValueError(err_msg)
        # Set state to incomplete until finished initializing.
        self.create_directories()
        self.state = StepState.INIT_INCOMPLETE

        # Initialization of dependencies after directory creation and setting
        # state to 'incomplete' helps to detect possible dependency cycles.
        self.init_dependencies()
        self.save_parameters()
        self.save_dependencies()
        self.create_command()

        # initialize state
        logger.info("[%s] Step Initialized.", self.step_label)
        self.state = StepState.INITED

    def init_dependencies(self) -> None:
        """Recursively call the init_step method of the step dependencies.

        Some steps can be dependants of multiple steps, we skip steps that
        are already initialized.
        """
        for dep in self.dependencies.values():
            if dep is None:
                continue
            if not dep.has_state(StepState.INITED):
                dep.init_step()

    def get_file_list(self) -> Optional[List[Path]]:
        """TODO"""
        return None

    def create_directories(self) -> None:
        """Create the internal step directory structure."""
        # create step dir
        if self.step_dir.is_dir():
            err_msg = f"Cannot create {self.step_dir}. Directory already exists."
            raise FileExistsError(err_msg)
        for d in [self.step_dir, self.log_dir, self.output_dir, self.tmp_dir]:
            d.mkdir(parents=True)
        logger.debug("[%s] Finished creating step directory.", self.step_label)

    def clean_directories(self, *, keep_finished: bool = False) -> None:
        """TODO"""
        for d in [self.log_dir, self.tmp_dir]:
            clean_dir(d, exclude="categories.json")

        if not keep_finished:
            clean_dir(self.output_dir, exclude="categories.json")
        logger.debug("[%s] Finished cleaning subdirectory contents (keep_finished=%s)", self.step_label, keep_finished)

    def create_command(self) -> None:
        """Save the string composed using into the compose_command method.

        This method only handles writing command into a file. Command creation
        is currently handled by the compose_command() method.
        """
        cmd_path = Path(self.step_dir, self.command_file)
        if cmd_path.exists():
            err_msg = f"File {cmd_path} already exists."
            raise FileExistsError(err_msg)

        with cmd_path.open("w") as fh:
            print(self.compose_command(), file=fh)
        cmd_path.chmod(0o755)
        logger.debug("[%s] Finished creating step.command.", self.step_label)

    def traceback_step(self, level: int = 0, *, full: bool = False) -> None:
        """Print the information about the step state and variables.

        If the step has any dependencies, call their respective traceback_step
        methods.
        """
        assert level >= 0
        print_indented(f"+ {self.step_label}: {self.state}", level)
        if full:
            for param in self.list_parameters():
                print_indented(f"|-- {param} = {getattr(self, param)}", level)
        for name, dep in self.dependencies.items():
            print_indented(f"└-+ {name}", level)
            if dep is None:
                print_indented("+ None", level + 1)
                continue
            dep.traceback_step(level + 1, full=full)

    @property
    def state(self) -> Optional[StepState]:
        state_path = Path(self.step_dir, self.state_file)
        if state_path.exists():
            with state_path.open("r") as fh:
                state = StepState(json.load(fh))
            assert state in StepState
            return state
        return None

    @state.setter
    def state(self, state: StepState) -> None:
        """Change the state of a step and save it into step.state file."""
        assert state in StepState
        old_state = self.state
        if state == old_state:
            logger.warning("[%s] The new step state is identical to the old one.", self.step_label)
            return

        state_path = Path(self.step_dir, self.state_file)
        with state_path.open("w") as fh:
            json.dump(state, fp=fh)
        logger.debug("[%s] Changed step state (old: %s -> new: %s).", self.step_label, old_state, state)

    def has_state(self, state: StepState) -> bool:
        """Check whether the step is in a specific state."""
        return self.state is not None and self.state == state

    @property
    def is_running_or_submitted(self) -> bool:
        return any(self.has_state(state) for state in [StepState.RUNNING, StepState.SUBMITTED])

    def get_command_targets(self) -> List[Path]:
        raise NotImplementedError()

    def run_main_task(self, runner: "OpusPocusRunner") -> None:  # noqa: F821
        logging.basicConfig(level=logging.INFO)
        self.state = StepState.RUNNING
        self.command_preprocess()

        task_info_list = []
        for target_file in self.get_command_targets():
            if target_file.exists():
                logger.info("[%s] File {target_file!s} already finished. Skipping...", self.step_label)
                continue

            cmd_path = Path(self.step_dir, self.command_file)
            task_info = runner.submit_task(
                cmd_path=cmd_path,
                target_file=target_file,
                dependencies=None,
                step_resources=runner.get_resources(self),
                stdout_file=Path(self.log_dir, f"{runner.runner}.{target_file.stem}.out"),
                stderr_file=Path(self.log_dir, f"{runner.runner}.{target_file.stem}.err"),
            )
            task_info_list.append(task_info)
            time.sleep(0.5)

        # Update the submission info
        submission_info = runner.load_submission_info(self)
        submission_info["subtasks"] = task_info_list
        runner.save_submission_info(self, submission_info)

        def cancel_signal_hander(signum, _) -> None:  # noqa: ANN001
            logger.info("[%s] Received signal %s. Terminating subtasks...", self.step_label, signum)
            logger.info("Current subtask list: %s", " ".join(task_info_list))
            for task_info in task_info_list:
                runner.send_signal(task_info, signum)
            self.state = StepState.FAILED
            sys.exit(signum)

        signal.signal(signal.SIGTERM, cancel_signal_hander)
        signal.signal(signal.SIGINT, cancel_signal_hander)

        def resubmit_signal_handler(signum, _) -> None:  # noqa: ANN001
            # If the main task receives SIGUSR1 or SIGUSR2, terminate all subtasks,
            # FAIL and resubmit it
            logger.info("[%s] Received signal %i. Terminating subtasks...", self.step_label, signum)
            for task_info in task_info_list:
                runner.send_signal(task_info, signum)
            self.state = StepState.FAILED  # change the state to enable .submit_step
            sub_info = runner.submit_step(self, keep_finished=(signum == signal.SIGUSR1))
            logger.info(
                "[%s] Resubmitted step with the following main_task task_info: %s",
                self.step_label,
                sub_info["main_task"],
            )
            runner.update_dependants(self)
            runner.run()
            sys.exit(0)

        signal.signal(signal.SIGUSR1, resubmit_signal_handler)
        signal.signal(signal.SIGUSR2, resubmit_signal_handler)

        runner.wait_for_tasks(task_info_list)
        self.command_postprocess()

        clean_dir(self.tmp_dir)
        self.state = StepState.DONE

    def command_preprocess(self) -> None:
        """TODO"""
        pass

    def command_postprocess(self) -> None:
        """TODO"""
        pass

    def run_subtask(self, target_file: Path) -> None:
        """TODO"""
        try:
            self.command(target_file)
        except Exception:
            if target_file is not None and target_file.exists():
                target_file.unlink()
            raise

    def command(self, target_file: Path) -> None:
        """TODO"""
        raise NotImplementedError()

    def compose_command(self) -> str:
        """Compose the step command.

        We define a general step.command structure here to reduce code
        duplication. The respective parts can be overwritten/reused if
        necessary.

        More fine-grained structure should be defined through cmd_body_str
        method.
        """
        return f"""#!/usr/bin/env python3
import sys
from pathlib import Path

from opuspocus.runners import load_runner
from opuspocus.pipeline_steps import StepState, load_step


def main(argv):
    try:
        step = load_step("{self.step_label}", Path("{self.pipeline_dir}"))
        target_file = None
        if len(argv) == 2:
            # Subtask
            target_file = Path(argv[1])
            step.run_subtask(target_file)
        elif len(argv) == 1:
            # Main task
            runner = load_runner(Path("{self.pipeline_dir}"))
            step.run_main_task(runner)
        else:
            ValueError("Wrong number of arguments.")
    except Exception as e:
        if len(argv) == 1:
            step.state = StepState.FAILED
        raise e


if __name__ == "__main__":
    main(sys.argv)
    sys.exit(0)
"""

    @property
    def default_resources(self) -> RunnerResources:
        """Definition of defeault runner resources for a specific step."""
        return RunnerResources()

    def __eq__(self, other: "OpusPocusStep") -> bool:
        """Object comparison logic."""
        for param in self.list_parameters(exclude_dependencies=False):
            if getattr(self, param, None) != getattr(other, param, None):
                return False
        return True
