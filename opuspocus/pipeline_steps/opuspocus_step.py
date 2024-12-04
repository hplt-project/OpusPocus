import enum
import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from attrs import asdict, define, field, fields

from opuspocus.utils import RunnerResources, clean_dir, print_indented

logger = logging.getLogger(__name__)


class StepState(str, enum.Enum):
    INIT_INCOMPLETE = "INIT_INCOMPLETE"
    FAILED = "FAILED"
    INITED = "INITED"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    DONE = "DONE"

    @staticmethod
    def list() -> List[str]:
        return [s.value for s in StepState]


@define(kw_only=True)
class OpusPocusStep:
    """Base class for OpusPocus pipeline steps."""

    step: str = field(converter=str)
    step_label: str = field(converter=str)
    pipeline_dir: Path = field(
        converter=Path, eq=False
    )  # enables comparing pipelines/steps from different pipeline dirs

    _cmd_filename = "step.command"
    _dependency_filename = "step.dependencies"
    _state_filename = "step.state"
    _parameter_filename = "step.parameters"

    @classmethod
    def build_step(cls: "OpusPocusStep", step: str, step_label: str, pipeline_dir: Path, **kwargs) -> "OpusPocusStep":  # noqa: ANN003
        """Build a specified step instance.

        Args:
            step (str): step class name in the step class registry
            step_label (str): unique step instance label
            pipeline_dir (Path): path to the pipeline directory
            **kwargs: additional parameters for the specific pipeline step class implementation

        Returns:
            An instance of the specified pipeline class.
        """
        return cls(step=step, step_label=step_label, pipeline_dir=pipeline_dir, **kwargs)

    @classmethod
    def list_parameters(cls: "OpusPocusStep", *, exclude_dependencies: bool = True) -> List[str]:
        """Return a list of arguments required for step initialization.

        Parameter list used mainly during step instance saving/loading.
        Step dependencies are handled differently, by saving/loading their respective dep.step_label properties instead
        of saving the whole class instance.

        Args:
            exclude_dependencies (bool): exlude the step dependencies parameters

        Returns:
            List of step parameters.
        """
        param_list = []
        for p in fields(cls):
            if p.name.startswith("_"):
                continue
            if exclude_dependencies and "_step" in p.name:
                continue
            param_list.append(p.name)
        return param_list

    @classmethod
    def load_parameters(
        cls: "OpusPocusStep",
        step_label: str,
        pipeline_dir: Path,
    ) -> Dict[str, Any]:
        """Load the previously initialized step instance parameters.

        Args:
            step_label (str): unique step instance label
            pipeline_dir (Path): path to the pipeline directory

        Returns:
            Dict containing key-value pairs for the step instance initialization. Step dependencies are represented
            by their unique step labels.
        """
        params_path = Path(pipeline_dir, step_label, cls._parameter_filename)
        logger.debug("[%s] Loading step variables from %s", step_label, params_path)

        with params_path.open("r") as fh:
            return yaml.safe_load(fh)

    @classmethod
    def load_dependencies(cls: "OpusPocusStep", step_label: str, pipeline_dir: Path) -> Dict[str, str]:
        """Load step dependecies based on their unique step_label values.

        Args:
            step_label (str): step label of the step whose dependencies should be loaded
            pipeline_dir (Path): path to the pipeline directory

        Returns:
            Dict containing attribute-to-step-label mapping of the invidual step dependencies.
        """
        deps_path = Path(pipeline_dir, step_label, cls._dependency_filename)
        logger.debug("[OpusPocusStep] Loading dependencies from %s", deps_path)
        with deps_path.open("r") as fh:
            return yaml.safe_load(fh)

    def get_parameters_dict(self, *, exclude_dependencies: bool = True) -> Dict[str, Any]:
        """Serialize step parameters.

        Args:
            exclude_dependencies (bool): exlude the step dependencies parameters

        Returns:
            Dict containing key-value pairs for the step instance initialization. Step dependencies are represented
            by their unique step labels.
        """
        param_dict = {}
        filter_list = [lambda attr, _: not attr.name.startswith("_")]
        if exclude_dependencies:
            filter_list.append(lambda attr, _: "_step" not in attr.name)
        attr_dict = asdict(self, filter=lambda attr, val: all(fn(attr, val) for fn in filter_list))
        for attr, value in attr_dict.items():
            if "_step" in attr:
                # Extract step_label from the dependencies
                param_dict[attr] = None
                if value is not None:
                    param_dict[attr] = value["step_label"]
            elif isinstance(value, Path):
                param_dict[attr] = str(value)
            elif isinstance(value, (list, tuple)) and any(isinstance(v, Path) for v in value):
                param_dict[attr] = [str(v) for v in value]
            else:
                param_dict[attr] = value
        return param_dict

    def save_parameters(self) -> None:
        """Save the step instance parameters."""
        with Path(self.step_dir, self._parameter_filename).open("w") as fh:
            yaml.dump(self.get_parameters_dict(), fh)

    def save_dependencies(self) -> None:
        """Save the step dependencies using their unique step_label values."""
        deps_dict = {k: v.step_label for k, v in self.dependencies.items() if v is not None}
        with Path(self.step_dir, self._dependency_filename).open("w") as fh:
            yaml.dump(deps_dict, fh)

    @property
    def dependencies(self) -> Dict[str, "OpusPocusStep"]:
        """Provide step-dependency attributes (denoted by a '_step' substring)."""
        return {attr.name: getattr(self, attr.name) for attr in fields(type(self)) if "_step" in attr.name}

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

    @property
    def state_path(self) -> Path:
        """Location of the state file."""
        return Path(self.step_dir, self._state_filename)

    @property
    def cmd_path(self) -> Path:
        """Location of the parameterized step executable."""
        return Path(self.step_dir, self._cmd_filename)

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
        if self.state is StepState.INIT_INCOMPLETE:
            logger.warning("[%s] Step is in %s state. Re-initializing...", self.step_label, self.state)
            clean_dir(self.step_dir)
        elif self.state is not None:
            if self.has_state(StepState.INITED):
                logger.info("[%s] Step already initialized. Skipping...", self.step_label)
                return
            err_msg = f"Trying to initialize step ({self.step_label}) in a {self.state} state."
            raise ValueError(err_msg)
        # Set state to incomplete until finished initializing.
        self.create_directories()
        self.state = StepState.INIT_INCOMPLETE

        # Initialization of dependencies after directory creation and setting
        # state to 'incomplete' helps to detect possible dependency cycles.
        self.init_dependencies()
        self.save_parameters()
        self.save_dependencies()
        self.create_cmd_file()

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

    def create_directories(self) -> None:
        """Create the internal step directory structure."""
        # create step dir
        if self.step_dir.is_dir() and len(list(self.step_dir.iterdir())) != 0:
            err_msg = f"Cannot create {self.step_dir}. Directory already exists and is not empty."
            raise FileExistsError(err_msg)
        for d in [self.log_dir, self.output_dir, self.tmp_dir]:
            d.mkdir(parents=True)
        logger.debug("[%s] Finished creating step directory.", self.step_label)

    def clean_directories(self, *, remove_finished_command_targets: bool = True) -> None:
        """Remove the contents of the output and tmp directories.

        In certain execution scenarios (e.g. execution restart/resubmit) we need to remove files created during
        the previous execution to avoid undesired behavior.

        Args:
            remove_finished_command_targets (bool): remove the target_files of the previous finished subtasks
        """
        clean_dir(self.tmp_dir)
        if remove_finished_command_targets:
            # TODO(varisd): this should not be hard-wired in case we change naming in the derived
            #   (e.g. CorpusStep class)
            clean_dir(self.output_dir, exclude="categories.json")
        logger.debug(
            "[%s] Finished cleaning subdirectory contents (remove_finished_command_targets=%s)",
            self.step_label,
            remove_finished_command_targets,
        )

    @property
    def state(self) -> Optional[StepState]:
        """Current step state.

        In practice, we might run multiple instances of a single step during execution, therefore, we use a state file
        to synchronize the state between these instances.

        TODO(varisd): better handling of the state file race conditions
        """
        if self.state_path.exists():
            with self.state_path.open("r") as fh:
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

        with self.state_path.open("w") as fh:
            json.dump(state, fp=fh)
        logger.debug("[%s] Changed step state (old: %s -> new: %s).", self.step_label, old_state, state)

    def has_state(self, state: StepState) -> bool:
        """Check whether the step is in a specific state."""
        return self.state is not None and self.state == state

    @property
    def is_running_or_submitted(self) -> bool:
        """Check whether a step is in RUNNING or SUBMITTED state."""
        return any(self.has_state(state) for state in [StepState.RUNNING, StepState.SUBMITTED])

    def get_command_targets(self) -> List[Path]:
        """List of the step execution targets.

        Every OpusPocusStep implementation is required to define a list of execution targets (target files)
        which are generated during step execution. All files are expected to be present (and containing the expected
        results) at the end of the successful step execution.
        """
        raise NotImplementedError()

    def run_main_task(self, runner: "OpusPocusRunner") -> None:  # noqa: F821
        """Executes the main part of the step executable.

        This method handles preprocessing (optional), subtask submission (for each target_file) and postprocessing
        after all subtasks successfully finished execution.
        Additionally it handles termination signals (SIGTERM, SIGINT) and resubmission signals (SIGUSR1, SIGUSR2)
        that can be received during execution.
        """
        logging.basicConfig(level=logging.INFO)
        self.main_task_preprocess()

        # we keep track of the submitted subtasks
        task_info_list = []
        submission_info = runner.load_submission_info(self)

        def cancel_signal_hander(signum, _) -> None:  # noqa: ANN001
            """Handler for task cancellation signals."""
            logger.info("[%s] Received signal %s. Terminating subtasks...", self.step_label, signum)
            self.state = StepState.FAILED
            logger.info("[%s] Current subtask list: %s", self.step_label, " ".join(task_info_list))
            for task_info in task_info_list:
                runner.send_signal(task_info, signum)
            sys.exit(signum)

        for sig in [signal.SIGTERM, signal.SIGINT]:
            signal.signal(sig, cancel_signal_hander)

        def resubmit_signal_handler(signum, _) -> None:  # noqa: ANN001
            """Handler for task resubmission signals.

            We use SIGUSR1 and SIGUSR2 to signal resubmission. All subtasks are terminated using SIGTERM and the main
            task is resubmitted afterwards.
            SIGUSR1 indicates skipping of the finished target files during resubmission.
            SIGUSR2 indicates resubmission of all target files.
            """
            # If the main task receives SIGUSR1 or SIGUSR2, terminate all subtasks,
            # FAIL and resubmit it
            logger.info("[%s] Received signal %i. Terminating subtasks...", self.step_label, signum)
            self.state = StepState.FAILED  # change the state to enable .submit_step method
            for task_info in task_info_list:
                logger.info("[%s] Sending SIGTERM to task %i", self.step_label, task_info["id"])
                runner.send_signal(task_info, signal.SIGTERM)
            runner.wait_for_tasks(task_info_list, ignore_returncode=True)
            old_sub_info = runner.load_submission_info(self)
            new_sub_info = runner.submit_step(self, resubmit_finished_subtasks=(signum == signal.SIGUSR2))

            logger.info("[%s] Updating task dependants...")
            runner.update_dependants(
                self, remove_task_list=[old_sub_info["main_task"]], add_task_list=[new_sub_info["main_task"]]
            )
            runner.run()
            sys.exit(0)

        for sig in [signal.SIGUSR1, signal.SIGUSR2]:
            signal.signal(sig, resubmit_signal_handler)

        for target_file in self.get_command_targets():
            # skip target files that are already being processed
            t_infos = [t_info for t_info in submission_info["subtasks"] if t_info["file_path"] == str(target_file)]
            if len(t_infos) == 1 and runner.is_task_running(t_infos[0]):
                logger.info(
                    "[%s] File %s is already being processed by runner (%s), id %i. Skipping submission...",
                    self.step_label,
                    str(target_file),
                    runner.runner,
                    t_infos[0]["id"],
                )
                task_info_list.append(t_infos[0])
                continue

            # skip finished target files
            if target_file.exists():
                logger.info("[%s] File %s already finished. Skipping submission...", self.step_label, str(target_file))
                continue

            timestamp = time.time()
            task_info = runner.submit_task(
                cmd_path=self.cmd_path,
                target_file=target_file,
                dependencies=None,
                step_resources=runner.get_resources(self),
                stdout_file=Path(self.log_dir, f"{runner.runner}.{target_file.stem}.{timestamp}.out"),
                stderr_file=Path(self.log_dir, f"{runner.runner}.{target_file.stem}.{timestamp}.err"),
            )
            task_info_list.append(task_info)

            # update the submission info
            submission_info = runner.load_submission_info(self)
            submission_info["subtasks"] = task_info_list
            runner.save_submission_info(self, submission_info)

            time.sleep(0.5)

        self.state = StepState.RUNNING
        runner.wait_for_tasks(task_info_list)
        self.main_task_postprocess()

        clean_dir(self.tmp_dir)  # cleanup
        self.state = StepState.DONE

    def command(self, target_file: Path) -> None:
        """A step-specific definition of execution steps required to create a give target_file.

        This method should contain the implementation a given step's algorithm. At the end of the command execution
        the target file must be created by the method's implementation.
        """
        raise NotImplementedError()

    def main_task_preprocess(self) -> None:
        """(Optional) preprocessing called before subtask execution."""
        pass

    def main_task_postprocess(self) -> None:
        """Postprocessing called after all subtasks successfully finished.

        By default, we do a sanity check (all target files exist).
        In practice, we use this method after parallel execution of step code, e.g. translating smaller parts of a
        large corpus. After the execution the sharded output can be merged into a single output corpus.
        """
        for target_file in self.get_command_targets():
            if not target_file.exists():
                err_msg = (
                    f"Target file {target_file} does not exists after the step finished {self.step_label} executing."
                )
                raise FileNotFoundError(err_msg)

    def run_subtask(self, target_file: Path) -> None:
        """A wrapper for the self.command() implementation.

        Calls the self.command(target_file) with the give target_file and handles runtime exceptions.
        """
        try:
            self.command(target_file)
        except Exception:
            if target_file is not None and target_file.exists():
                target_file.unlink()
            raise

    def _generate_cmd_file_contents(self) -> None:
        """Create the contents of the step's executable file."""
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
    except Exception as err:
        if len(argv) == 1:
            step.state = StepState.FAILED
        raise err


if __name__ == "__main__":
    main(sys.argv)
    sys.exit(0)
"""

    def create_cmd_file(self) -> None:
        """Create the executable that can be submitted by runners or as a standalone.

        The executable has two modes based on whether it is provided with a target_file:
            1. Without target file - self.run_main_task() method is executed which takes care of the submission
                and management of subtasks for each of its predefined target files.
            2. With target file - self.run_subtask() method is executed which in turn runs the
                self.command(target_file) method
        """
        if self.cmd_path.exists():
            err_msg = f"File {self.cmd_path} already exists."
            raise FileExistsError(err_msg)

        with self.cmd_path.open("w") as fh:
            print(self._generate_cmd_file_contents(), file=fh)
        self.cmd_path.chmod(0o755)
        logger.debug("[%s] Finished creating %s.", self.step_label, self._cmd_filename)

    def print_traceback(self, level: int = 0, *, full: bool = False) -> None:
        """Recursively print the information about the step state, parameters and dependencies.

        Args:
            level (int): indentation level for nicer output
            full (bool): print the full traceback, including step parameters
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
            dep.print_traceback(level + 1, full=full)

    @property
    def default_resources(self) -> RunnerResources:
        """Definition of default runner resources for a specific step."""
        return RunnerResources()
