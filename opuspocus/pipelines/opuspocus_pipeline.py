import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from attrs import converters, define, field, validators
from omegaconf import OmegaConf

from opuspocus.pipeline_steps import OpusPocusStep, StepState, build_step
from opuspocus.pipelines.exceptions import PipelineInitError, PipelineStateError
from opuspocus.utils import clean_dir, file_path

logger = logging.getLogger(__name__)

# We use the same set of states, we distinguish PipelineState and StepState for code clarity
PipelineState = StepState


@define(kw_only=True)
class PipelineGraph:
    """Class representing the pipelien graph structure."""

    config: "PipelineConfig" = field(validator=validators.instance_of("PipelineConfig"))
    steps: Dict[str, OpusPocusStep] = field(init=False, factory=dict)
    targets: List[OpusPocusStep] = field(init=False, factory=list)

    def __attrs_post_init__(self) -> None:
        self.steps, self.targets = self.build_graph(self.config)

    def build_graph(self, pipeline_config: OmegaConf) -> Tuple[Dict[str, OpusPocusStep], List[OpusPocusStep]]:
        """TODO"""
        pipeline_dir = pipeline_config.pipeline.pipeline_dir

        steps = {}  # type: Dict[str, OpusPocusStep]
        steps_configs = {s.step_label: OmegaConf.to_object(s) for s in pipeline_config.pipeline.steps}

        def _build_step_inner(step_label: str) -> OpusPocusStep:
            """Build step and its dependencies."""
            if step_label in steps:
                return steps[step_label]

            # Create the arguments for the step instance initialization
            step_args = {}
            logger.info("Creating parameters to build %s object.", step_label)
            for k, v in steps_configs[step_label].items():
                # Simply assing the value if None or not a dependency parameter
                if "_step" not in k or v is None:
                    step_args[k] = v
                else:
                    if v not in steps_configs:
                        err_msg = f"Step '{step_label}' has an undefined dependency '{k}={v}'."
                        raise ValueError(err_msg)
                    step_args[k] = _build_step_inner(v)

            # Set default (global) pipeline_dir
            if "pipeline_dir" not in steps_configs[step_label]:
                step_args["pipeline_dir"] = pipeline_dir

            try:
                steps[step_label] = build_step(**step_args)
            except Exception as e:
                logger.exception("Step parameters:\n%s", step_args)
                raise e  # noqa: TRY201

            return steps[step_label]

        # Create pipeline step objects
        for step_label in steps_configs:
            _build_step_inner(step_label)

        targets = []
        if "targets" in pipeline_config.pipeline:
            targets = [steps[step_label] for step_label in pipeline_config.pipeline.targets]
        return steps, targets


@define(kw_only=True)
class OpusPocusPipeline:
    """Base class for OpusPocus pipelines."""

    pipeline_dir: Path = field(converter=converters.optional(Path), default=None)
    pipeline_config: "PipelineConfig" = field(
        converter=validators.optional(validators.instance_of("PipelineConfig")), default=None
    )
    pipeline_graph: PipelineGraph = field(init=False, default=None)

    _config_file = "pipeline.config"

    def __attrs_post_init__(self) -> None:
        if self.pipeline_config is None and self.pipeline_dir is None:
            err_msg = (
                "Either the pipeline_config or the pipeline_dir containing a pipeline config file must be provided"
            )
            raise ValueError(err_msg)
        if self.pipeline_dir is None:
            self.pipeline_dir = self.pipeline_config.pipeline_dir
        elif self.pipeline_config is None:
            self.pipeline_config = PipelineConfig.load(Path(self.pipeline_dir, self._config_file))
        else:
            self.pipeline_config.pipeline_dir = self.pipeline_dir
        self.pipeline_graph = PipelineGraph(self.pipeline_config)

    @staticmethod
    def add_args(parser: argparse.ArgumentParser, *, pipeline_dir_required: bool = True) -> None:
        """Add pipeline-specific arguments to the parser."""
        parser.add_argument(
            "--pipeline-dir",
            type=file_path,
            default=None,
            required=pipeline_dir_required,
            help="Pipeline root directory location.",
        )

    @property
    def steps(self) -> Dict[str, OpusPocusStep]:
        """TODO"""
        return self.pipeline_graph.steps

    @property
    def targets(self) -> List[OpusPocusStep]:
        """TODO"""
        return self.pipeline_graph.targets

    @property
    def state(self) -> Optional[PipelineState]:  # noqa: PLR0911
        """Return the current state of the pipeline."""
        step_states = [s.state for s in self.steps]
        if all(state == StepState.DONE for state in step_states):
            return PipelineState.DONE
        if StepState.RUNNING in step_states:
            return PipelineState.RUNNING
        if StepState.SUBMITTED in step_states:
            return PipelineState.SUBMITTED
        if StepState.FAILED in step_states:
            return PipelineState.FAILED
        if all(state == StepState.INITED for state in step_states):
            return PipelineState.INITED
        if StepState.INIT_INCOMPLETE in step_states:
            return PipelineState.INIT_INCOMPLETE
        return None

    @classmethod
    def build_pipeline(cls: "OpusPocusPipeline", pipeline_config_path: Path, pipeline_dir: Path) -> "OpusPocusPipeline":
        """Build a specified pipeline instance.

        Args:
            pipeline_config_path (Path): Location of the config file
            pipeline_dir (Path): Location of the pipeline directory

        Returns:
            An instance of the pipeline.
        """
        pipeline_config = None
        if pipeline_config_path is not None:
            pipeline_config = PipelineConfig.load(pipeline_config_path)
        return cls(pipeline_config=pipeline_config, pipeline_dir=pipeline_dir)

    @classmethod
    def load_pipeline(cls: "OpusPocusPipeline", pipeline_dir: Path) -> "OpusPocusPipeline":
        """Load the existing pipeline."""
        if not pipeline_dir.exists():
            err_msg = f"Pipeline directory ({pipeline_dir}) does not exist."
            raise FileNotFoundError(err_msg)
        if not pipeline_dir.is_dir():
            err_msg = f"{pipeline_dir} is not a directory."
            raise NotADirectoryError(err_msg)

        pipeline_config_path = Path(pipeline_dir, cls._config_file)
        return cls(pipeline_config_path, pipeline_dir)

    def save_pipeline(self) -> None:
        """Save the pipeline information.

        Saves the dependency structure of the pipeline steps, pipeline target
        steps and pipeline parameters in their respective YAML files.
        """
        config = PipelineConfig.create(self.pipeline_dir, self.pipeline_graph)
        config.save(Path(self.pipeline_dir, self._config_file))

    def init(self) -> None:
        """Initialize the pipeline."""
        logger.info("Initializing pipeline (%s)", self.pipeline_dir)
        if self.state is None and (self.pipeline_dir.exists() and len(list(self.pipeline_dir.iterdir())) != 0):
            err_msg = f"{self.pipeline_dir} must be an empty or non-existing directory."
            raise PipelineInitError(err_msg)
        if self.state in [PipelineState.SUBMITTED, PipelineState.RUNNING, PipelineState.FAILED, PipelineState.DONE]:
            err_msg = f"Trying to initialize pipeline (self.pipeline_dir) in a {self.state} state."
            raise PipelineStateError(err_msg)

        for v in self.pipeline_graph.values():
            v.init_step()

        self.save_pipeline()
        logger.info("Pipeline (%s) initialized successfully.", self.pipeline_dir)

    def reinit(self) -> None:
        """Reinitialize the pipeline."""
        if self.state in [PipelineState.RUNNING, PipelineState.SUBMITTED]:
            err_msg = f"Trying to re-initialize a pipeline in {self.state} state. Stop the pipeline execution first."
            raise ValueError(err_msg)
        clean_dir(self.pipeline_dir)
        self.init()

    def print_status(self, steps: List[OpusPocusStep]) -> None:
        """TODO"""
        header = f"{self.pipeline_dir.stem}{self.pipeline_dir.suffix}|{self.__class__.__name__}|{self.state.value!s}"
        print(header)  # noqa: T201
        print("-" * len(header))  # noqa: T201
        for s in steps:
            print(f"{s.step_label}|{s.__class__.__name__}|{s.state.value!s}")  # noqa: T201

    def print_traceback(self, target_labels: Optional[List[str]] = None, *, full: bool = False) -> None:
        """Print the pipeline structure and status of the individual steps."""
        targets = self.get_targets(target_labels)
        for i, v in enumerate(targets):
            print(f"Target {i}: {v.step_label}")  # noqa: T201
            v.print_traceback(level=0, full=full)
            print()  # noqa: T201

    def _get_step(self, step_label: str) -> Optional[OpusPocusStep]:
        """TODO"""
        output = [s for s in self.steps if s.step_label == step_label]
        assert len(output) <= 1
        if output:
            return output[0]
        return None

    def get_targets(self, target_labels: Optional[List[str]] = None) -> List[OpusPocusStep]:
        """TODO"""
        if target_labels is not None:
            targets = []
            for t_label in target_labels:
                t_step = self._get_step(t_label)
                if t_step is None:
                    err_msg = f"Unknown pipeline step (label: {t_label}) requested as a target."
                    raise ValueError(err_msg)
                targets.append(t_step)
            return targets
        if self.targets:
            logger.info("No target steps were specified. Using default targets.")
            return self.targets
        err_msg = (
            "The pipeline does not contain any default target steps. "
            'Please specify the targets using the "--pipeline-targets" option.'
        )
        raise ValueError(err_msg)

    def get_dependants(self, step: OpusPocusStep) -> List[OpusPocusStep]:
        """TODO"""
        logger.info("dep: %s", step.step_label)
        ret = []
        for s in self.steps:
            for dep in s.dependencies.values():
                if dep is None:
                    continue
                if dep.step_label == step.step_label:
                    ret.append(s)
        return ret


class PipelineConfig(OmegaConf):
    """OmegaConf wrapper for storing pipeline config.

    TODO: convert to dataclass (?)

    """

    top_keys = ["global", "pipeline"]  # noqa: RUF012
    pipeline_keys = ["pipeline_dir", "steps", "targets"]  # noqa: RUF012

    @staticmethod
    def create(
        pipeline_dir: Path,
        pipeline_steps: Dict[str, OpusPocusStep],
        default_targets: List[OpusPocusStep],
    ) -> OmegaConf:
        return OmegaConf.create(
            {
                "pipeline": {
                    "pipeline_dir": str(pipeline_dir),
                    "steps": [s.get_parameters_dict(exclude_dependencies=False) for s in pipeline_steps.values()],
                    "targets": [s.step_label for s in default_targets],
                }
            }
        )

    @staticmethod
    def save(config: OmegaConf, config_path: Path) -> None:
        OmegaConf.save(config, f=config_path)

    @classmethod
    def load(cls, config_file: Path, overwrite_args: Optional[argparse.Namespace] = None) -> OmegaConf:  # noqa: ANN102
        config = OmegaConf.load(config_file)
        cls._valid_yaml_structure(config)
        return cls._overwrite(config, overwrite_args)

    @staticmethod
    def _overwrite(config, args):  # noqa: ANN001, ANN205, ARG004
        # TODO: implement overwrite mechanisms
        logger.warning("(NOT IMPLEMENTED) Overwriting the config file values with command line arguments.")
        return config

    @classmethod
    def _valid_yaml_structure(cls, config: OmegaConf) -> bool:  # noqa: ANN102
        """TODO."""
        # TODO: other checks?

        # Top has known keys
        for key in config.keys():  # noqa: SIM118
            if key not in cls.top_keys:
                logger.warning("Config file contains unsupported top key (%s). Ignoring...", key)
        # Contains "pipeline" top key
        if "pipeline" not in config:
            err_msg = 'Config file must contain pipeline definition of the ("pipeline" top key).'
            raise ValueError(err_msg)
        # Pipeline has known keys
        for key in config.pipeline.keys():  # noqa: SIM118
            if key not in cls.pipeline_keys:
                logger.warning("Pipeline definition contains unsupported key (%s). Ignoring...", key)
        # Contains "pipeline.steps" key
        if "steps" not in config.pipeline:
            err_msg = 'Config file must contain the list of steps ("pipeline.steps")'
            raise ValueError(err_msg)

        # All steps have an unique step_label
        steps = {}
        for step in config.pipeline.steps:
            if step.step_label in steps:
                err_msg = (
                    "Duplicate step_label found in pipeline definition. Please "
                    "make sure that each pipeline step has a unique step_label "
                    "value.\n"
                    f"Step-1: {step[step.step_label]},\nStep-{step}"
                )
                raise ValueError(err_msg)
            steps[step.step_label] = step
        return True
