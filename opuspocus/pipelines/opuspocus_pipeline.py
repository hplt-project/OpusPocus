import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from omegaconf import OmegaConf

from opuspocus.pipeline_steps import OpusPocusStep, build_step
from opuspocus.utils import file_path

logger = logging.getLogger(__name__)


class OpusPocusPipeline:
    """Base class for OpusPocus pipelines."""

    config_file = "pipeline.config"

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

    def __init__(
        self,
        pipeline_config_path: Path,
        pipeline_dir: Optional[Path] = None,
    ) -> None:
        """TODO: describe the overwrite order"""
        # Load pipeline config
        pipeline_config = PipelineConfig.load(pipeline_config_path)
        if pipeline_dir is not None:
            pipeline_config.pipeline.pipeline_dir = pipeline_dir

        # Construct the pipeline graph
        graph_tuple = self.build_pipeline_graph(pipeline_config)
        self.pipeline_graph = graph_tuple[0]
        self.default_targets = graph_tuple[1]

        # Create the pipeline config without the (global) wildcards
        # Actually set the class attribute
        self.pipeline_config = PipelineConfig.create(
            pipeline_config.pipeline.pipeline_dir,
            self.pipeline_graph,
            self.default_targets,
        )

    @property
    def pipeline_dir(self) -> Path:
        return self.pipeline_config.pipeline.pipeline_dir

    @property
    def steps(self) -> List[OpusPocusStep]:
        return self.pipeline_graph.values()

    @classmethod
    def build_pipeline(
        cls: "OpusPocusPipeline",
        pipeline_config_path: Path,
        pipeline_dir: Path,
    ) -> "OpusPocusPipeline":
        """Build a specified pipeline instance.

        Args:
            pipeline_config_path (Path): TODO
            pipeline_dir (Path): TODO

        Returns:
            An instance of the specified pipeline class.
        """
        return cls(pipeline_config_path, pipeline_dir)

    @classmethod
    def load_pipeline(
        cls: "OpusPocusPipeline",
        pipeline_dir: Path,
    ) -> "OpusPocusPipeline":
        """TODO"""
        if not pipeline_dir.exists():
            err_msg = f"Pipeline directory ({pipeline_dir}) does not exist."
            raise FileNotFoundError(err_msg)
        if not pipeline_dir.is_dir():
            err_msg = f"{pipeline_dir} is not a directory."
            raise NotADirectoryError(err_msg)

        pipeline_config_path = Path(pipeline_dir, cls.config_file)
        return cls(pipeline_config_path, pipeline_dir)

    def save_pipeline(self) -> None:
        """Save the pipeline information.

        Saves the dependency structure of the pipeline steps, pipeline target
        steps and pipeline parameters in their respective YAML files.
        """
        PipelineConfig.save(self.pipeline_config, Path(self.pipeline_dir, self.config_file))

    def build_pipeline_graph(
        self, pipeline_config: Optional[OmegaConf] = None
    ) -> Tuple[Dict[str, OpusPocusStep], List[OpusPocusStep]]:
        # TODO: make this into a separate class -> Pipeline will contain a
        #   Graph object (motivation: better testing, converting config
        #   to graph, code maintenance)
        """TODO"""
        pipeline_dir = pipeline_config.pipeline.pipeline_dir

        pipeline_steps = {}  # type: Dict[str, OpusPocusStep]
        pipeline_steps_configs = {s.step_label: OmegaConf.to_object(s) for s in pipeline_config.pipeline.steps}

        def _build_step_inner(step_label: str) -> OpusPocusStep:
            """Build step and its dependencies."""
            if step_label in pipeline_steps:
                return pipeline_steps[step_label]

            # Create the arguments for the step instance initialization
            step_args = {}
            logger.info("Creating parameters to build %s object.", step_label)
            for k, v in pipeline_steps_configs[step_label].items():
                # Simply assing the value if None or not a dependency parameter
                if "_step" not in k or v is None:
                    step_args[k] = v
                else:
                    if v not in pipeline_steps_configs:
                        err_msg = f"Step '{step_label}' has an undefined dependency '{k}={v}'."
                        raise ValueError(err_msg)
                    step_args[k] = _build_step_inner(v)

            # Set default (global) pipeline_dir
            if "pipeline_dir" not in pipeline_steps_configs[step_label]:
                step_args["pipeline_dir"] = pipeline_dir

            try:
                pipeline_steps[step_label] = build_step(**step_args)
            except Exception as e:
                logger.exception("Step parameters:\n%s", step_args)
                raise e  # noqa: TRY201

            return pipeline_steps[step_label]

        # Create pipeline step objects
        for step_label in pipeline_steps_configs:
            _build_step_inner(step_label)

        default_targets = []
        if "default_targets" in pipeline_config.pipeline:
            default_targets = [pipeline_steps[step_label] for step_label in pipeline_config.pipeline.default_targets]
        return pipeline_steps, default_targets

    def init(self) -> None:
        """Initialize the pipeline."""
        logger.info("Initializing pipeline (%s)", self.pipeline_dir)
        for v in self.pipeline_graph.values():
            v.init_step()

        self.save_pipeline()
        logger.info("Pipeline (%s) initialized successfully.", self.pipeline_dir)

    def status(self, steps: List[OpusPocusStep]) -> None:
        for s in steps:
            print(f"{s.step_label}: {s.state!s}")  # noqa: T201

    def traceback(self, targets: Optional[List[str]] = None, *, full: bool = False) -> None:
        """Print the pipeline structure and status of the individual steps."""
        targets = self.get_targets(targets)
        for i, v in enumerate(targets):
            print(f"Target {i}: {v.step_label}")  # noqa: T201
            v.traceback_step(level=0, full=full)
            print()  # noqa: T201

    def get_targets(self, targets: Optional[List[str]] = None) -> List[OpusPocusStep]:
        if targets is not None:
            return targets
        if self.default_targets is not None:
            logger.info("No target steps were specified. Using default targets.")
            return self.default_targets
        err_msg = (
            "The pipeline does not contain any default target steps. "
            'Please specify the targets using the "--pipeline-targets" option.'
        )
        raise ValueError(err_msg)

    def get_dependants(self, step: OpusPocusStep) -> List[OpusPocusStep]:
        return [s for s in self.steps if any(dep.step_label == step.step_label for dep in s.dependencies.values())]

    def __eq__(self, other: "OpusPocusPipeline") -> bool:
        """Object comparison logic."""
        if self.pipeline_graph != other.pipeline_graph:
            return False
        if len(self.default_targets) != len(other.default_targets):
            return False
        return all(target in other.default_targets for target in self.default_targets)
        # if self.pipeline_config != other.pipeline_config:
        #    return False
        return True


class PipelineConfig(OmegaConf):
    """OmegaConf wrapper for storing pipeline config.

    TODO: convert to dataclass (?)

    """

    top_keys = ["global", "pipeline"]  # noqa: RUF012
    pipeline_keys = ["pipeline_dir", "steps", "default_targets"]  # noqa: RUF012

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
                    "default_targets": [s.step_label for s in default_targets],
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
