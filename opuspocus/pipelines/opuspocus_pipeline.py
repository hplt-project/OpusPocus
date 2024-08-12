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
        cls,  # noqa: ANN102
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
        cls,  # noqa: ANN102
        pipeline_dir: Path,
    ) -> "OpusPocusPipeline":
        """TODO"""
        if not pipeline_dir.exists():
            raise FileNotFoundError(f"Pipeline directory ({pipeline_dir}) does not exist.")  # noqa: EM102, TRY003
        if not pipeline_dir.is_dir():
            raise NotADirectoryError(f"{pipeline_dir} is not a directory.")  # noqa: EM102, TRY003

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
            logger.info(f"Creating parameters to build {step_label} object.")  # noqa: G004
            for k, v in pipeline_steps_configs[step_label].items():
                # Simply assing the value if None or not a dependency parameter
                if "_step" not in k or v is None:
                    step_args[k] = v
                else:
                    if v not in pipeline_steps_configs:
                        raise ValueError(f'Step "{step_label}" has an undefined dependency "{k}={v}".')  # noqa: EM102, TRY003
                    step_args[k] = _build_step_inner(v)

            # Set default (global) pipeline_dir
            if "pipeline_dir" not in pipeline_steps_configs[step_label]:
                step_args["pipeline_dir"] = pipeline_dir

            try:
                pipeline_steps[step_label] = build_step(**step_args)
            except Exception as e:
                print(f"Step parameters:\n{step_args}")  # noqa: T201
                raise e  # noqa: TRY201

            return pipeline_steps[step_label]

        # Create pipeline step objects
        for step_label in pipeline_steps_configs.keys():  # noqa: SIM118
            _build_step_inner(step_label)

        default_targets = []
        if "default_targets" in pipeline_config.pipeline:
            default_targets = [pipeline_steps[step_label] for step_label in pipeline_config.pipeline.default_targets]
        return pipeline_steps, default_targets

    def init(self) -> None:
        """Initialize the pipeline."""
        logger.info(f"Initializing pipeline ({self.pipeline_dir})")  # noqa: G004
        for _, v in self.pipeline_graph.items():  # noqa: PERF102
            v.init_step()

        self.save_pipeline()
        logger.info(f"Pipeline ({self.pipeline_dir}) initialized successfully.")  # noqa: G004

    def status(self, steps: List[OpusPocusStep]) -> None:
        for s in steps:
            print(f"{s.step_label}: {str(s.state)}")  # noqa: T201

    def traceback(self, targets: List[str] = None, full: bool = False) -> None:  # noqa: FBT001, FBT002
        """Print the pipeline structure and status of the individual steps."""
        targets = self.get_targets(targets)
        for i, v in enumerate(targets):
            print(f"Target {i}: {v.step_label}")  # noqa: T201
            v.traceback_step(level=0, full=full)
            print()  # noqa: T201

    def get_targets(self, targets: List[str] = None):  # noqa: ANN201
        if targets is not None:
            return targets
        if self.default_targets is not None:
            logger.info("No target steps were specified. Using default targets.")
            return self.default_targets
        raise ValueError(  # noqa: TRY003
            "The pipeline does not contain any default target steps. "  # noqa: EM101
            'Please specify the targets using the "--pipeline-targets" '
            "option."
        )

    def __eq__(self, other):  # noqa: ANN001, ANN204
        """Object comparison logic."""
        if self.pipeline_graph != other.pipeline_graph:
            return False
        if len(self.default_targets) != len(other.default_targets):
            return False
        for target in self.default_targets:  # noqa: SIM110
            if target not in other.default_targets:
                return False
        # if self.pipeline_config != other.pipeline_config:
        #    return False
        return True


class PipelineConfig(OmegaConf):
    """OmegaConf wrapper for storing pipeline config.

    TODO: convert to dataclass (?)

    """

    top_keys = ["global", "pipeline"]
    pipeline_keys = ["pipeline_dir", "steps", "default_targets"]

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
                logger.warning(f"Config file contains unsupported top key ({key}). Ignoring...")  # noqa: G004
        # Contains "pipeline" top key
        if "pipeline" not in config:
            raise ValueError("Config file must contain pipeline definition " '("pipeline" top key).')  # noqa: EM101, ISC001, TRY003
        # Pipeline has known keys
        for key in config.pipeline.keys():  # noqa: SIM118
            if key not in cls.pipeline_keys:
                logger.warning(f"Pipeline definition contains unsupported key ({key}). " "Ignoring...")  # noqa: G004, ISC001
        # Contains "pipeline.steps" key
        if "steps" not in config.pipeline:
            raise ValueError('Config file must contain the list of steps ("pipeline.steps")')  # noqa: EM101, TRY003

        # All steps have an unique step_label
        steps = {}
        for step in config.pipeline.steps:
            if step.step_label in steps:
                raise ValueError(  # noqa: TRY003
                    "Duplicate step_label found in pipeline definition. Please "  # noqa: EM102
                    "make sure that each pipeline step has a unique step_label "
                    "value.\n"
                    f"Step-1: {step[step.step_label]},\nStep-{step}"
                )
            steps[step.step_label] = step
        return True
