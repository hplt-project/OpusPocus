from typing import Any, Dict, List, Optional, Tuple
from abc import abstractmethod

import argparse
import logging
import yaml
from omegaconf import OmegaConf
from omegaconf.listconfig import ListConfig
from omegaconf.dictconfig import DictConfig
from pathlib import Path

from opuspocus.pipeline_steps import (
    build_step,
    load_step,
    OpusPocusStep
)

logger = logging.getLogger(__name__)


class OpusPocusPipeline(object):
    """Base class for OpusPocus pipelines."""
    config_file = 'pipeline.config'
    variables_file = 'pipeline.variables'

    @staticmethod
    def add_args(parser):
        """Add pipeline-specific arguments to the parser."""
        pass

    def __init__(
        self,
        pipeline: str,
        args: argparse.Namespace,
        pipeline_dir: Optional[Path] = None,
        pipeline_config_path: Optional[Path] = None,
    ):
        """TODO: describe the overwrite order"""
        self.pipeline = pipeline
        if pipeline_dir is None and pipeline_config_path is None:
            raise ValueError(
                'Pipeline initialization requires to be provided either '
                '"pipeline_dir" or "pipeline_config" path value.'
            )

        # Load pipeline config
        pipeline_config = None
        if pipeline_config_path is not None:
            pipeline_config = PipelineConfig.load(pipeline_config_path)

        # Resolve global pipeline_dir
        self.pipeline_dir = pipeline_dir
        print(pipeline_dir)
        if self.pipeline_dir is None:
            self.pipeline_dir = getattr(
                pipeline_config.pipeline, 'pipeline_dir', None
            )
            if self.pipeline_dir is None:
                raise ValueError(
                    'You must specify either "--pipeline-dir" option or the '
                    '"pipeline.pipeline_dir" in the "--pipeline-config" file.'
                )

        # Construct the pipeline graph
        self.pipeline_graph, self.default_targets = self.build_pipeline_graph(
            args, self.pipeline_dir, pipeline_config
        )

        # Create the pipeline config without the (global) wildcards
        # Actually set the class attribute
        self.pipeline_config = PipelineConfig.create(
            self.pipeline_dir, self.pipeline_graph, self.default_targets
        )

    @classmethod
    def build_pipeline(
        cls,
        pipeline: str,
        pipeline_dir: Path,
        pipeline_config_path: Path,
        args: argparse.Namespace,
    ) -> 'OpusPocusPipeline':
        """Build a specified pipeline instance.

        Args:
            pipeline (str): pipeline class name in the registry
            pipeline_config (Path): TODO
            args (argparse.Namespace): parsed command-line arguments

        Returns:
            An instance of the specified pipeline class.
        """
        return cls(pipeline, args, pipeline_dir, pipeline_config_path)

    def save_pipeline(self) -> None:
        """Save the pipeline information.

        Saves the dependency structure of the pipeline steps, pipeline target
        steps and pipeline parameters in their respective YAML files.
        """
        PipelineConfig.save(
            self.pipeline_config,
            Path(self.pipeline_dir, self.config_file)
        )

        yaml.dump(
            self.get_variables_dict(),
            open(Path(self.pipeline_dir, self.variables_file), 'w')
        )

    def get_variables_dict(self) -> Dict[str, Any]:
        """TODO: revise this implementation"""
        return { 'pipeline': self.pipeline }

    def build_pipeline_graph(
        self,
        args: argparse.Namespace,
        pipeline_dir: Optional[Path] = None,
        pipeline_config: Optional[OmegaConf] = None
    ) -> Tuple[Dict[str, OpusPocusStep], List[OpusPocusStep]]:
        """TODO"""
        if pipeline_config is None:
            logger.info(
                '"--pipeline-config" was not provided. Building pipeline '
                'using the "._build_pipeline_graph" method.'
            )
            return self._build_pipeline_graph(args)

        logger.info(
            'Building {} pipeline from the config.'.format(self.pipeline)
        )
        if callable(getattr(self, '_build_pipeline_graph', None)):
            logger.warn(
                'Pipeline {} has a hard-coded pipeline structure in '
                '{}._build_pipeline_graph() method. Overwriting '
                'the structure with the provided config.'.format(
                    self.pipeline,
                    self.__class__.__name__,
                )
            )
        return self._build_pipeline_graph_from_config(
            args, pipeline_dir, pipeline_config
        )

    def _build_pipeline_graph_from_config(
        self,
        args: argparse.Namespace,
        pipeline_dir: Path,
        pipeline_config: OmegaConf
    ) -> Tuple[Dict[str, OpusPocusStep], List[OpusPocusStep]]:
        """TODO"""
        pipeline_steps = {} # type: Dict[str, OpusPocusStep]
        pipeline_steps_configs = {
            s.step_label: OmegaConf.to_object(s)
            for s in pipeline_config.pipeline.steps
        }
        def _build_step_inner(step_label: str) -> OpusPocusStep:
            """Build step and its dependencies."""
            if step_label in pipeline_steps:
                return pipeline_steps[step_label]

            # Create the arguments for the step instance initialization
            step_args = {}
            logger.info('Building arguments for {}.'.format(step_label))
            for k, v in pipeline_steps_configs[step_label].items():
                # Simply assing the value if None or not a dependency parameter
                if '_step' not in k or v is None:
                    step_args[k] = v
                else:
                    if v not in pipeline_steps_configs:
                        raise ValueError(
                            'Step "{}" has an undefined dependency "{}={}".'
                            .format(step_label, k, v)
                        )
                    step_args[k] = _build_step_inner(v)

            # Set default (global) pipeline_dir
            if 'pipeline_dir' not in pipeline_steps_configs[step_label]:
                step_args['pipeline_dir'] = pipeline_dir

            pipeline_steps[step_label] = build_step(**step_args)
            return pipeline_steps[step_label]

        # Create pipeline step objects
        for step_label in pipeline_steps_configs.keys():
            _build_step_inner(step_label)

        default_targets = []
        if 'default_targets' in pipeline_config.pipeline:
            default_targets = [
                pipeline_steps[step_label]
                for step_label in pipeline_config.pipeline.default_targets
            ]
        return pipeline_steps, default_targets

    @abstractmethod
    def _build_pipeline_graph(
        self,
        args: argparse.Namespace,
    ) -> Tuple[Dict[str, OpusPocusStep], List[OpusPocusStep]]:
        """TODO"""
        raise NotImplementedError()

    def init(self) -> None:
        """Initialize the pipeline."""
        for _, v in self.pipeline_graph.items():
            v.init_step()
        self.save_pipeline()

    def traceback(
        self,
        targets: List[str] = None,
        full: bool = False
    ) -> None:
        """Print the pipeline structure and status of the individual steps."""
        targets = self.get_targets(targets)
        for i, v in enumerate(targets):
            print('Target {}: {}'.format(i, v.step_label))
            v.traceback_step(level=0, full=full)
            print('')

    def get_targets(self, targets: List[str] = None):
        if targets is not None:
            return targets
        if self.default_targets is not None:
            logger.info(
                'No target steps were specified. Using default targets.'
            )
            return self.default_targets
        raise ValueError(
            'The pipeline does not contain any default target steps. '
            'Please specify the targets using the "--pipeline-targets" '
            'option.'
        )


class PipelineConfig(OmegaConf):
    """OmegaConf wrapper for storing pipeline config.

    TODO

    """
    top_keys = ['global', 'pipeline']
    pipeline_keys = ['pipeline_dir', 'steps', 'default_targets']

    @staticmethod
    def create(
        pipeline_dir: Path,
        pipeline_steps: Dict[str, OpusPocusStep],
        default_targets: List[OpusPocusStep]
    ) -> OmegaConf:
        return OmegaConf.create({
            'pipeline': {
                'pipeline_dir': str(pipeline_dir),
                'steps': [
                    s.get_parameters_dict(exclude_dependencies=False)
                    for s in pipeline_steps.values()
                ],
                'default_targets': [s.step_label for s in default_targets]
            }
        })

    @staticmethod
    def save(config: OmegaConf, config_path: Path) -> None:
        OmegaConf.save(config, f=config_path)

    @classmethod
    def load(
        cls,
        config_file: Path,
        overwrite_args: Optional[argparse.Namespace] = None
    ) -> OmegaConf:
        config = OmegaConf.load(config_file)
        cls._valid_yaml_structure(config)
        return cls._overwrite(config, overwrite_args)

    @staticmethod
    def _overwrite(config, args):
        # TODO: implement overwrite mechanisms
        logger.warn(
            '(NOT IMPLEMENTED) Overwriting the config file values with '
            'command line arguments.'
        )
        return config

    @classmethod
    def _valid_yaml_structure(cls, config: OmegaConf) -> bool:
        """TODO."""
        # TODO: other checks?

        # Top has known keys
        for key in config.keys():
            if key not in cls.top_keys:
                logger.warn(
                    'Config file contains unsupported top key ({}). Ignoring...'
                    .format(key)
                )
        # Contains "pipeline" top key
        if 'pipeline' not in config:
            raise ValueError(
                'Config file must contain pipeline definition '
                '("pipeline" top key).'
            )
        # Pipeline has known keys
        for key in config.pipeline.keys():
            if key not in cls.pipeline_keys:
                logger.warn(
                    'Pipeline definition contains unsupported key ({}). '
                    'Ignoring...'.format(key)
                )
        # Contains "pipeline.steps" key
        if 'steps' not in config.pipeline:
            raise ValueError(
                'Config file must contain the list of steps ("pipeline.steps")'
            )

        # All steps have an unique step_label
        steps = {}
        for step in config.pipeline.steps:
            if step.step_label in steps:
                raise ValueError(
                    'Duplicate step_label found in pipeline definition. Please '
                    'make sure that each pipeline step has a unique step_label '
                    'value.\n'
                    'Step-1: {},\nStep-{}'.format(step[step.step_label], step)
                )
            steps[step.step_label] = step
        return True
