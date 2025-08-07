from argparse import Namespace
from pathlib import Path
from typing import Any, Dict, Union

from attrs import Attribute, define, field
from omegaconf import DictConfig, OmegaConf

from opuspocus.pipeline_steps import OpusPocusStep


@define(kw_only=True)
class PipelineConfig:
    """Wrapper class for the OmegaConf representation of pipeline config.

    Instances of this class should not be created directly using the __init__() method.
    Instead use the PipelineConfig.load() static loader or the PipelineConfig.create() static method.

    TODO(varisd): Improve config validation, e.g. add validation of the step/runner initialization attributes.
    """

    config: DictConfig = field(factory=dict)
    _required_fields = frozenset(["pipeline", "pipeline.steps", "runner"])

    @config.validator
    def has_required_fields(self, _: Attribute, value: DictConfig) -> None:
        """Validates the OpusPocus config structure."""
        for req_field in self._required_fields:
            if OmegaConf.select(value, req_field) is None:
                err_msg = f"Loaded PipelineConfig is missing the required `{req_field}` field."
                raise ValueError(err_msg)

    @classmethod
    def load(cls: "PipelineConfig", config_file: Path, args: Namespace = None) -> "PipelineConfig":
        """Load the config from a file."""
        config = OmegaConf.load(config_file)
        # overwrite runner config with command-line arguments
        for k, v in dict(args.runner).items():
            setattr(config.runner, k, v)
        # overwrite general pipeline config with command-line arguments
        for k, v in dict(args.pipeline).items():
            setattr(config.pipeline, k, v)
        # overwrite the configs of individual pipeline steps with command-line arguments
        label2idx = {config.pipeline.steps[idx].step_label: idx for idx in range(config.pipeline.steps)}
        for step_label in dict(args.steps).keys():
            for k, v in args.steps[step_label].items():
                idx = label2idx[step_label]
                setattr(config.pipeline.steps[idx], k, v)
        return cls(config=config)

    @classmethod
    def create(cls: "PipelineConfig", config_dict: Union[Dict[str, Any], DictConfig]) -> "PipelineConfig":
        """Create a new pipeline config using its dictionary definition."""
        config = config_dict
        if config_dict is not DictConfig:
            config = OmegaConf.create(config_dict)
        return cls(config=config)

    def save(self, config_path: Path) -> None:
        """Save the existing pipeline config."""
        OmegaConf.save(self.config, f=config_path)

    def select(self, key: str) -> Any:  # noqa: ANN401
        """Wrapper of the OmegaConf.select() method.

        This method enables a shotcut for accessing individual pipeline step configs using a direct access via
        step_label (e.g. <step_label>, or <step_label>.<step_atribute>).
        Otherwise it works exacly as the OmegaConf.select() method.

        Raises ValueError if the key is not found in the config.
        """
        key_split = key.split(".")
        if key_split[0] in self.steps:
            val = OmegaConf.select(self.steps[key_split[0]], ".".join(key_split[1:]))
        else:
            val = OmegaConf.select(self.config, key)
        if val is None:
            err_msg = f"PipelineConfig does not contain `{key}`."
            raise ValueError(err_msg)
        return val

    def update(self, key: str, value: Any) -> None:  # noqa: ANN401
        """Wrapper for updating an existing entry in the pipeline config.

        Raises ValueError if the key is not found in the config.
        """
        if OmegaConf.select(self.config, key) is None:
            err_msg = f"Pipeline config does not contain `{key}`."
            raise ValueError(err_msg)

        orig_type = type(OmegaConf.select(self.config, key))
        repl_type = type(value)
        if orig_type != repl_type:
            err_msg = (
                f"Original and replacement value must have same object type (Orig: {orig_type}, Repl: {repl_type})."
            )
            raise ValueError(err_msg)

        key_split = key.split(".")
        sub_dict = self.config
        for k in key_split[:-1]:
            sub_dict = sub_dict[k]

        sub_dict[key_split[-1]] = value

    def add_step(self, step: OpusPocusStep) -> None:
        if step.step_label in self.steps:
            err_msg = f"Step `{step.step_label}` is already in the pipeline config."
            raise ValueError(err_msg)
        self.pipeline.steps.append(step.get_parameters_dict(exclude_dependencies=False))

    @property
    def pipeline(self) -> DictConfig:
        return self.config.pipeline

    @property
    def pipeline_attrs(self) -> Dict[str, Any]:
        return {k: v for k, v in self.pipeline.items() if k not in ["steps", "targets"]}

    @property
    def pipeline_dir(self) -> Path:
        return Path(self.pipeline.pipeline_dir)

    @property
    def steps(self) -> Dict[str, DictConfig]:
        return {s.step_label: s for s in self.pipeline.steps}

    @property
    def runner(self) -> DictConfig:
        return self.config.runner
