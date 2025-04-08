import yaml
from omegaconf import OmegaConf

from typing import Dict, List
from attrs import define, field


@define(kw_only=True)
class PipelineConfig():
    """Wrapper class for the OmegaConf representation of pipeline config.

    Instances of this class should not be created directly using the __init__() method.
    Instead use the PipelineConfig.load_config() static loader or the PipelineConfig.create_config() static method.

    TODO(varisd): Improve config validation, e.g. add validation of the step/runner initialization attributes.
    """
    config: DictConfig = field(factory=dict)
    _required_fields = ["pipeline", "pipeline.steps", "runner"]

    @config.validator
    def has_required_fields(self, attribute: Attribute, value: Path):
        """Validates the OpusPocus config structure."""
        for req_field in self._required_fields:
            if OmegaConf.select(self.config, req_field) is None:
                err_msg = f"Loaded PipelineConfig is missing the required `{req_field}` field."
                raise ValueError(err_msg)

    @staticmethod
    def load_config(config_file: Path) -> PipelineConfig:
        """Load the config from a file."""
        return PipelineConfig(config=OmegaConf.load(config_file))

    @staticmethod
    def create_config(config_dict: Union[Dict[str, Any], DictConfig]) -> PipelineConfig:
        """Create a new pipeline config using its dictionary definition."""
        return PipelineConfig(config=config_dict)

    def save_config(self, config_path: Path) -> None:
        """Save the existing pipeline config."""
        OmegaConf.save(self.config, f=config_path)

    def select(self, key: str) -> Any:
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

    def update(self, key: str, value: Any) -> None:
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

    @proprty
    def pipeline_dir(self) -> Path:
        return Path(self.pipeline.pipeline_dir)

    @property
    def steps(self) -> Dict[str, DictConfig]:
        return {s.step_label: s for s in self.pipeline.steps}

    @property
    def runner(self) -> DictConfig:
        return self.config.runner:
