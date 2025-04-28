import inspect
import logging
import os
from typing import Dict, List

logger = logging.getLogger(__name__)


class RunnerResources:
    """Runner-agnostic resources object.

    This class aims at unifying various ways different schedulers represent
    resources and resource variables.

    TODO(varisd): add more resource attributes if necessary
    """

    def __init__(
        self,
        cpus: int = 1,
        gpus: int = 0,
        mem: str = "1g",
    ) -> None:
        self.cpus = cpus
        self.gpus = gpus
        self.mem = mem

    @classmethod
    def list_parameters(cls: "RunnerResources") -> List[str]:
        """List all represented parameters."""
        return [param for param in inspect.signature(cls.__init__).parameters if param != "self"]

    def overwrite(self, resources: "RunnerResources") -> "RunnerResources":
        """Overwrite the resources using a different RunnerResources object."""
        res_dict = {}
        for param in self.list_parameters():
            res_dict[param] = getattr(self, param)
            if hasattr(resources, param):
                res_dict[param] = getattr(resources, param)
        return RunnerResources(**res_dict)

    @classmethod
    def get_env_name(cls: "RunnerResources", name: str) -> str:
        """Get the environment name of a specific resource parameter."""
        assert name in cls.list_parameters()
        return f"OPUSPOCUS_{name}"

    def get_env_dict(self) -> Dict[str, str]:
        """Get the dictionary with the resource environment variables."""
        env_dict = {}
        for param in self.list_parameters():
            param_val = getattr(self, param)
            if param_val is not None:
                env_dict[self.get_env_name(param)] = str(param_val)
        return {**os.environ.copy(), **env_dict}
