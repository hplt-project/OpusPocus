from typing import Dict, List, Optional

import inspect
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class RunnerResources(object):
    """Runner-agnostic resources object.

    TODO
    """

    def __init__(
        self,
        cpus: int = 1,
        gpus: Optional[int] = None,
        mem: Optional[str] = None,
        partition: Optional[str] = None,
        account: Optional[str] = None,
    ):
        self.cpus = cpus
        self.gpus = gpus
        self.mem = mem
        self.partition = partition
        self.account = account

    @classmethod
    def list_parameters(cls) -> List[str]:
        """TODO"""
        return [
            param for param in inspect.signature(cls.__init__).parameters
            if param != 'self'
        ]

    def overwrite(self, resource_overwrite: 'RunnerResources') -> 'RunnerResources':
        params = {}
        for param in self.list_parameters():
            val = getattr(resource_overwrite, param)
            if val is None:
                val = getattr(self, param)
            params[param] = val
        return RunnerResources(**params)

    def to_json(self, json_path: Path) -> None:
        """Serialize the object (to JSON).

        TODO
        """
        json_dict = {
            param: getattr(self, param)
            for param in self.list_parameters()
        }
        json.dump(json_dict, open(json_path, 'w'), indent=2)

    @classmethod
    def from_json(cls, json_path: Path) -> 'RunnerResources':
        """TODO"""
        json_dict = json.load(open(json_path, 'r'))

        cls_params = cls.list_parameters()
        params = {}
        for k, v in json_dict.items():
            if k not in cls_params:
                logger.warn('Resource {} not supported. Ignoring'.format(k))
            params[k] = v
        return RunnerResources(**params)

    @classmethod
    def get_env_name(cls, name) -> str:
        """TODO"""
        assert name in self.list_parameters()
        return 'OPUSPOCUS_{}'.format(name)

    def get_env_dict(self) -> Dict[str, str]:
        env_dict = {}
        for param in self.list_parameters():
            param_val = getattr(self, param)
            if param_val is not None:
                env_dict[self.get_env_name(param)] = str(param_val)
        return env_dict
