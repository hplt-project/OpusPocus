from typing import Any, Dict, List, Optional

import argparse
from pathlib import Path

from opuspocus.runners import (
    OpusPocusRunner,
    RunnerOutput,
    RunnerResources,
    register_runner
)

SLEEP_TIME = 0.1


@register_runner('bash')
class BashRunner(OpusPocusRunner):
    """TODO"""

    def __init__(
        self,
        args: argparse.Namespace,
    ):
        super().__init__(args)

    def submit(
        cmd_path: Path,
        file_list: Optional[List[str]] = None,
        dependencies: Optional[List[BashOutput]] = None,
        step_resources: Optional[RunnerResources] = None
    ) -> List[BashOutput]:
        if dependencies:
            dependencies = [dep.pid for dep in dependencies]

        # TODO: can we replace this with a proper Python API?
        cmd = ['bash']

        if dependencies:
            # TODO: finish this
            pass

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False
        )
        return [BashOutput(proc.pid)]


class BashOutput(RunnerOutput):
    """TODO"""

    def __init__(self, process_id: int):
        self.pid = process_id

    def __str__(self) -> str:
        return str(self.pid)
