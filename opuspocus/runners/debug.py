import logging
import os
from pathlib import Path
from typing import List, Optional

from opuspocus.pipeline_steps import StepState, load_step
from opuspocus.runners import OpusPocusRunner, TaskId
from opuspocus.utils import RunnerResources, clean_dir

logger = logging.getLogger(__name__)


class DebugRunner(OpusPocusRunner):
    """Executes the pipeline and waits for its termination in the current
    process.

    This runner is aimed at more comprehensible (isolated) pipeline testing.
    This runner is not registered with the other runner implementation for
    two reasons:
        1. It does not fully implement the full OpusPocusRunner inteface.
        2. It does not follow the OpusPocusRunner philosophy, that is,
            the execution happens in the same process as the pipeline manager
            execution.
    """

    def __init__(
        self,
        runner: str,
        pipeline_dir: Path,
    ):
        super().__init__(
            runner=runner,
            pipeline_dir=pipeline_dir,
        )

    def submit_task(
        self,
        cmd_path: Path,
        target_file: Optional[Path] = None,
        dependencies: Optional[List[TaskId]] = None,
        step_resources: Optional[RunnerResources] = None,
        stdout_file: Optional[Path] = None,
        stderr_file: Optional[Path] = None,
    ) -> TaskId:
        """TODO"""
        step_label = cmd_path.parts[-2]

        # Return an already instantiated step
        step = load_step(step_label, self.pipeline_dir)

        # Process a specific target file
        if target_file is not None:
            os.environ = step_resources.get_env_dict()
            step.command(target_file)
            return TaskId(file_path=target_file, id=-1)

        step.set_state(StepState.RUNNING)
        step.command_preprocess()

        # Recursively process all the target files
        for t_file in step.get_command_targets():
            if t_file.exists():
                continue
            self.submit_task(
                cmd_path=cmd_path,
                target_file=t_file,
                dependencies=None,
                step_resources=self.get_resources(step),
            )
        step.command_postprocess()
        clean_dir(step.tmp_dir)
        step.set_state(StepState.DONE)

        return TaskId(file_path=target_file, id=-1)
