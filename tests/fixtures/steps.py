import time
from pathlib import Path
from typing import List, Optional

import pytest

from opuspocus import pipeline_steps
from opuspocus.pipeline_steps import OpusPocusStep, build_step, register_step


@register_step("foo")
class FooStep(OpusPocusStep):
    SLEEP_TIME = 5

    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        dependency_step: Optional["FooStep"] = None,
    ) -> None:
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            dependency_step=dependency_step,
        )

    @property
    def dep_step(self) -> "FooStep":
        return self.dependencies["dependency_step"]

    def get_command_targets(self) -> List[Path]:
        return [Path(self.output_dir, f"out_{x}.txt") for x in ["A", "B"]]

    def get_output_str(self, outfile: Path) -> str:
        return outfile.stem + outfile.suffix

    def command(self, target_file: Path) -> None:
        time.sleep(self.SLEEP_TIME)
        with target_file.open("w") as fh:
            print(self.get_output_str(target_file), file=fh)

    def compose_command(self) -> str:
        cmd = super().compose_command()
        cmd_split = cmd.split("\n")
        cmd_split = [cmd_split[0], "import tests.fixtures.steps", *cmd_split[1:]]
        return "\n".join(cmd_split)


@pytest.fixture()
def foo_step(tmp_path_factory):
    pipeline_steps.STEP_INSTANCE_REGISTRY = {}
    pipeline_dir = tmp_path_factory.mktemp("foo.mock")
    return build_step(step="foo", step_label="foo.test", pipeline_dir=pipeline_dir)


@pytest.fixture()
def foo_step_inited(foo_step):
    foo_step.init_step()
    return foo_step


@pytest.fixture()
def bar_step(foo_step):
    return build_step(
        step="foo",
        step_label="bar.test",
        pipeline_dir=foo_step.pipeline_dir,
        **{"dependency_step": foo_step},
    )


@pytest.fixture()
def bar_step_inited(bar_step):
    bar_step.init_step()
    return bar_step
