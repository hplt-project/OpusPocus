import time
from pathlib import Path
from typing import List, Optional

import pytest

from opuspocus.pipeline_steps import OpusPocusStep, build_step, register_step
from tests.utils import teardown_step


@register_step("foo")
class FooStep(OpusPocusStep):
    """Mock step for ligthweight unit testing.

    Waits for the SLEEP_TIME seconds and prints the output filenames into the respective output files.
    """

    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        dependency_step: Optional["FooStep"] = None,
        sleep_time: int = 5,
    ) -> None:
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            dependency_step=dependency_step,
            sleep_time=sleep_time,
        )

    @property
    def dep_step(self) -> "FooStep":
        return self.dependencies["dependency_step"]

    def get_command_targets(self) -> List[Path]:
        return [Path(self.output_dir, f"out_{x}.txt") for x in ["A", "B"]]

    def get_output_str(self, outfile: Path) -> str:
        return outfile.stem + outfile.suffix

    def command(self, target_file: Path) -> None:
        time.sleep(self.sleep_time)
        with target_file.open("w") as fh:
            print(self.get_output_str(target_file), file=fh)

    def compose_command(self) -> str:
        cmd = super().compose_command()
        cmd_split = cmd.split("\n")
        cmd_split = [cmd_split[0], "import tests.fixtures.steps", *cmd_split[1:]]
        return "\n".join(cmd_split)


@pytest.fixture()
def foo_step(tmp_path_factory):
    """Basic mock step without dependencies."""
    pipeline_dir = tmp_path_factory.mktemp("foo.pipeline")
    step = build_step(step="foo", step_label="foo.test", pipeline_dir=pipeline_dir)
    yield step

    teardown_step(step)


@pytest.fixture()
def foo_step_inited(foo_step):
    """Basic mock step without dependencies (INITED)."""
    foo_step.init_step()
    return foo_step


@pytest.fixture()
def bar_step(foo_step):
    """Basic mock step with a single dependency."""
    step = build_step(
        step="foo",
        step_label="bar.test",
        pipeline_dir=foo_step.pipeline_dir,
        **{"dependency_step": foo_step},
    )
    yield step

    teardown_step(step)


@pytest.fixture()
def bar_step_inited(bar_step):
    """Basic mock step with a single dependency (INITED)."""
    bar_step.init_step()
    return bar_step
