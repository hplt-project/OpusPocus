import time
from pathlib import Path
from typing import List

import pytest
from attrs import define, field

from opuspocus import pipeline_steps
from opuspocus.pipeline_steps import OpusPocusStep, register_step
from tests.utils import teardown_step


@register_step("foo")
@define(kw_only=True)
class FooStep(OpusPocusStep):
    """Mock step for ligthweight unit testing.

    Waits for the SLEEP_TIME seconds and prints the output filenames into the respective output files.
    """

    dependency_step: "FooStep" = field(default=None)
    sleep_time: int = field(default=5)

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

    def _generate_cmd_file_contents(self) -> str:
        cmd = super()._generate_cmd_file_contents()
        cmd_split = cmd.split("\n")
        cmd_split = [cmd_split[0], "import tests.fixtures.steps", *cmd_split[1:]]
        return "\n".join(cmd_split)


@pytest.fixture()
def foo_step(tmp_path_factory):
    """Basic mock step without dependencies."""
    pipeline_steps.STEP_INSTANCE_REGISTRY = {}
    pipeline_dir = tmp_path_factory.mktemp("foo.pipeline")
    step = FooStep.build_step(step="foo", step_label="foo.test", pipeline_dir=pipeline_dir)
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
    pipeline_steps.STEP_INSTANCE_REGISTRY = {}
    step = FooStep.build_step(
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
