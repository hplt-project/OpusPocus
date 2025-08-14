import pytest
from omegaconf import DictConfig

from opuspocus.options import parse_run_args, parse_status_args, parse_stop_args, parse_traceback_args
from opuspocus.runners import RUNNER_REGISTRY

PARSER_FUNCTIONS = {
    "run": parse_run_args,
    "stop": parse_stop_args,
    "status": parse_status_args,
    "traceback": parse_traceback_args,
}


@pytest.fixture(params=["run", "stop", "status", "traceback"])
def parser_fn(request):
    return PARSER_FUNCTIONS[request.param]


@pytest.fixture(params=RUNNER_REGISTRY.keys())
def runner(request):
    return request.param


@pytest.mark.parametrize(
    ("cmd", "argv"),
    [
        ("run", "--reinit"),
        ("run", "--reinit-failed"),
        ("run", "--stop-previous-run"),
        ("run", "--resubmit-finished-subtasks"),
        ("traceback", "--verbose"),
    ],
)
def test_parse_args(cmd, argv, foo_pipeline_dir):
    """Test parsing of subcommand-specific CLI options."""
    attr_name = "_".join(argv.split(" ")[0].split("-")[2:])
    if cmd == "run":
        argv += " --runner bash"
    if cmd == "traceback":
        path = str(foo_pipeline_dir)
        argv += f" --pipeline-dir {path}"

    parser_fn = PARSER_FUNCTIONS[cmd]
    args = parser_fn(argv.split(" "))
    assert hasattr(args.cli_options, attr_name)


@pytest.mark.parametrize("argv", ["--log-level info", "--log-level debug"])
def test_parse_general_args(argv, parser_fn, foo_pipeline_dir):
    """Test parsing of general CLI options."""
    attr_name = "_".join(argv.split(" ")[0].split("-")[2:])
    argv += " --pipeline-dir " + str(foo_pipeline_dir)
    args = parser_fn(argv.split(" "))
    assert hasattr(args.cli_options, attr_name)


@pytest.mark.parametrize(
    "argv", ["--targets foo_target", "--targets foo_target_1 foo_target_2"]
)
def test_parse_pipeline_args(argv, parser_fn, foo_pipeline_dir):
    """Test parsing of pipeline-specific CLI options."""
    attr_name = "_".join(argv.split(" ")[0].split("-")[2:])
    argv += " --pipeline-dir " + str(foo_pipeline_dir)
    args = parser_fn(argv.split(" "))
    assert hasattr(args.pipeline, attr_name)


def test_parse_run_runner_name(runner, foo_pipeline_dir):
    """Test parsing of runner name from CLI."""
    args = parse_run_args(["--runner", runner])
    assert hasattr(args.runner, "runner")
    assert args.runner.runner == runner


@pytest.mark.parametrize("argv", ["--runner-resources {}"])
def test_parse_run_runner_args(argv, runner):
    """Test parsing of runner-specific CLI options."""
    attr_name = "_".join(argv.split(" ")[0].split("-")[2:])
    args = parse_run_args(["--runner", runner, *argv.split(" ")])
    assert hasattr(args.runner, attr_name)


@pytest.mark.parametrize("argv", ["foo.attr=bar", "foo.foo.attr=bar"])
def test_parse_step_args(argv, parser_fn, foo_pipeline_dir):
    """Test other CLI arguments (step attribute overrides)."""
    key, value = argv.split("=")

    argv = "--pipeline-dir " + str(foo_pipeline_dir) + " " + argv
    args = parser_fn(argv.split(" "))
    assert hasattr(args, "steps")
    assert isinstance(args.steps, DictConfig)

    step_label = ".".join(key.split(".")[:-1])
    step_param = key.split(".")[-1]
    assert step_label in args.steps
    assert step_param in args.steps[step_label]
    assert args.steps[step_label][step_param] == value


@pytest.mark.parametrize("argv", ["pipeline.attr=bar", "runner.attr=bar"])
def test_parse_nonstep_args_alt(argv, parser_fn, foo_pipeline_dir):
    """Test other CLI arguments (direct pipeline/runnner attribute overrides)."""
    key, value = argv.split("=")

    argv = "--pipeline-dir " + str(foo_pipeline_dir) + " " + argv
    args = parser_fn(argv.split(" "))
    label, param = key.split(".")
    assert hasattr(args, label)
    assert hasattr(getattr(args, label), param)
    assert getattr(getattr(args, label), param) == value


@pytest.mark.parametrize("argv", ["foo=bar", "pipeline.steps=bar"])
def test_parse_invalid_args_alt_fail(argv, parser_fn, foo_pipeline_dir):
    """Test handling of invalid attribute overrides (step without attribute, direct pipeline.steps overrride)>"""
    argv = "--pipeline-dir " + str(foo_pipeline_dir) + " " + argv
    with pytest.raises(AttributeError):
        parser_fn(argv.split(" "))
