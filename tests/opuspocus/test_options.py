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


@pytest.fixture()
def foo_pipeline_conf_or_dir_opt(request, parser_fn):
    parser_key = next(iter([k for k, v in PARSER_FUNCTIONS.items() if v == parser_fn]))
    if parser_key == "run":
        return "--pipeline-config " + str(request.getfixturevalue("foo_pipeline_config_file"))
    return "--pipeline-dir " + str(request.getfixturevalue("foo_pipeline_inited").pipeline_dir)


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
def test_parse_args(cmd, argv, parser_fn, foo_pipeline_inited):
    """Test parsing of subcommand-specific CLI options."""
    attr_name = "_".join(argv.split(" ")[0].split("-")[2:])
    if cmd == "run":
        argv += " --runner bash"
    if cmd == "traceback":
        path = str(foo_pipeline_inited.pipeline_dir)
        argv += f" --pipeline-dir {path}"

    parser_fn = PARSER_FUNCTIONS[cmd]
    config = parser_fn(argv.split(" "))
    assert hasattr(config.cli_options, attr_name)


@pytest.mark.parametrize("argv", ["--log-level info", "--log-level debug"])
def test_parse_general_args(argv, parser_fn, foo_pipeline_conf_or_dir_opt):
    """Test parsing of general CLI options."""
    attr_name = "_".join(argv.split(" ")[0].split("-")[2:])
    config = parser_fn((foo_pipeline_conf_or_dir_opt + " " + argv).split(" "))
    assert hasattr(config.cli_options, attr_name)


@pytest.mark.parametrize("argv", ["--targets foo_target", "--targets foo_target_1 foo_target_2"])
def test_parse_pipeline_args(argv, parser_fn, foo_pipeline_conf_or_dir_opt):
    """Test parsing of pipeline-specific CLI options."""
    attr_name = "_".join(argv.split(" ")[0].split("-")[2:])
    config = parser_fn((foo_pipeline_conf_or_dir_opt + " " + argv).split(" "))
    assert hasattr(config.pipeline, attr_name)


def test_parse_run_runner_name(runner):
    """Test parsing of runner name from CLI."""
    config = parse_run_args(["--runner", runner])
    assert hasattr(config.runner, "runner")
    assert config.runner.runner == runner


@pytest.mark.parametrize("argv", ["--runner-resources {}"])
def test_parse_run_runner_args(argv, runner):
    """Test parsing of runner-specific CLI options."""
    attr_name = "_".join(argv.split(" ")[0].split("-")[2:])
    config = parse_run_args(["--runner", runner, *argv.split(" ")])
    assert hasattr(config.runner, attr_name)


@pytest.mark.parametrize("argv", ["foo.attr=bar", "foo.foo.attr=bar"])
def test_parse_step_args(argv, parser_fn, foo_pipeline_conf_or_dir_opt):
    """Test other CLI arguments (step attribute overrides)."""
    key, value = argv.split("=")

    config = parser_fn((foo_pipeline_conf_or_dir_opt + " " + argv).split(" "))
    assert hasattr(config, "steps")
    assert isinstance(config.steps, DictConfig)

    step_label = ".".join(key.split(".")[:-1])
    step_param = key.split(".")[-1]
    assert step_label in config.steps
    assert step_param in config.steps[step_label]
    assert config.steps[step_label][step_param] == value


@pytest.mark.parametrize("argv", ["pipeline.attr=bar", "runner.attr=bar"])
def test_parse_nonstep_args_alt(argv, parser_fn, foo_pipeline_conf_or_dir_opt):
    """Test other CLI arguments (direct pipeline/runnner attribute overrides)."""
    key, value = argv.split("=")

    config = parser_fn((foo_pipeline_conf_or_dir_opt + " " + argv).split(" "))
    label, param = key.split(".")
    assert hasattr(config, label)
    assert hasattr(getattr(config, label), param)
    assert getattr(getattr(config, label), param) == value


@pytest.mark.parametrize("argv", ["foo=bar", "pipeline.steps=bar"])
def test_parse_invalid_args_alt_fail(argv, parser_fn, foo_pipeline_conf_or_dir_opt):
    """Test handling of invalid attribute overrides (step without attribute, direct pipeline.steps overrride)>"""
    with pytest.raises(AttributeError):
        parser_fn((foo_pipeline_conf_or_dir_opt + " " + argv).split(" "))
