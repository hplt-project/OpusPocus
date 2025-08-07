import pytest

from opuspocus.options import parse_run_args, parse_stop_args, parse_status_args, parse_traceback_args
from opuspocus.runners import RUNNER_REGISTRY
from opuspocus.utils import count_lines, file_line_index, open_file, read_shard

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
        ("run", "--stop-previous-run"),
        ("run", "--resubmit-finished-subtasks"),
        ("run", "--pipeline-config foo_path"),
        ("traceback", "--verbose")
    ]
)
def test_parse_args(cmd, argv):
    if cmd == "traceback":
        argv += " --pipeline-dir foo_dir"

    parser_fn = PARSER_FUNCTIONS[cmd]
    args = parser_fn(argv.split(" "))
    attr_name = "_".join(argv.split(" ")[0].split("-")[2:])
    assert hasattr(args, attr_name)


@pytest.mark.parametrize("argv", ["--log-level info","--log-level debug"])
def test_parse_general_args(argv, parser_fn):
    args = parser_fn(argv.split(" "))
    attr_name = "_".join(argv.split(" ")[0].split("-")[2:])
    assert hasattr(args, attr_name)


@pytest.mark.parametrize(
    "argv", ["--pipeline-dir foo_dir", "--targets foo_target", "--targets foo_target_1 foo_target_2"]
)
def test_parse_pipeline_args(argv, parser_fn):
    args = parser_fn(argv.split(" "))
    attr_name = "_".join(argv.split(" ")[0].split("-")[2:])
    assert hasattr(args.pipeline, attr_name)


def test_parser_runner_arg_runner(parser_fn, runner):
    args = parse_fn(["--runner", runner])
    assert hasattr(args, "runner")
    assert getattr(args.runner, "runner") == runner


@pytest.mark.parametrize(
    "argv", [""]
)
def test_parse_runner_args(argv, parser_fn, runner):
    args = parser_fn(["--runner", runner] + argv.split(" "))
    attr_name = "_".join(argv.split(" ")[0].split("-")[2:])
    assert hasattr(args.runner, attr_name)


@pytest.mark.parametrize("argv", ["foo.attr bar", "foo.foo.attr bar"])
def test_parse_step_args(argv, parser_fn):
    args = parser_fn(argv)
    assert hasattr(args, "steps")
    assert isinstance(getattr(args, "steps"), dict)

    key, value = argv.split(" ")
    step_label = ".".join(key.split(".")[:-1])
    step_param = key.split(".")[-1]
    assert step_label in args.steps
    assert step_param in args.steps[step_label]
    assert args.steps[step_label][step_param] == value


@pytest.mark.parametrize("argv", ["pipeline.attr bar", "runner.attr bar"])
def test_parse_nonstep_args_alt(argv, parser_fn):
    args = parser_fn(argv)
    key, value = argv.split(" ")
    label, param = key.split(".")
    assert hasattr(args, label)
    assert hasattr(getattr(args, label), param)
    assert getattr(getattr(args, label), param) == value


@pytest.mark.parametrize("argv", ["foo bar", "pipeline.steps bar"])
def test_parse_invalid_args_alt_fail(argv, parser_fn):
    with pytest.raises(ValueError):
        parser_fn(argv)
