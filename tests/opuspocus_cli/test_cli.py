import importlib
import sys

import pytest

from opuspocus_cli import CMD_MODULES, main


@pytest.mark.parametrize("cmd_name", CMD_MODULES.keys())
def test_submodule_has_main(cmd_name):
    """Every subcommand module must contain "main" implementation."""
    module_name = "opuspocus_cli.{}".format(cmd_name)
    importlib.import_module(module_name)
    assert hasattr(sys.modules[module_name], "main")


@pytest.mark.parametrize("cmd_name", CMD_MODULES.keys())
def test_submodule_has_parse_args(cmd_name):
    """Every subcommand module must contain "parse_args" implementation."""
    module_name = "opuspocus_cli.{}".format(cmd_name)
    importlib.import_module(module_name)
    assert hasattr(sys.modules[module_name], "parse_args")


@pytest.mark.parametrize("cmd_name", CMD_MODULES.keys())
def test_subcommand_help(cmd_name, capsys):
    """Show correct usage message when no parameters are provided."""
    with pytest.raises(SystemExit) as e:
        main([cmd_name])
    assert e.type is SystemExit
    assert e.value.code != 0

    output = capsys.readouterr()
    # TODO: more sophisticated output message checking
    assert "usage: " in output.out


def test_unknown_subcommand(capsys):
    """Show usage and exit if provided with unknown subcommand."""
    with pytest.raises(SystemExit) as e:
        main(["foo"])
    assert e.type is SystemExit
    assert e.value.code != 0

    output = capsys.readouterr()
    assert "usage: " in output.out
    assert "{" + ",".join(CMD_MODULES.keys()) + "}" in output.out


def test_subcommand_not_first(capsys):
    """Show usage and exit if the first argument is not a known subcommand."""
    with pytest.raises(SystemExit) as e:
        main(["--pipeline-dir", "stop"])
    assert e.type is SystemExit
    assert e.value.code != 0

    output = capsys.readouterr()
    assert "{" + ",".join(CMD_MODULES.keys()) + "} [options]" in output.out


@pytest.mark.parametrize("cmd_name", ["run", "stop", "status", "traceback"])
def test_required_pipeline_dir_option(cmd_name, capsys):
    """Subcommands, except init, require --pipeline-dir option."""
    with pytest.raises(SystemExit) as e:
        main([cmd_name])
    assert e.type is SystemExit
    assert e.value.code != 0

    output = capsys.readouterr()
    assert " --pipeline-dir PIPELINE_DIR" in output.out


def test_defaults_init(pipeline_preprocess_tiny_config_file):
    """Execute init subcommand with default arguments."""
    rc = main(["init", "--pipeline-config", str(pipeline_preprocess_tiny_config_file)])
    assert rc == 0


@pytest.mark.xfail(reason="Requires a simulation of a submitted/running pipeline")
def test_defaults_stop(pipeline):
    """Executed stop command with default arguments."""
    # TODO(varisd): implemented a running "pipeline" fixture
    rc = main(
        [
            "stop",
            "--pipeline-dir",
            pipeline.pipeline_dir,
            "--runner",
            "bash",
        ]
    )
    assert rc == 0


@pytest.mark.parametrize(
    "cmd_name, non_default_opts",
    [("run", "--runner bash"), ("status", ""), ("traceback", "")],
)
def test_defaults_other(cmd_name, non_default_opts, pipeline_preprocess_tiny_inited):
    """Run the rest of the subcommands with default arguments."""
    argv = [cmd_name, "--pipeline-dir", pipeline_preprocess_tiny_inited.pipeline_dir]
    if non_default_opts:
        argv += non_default_opts.split(" ")
    rc = main(argv)
    assert rc == 0
