import importlib
import sys

import pytest

from opuspocus_cli import CMD_MODULES, main


@pytest.mark.parametrize("cmd_name", CMD_MODULES.keys())
def test_submodule_has_main(cmd_name):
    """Every subcommand module must contain "main" implementation."""
    module_name = f"opuspocus_cli.{cmd_name}"
    importlib.import_module(module_name)
    assert hasattr(sys.modules[module_name], "main")


@pytest.mark.parametrize("cmd_name", CMD_MODULES.keys())
def test_submodule_has_parse_args(cmd_name):
    """Every subcommand module must contain "parse_args" implementation."""
    module_name = f"opuspocus_cli.{cmd_name}"
    importlib.import_module(module_name)
    assert hasattr(sys.modules[module_name], "parse_args")


@pytest.mark.parametrize("cmd_name", CMD_MODULES.keys())
def test_subcommand_help(cmd_name, capsys):
    """Show correct usage message when no parameters are provided."""
    with pytest.raises(SystemExit) as err:
        main([cmd_name])
    assert err.type is SystemExit
    assert err.value.code != 0

    output = capsys.readouterr()
    # TODO: more sophisticated output message checking
    assert "usage: " in output.out


def test_unknown_subcommand(capsys):
    """Show usage and exit if provided with unknown subcommand."""
    with pytest.raises(SystemExit) as err:
        main(["foo"])
    assert err.type is SystemExit
    assert err.value.code != 0

    output = capsys.readouterr()
    assert "usage: " in output.out
    assert "{" + ",".join(CMD_MODULES.keys()) + "}" in output.out


def test_subcommand_not_first(capsys):
    """Show usage and exit if the first argument is not a known subcommand."""
    with pytest.raises(SystemExit) as err:
        main(["--pipeline-dir", "stop"])
    assert err.type is SystemExit
    assert err.value.code != 0

    output = capsys.readouterr()
    assert "{" + ",".join(CMD_MODULES.keys()) + "} [options]" in output.out


@pytest.mark.parametrize("cmd_name", ["stop", "status", "traceback"])
def test_required_pipeline_dir_option(cmd_name, capsys):
    """Subcommands, except init, require --pipeline-dir option."""
    with pytest.raises(SystemExit) as err:
        main([cmd_name])
    assert err.type is SystemExit
    assert err.value.code != 0

    output = capsys.readouterr()
    assert " --pipeline-dir PIPELINE.PIPELINE_DIR" in output.out
