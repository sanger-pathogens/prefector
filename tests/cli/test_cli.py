import click
import pytest
from click.testing import CliRunner

from prefector.cli import cli
from prefector.errors import handle_errors


@click.command()
@click.pass_context
def _raises_value_error(ctx):
    with handle_errors():
        raise ValueError("something went wrong")


@click.command()
@click.pass_context
def _raises_click_exception(ctx):
    with handle_errors():
        raise click.ClickException("already a click error")


@click.group()
@click.option("--debug/--no-debug", default=False)
@click.pass_context
def _debug_cli(ctx, debug):
    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug


_debug_cli.add_command(_raises_value_error, name="cmd")


def test_handle_errors_wraps_value_error():
    result = CliRunner().invoke(_raises_value_error, [], obj={})
    assert result.exit_code == 1
    assert "something went wrong" in result.output
    assert "Traceback" not in result.output


def test_handle_errors_passes_through_click_exception():
    result = CliRunner().invoke(_raises_click_exception, [], obj={})
    assert result.exit_code == 1
    assert "already a click error" in result.output


def test_handle_errors_reraises_in_debug_mode():
    with pytest.raises(ValueError, match="something went wrong"):
        CliRunner().invoke(_debug_cli, ["--debug", "cmd"], catch_exceptions=False)


def test_handle_errors_no_context_defaults_to_non_debug():
    # handle_errors must not crash when there is no Click context at all
    result = CliRunner().invoke(_raises_value_error, [])
    assert result.exit_code == 1
    assert "something went wrong" in result.output


def test_cli_debug_is_flag(runner):
    result = runner.invoke(cli, ["--debug", "--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.output


def test_cli_help_lists_subcommands(runner):
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "blocks" in result.output
    assert "deployments" in result.output


def test_cli_blocks_subcommand_is_wired(runner):
    result = runner.invoke(cli, ["blocks", "--help"])
    assert result.exit_code == 0
    assert "list" in result.output
    assert "deploy" in result.output


def test_cli_deployments_subcommand_is_wired(runner):
    result = runner.invoke(cli, ["deployments", "--help"])
    assert result.exit_code == 0
    assert "list" in result.output
    assert "deploy" in result.output
    assert "run" in result.output
