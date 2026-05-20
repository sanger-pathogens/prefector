import pytest
from click.testing import CliRunner

from prefector.click.cli import cli


@pytest.fixture
def runner():
    return CliRunner()


def test_cli_blocks_list_succeeds(runner, monkeypatch):
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    result = runner.invoke(cli, ["blocks", "list", "--api-url", "http://test/api"])
    assert result.exit_code == 0
    assert "Listing blocks" in result.output


def test_cli_deployments_list_succeeds(base_args, runner):
    result = runner.invoke(cli, ["deployments", "list"] + base_args)
    assert result.exit_code == 0
    assert "Listing deployments" in result.output


def test_cli_help_lists_subcommands(runner):
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "blocks" in result.output
    assert "deployments" in result.output
