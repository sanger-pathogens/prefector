import pytest
from click.testing import CliRunner

from prefector.cli import cli


@pytest.fixture
def runner():
    return CliRunner()


def test_cli_blocks_list_succeeds(runner, monkeypatch, tmp_path):
    result = runner.invoke(cli, ["blocks", "list", "--blocks-dir", str(tmp_path)])
    assert result.exit_code == 0


def test_cli_deployments_list_succeeds(runner, monkeypatch, tmp_path):
    result = runner.invoke(cli, ["deployments", "list", "--deployments-dir", str(tmp_path)])
    assert result.exit_code == 0


def test_cli_help_lists_subcommands(runner):
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "blocks" in result.output
    assert "deployments" in result.output
