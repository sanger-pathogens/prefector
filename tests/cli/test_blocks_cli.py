import pytest
from click.testing import CliRunner

from prefector.blocks.cli import blocks_command as blocks

BASE_ARGS = ["--api-url", "http://test/api"]


@pytest.fixture
def runner():
    return CliRunner()


def test_blocks_list_without_api_url_exits_nonzero(runner, monkeypatch):
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    result = runner.invoke(blocks, ["list"])
    assert result.exit_code != 0
    assert "Missing option '--api-url'" in result.output


def test_blocks_list_with_api_url_succeeds(runner, monkeypatch, tmp_path):
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    result = runner.invoke(blocks, ["list"] + BASE_ARGS + ["--blocks-dir", str(tmp_path)])
    assert result.exit_code == 0


def test_blocks_deploy_with_api_url_succeeds(runner, monkeypatch, tmp_path):
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    result = runner.invoke(blocks, ["deploy"] + BASE_ARGS + ["--blocks-dir", str(tmp_path)])
    assert result.exit_code == 0


def test_blocks_list_accepts_custom_blocks_dir(runner, monkeypatch, tmp_path):
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    result = runner.invoke(blocks, ["list"] + BASE_ARGS + ["--blocks-dir", str(tmp_path)])
    assert result.exit_code == 0
