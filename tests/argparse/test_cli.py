import pytest

from prefector.argparse import cli


def test_list_blocks_delegates_to_blocks_cli(monkeypatch):
    calls = []

    monkeypatch.setattr(cli, "blocks_main", lambda args: calls.append(args) or 0)
    monkeypatch.setattr(cli, "deployments_main", lambda _args: pytest.fail("unexpected"))

    result = cli.main(["blocks", "list"])

    assert result == 0
    assert calls == [["list"]]


def test_list_deployments_delegates_to_deployments_cli(monkeypatch):
    calls = []

    monkeypatch.setattr(cli, "deployments_main", lambda args: calls.append(args) or 0)
    monkeypatch.setattr(cli, "blocks_main", lambda _args: pytest.fail("unexpected"))

    result = cli.main(["deployments", "list"])

    assert result == 0
    assert calls == [["list"]]


def test_deploy_blocks_passes_through_extra_args(monkeypatch):
    calls = []

    monkeypatch.setattr(cli, "blocks_main", lambda args: calls.append(args) or 0)
    monkeypatch.setattr(cli, "deployments_main", lambda _args: pytest.fail("unexpected"))

    result = cli.main(["blocks", "deploy", "--target", "bronze", "--dry-run"])

    assert result == 0
    assert calls == [["deploy", "--target", "bronze", "--dry-run"]]


def test_deploy_deployments_passes_through_extra_args(monkeypatch):
    calls = []

    monkeypatch.setattr(cli, "deployments_main", lambda args: calls.append(args) or 0)
    monkeypatch.setattr(cli, "blocks_main", lambda _args: pytest.fail("unexpected"))

    result = cli.main(["deployments", "deploy", "--target", "redcap-bronze"])

    assert result == 0
    assert calls == [["deploy", "--target", "redcap-bronze"]]


def test_blocks_help_is_passthrough(monkeypatch):
    calls = []

    monkeypatch.setattr(cli, "blocks_main", lambda args: calls.append(args) or 0)
    monkeypatch.setattr(cli, "deployments_main", lambda _args: pytest.fail("unexpected"))

    result = cli.main(["blocks", "--help"])

    assert result == 0
    assert calls == [["--help"]]
