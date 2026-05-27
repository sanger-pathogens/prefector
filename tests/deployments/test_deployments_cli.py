from unittest.mock import AsyncMock, MagicMock

import anyio
import click
import httpx
import pytest
from click.testing import CliRunner
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.exceptions import ObjectNotFound

import prefector.deployments.deploy as deploy_cmd
import prefector.deployments.run as run_cmd
from prefector.cli import cli
from prefector.deployments.base import DeploymentSpec


@pytest.fixture
def build_spec(deployment_spec_dict):
    def _build_spec(**overrides) -> DeploymentSpec:
        return DeploymentSpec(**(deployment_spec_dict | overrides))

    return _build_spec


@pytest.fixture
def deployments(build_spec) -> list[DeploymentSpec]:
    return [
        build_spec(name="deploy-a"),
        build_spec(name="deploy-b"),
    ]


def test_select_targets_returns_all_when_selected_empty(deployments):
    assert deploy_cmd._select_targets(set(), deployments) == deployments


def test_select_targets_returns_only_selected(deployments):
    selected = deploy_cmd._select_targets(["deploy-b"], deployments)
    assert [item.name for item in selected] == ["deploy-b"]


def test_select_targets_deduplicates_input(deployments):
    selected = deploy_cmd._select_targets(["deploy-b", "deploy-b"], deployments)
    assert [item.name for item in selected] == ["deploy-b"]


def test_select_targets_raises_for_unknown_target(deployments):
    with pytest.raises(click.UsageError, match="Unknown deployment name\\(s\\): missing"):
        deploy_cmd._select_targets({"missing"}, deployments)


def test_build_image_name_builds_from_prefix_and_manifest_name():
    result = deploy_cmd._build_image_name(
        image_prefix="ghcr.io/acme/",
        image_name="icddrb-dbt",
        image_tag="2026.03.26",
    )

    assert result == "ghcr.io/acme/icddrb-dbt:2026.03.26"


def test_build_image_name_supports_empty_prefix():
    result = deploy_cmd._build_image_name(image_prefix="", image_name="icddrb-dbt", image_tag="latest")

    assert result == "icddrb-dbt:latest"


def test_deploy_target(monkeypatch, prefect_test_fixture, deployment_spec_dict, deployment_flow_dir):
    pool_name = "test-pool"

    with get_client(sync_client=True) as client:
        client.create_work_pool(
            WorkPoolCreate(
                name=pool_name,
                base_job_template={
                    "job_configuration": {"image": "{{ image }}"},
                    "variables": {"type": "object", "properties": {"image": {"title": "Image", "type": "string"}}},
                },
            ),
            overwrite=True,
        )

    deployment_spec_dict["cron"] = "0 0 * * *"
    spec = DeploymentSpec(**deployment_spec_dict)

    monkeypatch.chdir(deployment_flow_dir)

    deploy_cmd.deploy_target(
        spec=spec,
        work_pool_name=pool_name,
        work_queue_name=None,
        image="test-registry/icddrb-redcap:test",
        dry_run=False,
    )

    with get_client(sync_client=True) as client:
        deployments = client.read_deployments()

    assert len(deployments) == 1

    created = deployments[0]
    assert created.tags == spec.tags
    assert created.parameters == spec.parameters
    assert len(created.schedules) == 1
    assert created.schedules[0].schedule.cron == spec.cron
    assert created.job_variables["image"] == "test-registry/icddrb-redcap:test"
    assert created.entrypoint == spec.flow.replace(":", ".")


def test_deploy_target_raises_for_invalid_parameters(deployment_spec_dict):
    spec = DeploymentSpec(**(deployment_spec_dict | {"parameters": {"retries": "three"}}))

    with pytest.raises(ValueError, match="Invalid parameters for deployment 'deploy-a'"):
        deploy_cmd.deploy_target(
            spec=spec,
            work_pool_name="test-pool",
            work_queue_name=None,
            image="test-registry/icddrb-redcap:test",
            dry_run=False,
        )


def test_deployment_label_with_flow_name():
    d = MagicMock()
    d.flow_name = "my-flow"
    d.name = "my-deployment"
    assert run_cmd._deployment_label(d) == "my-flow/my-deployment"


def test_deployment_label_falls_back_to_name():
    d = MagicMock(spec=["name"])
    d.name = "my-deployment"
    assert run_cmd._deployment_label(d) == "my-deployment"


def _object_not_found():
    req = httpx.Request("GET", "http://example.com")
    resp = httpx.Response(404, request=req)
    return ObjectNotFound(httpx.HTTPStatusError("404", request=req, response=resp))


def test_find_deployments_by_slash_name_returns_single():
    mock_deployment = MagicMock()
    client = AsyncMock()
    client.read_deployment_by_name.return_value = mock_deployment

    result = anyio.run(run_cmd._find_deployments, client, "my-flow/my-deployment")

    assert result == [mock_deployment]
    client.read_deployment_by_name.assert_called_once_with("my-flow/my-deployment")


def test_find_deployments_raises_for_not_found_slash_name():
    client = AsyncMock()
    client.read_deployment_by_name.side_effect = _object_not_found()

    with pytest.raises(ValueError, match="Deployment 'my-flow/foo' not found"):
        anyio.run(run_cmd._find_deployments, client, "my-flow/foo")


def test_find_deployments_by_bare_name_returns_all_matches():
    d1, d2 = MagicMock(), MagicMock()
    client = AsyncMock()
    client.read_deployments.return_value = [d1, d2]

    result = anyio.run(run_cmd._find_deployments, client, "my-deployment")

    assert result == [d1, d2]


def test_find_deployments_raises_when_no_match():
    client = AsyncMock()
    client.read_deployments.side_effect = [[], []]

    with pytest.raises(ValueError, match="No deployment named 'foo'"):
        anyio.run(run_cmd._find_deployments, client, "foo")


def test_find_deployments_lists_available_in_error():
    available = MagicMock()
    available.flow_name = "some-flow"
    available.name = "other-deployment"
    client = AsyncMock()
    client.read_deployments.side_effect = [[], [available]]

    with pytest.raises(ValueError, match="some-flow/other-deployment"):
        anyio.run(run_cmd._find_deployments, client, "foo")


def test_find_deployments_by_tags_returns_matches():
    d = MagicMock()
    client = AsyncMock()
    client.read_deployments.return_value = [d]

    result = anyio.run(run_cmd._find_deployments_by_tags, client, ["tag1", "tag2"])

    assert result == [d]


def test_find_deployments_by_tags_raises_when_no_match():
    client = AsyncMock()
    client.read_deployments.return_value = []

    with pytest.raises(ValueError, match="No deployments found with tags: nightly, prod"):
        anyio.run(run_cmd._find_deployments_by_tags, client, ["nightly", "prod"])


def _make_flow_run(run_id: str, final: bool, completed: bool = True, state_name: str = "Completed"):
    flow_run = MagicMock()
    flow_run.id = run_id
    flow_run.state = MagicMock()
    flow_run.state.is_final.return_value = final
    flow_run.state.is_completed.return_value = completed
    flow_run.state.name = state_name
    return flow_run


def test_watch_flow_runs_polls_until_all_final(monkeypatch):
    pending = _make_flow_run("run-1", final=False)
    completed = _make_flow_run("run-1", final=True, completed=True)
    calls = iter([pending, completed])
    monkeypatch.setattr(run_cmd, "_prefect", lambda fn: next(calls))
    monkeypatch.setattr(run_cmd.time, "sleep", lambda _: None)

    run_cmd._watch_flow_runs([("my-flow/my-deployment", pending)])


def test_watch_flow_runs_raises_when_any_failed(monkeypatch):
    ok = _make_flow_run("run-1", final=True, completed=True)
    failed = _make_flow_run("run-2", final=True, completed=False, state_name="Failed")
    calls = iter([ok, failed])
    monkeypatch.setattr(run_cmd, "_prefect", lambda fn: next(calls))
    monkeypatch.setattr(run_cmd.time, "sleep", lambda _: None)

    with pytest.raises(click.ClickException, match="orchestrator-b"):
        run_cmd._watch_flow_runs([("orchestrator-a", ok), ("orchestrator-b", failed)])


def test_watch_flow_runs_waits_for_all_before_raising(monkeypatch):
    """All runs are triggered and polled before the error is raised."""
    ok = _make_flow_run("run-1", final=True, completed=True)
    failed = _make_flow_run("run-2", final=True, completed=False, state_name="Failed")
    calls = iter([ok, failed])
    monkeypatch.setattr(run_cmd, "_prefect", lambda fn: next(calls))
    monkeypatch.setattr(run_cmd.time, "sleep", lambda _: None)

    with pytest.raises(click.ClickException, match="did not complete"):
        run_cmd._watch_flow_runs([("a", ok), ("b", failed)])


def test_run_flow_requires_name_or_tag(base_args):
    result = CliRunner().invoke(cli, ["deployments", "run"] + base_args)
    assert result.exit_code != 0
    assert "Provide a deployment name or --tag" in result.output


def test_run_flow_rejects_name_and_tag(base_args):
    result = CliRunner().invoke(cli, ["deployments", "run", "my-deployment", "--tag", "nightly"] + base_args)
    assert result.exit_code != 0
    assert "not both" in result.output
