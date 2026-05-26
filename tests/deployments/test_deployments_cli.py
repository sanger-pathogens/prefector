from unittest.mock import AsyncMock, MagicMock

import anyio
import click
import httpx
import pytest
from click.testing import CliRunner
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.exceptions import ObjectNotFound

import prefector.deployments.cli as deploy
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
    assert deploy._select_targets(set(), deployments) == deployments


def test_select_targets_returns_only_selected(deployments):
    selected = deploy._select_targets(["deploy-b"], deployments)
    assert [item.name for item in selected] == ["deploy-b"]


def test_select_targets_deduplicates_input(deployments):
    selected = deploy._select_targets(["deploy-b", "deploy-b"], deployments)
    assert [item.name for item in selected] == ["deploy-b"]


def test_select_targets_raises_for_unknown_target(deployments):
    with pytest.raises(click.UsageError, match="Unknown deployment name\\(s\\): missing"):
        deploy._select_targets({"missing"}, deployments)


def test_build_image_name_builds_from_prefix_and_manifest_name():
    result = deploy._build_image_name(
        image_prefix="ghcr.io/acme/",
        image_name="icddrb-dbt",
        image_tag="2026.03.26",
    )

    assert result == "ghcr.io/acme/icddrb-dbt:2026.03.26"


def test_build_image_name_supports_empty_prefix():
    result = deploy._build_image_name(image_prefix="", image_name="icddrb-dbt", image_tag="latest")

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

    deploy.deploy_target(
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
        deploy.deploy_target(
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
    assert deploy._deployment_label(d) == "my-flow/my-deployment"


def test_deployment_label_falls_back_to_name():
    d = MagicMock(spec=["name"])
    d.name = "my-deployment"
    assert deploy._deployment_label(d) == "my-deployment"


def _object_not_found():
    req = httpx.Request("GET", "http://example.com")
    resp = httpx.Response(404, request=req)
    return ObjectNotFound(httpx.HTTPStatusError("404", request=req, response=resp))


def test_find_deployment_by_slash_name():
    mock_deployment = MagicMock()
    client = AsyncMock()
    client.read_deployment_by_name.return_value = mock_deployment

    result = anyio.run(deploy._find_deployment, client, "my-flow/my-deployment")

    assert result is mock_deployment
    client.read_deployment_by_name.assert_called_once_with("my-flow/my-deployment")


def test_find_deployment_raises_for_not_found_slash_name():
    client = AsyncMock()
    client.read_deployment_by_name.side_effect = _object_not_found()

    with pytest.raises(ValueError, match="Deployment 'my-flow/foo' not found"):
        anyio.run(deploy._find_deployment, client, "my-flow/foo")


def test_find_deployment_by_bare_name():
    mock_deployment = MagicMock()
    client = AsyncMock()
    client.read_deployments.return_value = [mock_deployment]

    result = anyio.run(deploy._find_deployment, client, "my-deployment")

    assert result is mock_deployment


def test_find_deployment_raises_when_no_match():
    client = AsyncMock()
    client.read_deployments.side_effect = [[], []]

    with pytest.raises(ValueError, match="No deployment named 'foo'"):
        anyio.run(deploy._find_deployment, client, "foo")


def test_find_deployment_lists_available_in_error():
    available = MagicMock()
    available.flow_name = "some-flow"
    available.name = "other-deployment"
    client = AsyncMock()
    client.read_deployments.side_effect = [[], [available]]

    with pytest.raises(ValueError, match="some-flow/other-deployment"):
        anyio.run(deploy._find_deployment, client, "foo")


def test_find_deployment_raises_when_multiple_matches():
    d1, d2 = MagicMock(), MagicMock()
    d1.flow_name, d1.name = "flow-a", "my-deployment"
    d2.flow_name, d2.name = "flow-b", "my-deployment"
    client = AsyncMock()
    client.read_deployments.return_value = [d1, d2]

    with pytest.raises(ValueError, match="Multiple deployments named 'my-deployment'"):
        anyio.run(deploy._find_deployment, client, "my-deployment")


def test_find_deployments_by_tags_returns_matches():
    d = MagicMock()
    client = AsyncMock()
    client.read_deployments.return_value = [d]

    result = anyio.run(deploy._find_deployments_by_tags, client, ["tag1", "tag2"])

    assert result == [d]


def test_find_deployments_by_tags_raises_when_no_match():
    client = AsyncMock()
    client.read_deployments.return_value = []

    with pytest.raises(ValueError, match="No deployments found with tags: nightly, prod"):
        anyio.run(deploy._find_deployments_by_tags, client, ["nightly", "prod"])


def _make_flow_run(final: bool, completed: bool = True, state_name: str = "Completed"):
    flow_run = MagicMock()
    flow_run.id = "run-123"
    flow_run.state = MagicMock()
    flow_run.state.is_final.return_value = final
    flow_run.state.is_completed.return_value = completed
    flow_run.state.name = state_name
    return flow_run


def test_run_deployment_no_watch(monkeypatch):
    flow_run = _make_flow_run(final=False)
    monkeypatch.setattr(deploy, "_prefect", lambda fn: flow_run)

    deploy._run_deployment(MagicMock(), watch=False)


def test_run_deployment_watch_polls_until_final(monkeypatch):
    pending = _make_flow_run(final=False)
    pending.state = None
    completed = _make_flow_run(final=True, completed=True)
    calls = iter([pending, completed])
    monkeypatch.setattr(deploy, "_prefect", lambda fn: next(calls))
    monkeypatch.setattr(deploy.time, "sleep", lambda _: None)

    deploy._run_deployment(MagicMock(), watch=True)


def test_run_deployment_watch_raises_on_failed_state(monkeypatch):
    failed = _make_flow_run(final=True, completed=False, state_name="Failed")
    monkeypatch.setattr(deploy, "_prefect", lambda fn: failed)

    with pytest.raises(click.ClickException, match="Flow run ended in state: Failed"):
        deploy._run_deployment(MagicMock(), watch=True)


def test_run_flow_requires_name_or_tag(base_args):
    result = CliRunner().invoke(cli, ["deployments", "run"] + base_args)
    assert result.exit_code != 0
    assert "Provide a deployment name or --tag" in result.output


def test_run_flow_rejects_name_and_tag(base_args):
    result = CliRunner().invoke(cli, ["deployments", "run", "my-deployment", "--tag", "nightly"] + base_args)
    assert result.exit_code != 0
    assert "not both" in result.output
