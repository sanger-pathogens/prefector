from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import anyio
import click
import httpx
import pytest
from click.testing import CliRunner
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.client.schemas.objects import Flow as FlowSchema
from prefect.client.schemas.objects import FlowRun, State, StateType
from prefect.client.schemas.responses import DeploymentResponse
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


def _create_work_pool(name: str) -> None:
    with get_client(sync_client=True) as client:
        client.create_work_pool(
            WorkPoolCreate(
                name=name,
                base_job_template={
                    "job_configuration": {"image": "{{ image }}"},
                    "variables": {"type": "object", "properties": {"image": {"title": "Image", "type": "string"}}},
                },
            ),
            overwrite=True,
        )


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
    _create_work_pool(pool_name)

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


def test_deploy_target_applies_concurrency_options(
    monkeypatch, prefect_test_fixture, deployment_spec_dict, deployment_flow_dir
):
    pool_name = "test-pool-concurrency"
    _create_work_pool(pool_name)

    spec = DeploymentSpec(**(deployment_spec_dict | {"concurrency_limit": 1, "collision_strategy": "CANCEL_NEW"}))

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

    created = deployments[0]
    assert created.global_concurrency_limit.limit == 1
    assert created.concurrency_options.collision_strategy == "CANCEL_NEW"


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


def _make_deployment(name: str, flow_id: UUID | None = None) -> DeploymentResponse:
    return DeploymentResponse(name=name, flow_id=flow_id or uuid4())


def test_deployment_label_formats_flow_and_deployment_name():
    d = _make_deployment("my-deployment")
    assert run_cmd._deployment_label("my-flow", d) == "my-flow/my-deployment"


def test_flow_names_by_id_batches_lookup_by_flow_id():
    flow = FlowSchema(name="my-flow")
    d = _make_deployment("my-deployment", flow_id=flow.id)
    client = AsyncMock()
    client.read_flows.return_value = [flow]

    result = anyio.run(run_cmd._flow_names_by_id, client, [d])

    assert result == {flow.id: "my-flow"}
    requested_ids = client.read_flows.call_args.kwargs["flow_filter"].id.any_
    assert requested_ids == [flow.id]


def _object_not_found():
    req = httpx.Request("GET", "http://example.com")
    resp = httpx.Response(404, request=req)
    return ObjectNotFound(httpx.HTTPStatusError("404", request=req, response=resp))


def test_find_deployments_by_slash_name_returns_single():
    mock_deployment = _make_deployment("my-deployment")
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
    d1, d2 = _make_deployment("deploy-a"), _make_deployment("deploy-b")
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
    flow = FlowSchema(name="some-flow")
    available = _make_deployment("other-deployment", flow_id=flow.id)
    client = AsyncMock()
    client.read_deployments.side_effect = [[], [available]]
    client.read_flows.return_value = [flow]

    with pytest.raises(ValueError, match="some-flow/other-deployment"):
        anyio.run(run_cmd._find_deployments, client, "foo")


def test_find_deployments_by_tags_returns_matches():
    d = _make_deployment("my-deployment")
    client = AsyncMock()
    client.read_deployments.return_value = [d]

    result = anyio.run(run_cmd._find_deployments_by_tags, client, ["tag1", "tag2"])

    assert result == [d]


def test_find_deployments_by_tags_raises_when_no_match():
    client = AsyncMock()
    client.read_deployments.return_value = []

    with pytest.raises(ValueError, match="No deployments found with tags: nightly, prod"):
        anyio.run(run_cmd._find_deployments_by_tags, client, ["nightly", "prod"])


def _make_flow_run(
    *, flow_run_id: UUID | None = None, final: bool, completed: bool = True, state_name: str = "Completed"
) -> FlowRun:
    if not final:
        state_type = StateType.RUNNING
    elif completed:
        state_type = StateType.COMPLETED
    else:
        state_type = StateType.FAILED
    return FlowRun(id=flow_run_id or uuid4(), flow_id=uuid4(), state=State(type=state_type, name=state_name))


def test_watch_flow_runs_polls_until_all_final(monkeypatch):
    run_id = uuid4()
    pending = _make_flow_run(flow_run_id=run_id, final=False)
    completed = _make_flow_run(flow_run_id=run_id, final=True, completed=True)
    client = AsyncMock()
    client.read_flow_runs.side_effect = [[pending], [completed]]
    monkeypatch.setattr(run_cmd.anyio, "sleep", AsyncMock())

    anyio.run(run_cmd._watch_flow_runs, client, [("my-flow/my-deployment", pending)])


def test_watch_flow_runs_raises_when_any_failed(monkeypatch):
    ok = _make_flow_run(final=True, completed=True)
    failed = _make_flow_run(final=True, completed=False, state_name="Failed")
    client = AsyncMock()
    client.read_flow_runs.return_value = [ok, failed]
    monkeypatch.setattr(run_cmd.anyio, "sleep", AsyncMock())

    with pytest.raises(click.ClickException, match="orchestrator-b"):
        anyio.run(run_cmd._watch_flow_runs, client, [("orchestrator-a", ok), ("orchestrator-b", failed)])


def test_watch_flow_runs_waits_for_all_before_raising(monkeypatch):
    """All runs are triggered and polled before the error is raised."""
    ok = _make_flow_run(final=True, completed=True)
    failed = _make_flow_run(final=True, completed=False, state_name="Failed")
    client = AsyncMock()
    client.read_flow_runs.return_value = [ok, failed]
    monkeypatch.setattr(run_cmd.anyio, "sleep", AsyncMock())

    with pytest.raises(click.ClickException, match="did not complete"):
        anyio.run(run_cmd._watch_flow_runs, client, [("a", ok), ("b", failed)])


def test_run_flow_requires_name_or_tag(base_args):
    result = CliRunner().invoke(cli, ["deployments", "run"] + base_args)
    assert result.exit_code != 0
    assert "Provide a deployment name or --tag" in result.output


def test_run_flow_rejects_name_and_tag(base_args):
    result = CliRunner().invoke(cli, ["deployments", "run", "my-deployment", "--tag", "nightly"] + base_args)
    assert result.exit_code != 0
    assert "not both" in result.output


def test_run_flow_triggers_deployment_end_to_end(
    monkeypatch, prefect_test_fixture, deployment_spec_dict, deployment_flow_dir, base_args
):
    pool_name = "test-pool-run"
    _create_work_pool(pool_name)

    spec = DeploymentSpec(**deployment_spec_dict)
    monkeypatch.chdir(deployment_flow_dir)
    deploy_cmd.deploy_target(
        spec=spec,
        work_pool_name=pool_name,
        work_queue_name=None,
        image="test-registry/icddrb-redcap:test",
        dry_run=False,
    )

    # The test harness already points at the ephemeral test API; avoid letting the CLI's
    # own --api-url (required, but irrelevant here) override those settings.
    monkeypatch.setattr(run_cmd, "generate_prefect_settings", lambda _connection: {})

    result = CliRunner().invoke(cli, ["deployments", "run", spec.name] + base_args)

    assert result.exit_code == 0, result.output
    assert "Triggering" in result.output
    assert f"/{spec.name}" in result.output
    assert "/runs/flow-run/" in result.output

    with get_client(sync_client=True) as client:
        deployments = client.read_deployments()
        flow_runs = client.read_flow_runs()

    assert len(flow_runs) == 1
    assert flow_runs[0].deployment_id == deployments[0].id
