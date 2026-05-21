import click
import pytest
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate

import prefector.deployments.cli as deploy
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
