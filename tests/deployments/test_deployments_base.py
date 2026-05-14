import yaml
import pytest
from pathlib import Path

from prefector.deployments.base import ImageManifest, DeploymentSpec, load_deployments, load_image_manifest


def test_deployment_spec_flow_helpers(deployment_spec_dict):
    spec = DeploymentSpec(**deployment_spec_dict)
    expected_module, expected_function = deployment_spec_dict["flow"].split(":", maxsplit=1)

    assert spec.module == expected_module
    assert spec.function == expected_function


def test_deployment_spec_str(deployment_spec_dict):
    spec = DeploymentSpec(**deployment_spec_dict)

    assert str(spec) == (
        f"{deployment_spec_dict['name']}\n"
        f"  flow: {deployment_spec_dict['flow']}\n"
        f"  image_key: {deployment_spec_dict['image_key']}"
    )


def test_deployment_spec_requires_flow_separator(deployment_spec_dict):
    deployment_spec_dict["flow"] = "pkg.module.run"

    with pytest.raises(ValueError, match="flow must use '<module>:<function>' format"):
        DeploymentSpec(**deployment_spec_dict)


def test_deployment_spec_requires_existing_flow_module(deployment_spec_dict):
    deployment_spec_dict["flow"] = "not.a.real.module:run"

    with pytest.raises(ValueError, match="flow module file does not exist"):
        DeploymentSpec(**deployment_spec_dict)


def test_deployment_spec_requires_existing_flow_function(deployment_spec_dict):
    module, _ = deployment_spec_dict["flow"].split(":", maxsplit=1)
    deployment_spec_dict["flow"] = f"{module}:not_present"

    with pytest.raises(ValueError, match="flow function does not exist"):
        DeploymentSpec(**deployment_spec_dict)


def test_from_yaml_rejects_empty_file(tmp_path: Path):
    path = tmp_path / "empty.yaml"
    path.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="Empty YAML file"):
        DeploymentSpec.from_yaml(path)


def test_from_yaml_rejects_non_mapping_payload(tmp_path: Path):
    path = tmp_path / "invalid.yaml"
    path.write_text("- a\n- b\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Expected mapping"):
        DeploymentSpec.from_yaml(path)


def test_load_deployments_loads_yaml_files(tmp_path: Path, deployment_spec_dict):
    deployment_spec_dict["cron"] = "0 0 * * *"
    file1 = tmp_path / "a.yaml"
    file1.write_text(
        yaml.dump(deployment_spec_dict),
        encoding="utf-8",
    )

    file2 = tmp_path / "b.yml"
    file2.write_text(
        "\n".join(
                [
                    "name: deploy-b",
                    f"flow: {deployment_spec_dict['flow']}",
                    f"image_key: {deployment_spec_dict['image_key']}",
                ],
            ),
        encoding="utf-8",
    )

    specs = load_deployments(tmp_path)

    assert len(specs) == 2
    assert [spec.name for spec in specs] == ["deploy-a", "deploy-b"]
    assert specs[0].cron == "0 0 * * *"
    assert specs[1].cron is None
    assert specs[0].tags == ["etl", "nightly"]
    assert specs[0].parameters == {"retries": 2}


def test_load_deployments_wraps_validation_error(tmp_path: Path, deployment_spec_dict) -> None:
    deployment_spec_dict["flow"] = "missing_colon"

    file = tmp_path / "bad.yaml"
    file.write_text(
        yaml.dump(deployment_spec_dict),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid deployment config"):
        load_deployments(tmp_path)


def test_load_deployments_rejects_duplicate_names(tmp_path: Path, deployment_spec_dict) -> None:
    for name in ("first.yaml", "second.yaml"):
        file = tmp_path / name
        file.write_text(
            yaml.dump(deployment_spec_dict),
            encoding="utf-8",
        )

    with pytest.raises(ValueError, match="Duplicate deployment name\\(s\\): deploy-a"):
        load_deployments(tmp_path)


def test_image_manifest_get_returns_entry():
    manifest = ImageManifest.model_validate([
        {"key": "dbt", "name": "icddrb-dbt"},
    ])

    assert manifest.get("dbt").name == "icddrb-dbt"


def test_image_manifest_get_raises_for_unknown_key():
    manifest = ImageManifest.model_validate([
        {"key": "dbt", "name": "icddrb-dbt"},
    ])

    with pytest.raises(ValueError, match="Unknown image key 'missing'"):
        manifest.get("missing")


def test_load_image_manifest(tmp_path):
    path = tmp_path / "images.yaml"
    path.write_text(
        "- key: redcap\n"
        "  name: icddrb-redcap\n"
        "- key: dbt\n"
        "  name: icddrb-dbt\n",
        encoding="utf-8",
    )

    manifest = load_image_manifest(path)

    assert isinstance(manifest, ImageManifest)
    assert manifest.get("redcap").name == "icddrb-redcap"
    assert manifest.get("dbt").name == "icddrb-dbt"


def test_load_image_manifest_raises_for_invalid_shape(tmp_path):
    path = tmp_path / "images.yaml"
    path.write_text("key: dbt\nname: icddrb-dbt\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid image manifest in"):
        load_image_manifest(path)


def test_load_image_manifest_raises_for_duplicate_keys(tmp_path):
    path = tmp_path / "images.yaml"
    path.write_text(
        "- key: dbt\n"
        "  name: icddrb-dbt\n"
        "- key: dbt\n"
        "  name: icddrb-dbt-alt\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate image key\\(s\\): dbt"):
        load_image_manifest(path)
