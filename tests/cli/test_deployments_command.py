from prefector.deployments.cli import deployments_command as deployments

BASE_ARGS = ["--api-url", "http://test/api"]


def test_deployments_deploy_with_no_deployments_succeeds(runner, monkeypatch, tmp_path):
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    deployments_dir = tmp_path / "deployments"
    deployments_dir.mkdir()
    images_manifest = tmp_path / "images.yaml"
    images_manifest.write_text("[]\n", encoding="utf-8")

    result = runner.invoke(
        deployments,
        ["deploy"]
        + BASE_ARGS
        + [
            "--deployments-dir",
            str(deployments_dir),
            "--images-manifest",
            str(images_manifest),
            "--image-prefix",
            "ghcr.io/example",
        ],
    )
    assert result.exit_code == 0, result.output


def test_deployments_list_accepts_custom_deployments_dir(runner, monkeypatch, tmp_path):
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    result = runner.invoke(deployments, ["list", "--deployments-dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
