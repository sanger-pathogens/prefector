import importlib
import sys
from pathlib import Path
from typing import Any

import pytest
from prefect.filesystems import LocalFileSystem
from prefect.testing.utilities import prefect_test_harness

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture
def deployment_flow_dir(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def deployment_flow(deployment_flow_dir) -> str:
    module_name = "test_deployment_flow"
    function_name = "flow_entrypoint"

    module_path = deployment_flow_dir / f"{module_name}.py"
    module_path.write_text(
        f"from prefect import flow\n@flow\ndef {function_name}(retries: int = 0):\n    return None\n",
        encoding="utf-8",
    )

    sys.path.insert(0, str(deployment_flow_dir))
    importlib.invalidate_caches()
    try:
        yield f"{module_name}:{function_name}"
    finally:
        sys.modules.pop(module_name, None)
        if str(deployment_flow_dir) in sys.path:
            sys.path.remove(str(deployment_flow_dir))
        importlib.invalidate_caches()


@pytest.fixture
def deployment_spec_dict(deployment_flow: str) -> dict[str, Any]:
    return {
        "name": "deploy-a",
        "flow": deployment_flow,
        "image_key": "redcap",
        "tags": ["etl", "nightly"],
        "parameters": {"retries": 2},
    }


@pytest.fixture(scope="module")
def prefect_test_fixture():
    # Everything inside this 'with' block uses the temporary backend
    with prefect_test_harness():
        yield


@pytest.fixture
def local_storage_path(tmp_path: Path) -> Path:
    path = tmp_path / "storage"
    path.mkdir()
    return path


@pytest.fixture
def local_storage(local_storage_path) -> LocalFileSystem:
    return LocalFileSystem(basepath=str(local_storage_path))


@pytest.fixture
def local_storage_block(prefect_test_fixture, local_storage):
    name = "local-storage-block"
    local_storage.save(name, overwrite=True)
    return name


@pytest.fixture
def base_args():
    base_args = {
        "--api-url": "http://localhost:4200/api",
    }
    args = []
    for key in base_args.keys():
        args += [key, base_args[key]]
    return args
