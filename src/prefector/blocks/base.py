import hashlib
import importlib.util
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Annotated, Any, Iterable

from prefect.blocks.core import Block
from prefect.exceptions import PrefectException
from pydantic import StringConstraints, ValidationError
from pydantic_settings import BaseSettings

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class BlockBuildError(ValueError):
    """Raised when block settings validation fails during block construction."""

    def __init__(self, name: str, settings_cls: type[BaseSettings], exception: ValidationError):
        details = []
        for error in exception.errors():
            loc = error.get("loc", ())
            field = ".".join(str(part) for part in loc) if loc else "unknown"
            message = error.get("msg", "Invalid value")
            env_var = _loc_to_env_var(settings_cls, loc)
            if env_var:
                details.append(f"{field}: {message}. Set {env_var}")
            else:
                details.append(f"{field}: {message}")

        details_text = "\n".join(details) if details else str(exception)
        super().__init__(f"Failed to build block '{name}':\n{details_text}")


def _loc_to_env_var(settings_cls: type[BaseSettings], loc: tuple[Any, ...]) -> str | None:
    """Map a Pydantic error location tuple to the corresponding environment variable name.

    Only handles top-level fields: nested fields have no single conventional env var name
    to suggest, since they're resolved via an explicit `nested_fields` mapping rather than
    a fixed naming convention.
    """
    if len(loc) != 1 or not isinstance(loc[0], str):
        return None

    env_prefix = settings_cls.model_config.get("env_prefix")
    if env_prefix is None:
        return None

    head = loc[0]
    field = settings_cls.model_fields.get(head)
    alias = field.validation_alias if field is not None and isinstance(field.validation_alias, str) else head
    return f"{env_prefix}{alias.upper()}"


@dataclass(frozen=True)
class BlockSpec:
    name: str
    settings_cls: type[BaseSettings]
    block_cls: type[Block]

    def __post_init__(self) -> None:
        if not issubclass(self.settings_cls, BaseSettings):
            raise ValueError("settings_cls must inherit from BaseSettings")
        if not issubclass(self.block_cls, Block):
            raise ValueError("block_cls must inherit from Block")

    def build(self) -> Block:
        try:
            settings = self.settings_cls()
        except ValidationError as exc:
            raise BlockBuildError(self.name, self.settings_cls, exc) from exc

        payload = self._resolve_settings(settings)
        return self.block_cls(**payload)

    def _resolve_settings(self, settings: BaseSettings) -> dict[str, Any]:
        payload = settings.model_dump()

        for field_name in settings.model_fields.keys():
            value = getattr(settings, field_name)
            if isinstance(value, BlockSpec):
                try:
                    payload[field_name] = value.block_cls.load(value.name)
                except PrefectException as exc:
                    raise ValueError(
                        f"Failed to load dependency block '{value.name}' "
                        f"for '{self.name}' ({value.block_cls.__name__})."
                    ) from exc

        return payload


def _already_loaded_module(module_name: str, module_path: Path):
    module = sys.modules.get(module_name)
    module_file = getattr(module, "__file__", None) if module is not None else None
    if not module_file:
        return None
    return module if Path(module_file).resolve() == module_path.resolve() else None


def _load_specs_module(module_path: Path):
    resolved_path = module_path.resolve()
    namespace = hashlib.sha256(str(resolved_path.parent).encode("utf-8")).hexdigest()[:12]
    module_name = f"prefector_external_blocks.{namespace}.{module_path.stem}"
    existing = _already_loaded_module(module_name, module_path)
    if existing is not None:
        return existing

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Unable to load module from: {module_path}")
    _ensure_synthetic_package("prefector_external_blocks")
    _ensure_synthetic_package(f"prefector_external_blocks.{namespace}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except (ModuleNotFoundError, ImportError) as exc:
        sys.modules.pop(module_name, None)
        raise ValueError(_missing_dependency_message(module_path, exc)) from exc
    return module


def _ensure_synthetic_package(module_name: str) -> None:
    if module_name in sys.modules:
        return

    module = ModuleType(module_name)
    module.__path__ = []
    sys.modules[module_name] = module


def _missing_dependency_message(module_path: Path, exc: ModuleNotFoundError | ImportError) -> str:
    missing_name = getattr(exc, "name", None)
    if not missing_name:
        return f"Block spec '{module_path}' could not be imported: {exc}"

    if missing_name.startswith("prefect_"):
        package_name = missing_name.replace("_", "-")
        return (
            f"Block spec '{module_path}' requires missing Python module '{missing_name}'. "
            f"Install the Prefect collection package: {package_name}"
        )

    return (
        f"Block spec '{module_path}' requires missing Python module '{missing_name}'. "
        "Install it in the current environment and try again."
    )


def _validate_specs(specs: list[BlockSpec]) -> None:
    name_counts = Counter(spec.name for spec in specs)
    duplicates = sorted(name for name, count in name_counts.items() if count > 1)
    if duplicates:
        raise ValueError(f"Duplicate block name(s): {', '.join(duplicates)}")


def load_blocks(spec_dir: Path) -> list[BlockSpec]:
    specs: list[BlockSpec] = []

    for path in sorted(spec_dir.glob("*.py")):
        if path.name == "__init__.py" or path.name.startswith("_"):
            continue

        module = _load_specs_module(path)
        module_specs = getattr(module, "BLOCKS", None)
        if not isinstance(module_specs, list):
            raise ValueError(f"Spec module must expose BLOCKS as list: {path}")
        if not all(isinstance(spec, BlockSpec) for spec in module_specs):
            raise ValueError(f"Spec module has non-BlockSpec entries in BLOCKS: {path}")

        specs.extend(module_specs)

    _validate_specs(specs)
    return specs


def select_targets(selected: Iterable[str], specs: list[BlockSpec]) -> list[BlockSpec]:
    if not selected:
        return specs

    selected = set(selected)
    index = {spec.name: spec for spec in specs if spec.name in selected}
    missing = sorted(selected - set(index))
    if missing:
        available = ", ".join(sorted(spec.name for spec in specs))
        raise ValueError(f"Unknown block name(s): {', '.join(missing)}. Available: {available}")
    return list(index.values())
