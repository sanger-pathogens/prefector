import importlib
import itertools
from collections import Counter
from pathlib import Path
from typing import Annotated, Any

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class ImageEntry(BaseModel):
    key: NonEmptyStr
    name: NonEmptyStr


class ImageManifest(RootModel[list[ImageEntry]]):
    @model_validator(mode="after")
    def _ensure_unique_keys(self) -> "ImageManifest":
        key_counts = Counter(entry.key for entry in self.root)
        duplicates = sorted(key for key, count in key_counts.items() if count > 1)
        if duplicates:
            raise ValueError(f"Duplicate image key(s): {', '.join(duplicates)}")
        return self

    def get(self, key: str) -> ImageEntry:
        for entry in self.root:
            if entry.key == key:
                return entry

        available = ", ".join(sorted(item.key for item in self.root))
        raise ValueError(f"Unknown image key '{key}'. Available: {available}")


class DeploymentSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: NonEmptyStr
    flow: str
    image_key: NonEmptyStr
    cron: NonEmptyStr | None = None
    tags: list[str] = Field(default_factory=list)
    parameters: dict[str, Any] = Field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.name}\n  flow: {self.flow}\n  image_key: {self.image_key}"

    @field_validator("flow")
    @classmethod
    def _validate_flow(cls, value: str) -> str:
        module, separator, function = value.partition(":")
        if separator != ":" or not module or not function:
            raise ValueError("flow must use '<module>:<function>' format")
        try:
            spec = importlib.util.find_spec(module)
        except ModuleNotFoundError:
            spec = None
        if spec is None or spec.origin is None or not Path(spec.origin).is_file():
            raise ValueError(f"flow module file does not exist: {module}")
        try:
            module_obj = importlib.import_module(module)
        except Exception as exc:
            raise ValueError(f"flow module could not be imported: {module}") from exc
        if not hasattr(module_obj, function):
            raise ValueError(f"flow function does not exist: {module}:{function}")
        return value

    @property
    def module(self) -> str:
        return self.flow.split(":", maxsplit=1)[0]

    @property
    def function(self) -> str:
        return self.flow.split(":", maxsplit=1)[1]

    @classmethod
    def from_yaml(cls, path: Path) -> "DeploymentSpec":
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        if payload is None:
            raise ValueError(f"Empty YAML file: {path}")
        if not isinstance(payload, dict):
            raise ValueError(f"Expected mapping in {path}, got {type(payload).__name__}")
        try:
            return cls.model_validate(payload)
        except ValidationError as exc:
            raise ValueError(f"Invalid deployment config in {path}:\n{exc}") from exc


def load_deployments(config_dir: Path) -> list[DeploymentSpec]:
    specs = []

    config_paths = itertools.chain(config_dir.glob("*.yaml"), config_dir.glob("*.yml"))
    for path in config_paths:
        spec = DeploymentSpec.from_yaml(path)
        specs.append(spec)

    name_counts = Counter(spec.name for spec in specs)
    duplicates = sorted(name for name, count in name_counts.items() if count > 1)
    if duplicates:
        raise ValueError(f"Duplicate deployment name(s): {', '.join(duplicates)}")

    return specs


def load_image_manifest(path: Path) -> ImageManifest:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"Unable to read image manifest: {path}") from exc

    try:
        return ImageManifest.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(f"Invalid image manifest in {path}:\n{exc}") from exc
